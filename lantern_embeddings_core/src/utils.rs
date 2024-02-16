use anyhow::anyhow;
use isahc::config::RedirectPolicy;
use isahc::{prelude::*, HttpClient};
use nvml_wrapper::Nvml;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs::create_dir_all, time::Duration};
use sysinfo::{System, SystemExt};

use crate::runtime::EmbeddingResult;

type GetResponseFn = Box<dyn Fn(Vec<u8>) -> Result<EmbeddingResult, anyhow::Error> + Send + Sync>;

pub fn download_file(url: &str, path: &PathBuf) -> Result<(), anyhow::Error> {
    let client = HttpClient::builder()
        .timeout(Duration::from_secs(600))
        .redirect_policy(RedirectPolicy::Limit(2))
        .build()?;

    let mut response = client.get(url)?;
    // Copy the response body to the local file
    create_dir_all(path.parent().unwrap())?;
    let mut file = std::fs::File::create(path)?;
    std::io::copy(response.body_mut(), &mut file).expect("Failed writing response to file");
    Ok(())
}

pub fn get_available_memory() -> Result<u64, anyhow::Error> {
    let mut _nvml_instance = None;
    let mut gpu_device = None;

    if let Ok(nvml) = Nvml::init() {
        _nvml_instance = Some(nvml);
        let _nvml_insteance = _nvml_instance.as_mut().unwrap();
        let nvml_device = _nvml_insteance.device_by_index(0);
        if let Ok(device) = nvml_device {
            gpu_device = Some(device);
        }
    }

    if gpu_device.is_none() {
        let mut sys = System::new_all();
        sys.refresh_all();
        return Ok(sys.total_memory() - sys.used_memory());
    }

    let gpu_device = gpu_device.as_ref().unwrap();

    let mem_info = gpu_device.memory_info()?;
    Ok(mem_info.free)
}

pub fn percent_gpu_memory_used() -> Result<f64, anyhow::Error> {
    let mut _nvml_instance = None;
    let mut gpu_device = None;

    if let Ok(nvml) = Nvml::init() {
        _nvml_instance = Some(nvml);
        let _nvml_insteance = _nvml_instance.as_mut().unwrap();
        let nvml_device = _nvml_insteance.device_by_index(0);
        if let Ok(device) = nvml_device {
            gpu_device = Some(device);
        }
    }

    if gpu_device.is_none() {
        return Ok(0.0);
    }

    let gpu_device = gpu_device.as_ref().unwrap();

    let mem_info = gpu_device.memory_info()?;
    // Sometimes models consume much more memory based on input size.
    // In my experiments the 200mb model was consuming 3GB memory.
    // So comparing model size with free GPU memory will not be correct here.
    // Instead we will check if the used memory in GPU is more than the threshold
    // For GPU we will just clear the cache but not throw an error
    // As the error from ORT is handled and will not cause an OOM like RAM
    Ok((mem_info.used as f64 / mem_info.total as f64) * 100.0)
}

pub async fn post_with_retries(
    client: Arc<HttpClient>,
    url: String,
    body: String,
    get_response_fn: GetResponseFn,
    max_retries: usize,
) -> Result<EmbeddingResult, anyhow::Error> {
    let starting_interval = 4000; // ms
    let mut last_error = "".to_string();

    for i in 0..max_retries {
        match client.post_async(&url, body.deref()).await {
            Err(e) => {
                // TODO:: use logger
                eprintln!("Request error: url: {url}, error: {e}, retry: {i}");
                // Wait for the next backoff interval before retrying
                last_error = e.to_string();
                tokio::time::sleep(Duration::from_millis((starting_interval * (i + 1)) as u64))
                    .await;
            }
            Ok(mut response) => {
                let mut body: Vec<u8> = Vec::with_capacity(body.capacity());
                response.copy_to(&mut body).await?;
                let embedding_response = get_response_fn(body);

                match embedding_response {
                    Err(e) => {
                        eprintln!("Error parsing request body: url: {url}, error: {e}, retry: {i}");
                        // Wait for the next backoff interval before retrying
                        last_error = e.to_string();
                        tokio::time::sleep(Duration::from_millis(
                            (starting_interval * (i + 1)) as u64,
                        ))
                        .await;
                    }
                    Ok(result) => {
                        return Ok(result);
                    }
                }
            }
        }
    }

    Err(anyhow!(
        "All {max_retries} requests failed. Last error was: {last_error}"
    ))
}

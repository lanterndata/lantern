use isahc::HttpClient;

pub trait IHTTPRuntime {
    fn get_client(&self) -> Result<HttpClient, anyhow::Error>;
    fn post_request(
        &self,
        endpoint: &str,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<Vec<Vec<f32>>, anyhow::Error>;
}

#[macro_export]
macro_rules! HTTPRuntime {
    ($a:ident) => {
        use crate::http_runtime::IHTTPRuntime;
        use core::time::Duration;
        use isahc::{config::RedirectPolicy, prelude::*, HttpClient};
        use std::sync::Arc;
        use tokio::runtime::Runtime;
        use url::Url;
        use crate::utils::post_with_retries;

        impl<'a> IHTTPRuntime for $a<'a> {
            fn get_client(&self) -> Result<HttpClient, anyhow::Error> {
                let mut client = HttpClient::builder()
                    .timeout(Duration::from_secs(self.request_timeout))
                    .redirect_policy(RedirectPolicy::Limit(2));

                for header in &self.headers {
                    client = client.default_header(header.0.clone(), header.1.clone());
                }

                Ok(client.build()?)
            }

            fn post_request(
                &self,
                endpoint: &str,
                model_name: &str,
                inputs: &Vec<&str>,
            ) -> Result<Vec<Vec<f32>>, anyhow::Error> {
                let tokio_runtime = Runtime::new()?;
                let client = Arc::new(self.get_client()?);
                let mut tasks = Vec::new();
                let url = Url::parse(&self.base_url)?.join(endpoint)?.to_string();

                for request_body in self.chunk_inputs(model_name, inputs)? {
                    let client = client.clone();
                    let url = url.clone();
                    let task = tokio_runtime
                        .spawn(async move { post_with_retries(client, url, request_body, 3).await });
                    tasks.push(task);
                }

                let responses = tokio_runtime.block_on(async move {
                    let mut responses = Vec::with_capacity(inputs.len());
                    for task in tasks {
                        let mut response = task.await??;
                        let mut body: Vec<u8> = vec![];
                        response.copy_to(&mut body).await?;
                        responses.extend(self.get_response(body)?);
                    }
                    Ok::<Vec<Vec<f32>>, anyhow::Error>(responses)
                })?;

                Ok(responses)
            }
        }
    };
}

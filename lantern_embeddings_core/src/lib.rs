use futures::stream::StreamExt;
use image::imageops::FilterType;
use image::io::Reader as ImageReader;
use image::GenericImageView;
use itertools::Itertools;
use ndarray::{s, Array2, Array4, CowArray, Dim};
use ort::session::Session;
use ort::{Environment, ExecutionProvider, GraphOptimizationLevel, SessionBuilder, Value};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokenizers::{PaddingParams, Tokenizer, TruncationParams};
use tokio::{fs, runtime};

#[macro_use]
extern crate lazy_static;

pub struct EncoderService {
    name: String,
    tokenizer: Option<Tokenizer>,
    vision_size: Option<usize>,
    encoder: Session,
}

pub struct EncoderOptions {
    pub visual: bool,
    pub use_tokenizer: bool,
    padding_params: Option<PaddingParams>,
    truncation_params: Option<TruncationParams>,
    pub input_image_size: Option<usize>,
}

const DATA_PATH: &'static str = ".ldb_extras_data/";

struct ModelInfo {
    url: String,
    tokenizer_url: Option<String>,
    encoder_args: EncoderOptions,
    encoder: Option<EncoderService>,
}

struct ModelInfoBuilder {
    base_url: &'static str,
    use_tokenizer: Option<bool>,
    visual: Option<bool>,
    input_image_size: Option<usize>,
    padding_params: Option<PaddingParams>,
    truncation_params: Option<TruncationParams>,
}

impl ModelInfoBuilder {
    fn new(base_url: &'static str) -> Self {
        ModelInfoBuilder {
            base_url,
            use_tokenizer: None,
            visual: None,
            input_image_size: None,
            padding_params: None,
            truncation_params: None,
        }
    }

    fn with_tokenizer(&mut self, status: bool) -> &mut Self {
        self.use_tokenizer = if status { Some(true) } else { None };
        self
    }

    fn with_visual(&mut self, status: bool) -> &mut Self {
        self.visual = if status { Some(true) } else { None };
        self
    }

    fn with_input_image_size(&mut self, len: usize) -> &mut Self {
        self.input_image_size = Some(len);
        self
    }

    fn build(&self) -> ModelInfo {
        let model_url = format!("{}/model.onnx", self.base_url);
        let mut tokenizer_url = None;

        if self.use_tokenizer.is_some() {
            tokenizer_url = Some(format!("{}/tokenizer.json", self.base_url));
        }

        let encoder_args = EncoderOptions {
            visual: self.visual.is_some(),
            use_tokenizer: self.use_tokenizer.is_some(),
            input_image_size: self.input_image_size.clone(),
            padding_params: self.padding_params.clone(),
            truncation_params: self.truncation_params.clone(),
        };

        ModelInfo {
            url: model_url,
            tokenizer_url,
            encoder: None,
            encoder_args,
        }
    }
}

lazy_static! {
    static ref MODEL_INFO_MAP: RwLock<HashMap<&'static str, ModelInfo>> = RwLock::new(HashMap::from([
        ("clip/ViT-B-32-textual", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/textual").with_tokenizer(true).build()),
        ("clip/ViT-B-32-visual", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/visual").with_input_image_size(224).with_visual(true).build()),
        ("BAAI/bge-small-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-small-en-v1.5").with_tokenizer(true).build()),
        ("BAAI/bge-base-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-base-en-v1.5").with_tokenizer(true).build()),
        ("BAAI/bge-large-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-large-en-v1.5").with_tokenizer(true).build()),
        ("intfloat/e5-base-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/intfloat/e5-base-v2").with_tokenizer(true).build()),
        ("intfloat/e5-large-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/intfloat/e5-large-v2").with_tokenizer(true).build()),
        ("llmrails/ember-v1", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/llmrails/ember-v1").with_tokenizer(true).build()),
        ("thenlper/gte-base", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/thenlper/gte-base").with_tokenizer(true).build()),
        ("thenlper/gte-large", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/thenlper/gte-large").with_tokenizer(true).build()),
        ("microsoft/all-MiniLM-L12-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/microsoft/all-MiniLM-L12-v2").with_tokenizer(true).build()),
        ("microsoft/all-mpnet-base-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/microsoft/all-mpnet-base-v2").with_tokenizer(true).build()),
        ("transformers/multi-qa-mpnet-base-dot-v1", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/transformers/multi-qa-mpnet-base-dot-v1").with_tokenizer(true).build())
    ]));
}

lazy_static! {
    static ref ONNX_ENV: Arc<Environment> = Environment::builder()
        .with_name("ldb_extras")
        .with_execution_providers([
            ExecutionProvider::CUDA(Default::default()),
            ExecutionProvider::OpenVINO(Default::default()),
            ExecutionProvider::CPU(Default::default()),
        ])
        .build()
        .unwrap()
        .into_arc();
}

fn default_logger(text: &str) {
    println!("{}", text);
}

pub type LoggerFn = fn(&str);

impl EncoderService {
    pub fn new(
        environment: &Arc<Environment>,
        model_name: &str,
        model_folder: &PathBuf,
        args: &EncoderOptions,
    ) -> Result<EncoderService, Box<dyn std::error::Error + Send + Sync>> {
        let mut tokenizer = None;

        if args.use_tokenizer {
            let mut tokenizer_instance =
                Tokenizer::from_file(Path::join(model_folder, "tokenizer.json")).unwrap();

            // In case tokenizer will not contain padding and truncation params
            // We will specify them manually
            if args.padding_params.is_some() {
                tokenizer_instance.with_padding(args.padding_params.clone());
            }

            if args.truncation_params.is_some() {
                tokenizer_instance.with_truncation(args.truncation_params.clone())?;
            }

            tokenizer = Some(tokenizer_instance);
        }

        let num_cpus = num_cpus::get();

        let encoder = SessionBuilder::new(environment)?
            .with_parallel_execution(true)?
            .with_intra_threads(num_cpus as i16)?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_model_from_file(Path::join(model_folder, "model.onnx"))?;

        Ok(EncoderService {
            name: model_name.to_string(),
            tokenizer,
            encoder,
            vision_size: args.input_image_size,
        })
    }

    fn process_text_bert(
        &self,
        text: &Vec<&str>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let text_len = text.len();
        let preprocessed = self
            .tokenizer
            .as_ref()
            .unwrap()
            .encode_batch(text.clone(), true)?;

        let mut vecs = Vec::with_capacity(session.inputs.len());

        for input in &session.inputs {
            let mut val = None;
            match input.name.as_str() {
                "input_ids" => {
                    let v: Vec<i64> = preprocessed
                        .iter()
                        .map(|i| i.get_ids().iter().map(|b| *b as i64).collect())
                        .concat();
                    val = Some(v);
                }
                "attention_mask" => {
                    let v: Vec<i64> = preprocessed
                        .iter()
                        .map(|i| i.get_attention_mask().iter().map(|b| *b as i64).collect())
                        .concat();
                    val = Some(v);
                }
                "token_type_ids" => {
                    let v: Vec<i64> = preprocessed
                        .iter()
                        .map(|i| i.get_type_ids().iter().map(|b| *b as i64).collect())
                        .concat();
                    val = Some(v);
                }
                _ => {}
            }
            if let Some(v) = val {
                vecs.push(
                    CowArray::from(Array2::from_shape_vec((text_len, v.len() / text_len), v)?)
                        .into_dyn(),
                );
            }
        }

        let inputs = vecs
            .iter()
            .map(|v| Value::from_array(session.allocator(), &v).unwrap())
            .collect();

        let outputs = session.run(inputs)?;

        let binding = outputs[0].try_extract()?;
        let embeddings = binding.view();
        let embeddings = embeddings.slice(s![.., 0, ..]);
        let embeddings: Vec<f32> = embeddings.iter().map(|s| *s).collect();
        let output_dims = session.outputs[0].dimensions.last().unwrap().unwrap() as usize;

        Ok(embeddings
            .iter()
            .map(|s| *s)
            .chunks(output_dims)
            .into_iter()
            .map(|b| b.collect())
            .collect())
    }

    fn process_text_clip(
        &self,
        text: &Vec<&str>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let preprocessed = self
            .tokenizer
            .as_ref()
            .unwrap()
            .encode_batch(text.clone(), true)?;

        let v1: Vec<i32> = preprocessed
            .iter()
            .map(|i| i.get_ids().iter().map(|b| *b as i32).collect())
            .concat();

        let v2: Vec<i32> = preprocessed
            .iter()
            .map(|i| i.get_attention_mask().iter().map(|b| *b as i32).collect())
            .concat();

        let ids = CowArray::from(Array2::from_shape_vec(
            (text.len(), v1.len() / text.len()),
            v1,
        )?)
        .into_dyn();

        let mask = CowArray::from(Array2::from_shape_vec(
            (text.len(), v2.len() / text.len()),
            v2,
        )?)
        .into_dyn();

        let outputs = session.run(vec![
            Value::from_array(session.allocator(), &ids)?,
            Value::from_array(session.allocator(), &mask)?,
        ])?;

        let binding = outputs[0].try_extract()?;
        let embeddings = binding.view();

        let seq_len = embeddings.shape().get(1).ok_or("not")?;

        Ok(embeddings
            .iter()
            .map(|s| *s)
            .chunks(*seq_len)
            .into_iter()
            .map(|b| b.collect())
            .collect())
    }

    fn process_text(
        &self,
        text: &Vec<&str>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        match self.name.as_str() {
            "clip/ViT-B-32-textual" => self.process_text_clip(text),
            _ => self.process_text_bert(text),
        }
    }

    pub fn process_image_clip(
        &self,
        images_bytes: &Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let mean = vec![0.48145466, 0.4578275, 0.40821073]; // CLIP Dataset
        let std = vec![0.26862954, 0.26130258, 0.27577711];

        let vision_size = self.vision_size.unwrap_or(224);

        let mut pixels = CowArray::from(Array4::<f32>::zeros(Dim([
            images_bytes.len(),
            3,
            vision_size,
            vision_size,
        ])));
        for (index, image_bytes) in images_bytes.iter().enumerate() {
            let image = ImageReader::new(Cursor::new(image_bytes))
                .with_guessed_format()?
                .decode()?;
            let image = image.resize_exact(
                vision_size as u32,
                vision_size as u32,
                FilterType::CatmullRom,
            );
            for (x, y, pixel) in image.pixels() {
                pixels[[index, 0, x as usize, y as usize]] =
                    (pixel.0[0] as f32 / 255.0 - mean[0]) / std[0];
                pixels[[index, 1, x as usize, y as usize]] =
                    (pixel.0[1] as f32 / 255.0 - mean[1]) / std[1];
                pixels[[index, 2, x as usize, y as usize]] =
                    (pixel.0[2] as f32 / 255.0 - mean[2]) / std[2];
            }
        }

        let outputs = session.run(vec![Value::from_array(
            session.allocator(),
            &pixels.into_dyn(),
        )?])?;
        let binding = outputs[0].try_extract()?;
        let embeddings = binding.view();

        let seq_len = embeddings.shape().get(1).unwrap();

        Ok(embeddings
            .iter()
            .map(|s| *s)
            .chunks(*seq_len)
            .into_iter()
            .map(|b| b.collect())
            .collect())
    }

    fn process_image(
        &self,
        images_bytes: &Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        match self.name.as_str() {
            "clip/ViT-B-32-visual" => self.process_image_clip(images_bytes),
            _ => self.process_image_clip(images_bytes),
        }
    }
}

pub mod clip {

    use std::{fs::create_dir_all, sync::Mutex, time::Duration};
    use url::Url;

    fn download_file(url: &str, path: &PathBuf) -> Result<(), anyhow::Error> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(600))
            .build()?;

        let response = client.get(url).send()?;
        let mut content = Cursor::new(response.bytes()?);
        create_dir_all(path.parent().unwrap())?;
        let mut file = std::fs::File::create(path)?;
        std::io::copy(&mut content, &mut file).expect("Failed writing response to file");
        Ok(())
    }

    fn check_and_download_files(
        model_name: &str,
        logger: &LoggerFn,
        data_path: &str,
    ) -> Result<(), anyhow::Error> {
        {
            let map = MODEL_INFO_MAP.read().unwrap();
            let model_info = map.get(model_name);

            if model_info.is_none() {
                anyhow::bail!("Model \"{}\" not found", model_name)
            }

            let model_info = model_info.unwrap();

            if model_info.encoder.is_some() {
                // if encoder exists return
                return Ok(());
            }
        }

        let mut map_write = MODEL_INFO_MAP.write().unwrap();
        let model_info = map_write.get_mut(model_name).unwrap();

        let model_folder = Path::join(&Path::new(data_path), model_name);
        let model_path = Path::join(&model_folder, "model.onnx");
        let tokenizer_path = Path::join(&model_folder, "tokenizer.json");

        // TODO parallel download with tokio
        if !model_path.exists() {
            // model is not downloaded, we should download it
            logger("Downloading model [this is one time operation]");
            download_file(&model_info.url, &model_path)?;
        }

        if !tokenizer_path.exists() && model_info.tokenizer_url.is_some() {
            logger("Downloading tokenizer [this is one time operation]");
            // tokenizer is not downloaded, we should download it
            download_file(&model_info.tokenizer_url.as_ref().unwrap(), &tokenizer_path)?;
        }

        let encoder = EncoderService::new(
            &ONNX_ENV,
            model_name,
            &model_folder,
            &model_info.encoder_args,
        );

        match encoder {
            Ok(enc) => model_info.encoder = Some(enc),
            Err(err) => {
                drop(map_write);
                anyhow::bail!(err)
            }
        }

        Ok(())
    }

    use super::*;

    async fn get_image_buffer(path_or_url: &str) -> Result<Vec<u8>, anyhow::Error> {
        if let Ok(url) = Url::parse(path_or_url) {
            let response = reqwest::get(url).await;

            if let Err(e) = response {
                anyhow::bail!(
                    "[X] Error while downloading image \"{}\" - {}",
                    path_or_url,
                    e
                );
            }
            return Ok(response.unwrap().bytes().await?.to_vec());
        } else {
            let response = fs::read(path_or_url).await;
            if let Err(e) = response {
                anyhow::bail!("[X] Error while reading file \"{}\" - {}", path_or_url, e);
            }
            return Ok(response.unwrap());
        }
    }

    fn get_images_parallel(
        paths_or_urls: &Vec<&str>,
        logger: &LoggerFn,
    ) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        let buffers: Arc<Mutex<Vec<Vec<u8>>>> =
            Arc::new(Mutex::new(Vec::with_capacity(paths_or_urls.len())));
        let threaded_rt = runtime::Runtime::new()?;
        let tasks: Vec<_> = paths_or_urls
            .iter()
            .map(|&path_or_url| get_image_buffer(path_or_url))
            .collect();
        let chunk_size = std::thread::available_parallelism().unwrap().into();

        logger(&format!(
            "[*] Trying to read/download {} images",
            paths_or_urls.len()
        ));

        let runtime_result = threaded_rt.block_on(async {
            let mut tasks = futures::stream::iter(tasks).buffered(chunk_size);
            while let Some(result) = tasks.next().await {
                let mut buffers = buffers.lock().unwrap();
                if let Err(e) = result {
                    anyhow::bail!("{}", e);
                }
                buffers.push(result.unwrap());
            }
            Ok::<(), anyhow::Error>(())
        });

        if let Err(e) = runtime_result {
            anyhow::bail!("{}", e);
        }

        logger("[*] All images read into buffer");
        let buffers = buffers.lock().unwrap();
        Ok(buffers.clone())
    }

    pub fn process(
        model_name: &str,
        input: &Vec<&str>,
        logger: Option<&LoggerFn>,
        data_path: Option<&str>,
        cache: bool,
    ) -> Result<Vec<Vec<f32>>, anyhow::Error> {
        let logger = logger.unwrap_or(&(default_logger as LoggerFn));

        let download_result =
            check_and_download_files(model_name, logger, data_path.unwrap_or(DATA_PATH));

        if let Err(err) = download_result {
            anyhow::bail!("Error happened while downloading model files: {:?}", err);
        }

        let map = MODEL_INFO_MAP.read().unwrap();
        let model_info = map.get(model_name).unwrap();

        let result;
        if model_info.encoder_args.visual {
            let buffers = get_images_parallel(input, logger)?;
            result = model_info.encoder.as_ref().unwrap().process_image(&buffers);
        } else {
            result = model_info.encoder.as_ref().unwrap().process_text(input);
        }

        drop(map);

        if !cache {
            let mut map = MODEL_INFO_MAP.write().unwrap();
            let model_info = map.get_mut(model_name).unwrap();
            model_info.encoder = None;
        }

        match result {
            Ok(res) => Ok(res),
            Err(err) => {
                anyhow::bail!("Error happened while generating embeddings {:?}", err);
            }
        }
    }

    pub fn get_available_models(data_path: Option<&str>) -> (String, Vec<(String, bool)>) {
        let map = MODEL_INFO_MAP.read().unwrap();
        let mut res = String::new();
        let data_path = data_path.unwrap_or(DATA_PATH);
        let mut models = Vec::with_capacity(map.len());
        for (key, value) in &*map {
            let model_exists =
                if Path::join(&Path::new(data_path), format!("{}/model.onnx", key)).exists() {
                    "true"
                } else {
                    "false"
                };
            let model_type = if !value.encoder_args.visual {
                "textual"
            } else {
                "visual"
            };

            res.push_str(&format!(
                "{} - type: {}, downloaded: {}\n",
                key, model_type, model_exists
            ));
            models.push((key.to_string(), value.encoder_args.visual));
        }

        return (res, models);
    }
}

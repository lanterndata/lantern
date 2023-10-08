use image::imageops::FilterType;
use image::io::Reader as ImageReader;
use image::GenericImageView;
use itertools::Itertools;
use ndarray::{Array2, Array4, CowArray, Dim};
use ort::session::Session;
use ort::{Environment, ExecutionProvider, GraphOptimizationLevel, SessionBuilder, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokenizers::{
    PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer, TruncationDirection,
    TruncationParams, TruncationStrategy,
};

pub struct EncoderService {
    name: String,
    tokenizer: Option<Tokenizer>,
    vision_size: Option<usize>,
    encoder: Session,
}

pub struct EncoderOptions {
    pub visual: bool,
    pub use_tokenizer: bool,
    pub pad_token_sequence: usize,
    pub input_image_size: Option<usize>,
}

const DATA_PATH: &'static str = ".ldb_extras_data/";

struct ModelInfo {
    url: &'static str,
    tokenizer_url: Option<&'static str>,
    encoder_args: EncoderOptions,
    encoder: Option<EncoderService>,
}

lazy_static! {
    static ref MODEL_INFO_MAP: RwLock<HashMap<&'static str, ModelInfo>> = RwLock::new(HashMap::from([
        ("clip/ViT-B-32-textual", ModelInfo{encoder: None, url: "https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/textual/model.onnx", tokenizer_url: Some("https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/textual/tokenizer.json"), encoder_args: EncoderOptions{visual:false, pad_token_sequence: 77, use_tokenizer: true, input_image_size: None}}),
        ("clip/ViT-B-32-visual", ModelInfo{encoder: None, url: "https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/visual/model.onnx", tokenizer_url: None, encoder_args: EncoderOptions{visual:true, input_image_size: Some(224), use_tokenizer: false, pad_token_sequence: 0} }),
        ("BAAI/bge-base-en", ModelInfo{encoder: None, url: "https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-base-en/model.onnx", tokenizer_url: Some("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-base-en/tokenizer.json"), encoder_args: EncoderOptions{visual:false, pad_token_sequence: 512, use_tokenizer: true, input_image_size: None}}),
        ("BAAI/bge-large-en", ModelInfo{encoder: None, url: "https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-large-en/model.onnx", tokenizer_url: Some("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-large-en/tokenizer.json"), encoder_args: EncoderOptions{visual:false, pad_token_sequence: 512, use_tokenizer: true, input_image_size: None}}),
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

            tokenizer_instance.with_padding(Some(PaddingParams {
                strategy: if args.pad_token_sequence > 0 {
                    PaddingStrategy::Fixed(args.pad_token_sequence)
                } else {
                    PaddingStrategy::BatchLongest
                },
                direction: PaddingDirection::Right,
                pad_to_multiple_of: None,
                pad_id: 0,
                pad_type_id: 0,
                pad_token: "[PAD]".to_string(),
            }));

            if args.pad_token_sequence > 0 {
                tokenizer_instance.with_truncation(Some(TruncationParams {
                    direction: TruncationDirection::Right,
                    max_length: args.pad_token_sequence,
                    strategy: TruncationStrategy::LongestFirst,
                    stride: 0,
                }))?;
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

    fn process_text_bge(
        &self,
        text: &Vec<&str>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let preprocessed = self
            .tokenizer
            .as_ref()
            .unwrap()
            .encode_batch(text.clone(), true)?;

        let v1: Vec<i64> = preprocessed
            .iter()
            .map(|i| i.get_ids().iter().map(|b| *b as i64).collect())
            .concat();

        let v2: Vec<i64> = preprocessed
            .iter()
            .map(|i| i.get_attention_mask().iter().map(|b| *b as i64).collect())
            .concat();

        let v3: Vec<i64> = preprocessed
            .iter()
            .map(|i| i.get_type_ids().iter().map(|b| *b as i64).collect())
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

        let type_ids = CowArray::from(Array2::from_shape_vec(
            (text.len(), v3.len() / text.len()),
            v3,
        )?)
        .into_dyn();

        let outputs = session.run(vec![
            Value::from_array(session.allocator(), &ids)?,
            Value::from_array(session.allocator(), &mask)?,
            Value::from_array(session.allocator(), &type_ids)?,
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
            "BAAI/bge-base-en" => self.process_text_bge(text),
            "BAAI/bge-large-en" => self.process_text_bge(text),
            _ => self.process_text_clip(text),
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

    use crate::{error, notice};
    use std::{fs::create_dir_all, time::Duration};
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

    fn check_and_download_files(model_name: &str) -> Result<(), anyhow::Error> {
        {
            let map = MODEL_INFO_MAP.read().unwrap();
            let model_info = map.get(model_name);

            if model_info.is_none() {
                anyhow::bail!("Model {} not found. Use 'SELECT get_available_models()' to view the list of avaialble models", model_name)
            }

            let model_info = model_info.unwrap();

            if model_info.encoder.is_some() {
                // if encoder exists return
                return Ok(());
            }
        }
        
        let mut map_write = MODEL_INFO_MAP.write().unwrap();
        let model_info = map_write.get_mut(model_name).unwrap();

        let model_folder = Path::join(&Path::new(DATA_PATH), model_name);
        let model_path = Path::join(&model_folder, "model.onnx");
        let tokenizer_path = Path::join(&model_folder, "tokenizer.json");

        // TODO parallel download with tokio
        if !model_path.exists() {
            // model is not downloaded, we should download it
            notice!(
                "Downloading model {} [this is one time operation]",
                model_name
            );
            download_file(model_info.url, &model_path)?;
        }

        if !tokenizer_path.exists() && model_info.tokenizer_url.is_some() {
            notice!("Downloading tokenizer [this is one time operation]");
            // tokenizer is not downloaded, we should download it
            download_file(model_info.tokenizer_url.unwrap(), &tokenizer_path)?;
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
    pub fn process_text(model_name: &str, text: String) -> Vec<f32> {
        let download_result = check_and_download_files(model_name);

        if let Err(err) = download_result {
            error!("Error happened while downloading model files {:?}", err);
        }

        let map = MODEL_INFO_MAP.read().unwrap();
        let model_info = map.get(model_name).unwrap();

        let result = model_info
            .encoder
            .as_ref()
            .unwrap()
            .process_text(&vec![&text]);

        match result {
            Ok(res) => res[0].clone(),
            Err(err) => {
                // remove lock
                drop(map);
                error!("Error happened while generating text embedding {:?}", err);
            }
        }
    }

    pub fn process_image(model_name: &str, path_or_url: String) -> Vec<f32> {
        let download_result = check_and_download_files(model_name);

        if let Err(err) = download_result {
            error!("Error happened while downloading model files {:?}", err);
        }

        let mut buffer = Vec::new();
        if let Ok(url) = Url::parse(&path_or_url) {
            notice!("Downloading image");
            let response = reqwest::blocking::get(url).expect("Failed to download image");
            buffer = response
                .bytes()
                .expect("Failed to read response body")
                .to_vec();
        } else {
            let mut f = File::open(Path::new(&path_or_url)).unwrap();
            f.read_to_end(&mut buffer).unwrap();
        }

        let map = MODEL_INFO_MAP.read().unwrap();
        let model_info = map.get(model_name).unwrap();

        let result = model_info
            .encoder
            .as_ref()
            .unwrap()
            .process_image(&vec![buffer]);

        match result {
            Ok(res) => res[0].clone(),
            Err(err) => {
                // remove lock
                drop(map);
                error!("Error happened while generating text embedding {:?}", err);
            }
        }
    }

    pub fn get_available_models() -> String {
        let map = MODEL_INFO_MAP.read().unwrap();
        let mut res = String::new();
        for (key, value) in &*map {
            let model_exists =
                if Path::join(&Path::new(DATA_PATH), format!("{}/model.onnx", key)).exists() {
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
        }

        return res;
    }
}

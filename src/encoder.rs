use image::imageops::FilterType;
use image::io::Reader as ImageReader;
use image::GenericImageView;
use itertools::Itertools;
use ndarray::{Array2, Array4, CowArray, Dim};
use ort::session::Session;
use ort::{Environment, ExecutionProvider, GraphOptimizationLevel, SessionBuilder, Value};
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;
use tokenizers::{PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer};

pub struct EncoderService {
    tokenizer: Tokenizer,
    encoder: Session,
    vision_size: usize,
}

pub struct EncoderOptions {
    pub visual: bool,
    pub pad_token_sequence: bool,
    pub input_image_size: usize,
}

lazy_static! {
    static ref ONNX_ENV: Arc<Environment> = Environment::builder()
        .with_name("clip")
        .with_execution_providers([
            ExecutionProvider::CUDA(Default::default()),
            ExecutionProvider::OpenVINO(Default::default()),
            ExecutionProvider::CPU(Default::default()),
        ])
        .build()
        .unwrap()
        .into_arc();
}

lazy_static! {
    static ref TEXT_PROCESSOR: EncoderService = {
        let args = EncoderOptions {
            input_image_size: 224,
            pad_token_sequence: true,
            visual: false,
        };

        EncoderService::new(&ONNX_ENV, args).expect("Failed building textual model processor")
    };
}

lazy_static! {
    static ref IMAGE_PROCESSOR: EncoderService = {
        let args = EncoderOptions {
            input_image_size: 224,
            pad_token_sequence: true,
            visual: true,
        };
        EncoderService::new(&ONNX_ENV, args).expect("Failed building visual  model processor")
    };
}

impl EncoderService {
    pub fn new(
        environment: &Arc<Environment>,
        args: EncoderOptions,
    ) -> Result<EncoderService, Box<dyn std::error::Error + Send + Sync>> {
        let text_model_path = "ldb_extras_models/textual.onnx";
        let image_model_path = "ldb_extras_models/visual.onnx";
        let tokenizer_path = "ldb_extras_models/tokenizer.json";

        let mut tokenizer = Tokenizer::from_file(tokenizer_path)?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: if args.pad_token_sequence {
                PaddingStrategy::Fixed(77)
            } else {
                PaddingStrategy::BatchLongest
            },
            direction: PaddingDirection::Right,
            pad_to_multiple_of: None,
            pad_id: 0,
            pad_type_id: 0,
            pad_token: "[PAD]".to_string(),
        }));

        let num_cpus = num_cpus::get();

        let model_path = if args.visual {
            image_model_path
        } else {
            text_model_path
        };

        let encoder = SessionBuilder::new(environment)?
            .with_parallel_execution(true)?
            .with_intra_threads(num_cpus as i16)?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_model_from_file(model_path)?;

        Ok(EncoderService {
            tokenizer,
            encoder,
            vision_size: args.input_image_size,
        })
    }

    pub fn process_text(
        &self,
        text: &Vec<String>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let preprocessed = self.tokenizer.encode_batch(text.clone(), true)?;
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

    pub fn process_image(
        &self,
        images_bytes: &Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let mean = vec![0.48145466, 0.4578275, 0.40821073]; // CLIP Dataset
        let std = vec![0.26862954, 0.26130258, 0.27577711];

        let mut pixels = CowArray::from(Array4::<f32>::zeros(Dim([
            images_bytes.len(),
            3,
            self.vision_size,
            self.vision_size,
        ])));
        for (index, image_bytes) in images_bytes.iter().enumerate() {
            let image = ImageReader::new(Cursor::new(image_bytes))
                .with_guessed_format()?
                .decode()?;
            let image = image.resize_exact(
                self.vision_size as u32,
                self.vision_size as u32,
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
}

// TODO::
// Declare image_processor and text_processor in lazy_static [done]
// bring is_url regex check and check if image path is url download it with reqwest
// in process_text check if textual model does not exist download it
// in process_image check if visual model does not exist download it
// in both functions check if tokenizer does not exist download it
// change CI/CD build to download onnxruntime and add necessary LD flags

pub mod clip {
    use super::*;
    pub fn process_text(text: String) -> Vec<f32> {
        let res = TEXT_PROCESSOR
            .process_text(&vec![text])
            .expect("Text prcoessing failed");
        return res[0].clone();
    }

    pub fn process_image(path_or_url: String) -> Vec<f32> {
        let mut buffer = Vec::new();
        let mut f = File::open(Path::new(&path_or_url)).unwrap();
        f.read_to_end(&mut buffer).unwrap();
        let res = IMAGE_PROCESSOR
            .process_image(&vec![buffer])
            .expect("Image processing failed");

        return res[0].clone();
    }
}

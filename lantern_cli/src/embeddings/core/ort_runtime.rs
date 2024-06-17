use futures::StreamExt;
use image::{imageops::FilterType, io::Reader as ImageReader, GenericImageView};
use isahc::{config::RedirectPolicy, prelude::*, HttpClient};
use itertools::Itertools;
use ndarray::{s, Array2, Array4, ArrayBase, Axis, CowArray, CowRepr, Dim, IxDynImpl};
use ort::session::Session;
use ort::tensor::ort_owned_tensor::ViewHolder;
use ort::{Environment, ExecutionProvider, GraphOptimizationLevel, SessionBuilder, Value};
use serde::Deserialize;
use std::{
    cmp,
    collections::HashMap,
    io::Cursor,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use sysinfo::{System, SystemExt};
use tokenizers::{PaddingParams, Tokenizer, TruncationParams};
use tokio::{fs, runtime};
use url::Url;

use super::runtime::{EmbeddingResult, EmbeddingRuntime};
use super::utils::{download_file, get_available_memory, percent_gpu_memory_used};
use super::LoggerFn;

type SessionInput<'a> = ArrayBase<CowRepr<'a, i64>, Dim<IxDynImpl>>;

#[derive(Debug, Clone)]
pub enum PoolingStrategy {
    CLS,
    Mean,
    ReluLogMaxPooling,
}

impl PoolingStrategy {
    fn relu_log_max_pooling(
        embeddings: ViewHolder<'_, f32, Dim<IxDynImpl>>,
        attention_mask: &SessionInput,
        output_dims: usize,
    ) -> Vec<Vec<f32>> {
        // Apply ReLU: max(0, x)
        let relu_embeddings = embeddings.mapv(|x| x.max(0.0));

        // Apply log(1 + x)
        let relu_log_embeddings = relu_embeddings.mapv(|x| (1.0 + x).ln());

        // Expand attention mask to match embeddings dimensions
        let attention_mask_shape = attention_mask.shape();
        let input_mask_expanded = attention_mask.clone().insert_axis(Axis(2)).into_owned();
        let input_mask_expanded = input_mask_expanded
            .broadcast((
                attention_mask_shape[0],
                attention_mask_shape[1],
                output_dims,
            ))
            .unwrap()
            .to_owned();
        let input_mask_expanded = input_mask_expanded.mapv(|v| v as f32);

        // Apply attention mask
        let relu_log_embeddings = relu_log_embeddings.to_owned();
        let masked_embeddings = &relu_log_embeddings * &input_mask_expanded;

        // Find the maximum value across the sequence dimension (Axis 1)
        let max_embeddings = masked_embeddings.map_axis(Axis(1), |view| {
            view.fold(f32::NEG_INFINITY, |a, &b| a.max(b))
        });

        // Convert the resulting max_embeddings to a Vec<Vec<f32>>
        max_embeddings
            .iter()
            .map(|s| *s)
            .chunks(output_dims)
            .into_iter()
            .map(|b| b.collect())
            .collect()
    }

    fn cls_pooling(
        embeddings: ViewHolder<'_, f32, Dim<IxDynImpl>>,
        output_dims: usize,
    ) -> Vec<Vec<f32>> {
        embeddings
            .slice(s![.., 0, ..])
            .iter()
            .map(|s| *s)
            .chunks(output_dims)
            .into_iter()
            .map(|b| b.collect())
            .collect()
    }

    fn mean_pooling(
        embeddings: ViewHolder<'_, f32, Dim<IxDynImpl>>,
        attention_mask: &SessionInput,
        output_dims: usize,
    ) -> Vec<Vec<f32>> {
        let attention_mask_shape = attention_mask.shape();
        let input_mask_expanded = attention_mask.clone().insert_axis(Axis(2)).into_owned();
        let input_mask_expanded = input_mask_expanded
            .broadcast((
                attention_mask_shape[0],
                attention_mask_shape[1],
                output_dims,
            ))
            .unwrap()
            .to_owned();
        let input_mask_expanded = input_mask_expanded.mapv(|v| v as f32);
        let embeddings = embeddings.to_owned();
        let masked_embeddings = &embeddings * &input_mask_expanded;

        let mean_embeddings = masked_embeddings.sum_axis(Axis(1));
        let divider = input_mask_expanded.sum_axis(Axis(1)).mapv(|x| x.max(1e-9));
        let embeddings = mean_embeddings / divider;

        embeddings
            .iter()
            .map(|s| *s)
            .chunks(output_dims)
            .into_iter()
            .map(|b| b.collect())
            .collect()
    }

    pub fn pool(
        &self,
        embeddings: ViewHolder<'_, f32, Dim<IxDynImpl>>,
        attention_mask: &SessionInput,
        output_dims: usize,
    ) -> Vec<Vec<f32>> {
        match self {
            &PoolingStrategy::CLS => PoolingStrategy::cls_pooling(embeddings, output_dims),
            &PoolingStrategy::Mean => {
                PoolingStrategy::mean_pooling(embeddings, attention_mask, output_dims)
            }
            &PoolingStrategy::ReluLogMaxPooling => {
                PoolingStrategy::relu_log_max_pooling(embeddings, attention_mask, output_dims)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelParams {
    layer_cnt: Option<usize>,
    head_cnt: Option<usize>,
    head_dim: Option<usize>,
    pooling_strategy: PoolingStrategy,
}

pub struct EncoderService {
    name: String,
    model_params: ModelParams,
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
const MAX_IMAGE_SIZE: usize = 1024 * 1024 * 20; // 20 MB

struct ModelInfo {
    url: String,
    params: ModelParams,
    tokenizer_url: Option<String>,
    onnx_data_url: Option<String>,
    encoder_args: EncoderOptions,
    encoder: Option<EncoderService>,
}

struct ModelInfoBuilder {
    base_url: &'static str,
    pooling_strategy: Option<PoolingStrategy>,
    use_tokenizer: Option<bool>,
    visual: Option<bool>,
    input_image_size: Option<usize>,
    padding_params: Option<PaddingParams>,
    truncation_params: Option<TruncationParams>,
    layer_cnt: Option<usize>,
    head_cnt: Option<usize>,
    head_dim: Option<usize>,
    onnx_data: bool,
}

impl ModelInfoBuilder {
    fn new(base_url: &'static str) -> Self {
        ModelInfoBuilder {
            base_url,
            pooling_strategy: None,
            use_tokenizer: None,
            visual: None,
            input_image_size: None,
            padding_params: None,
            truncation_params: None,
            layer_cnt: None,
            head_cnt: None,
            head_dim: None,
            onnx_data: false,
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

    fn with_layer_cnt(&mut self, layer_cnt: usize) -> &mut Self {
        self.layer_cnt = Some(layer_cnt);
        self
    }

    fn with_head_cnt(&mut self, head_cnt: usize) -> &mut Self {
        self.head_cnt = Some(head_cnt);
        self
    }

    fn with_head_dim(&mut self, head_dim: usize) -> &mut Self {
        self.head_dim = Some(head_dim);
        self
    }

    fn with_pooling_strategy(&mut self, strategy: PoolingStrategy) -> &mut Self {
        self.pooling_strategy = Some(strategy);
        self
    }

    fn with_onnx_data(&mut self, status: bool) -> &mut Self {
        self.onnx_data = status;
        self
    }

    fn build(&self) -> ModelInfo {
        let model_url = format!("{}/model.onnx", self.base_url);
        let mut tokenizer_url = None;
        let mut onnx_data_url = None;

        if self.use_tokenizer.is_some() {
            tokenizer_url = Some(format!("{}/tokenizer.json", self.base_url));
        }

        if self.onnx_data {
            onnx_data_url = Some(format!("{}/model.onnx_data", self.base_url));
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
            params: ModelParams {
                layer_cnt: self.layer_cnt.clone(),
                head_cnt: self.head_cnt.clone(),
                head_dim: self.head_dim.clone(),
                pooling_strategy: self
                    .pooling_strategy
                    .clone()
                    .unwrap_or(PoolingStrategy::CLS),
            },
            encoder: None,
            encoder_args,
            onnx_data_url,
        }
    }
}

lazy_static! {
    static ref MODEL_INFO_MAP: Mutex<HashMap<&'static str, ModelInfo>> = Mutex::new(HashMap::from([
        ("clip/ViT-B-32-textual", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/textual").with_tokenizer(true).build()),
        ("clip/ViT-B-32-visual", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/openai/ViT-B-32/visual").with_visual(true).with_input_image_size(224).build()),
        ("BAAI/bge-small-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-small-en-v1.5").with_tokenizer(true).build()),
        ("BAAI/bge-base-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-base-en-v1.5").with_tokenizer(true).build()),
        ("BAAI/bge-large-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-large-en-v1.5").with_tokenizer(true).build()),
        ("BAAI/bge-m3", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/BAAI/bge-m3").with_tokenizer(true).with_onnx_data(true).with_layer_cnt(8).with_head_cnt(4).with_head_dim(64).build()),
        ("intfloat/e5-base-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/intfloat/e5-base-v2").with_tokenizer(true).build()),
        ("intfloat/e5-large-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/intfloat/e5-large-v2").with_tokenizer(true).build()),
        ("llmrails/ember-v1", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/llmrails/ember-v1").with_tokenizer(true).build()),
        ("thenlper/gte-base", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/thenlper/gte-base").with_tokenizer(true).build()),
        ("thenlper/gte-large", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/thenlper/gte-large").with_tokenizer(true).build()),
        ("microsoft/all-MiniLM-L12-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/microsoft/all-MiniLM-L12-v2").with_tokenizer(true).build()),
        ("microsoft/all-mpnet-base-v2", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/microsoft/all-mpnet-base-v2").with_tokenizer(true).build()),
        ("transformers/multi-qa-mpnet-base-dot-v1", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/transformers/multi-qa-mpnet-base-dot-v1").with_tokenizer(true).build()),
        ("jinaai/jina-embeddings-v2-small-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/jinaai/jina-embeddings-v2-small-en").with_tokenizer(true).with_layer_cnt(4).with_head_cnt(4).with_head_dim(64).with_pooling_strategy(PoolingStrategy::Mean).build()),
        ("jinaai/jina-embeddings-v2-base-en", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/jinaai/jina-embeddings-v2-base-en").with_tokenizer(true).with_layer_cnt(12).with_head_cnt(12).with_head_dim(64).with_pooling_strategy(PoolingStrategy::Mean).build()),
        ("naver/splade-v3", ModelInfoBuilder::new("https://huggingface.co/varik77/onnx-models/resolve/main/naver/splade-v3").with_tokenizer(true).with_pooling_strategy(PoolingStrategy::ReluLogMaxPooling).build())
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

static MEM_PERCENT_THRESHOLD: f64 = 80.0;

impl EncoderService {
    pub fn new(
        environment: &Arc<Environment>,
        model_name: &str,
        model_params: ModelParams,
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
            model_params,
            vision_size: args.input_image_size,
        })
    }

    fn get_required_memory(&self, seq_length: usize) -> usize {
        let model_params = &self.model_params;

        if model_params.head_cnt.is_none()
            || model_params.head_dim.is_none()
            || model_params.layer_cnt.is_none()
        {
            return 1;
        }

        let num_layers = model_params.layer_cnt.unwrap();
        let num_heads = model_params.head_cnt.unwrap();
        let head_dim = model_params.head_dim.unwrap();
        /*
        R = n_tr_blocks = number of transformer blocks in the model (e.g layers)
        N = n_head = number of attention heads
        D = dim = dimension of each attention head
        S = sequence_length = input sequence length

        memory modal = 4 * R * N^2 * D^2
        memory activations = RBNS(S + 2D)
        total memory required = ((4 * R * N^2 * D^2) + RBNS(S + 2D)) * float64 memory in bytes
        Formula taken from: https://www.linkedin.com/pulse/estimating-memory-requirements-transformer-networks-schartz-rehan/
        */
        let float64_bytes = 8;
        let total_memory = ((4 * num_layers * num_heads.pow(2) * head_dim.pow(2))
            + num_layers * num_heads * seq_length * (seq_length + 2 * head_dim))
            * float64_bytes;
        // Add 20% additional memory for overhead
        return (total_memory as f64 * 1.2) as usize;
    }

    fn chunk_session_input<'a>(
        &self,
        vecs: Vec<Vec<i64>>,
        batch_size: usize,
    ) -> Result<Vec<Vec<SessionInput>>, anyhow::Error> {
        // Currently this function will only work for bert models
        // get token count for each text
        let token_cnt = vecs[0].len() / batch_size;
        let input_cnt = vecs.len();
        // Get available memory for GPU or RAM
        let available_memory = get_available_memory()? as usize;
        // Calculate memory consumption for one item
        // Then devide GPU memory / memory needed
        // Then devide array into chunks of that count
        let memory_needed_for_one_input = self.get_required_memory(token_cnt as usize);
        // Get max batch size
        let max_batch_size = cmp::max(1, (available_memory / memory_needed_for_one_input) as usize);
        // For models which does not need chunking the get_required_memory will return 1
        // And max_batch_size will be higher than provided batch_size, so we will take
        // The minimum of batch_size and max_batch_size
        let max_batch_size = cmp::min(batch_size, max_batch_size);
        let max_token_cnt = max_batch_size * token_cnt;

        let mut inputs = Vec::with_capacity(batch_size / max_batch_size);
        // Make vector of shape
        // [
        //   [tokenIds, tokenTypeIds, Mask],
        //   [tokenIds, tokenTypeIds, Mask],
        //   [tokenIds, tokenTypeIds, Mask]
        // ]
        for input in vecs {
            for (index, chunk) in input.chunks(max_token_cnt).enumerate() {
                if inputs.len() == index {
                    inputs.push(Vec::with_capacity(input_cnt));
                }
                let group: &mut Vec<SessionInput> = inputs.get_mut(index).unwrap();
                let group_batch_size = chunk.len() / token_cnt;
                group.push(
                    CowArray::from(Array2::from_shape_vec(
                        (group_batch_size, token_cnt),
                        chunk.to_vec(),
                    )?)
                    .into_dyn(),
                );
            }
        }

        Ok(inputs)
    }

    fn process_text_bert(
        &self,
        texts: &Vec<&str>,
    ) -> Result<EmbeddingResult, Box<dyn std::error::Error + Send + Sync>> {
        let session = &self.encoder;
        let text_len = texts.len();
        let preprocessed = self
            .tokenizer
            .as_ref()
            .unwrap()
            .encode_batch(texts.clone(), true)?;

        let mut vecs = Vec::with_capacity(session.inputs.len());
        let mut processed_tokens = 0;

        let mut attention_mask_idx = None;
        for input in &session.inputs {
            let mut val = None;
            match input.name.as_str() {
                "input_ids" => {
                    let v: Vec<i64> = preprocessed
                        .iter()
                        .map(|i| i.get_ids().iter().map(|b| *b as i64).collect())
                        .concat();
                    processed_tokens = v.len();
                    val = Some(v);
                }
                "attention_mask" => {
                    let v: Vec<i64> = preprocessed
                        .iter()
                        .map(|i| i.get_attention_mask().iter().map(|b| *b as i64).collect())
                        .concat();
                    val = Some(v);
                    attention_mask_idx = Some(vecs.len())
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
                vecs.push(v);
            }
        }

        if attention_mask_idx.is_none() {
            return Err(format!(
                "Could not get attention_mask_idx from sesssion inputs: {:?}",
                session.inputs
            )
            .into());
        }

        let attention_mask_idx = attention_mask_idx.unwrap();

        let input_chunks = self.chunk_session_input(vecs, text_len)?;
        let embeddings = input_chunks
            .iter()
            .map(|chunk| {
                // Iterate over each chunk and create embedding for that chunk
                let inputs: Vec<Value<'_>> = chunk
                    .iter()
                    .map(|v| Value::from_array(session.allocator(), &v).unwrap())
                    .collect();

                let result = session.run(inputs);

                if let Err(e) = result {
                    anyhow::bail!(e);
                }

                let outputs = result.unwrap();

                let binding = outputs[0].try_extract()?;
                let embeddings = binding.view();
                let attention_mask = &chunk[attention_mask_idx];
                let output_dims = session.outputs[0].dimensions.last().unwrap().unwrap() as usize;
                let embeddings: Vec<Vec<f32>> = self.model_params.pooling_strategy.pool(
                    embeddings,
                    attention_mask,
                    output_dims,
                );

                Ok(embeddings)
            })
            .collect::<Result<Vec<Vec<Vec<f32>>>, anyhow::Error>>();

        Ok(EmbeddingResult {
            processed_tokens,
            embeddings: embeddings.map(|vec_vec| vec_vec.into_iter().flatten().collect())?,
        })
    }

    fn process_text_clip(
        &self,
        text: &Vec<&str>,
    ) -> Result<EmbeddingResult, Box<dyn std::error::Error + Send + Sync>> {
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

        let processed_tokens = v1.len();

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

        Ok(EmbeddingResult {
            processed_tokens,
            embeddings: embeddings
                .iter()
                .map(|s| *s)
                .chunks(*seq_len)
                .into_iter()
                .map(|b| b.collect())
                .collect(),
        })
    }

    fn process_text(
        &self,
        texts: &Vec<&str>,
    ) -> Result<EmbeddingResult, Box<dyn std::error::Error + Send + Sync>> {
        match self.name.as_str() {
            "clip/ViT-B-32-textual" => self.process_text_clip(texts),
            _ => self.process_text_bert(texts),
        }
    }

    pub fn process_image_clip(
        &self,
        images_bytes: &Vec<&Vec<u8>>,
    ) -> Result<EmbeddingResult, Box<dyn std::error::Error + Send + Sync>> {
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

        let processed_tokens = pixels.len();

        let outputs = session.run(vec![Value::from_array(
            session.allocator(),
            &pixels.into_dyn(),
        )?])?;
        let binding = outputs[0].try_extract()?;
        let embeddings = binding.view();

        let seq_len = embeddings.shape().get(1).unwrap();

        Ok(EmbeddingResult {
            processed_tokens,
            embeddings: embeddings
                .iter()
                .map(|s| *s)
                .chunks(*seq_len)
                .into_iter()
                .map(|b| b.collect())
                .collect(),
        })
    }

    fn process_image(
        &self,
        images_bytes: &Vec<&Vec<u8>>,
    ) -> Result<EmbeddingResult, Box<dyn std::error::Error + Send + Sync>> {
        match self.name.as_str() {
            "clip/ViT-B-32-visual" => self.process_image_clip(images_bytes),
            _ => self.process_image_clip(images_bytes),
        }
    }
}

pub struct OrtRuntime<'a> {
    cache: bool,
    data_path: String,
    logger: &'a LoggerFn,
}

#[derive(Deserialize)]
pub struct OrtRuntimeParams {
    data_path: Option<String>,
    cache: Option<bool>,
}

impl<'a> OrtRuntime<'a> {
    pub fn new(logger: &'a LoggerFn, params: &'a str) -> Result<Self, anyhow::Error> {
        let runtime_params: OrtRuntimeParams = serde_json::from_str(&params)?;

        Ok(Self {
            logger,
            cache: runtime_params.cache.unwrap_or(false),
            data_path: runtime_params.data_path.unwrap_or(DATA_PATH.to_owned()),
        })
    }

    fn clear_model_cache(
        &self,
        model_map: &mut HashMap<&'static str, ModelInfo>,
    ) -> Result<(), anyhow::Error> {
        for (_, model_info) in model_map.iter_mut() {
            model_info.encoder = None;
        }

        Ok(())
    }

    fn check_available_memory(
        &self,
        model_path: &PathBuf,
        model_map: &mut HashMap<&'static str, ModelInfo>,
    ) -> Result<(), anyhow::Error> {
        let mut sys = System::new_all();
        sys.refresh_all();
        let total_free_mem =
            (sys.total_memory() - sys.used_memory()) + (sys.total_swap() - sys.used_swap());
        let total_free_mem = total_free_mem as f64;
        let model_file = std::fs::File::open(model_path)?;
        let metadata = model_file.metadata()?;
        let model_size = metadata.len() as f64;

        let percent_of_free_mem = (model_size / total_free_mem) * 100.0;

        let mut cache_cleared = false;
        if percent_of_free_mem >= MEM_PERCENT_THRESHOLD {
            // If not enough RAM try to clear model cache
            // and check again
            (self.logger)("System memory limit exceeded, trying to clear cache");
            self.clear_model_cache(model_map)?;
            cache_cleared = true;
            sys.refresh_all();
            let total_free_mem =
                (sys.total_memory() - sys.used_memory()) + (sys.total_swap() - sys.used_swap());
            let total_free_mem = total_free_mem as f64;
            let percent_of_free_mem = (model_size / total_free_mem) * 100.0;

            if percent_of_free_mem >= MEM_PERCENT_THRESHOLD {
                let mem_avail_in_mb = total_free_mem / 1024.0 / 1024.0;

                // We need available_memory + percent_diff % to run the model
                let percent_diff = percent_of_free_mem - MEM_PERCENT_THRESHOLD;
                let mem_needed_in_mb = mem_avail_in_mb + mem_avail_in_mb / (100.0 / percent_diff);
                anyhow::bail!(
                    "Not enough free memory to run the model. Memory needed: {:.2}MB, Memory available: {:.2}MB",
                    mem_needed_in_mb,
                    mem_avail_in_mb
                );
            }
        }

        if self.cache && percent_gpu_memory_used()? >= MEM_PERCENT_THRESHOLD {
            // The GPU memory will only be checked when the models are cached
            // If not enough GPU RAM and cache is not clearted already
            // try to clear model cache
            // We will not check again and instead let ort fail if not enough memory
            // the ort error will not kill the process as it is result
            if !cache_cleared {
                (self.logger)("GPU memory limit exceeded, trying to clear cache");
                self.clear_model_cache(model_map)?;
            }
        }

        Ok(())
    }

    fn check_and_download_files(
        &self,
        model_name: &str,
        mut models_map: &mut HashMap<&'static str, ModelInfo>,
    ) -> Result<(), anyhow::Error> {
        {
            let model_info = models_map.get(model_name);

            if model_info.is_none() {
                anyhow::bail!(
                    "Model \"{}\" not found.\nAvailable models: {}",
                    model_name,
                    models_map.keys().join(", ")
                )
            }

            let model_info = model_info.unwrap();

            if model_info.encoder.is_some() {
                // if encoder exists return
                return Ok(());
            }
        }

        let model_info = models_map.get_mut(model_name).unwrap();

        let model_folder = Path::join(&Path::new(&self.data_path), model_name);
        let model_path = Path::join(&model_folder, "model.onnx");
        let tokenizer_path = Path::join(&model_folder, "tokenizer.json");

        // TODO parallel download with tokio
        if !model_path.exists() {
            // model is not downloaded, we should download it
            (self.logger)("Downloading model [this is one time operation]");
            download_file(&model_info.url, &model_path)?;
        }

        if !tokenizer_path.exists() && model_info.tokenizer_url.is_some() {
            (self.logger)("Downloading tokenizer [this is one time operation]");
            // tokenizer is not downloaded, we should download it
            download_file(&model_info.tokenizer_url.as_ref().unwrap(), &tokenizer_path)?;
        }

        if let Some(onnx_data_url) = &model_info.onnx_data_url {
            let onnx_data_path = Path::join(&model_folder, "model.onnx_data");

            if !onnx_data_path.exists() {
                (self.logger)("Downloading model onnx data [this is one time operation]");
                download_file(onnx_data_url, &onnx_data_path)?;
            }
        }

        // Check available memory
        self.check_available_memory(&model_path, &mut models_map)?;

        let model_info = models_map.get_mut(model_name).unwrap();
        let encoder = EncoderService::new(
            &ONNX_ENV,
            model_name,
            model_info.params.clone(),
            &model_folder,
            &model_info.encoder_args,
        );

        match encoder {
            Ok(enc) => model_info.encoder = Some(enc),
            Err(err) => {
                anyhow::bail!(err)
            }
        }

        Ok(())
    }

    async fn get_image_buffer(&self, path_or_url: &str) -> Result<Vec<u8>, anyhow::Error> {
        if let Ok(url) = Url::parse(path_or_url) {
            let client = HttpClient::builder()
                .timeout(Duration::from_secs(15))
                .redirect_policy(RedirectPolicy::Limit(2))
                .build()?;

            let response = client.get_async(&url.to_string()).await;

            if let Err(e) = response {
                anyhow::bail!(
                    "[X] Error while downloading image \"{}\" - {}",
                    path_or_url,
                    e
                );
            }

            let mut response = response?;
            let status = response.status();

            if status.as_u16() > 304 {
                anyhow::bail!(
                    "[X] Request failed with status {status}. Error: {}",
                    response.text().await?
                )
            }

            let response = response.bytes().await?;

            if response.len() > MAX_IMAGE_SIZE {
                anyhow::bail!(
                    "[X] Maximum supported image size is {}MB, downloaded file size is {}MB",
                    MAX_IMAGE_SIZE / 1024 / 1024,
                    response.len() / 1024 / 1024
                );
            }
            return Ok(response.to_vec());
        } else if Path::new(path_or_url).is_absolute() {
            let response = fs::read(path_or_url).await;
            if let Err(e) = response {
                anyhow::bail!("[X] Error while reading file \"{}\" - {}", path_or_url, e);
            }
            return Ok(response.unwrap());
        } else {
            anyhow::bail!("[X] Expected URL or absolute path got: {path_or_url}");
        }
    }

    fn get_images_parallel(
        &self,
        paths_or_urls: &Vec<&str>,
    ) -> Result<Vec<Vec<u8>>, anyhow::Error> {
        let buffers: Arc<Mutex<Vec<Vec<u8>>>> =
            Arc::new(Mutex::new(Vec::with_capacity(paths_or_urls.len())));
        let threaded_rt = runtime::Runtime::new()?;
        let tasks: Vec<_> = paths_or_urls
            .iter()
            .map(|&path_or_url| self.get_image_buffer(path_or_url))
            .collect();
        let chunk_size = std::thread::available_parallelism().unwrap().into();

        (self.logger)(&format!(
            "[*] Trying to read/download {} images",
            paths_or_urls.len()
        ));

        let runtime_result = threaded_rt.block_on(async {
            let mut tasks = futures::stream::iter(tasks).buffered(chunk_size);
            while let Some(result) = tasks.next().await {
                let mut buffers = buffers.lock().unwrap();
                if let Err(e) = result {
                    eprintln!("{}", e);
                    buffers.push(Vec::new());
                } else {
                    buffers.push(result.unwrap());
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        if let Err(e) = runtime_result {
            anyhow::bail!("{}", e);
        }

        (self.logger)("[*] All images read into buffer");
        let buffers = buffers.lock().unwrap();
        Ok(buffers.clone())
    }
}

impl<'a> EmbeddingRuntime for OrtRuntime<'a> {
    fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error> {
        let mut map = MODEL_INFO_MAP.lock().unwrap();
        let download_result = self.check_and_download_files(model_name, &mut map);

        if let Err(err) = download_result {
            anyhow::bail!("{:?}", err);
        }

        let model_info = map.get(model_name).unwrap();

        let result;
        if model_info.encoder_args.visual {
            let buffers = self.get_images_parallel(inputs)?;

            // If buffer will return error while downloading
            // We will put an empty array instead, to omit that image
            // So here we are filtering the images which succeed to be downloaded
            let filtered_buffers = buffers
                .iter()
                .filter(|b| b.len() > 0)
                .collect::<Vec<&Vec<u8>>>();

            let model_result = if filtered_buffers.len() > 0 {
                model_info
                    .encoder
                    .as_ref()
                    .unwrap()
                    .process_image(&filtered_buffers)
            } else {
                Ok(EmbeddingResult {
                    embeddings: Vec::new(),
                    processed_tokens: 0,
                })
            };

            let model_result: EmbeddingResult = match model_result {
                Ok(res) => res,
                Err(err) => {
                    anyhow::bail!("Error happened while generating image embeddings {:?}", err);
                }
            };

            // Now we are mapping back the results, and for failed images
            // We will put an a vector filled with -1
            // We are not putting an empty vector here
            // Because on each daemon start it would take null values
            // And try to generate embeddings for them, so startup will slow down
            let mut idx = 0;
            // We are unwrapping here, as models are static
            // And we believe the model file to have the outputs
            // And output shuold have the dimensions
            // This should be checked when adding new model

            let output_dims = model_info
                .encoder
                .as_ref()
                .unwrap()
                .encoder
                .outputs
                .last()
                .unwrap()
                .dimensions()
                .last()
                .unwrap()
                .unwrap();

            let invalid_vec = vec![-1.0; output_dims];
            let return_res = buffers
                .iter()
                .map(|el| {
                    if el.len() == 0 {
                        invalid_vec.clone()
                    } else {
                        let vec = model_result.embeddings[idx].clone();
                        idx += 1;
                        vec
                    }
                })
                .collect();

            result = Ok(EmbeddingResult {
                embeddings: return_res,
                processed_tokens: model_result.processed_tokens,
            })
        } else {
            result = model_info.encoder.as_ref().unwrap().process_text(inputs);
        }

        if !self.cache {
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

    fn get_available_models(&self) -> (String, Vec<(String, bool)>) {
        let map = MODEL_INFO_MAP.lock().unwrap();
        let mut res = String::new();
        let data_path = &self.data_path;
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

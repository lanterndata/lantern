use itertools::Itertools;
use std::{collections::HashMap, sync::RwLock};

use crate::{
    core::LoggerFn,
    runtime::{EmbeddingResult, EmbeddingRuntime},
    HTTPRuntime,
};
use serde::{Deserialize, Serialize};

struct ModelInfo {
    name: String,
    sequence_len: usize,
    dimensions: usize,
}

#[derive(Deserialize)]
struct CohereMetaBilledUnits {
    input_tokens: usize,
}

#[derive(Deserialize)]
struct CohereMeta {
    billed_units: CohereMetaBilledUnits,
}

#[derive(Deserialize)]
struct CohereResponse {
    embeddings: Vec<Vec<f32>>,
    meta: CohereMeta,
}

impl ModelInfo {
    pub fn new(model_name: &str) -> Result<Self, anyhow::Error> {
        let name = model_name.split("/").last().unwrap().to_owned();
        match model_name {
            "cohere/embed-english-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "cohere/embed-multilingual-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "cohere/embed-english-light-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 384,
            }),
            "cohere/embed-multilingual-light-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 384,
            }),
            "cohere/embed-english-v2.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 4096,
            }),
            "cohere/embed-english-light-v2.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "cohere/embed-multilingual-v2.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 768,
            }),
            _ => anyhow::bail!("Unsupported model {model_name}"),
        }
    }
}

lazy_static! {
    static ref MODEL_INFO_MAP: RwLock<HashMap<&'static str, ModelInfo>> =
        RwLock::new(HashMap::from([
            (
                "cohere/embed-english-v3.0",
                ModelInfo::new("cohere/embed-english-v3.0").unwrap()
            ),
            (
                "cohere/embed-multilingual-v3.0",
                ModelInfo::new("cohere/embed-multilingual-v3.0").unwrap()
            ),
            (
                "cohere/embed-multilingual-light-v3.0",
                ModelInfo::new("cohere/embed-multilingual-light-v3.0").unwrap()
            ),
            (
                "cohere/embed-english-light-v3.0",
                ModelInfo::new("cohere/embed-english-light-v3.0").unwrap()
            ),
            (
                "cohere/embed-english-v2.0",
                ModelInfo::new("cohere/embed-english-v2.0").unwrap()
            ),
            (
                "cohere/embed-english-light-v2.0",
                ModelInfo::new("cohere/embed-english-light-v2.0").unwrap()
            ),
            (
                "cohere/embed-multilingual-v2.0",
                ModelInfo::new("cohere/embed-multilingual-v2.0").unwrap()
            ),
        ]));
}

pub struct CohereRuntime<'a> {
    request_timeout: u64,
    max_batch_size: usize,
    base_url: String,
    headers: Vec<(String, String)>,
    input_type: String,
    #[allow(dead_code)]
    logger: &'a LoggerFn,
}

#[derive(Serialize, Deserialize)]
pub struct CohereRuntimeParams {
    pub api_token: Option<String>,
    pub input_type: Option<String>,
}

impl<'a> CohereRuntime<'a> {
    pub fn new(logger: &'a LoggerFn, params: &'a str) -> Result<Self, anyhow::Error> {
        let runtime_params: CohereRuntimeParams = serde_json::from_str(&params)?;

        if runtime_params.api_token.is_none() {
            anyhow::bail!("'api_token' is required for OpenAi runtime");
        }

        Ok(Self {
            base_url: "https://api.cohere.ai".to_owned(),
            logger,
            request_timeout: 120,
            max_batch_size: 96,
            input_type: runtime_params
                .input_type
                .unwrap_or("search_document".to_owned()),
            headers: vec![
                ("Content-Type".to_owned(), "application/json".to_owned()),
                (
                    "Authorization".to_owned(),
                    format!("Bearer {}", runtime_params.api_token.unwrap()),
                ),
            ],
        })
    }

    fn chunk_inputs(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let model_map = MODEL_INFO_MAP.read().unwrap();
        let model_info = model_map.get(model_name);

        if model_info.is_none() {
            anyhow::bail!(
                "Unsupported model {model_name}\nAvailable models: {}",
                model_map.keys().join(", ")
            );
        }
        let model_info = model_info.unwrap();
        let name = &model_info.name;

        let batch_tokens: Vec<String> = inputs
            .chunks(self.max_batch_size)
            .map(|token_group| {
                let json_string = serde_json::to_string(token_group).unwrap();
                let input_type = &self.input_type;
                format!(
                    r#"
                 {{
                   "texts": {json_string},
                   "model": "{name}",
                   "input_type": "{input_type}",
                   "truncate": "END"
                 }}
                "#
                )
            })
            .collect();

        Ok(batch_tokens)
    }

    // Static functions
    pub fn get_response(body: Vec<u8>) -> Result<EmbeddingResult, anyhow::Error> {
        let result: Result<CohereResponse, serde_json::Error> = serde_json::from_slice(&body);
        if let Err(e) = result {
            anyhow::bail!(
                "Error: {e}. Cohere response: {:?}",
                serde_json::from_slice::<serde_json::Value>(&body)?
            );
        }

        let result = result.unwrap();

        Ok(EmbeddingResult {
            embeddings: result.embeddings,
            processed_tokens: result.meta.billed_units.input_tokens,
        })
    }
}

impl<'a> EmbeddingRuntime for CohereRuntime<'a> {
    fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error> {
        self.post_request("/v1/embed", model_name, inputs)
    }

    fn get_available_models(&self) -> (String, Vec<(String, bool)>) {
        let map = MODEL_INFO_MAP.read().unwrap();
        let mut res = String::new();
        let mut models = Vec::with_capacity(map.len());
        for (key, value) in &*map {
            res.push_str(&format!(
                "{} - sequence_len: {}, dimensions: {}\n",
                key, value.sequence_len, value.dimensions
            ));
            models.push((key.to_string(), false));
        }

        return (res, models);
    }
}
HTTPRuntime!(CohereRuntime);

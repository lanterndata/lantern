use itertools::Itertools;
use std::collections::HashMap;
use tokio::sync::RwLock;

use super::{
    runtime::{EmbeddingResult, EmbeddingRuntimeT},
    LoggerFn,
};
use crate::{check_and_get_model, embeddings::cli::EmbeddingJobType, HTTPRuntime};
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
            "embed-english-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "embed-multilingual-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "embed-english-light-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 384,
            }),
            "embed-multilingual-light-v3.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 384,
            }),
            "embed-english-v2.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 4096,
            }),
            "embed-english-light-v2.0" => Ok(Self {
                name,
                sequence_len: 512,
                dimensions: 1024,
            }),
            "embed-multilingual-v2.0" => Ok(Self {
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
                "embed-english-v3.0",
                ModelInfo::new("embed-english-v3.0").unwrap()
            ),
            (
                "embed-multilingual-v3.0",
                ModelInfo::new("embed-multilingual-v3.0").unwrap()
            ),
            (
                "embed-multilingual-light-v3.0",
                ModelInfo::new("embed-multilingual-light-v3.0").unwrap()
            ),
            (
                "embed-english-light-v3.0",
                ModelInfo::new("embed-english-light-v3.0").unwrap()
            ),
            (
                "embed-english-v2.0",
                ModelInfo::new("embed-english-v2.0").unwrap()
            ),
            (
                "embed-english-light-v2.0",
                ModelInfo::new("embed-english-light-v2.0").unwrap()
            ),
            (
                "embed-multilingual-v2.0",
                ModelInfo::new("embed-multilingual-v2.0").unwrap()
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

    async fn chunk_inputs(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let model_map = MODEL_INFO_MAP.read().await;
        let model_info = check_and_get_model!(model_map, model_name);

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

impl<'a> EmbeddingRuntimeT for CohereRuntime<'a> {
    async fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error> {
        self.post_request("/v1/embed", model_name, inputs).await
    }

    async fn get_available_models(
        &self,
        _job_type: EmbeddingJobType,
    ) -> (String, Vec<(String, bool)>) {
        let map = MODEL_INFO_MAP.read().await;
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

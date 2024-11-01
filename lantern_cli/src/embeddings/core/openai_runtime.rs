use itertools::Itertools;
use regex::Regex;
use serde_json::json;
use std::collections::HashMap;
use tokio::sync::RwLock;

use super::{
    runtime::{BatchCompletionResult, CompletionResult, EmbeddingResult, EmbeddingRuntimeT},
    LoggerFn,
};
use crate::{check_and_get_model, embeddings::cli::EmbeddingJobType, HTTPRuntime};
use serde::{Deserialize, Serialize};
use tiktoken_rs::{cl100k_base, CoreBPE};

struct ModelInfo {
    name: String,
    tokenizer: CoreBPE,
    sequence_len: usize,
    dimensions: usize,
    var_dimension: bool,
}

#[derive(Deserialize, Debug)]
struct OpenAiEmbedding {
    embedding: Vec<f32>,
}

#[derive(Deserialize, Debug)]
struct OpenAiUsage {
    total_tokens: usize,
}

#[derive(Deserialize, Debug)]
struct OpenAiResponse {
    data: Vec<OpenAiEmbedding>,
    usage: OpenAiUsage,
}

#[derive(Deserialize, Debug)]
struct OpenAiChatMessage {
    #[allow(dead_code)]
    role: String,
    content: String,
}

impl OpenAiChatMessage {
    fn new() -> OpenAiChatMessage {
        OpenAiChatMessage {
            role: "system".to_owned(),
            content: "".to_owned(),
        }
    }
}

#[derive(Deserialize, Debug)]
struct OpenAiChatChoice {
    message: OpenAiChatMessage,
}

#[derive(Deserialize, Debug)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChatChoice>,
    usage: OpenAiUsage,
}

#[derive(PartialEq, Debug)]
enum OpenAiDeployment {
    Azure,
    OpenAi,
    Custom,
}

static AZURE_OPENAI_REGEX: &'static str = r"^https:\/\/[a-zA-Z0-9_\-]+\.openai\.azure\.com\/openai\/deployments\/[a-zA-Z0-9_\-]+\/embeddings\?api-version=2023-05-15$";

impl ModelInfo {
    pub fn new(model_name: &str, job_type: EmbeddingJobType) -> Result<Self, anyhow::Error> {
        let name = model_name.split("/").last().unwrap().to_owned();
        match job_type {
            EmbeddingJobType::EmbeddingGeneration => match model_name {
                "text-embedding-ada-002" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 8190,
                    dimensions: 1536,
                    var_dimension: false,
                }),
                "text-embedding-3-small" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 8190,
                    dimensions: 1536,
                    var_dimension: true,
                }),
                "text-embedding-3-large" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 8190,
                    dimensions: 3072,
                    var_dimension: true,
                }),
                _ => anyhow::bail!("Unsupported model {model_name}"),
            },
            EmbeddingJobType::Completion => match model_name {
                "gpt-4" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 128000,
                    dimensions: 0,
                    var_dimension: false,
                }),
                "gpt-4o" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 128000,
                    dimensions: 0,
                    var_dimension: false,
                }),
                "gpt-4o-mini" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 128000,
                    dimensions: 0,
                    var_dimension: false,
                }),
                "gpt-4-turbo" => Ok(Self {
                    name,
                    tokenizer: cl100k_base()?,
                    sequence_len: 128000,
                    dimensions: 0,
                    var_dimension: false,
                }),
                _ => anyhow::bail!("Unsupported model {model_name}"),
            },
        }
    }
}

lazy_static! {
    static ref MODEL_INFO_MAP: RwLock<HashMap<&'static str, ModelInfo>> =
        RwLock::new(HashMap::from([
            (
                "text-embedding-ada-002",
                ModelInfo::new(
                    "text-embedding-ada-002",
                    EmbeddingJobType::EmbeddingGeneration
                )
                .unwrap()
            ),
            (
                "text-embedding-3-small",
                ModelInfo::new(
                    "text-embedding-3-small",
                    EmbeddingJobType::EmbeddingGeneration
                )
                .unwrap()
            ),
            (
                "text-embedding-3-large",
                ModelInfo::new(
                    "text-embedding-3-large",
                    EmbeddingJobType::EmbeddingGeneration
                )
                .unwrap()
            ),
        ]));
    static ref COMPLETION_MODEL_INFO_MAP: RwLock<HashMap<&'static str, ModelInfo>> =
        RwLock::new(HashMap::from([
            (
                "gpt-4",
                ModelInfo::new("gpt-4", EmbeddingJobType::Completion).unwrap()
            ),
            (
                "gpt-4-turbo",
                ModelInfo::new("gpt-4-turbo", EmbeddingJobType::Completion).unwrap()
            ),
            (
                "gpt-4o",
                ModelInfo::new("gpt-4o", EmbeddingJobType::Completion).unwrap()
            ),
            (
                "gpt-4o-mini",
                ModelInfo::new("gpt-4o-mini", EmbeddingJobType::Completion).unwrap()
            ),
        ]));
}

pub struct OpenAiRuntime<'a> {
    request_timeout: u64,
    base_url: String,
    headers: Vec<(String, String)>,
    system_prompt: serde_json::Value,
    dimensions: Option<usize>,
    deployment_type: OpenAiDeployment,
    #[allow(dead_code)]
    logger: &'a LoggerFn,
}

#[derive(Serialize, Deserialize)]
pub struct OpenAiRuntimeParams {
    pub base_url: Option<String>,
    pub api_token: Option<String>,
    pub azure_entra_token: Option<String>,
    pub system_prompt: Option<String>,
    pub dimensions: Option<usize>,
}

impl<'a> OpenAiRuntime<'a> {
    pub fn new(logger: &'a LoggerFn, params: &'a str) -> Result<Self, anyhow::Error> {
        let runtime_params: OpenAiRuntimeParams = serde_json::from_str(&params)?;

        let (deployment, base_url) = Self::get_base_url(&runtime_params.base_url)?;

        let auth_header = match deployment {
            OpenAiDeployment::OpenAi | OpenAiDeployment::Custom => {
                if runtime_params.api_token.is_none() {
                    anyhow::bail!("'api_token' is required for OpenAi runtime");
                }
                (
                    "Authorization".to_owned(),
                    format!("Bearer {}", runtime_params.api_token.unwrap()),
                )
            }
            OpenAiDeployment::Azure => {
                // https://learn.microsoft.com/en-us/azure/ai-services/openai/reference
                if runtime_params.api_token.is_none() && runtime_params.azure_entra_token.is_none()
                {
                    anyhow::bail!(
                        "'api_token' or 'azure_entra_id' is required for Azure OpenAi runtime"
                    );
                }

                if let Some(key) = runtime_params.api_token {
                    ("api-key".to_owned(), format!("{}", key))
                } else {
                    (
                        "Authorization".to_owned(),
                        format!("Bearer {}", runtime_params.azure_entra_token.unwrap()),
                    )
                }
            }
        };

        let system_prompt = match &runtime_params.system_prompt {
            Some(system_prompt) => json!({ "role": "system", "content": system_prompt.clone()}),
            None => json!({ "role": "system", "content": "" }),
        };

        Ok(Self {
            base_url,
            logger,
            request_timeout: 120,
            deployment_type: deployment,
            headers: vec![
                ("Content-Type".to_owned(), "application/json".to_owned()),
                auth_header,
            ],
            dimensions: runtime_params.dimensions,
            system_prompt,
        })
    }

    fn get_base_url(
        base_url: &Option<String>,
    ) -> Result<(OpenAiDeployment, String), anyhow::Error> {
        if base_url.is_none() {
            return Ok((
                OpenAiDeployment::OpenAi,
                "https://api.openai.com".to_owned(),
            ));
        }

        let base_url = base_url.as_ref().unwrap();
        let azure_openai_re = Regex::new(AZURE_OPENAI_REGEX).unwrap();

        if azure_openai_re.is_match(base_url) {
            return Ok((OpenAiDeployment::Azure, base_url.clone()));
        }

        return Ok((OpenAiDeployment::Custom, base_url.clone()));
    }

    fn group_vectors_by_token_count(
        &self,
        input: Vec<Vec<u32>>,
        max_token_count: usize,
    ) -> Vec<Vec<Vec<u32>>> {
        let mut result = Vec::new();
        let mut current_group = Vec::new();
        let mut current_group_token_count = 0;

        for inner_vec in input {
            let inner_vec_token_count = inner_vec.len();

            if current_group_token_count + inner_vec_token_count <= max_token_count {
                // Add the inner vector to the current group
                current_group.push(inner_vec);
                current_group_token_count += inner_vec_token_count;
            } else {
                // Start a new group
                result.push(current_group);
                current_group = vec![inner_vec];
                current_group_token_count = inner_vec_token_count;
            }
        }

        // Add the last group if it's not empty
        if !current_group.is_empty() {
            result.push(current_group);
        }

        result
    }

    async fn chunk_inputs(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<Vec<String>, anyhow::Error> {
        if self.deployment_type == OpenAiDeployment::Custom {
            let json_string = serde_json::to_string(inputs).unwrap();
            let batch_input = vec![format!(
                r#"
                 {{
                   "input": {json_string},
                   "model": "{model_name}"
                 }}
                "#
            )];
            return Ok(batch_input);
        };

        let model_map = MODEL_INFO_MAP.read().await;
        let model_info = check_and_get_model!(model_map, model_name);
        let token_groups: Vec<Vec<u32>> = inputs
            .iter()
            .map(|input| {
                let mut tokens = model_info.tokenizer.encode_with_special_tokens(input);
                if tokens.len() > model_info.sequence_len {
                    tokens.truncate(model_info.sequence_len);
                }
                tokens
            })
            .collect();

        // Dimensions for new openai models can be specified
        let dimensions_input = if model_info.var_dimension && self.dimensions.is_some() {
            format!(r#", "dimensions": {}"#, self.dimensions.as_ref().unwrap())
        } else {
            "".to_owned()
        };

        let name = &model_info.name;
        let batch_tokens: Vec<String> = self
            .group_vectors_by_token_count(token_groups, model_info.sequence_len)
            .iter()
            .map(|token_group| {
                let json_string = serde_json::to_string(token_group).unwrap();
                format!(
                    r#"
                 {{
                   "input": {json_string},
                   "model": "{name}"
                   {dimensions_input}
                 }}
                "#
                )
            })
            .collect();

        Ok(batch_tokens)
    }

    pub async fn completion(
        &self,
        model_name: &str,
        query: &str,
        retries: Option<usize>,
    ) -> Result<CompletionResult, anyhow::Error> {
        let client = Arc::new(self.get_client()?);
        let url = Url::parse(&self.base_url)?
            .join("/v1/chat/completions")?
            .to_string();
        let completion_response: CompletionResult = post_with_retries(
            client,
            url,
            serde_json::to_string(&json!({
            "model": model_name,
            "messages": [
              self.system_prompt,
              { "role": "user", "content": query }
            ]
            }))?,
            Box::new(Self::get_completion_response),
            retries.unwrap_or(5),
        )
        .await?;

        Ok(completion_response)
    }

    pub async fn batch_completion(
        self: Arc<&Self>,
        model_name: &str,
        queries: &Vec<&str>,
    ) -> Result<BatchCompletionResult, anyhow::Error> {
        if self.deployment_type != OpenAiDeployment::Custom {
            let model_map = COMPLETION_MODEL_INFO_MAP.read().await;
            check_and_get_model!(model_map, model_name);
        }

        let mut processed_tokens = 0;

        let completion_futures = queries.into_iter().map(|query| {
            let self_clone = Arc::clone(&self);
            let model_name_clone = model_name.to_owned();
            async move {
                self_clone
                    .completion(&model_name_clone, &query, Some(5))
                    .await
            }
        });

        let results = futures::future::join_all(completion_futures).await;

        let mut responses = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(msg) => {
                    processed_tokens += msg.processed_tokens;
                    responses.push(msg.message);
                }
                Err(e) => responses.push(format!("Error: {e}")),
            }
        }

        Ok(BatchCompletionResult {
            messages: responses,
            processed_tokens,
        })
    }

    // Static functions
    pub fn get_response(body: Vec<u8>) -> Result<EmbeddingResult, anyhow::Error> {
        let result: Result<OpenAiResponse, serde_json::Error> = serde_json::from_slice(&body);
        if let Err(e) = result {
            anyhow::bail!(
                "Error: {e}. OpenAI response: {:?}",
                serde_json::from_slice::<serde_json::Value>(&body)?
            );
        }

        let result = result.unwrap();

        Ok(EmbeddingResult {
            processed_tokens: result.usage.total_tokens,
            embeddings: result
                .data
                .iter()
                .map(|emb| emb.embedding.clone())
                .collect(),
        })
    }

    pub fn get_completion_response(body: Vec<u8>) -> Result<CompletionResult, anyhow::Error> {
        let result: Result<OpenAiChatResponse, serde_json::Error> = serde_json::from_slice(&body);
        if let Err(e) = result {
            anyhow::bail!(
                "Error: {e}. OpenAI response: {:?}",
                serde_json::from_slice::<serde_json::Value>(&body)?
            );
        }

        let result = result.unwrap();

        Ok(CompletionResult {
            processed_tokens: result.usage.total_tokens,
            message: result
                .choices
                .first()
                .unwrap_or(&OpenAiChatChoice {
                    message: OpenAiChatMessage::new(),
                })
                .message
                .content
                .clone(),
        })
    }
}

impl<'a> EmbeddingRuntimeT for OpenAiRuntime<'a> {
    async fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error> {
        self.post_request("/v1/embeddings", model_name, inputs)
            .await
    }

    async fn get_available_models(
        &self,
        job_type: EmbeddingJobType,
    ) -> (String, Vec<(String, bool)>) {
        let map = match job_type {
            EmbeddingJobType::EmbeddingGeneration => MODEL_INFO_MAP.read().await,
            EmbeddingJobType::Completion => COMPLETION_MODEL_INFO_MAP.read().await,
        };

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

HTTPRuntime!(OpenAiRuntime);

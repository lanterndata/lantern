pub mod cohere_runtime;
pub mod http_runtime;
pub mod openai_runtime;
pub mod ort_runtime;
pub mod runtime;
pub mod utils;

use std::str::FromStr;
use strum::{EnumIter, IntoEnumIterator};

use cohere_runtime::CohereRuntime;
use openai_runtime::OpenAiRuntime;
use ort_runtime::OrtRuntime;
use runtime::EmbeddingRuntimeT;

use self::runtime::EmbeddingResult;

fn default_logger(text: &str) {
    println!("{}", text);
}

#[derive(Debug, PartialEq, Clone, EnumIter)]
pub enum Runtime {
    Ort,
    OpenAi,
    Cohere,
}

pub type LoggerFn = fn(&str);
impl FromStr for Runtime {
    type Err = anyhow::Error;
    fn from_str(input: &str) -> Result<Runtime, anyhow::Error> {
        match input {
            "ort" => Ok(Runtime::Ort),
            "openai" => Ok(Runtime::OpenAi),
            "cohere" => Ok(Runtime::Cohere),
            _ => anyhow::bail!("Invalid runtime {input}"),
        }
    }
}

impl TryFrom<&str> for Runtime {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl ToString for Runtime {
    fn to_string(&self) -> String {
        match self {
            Runtime::Ort => "ort".to_owned(),
            Runtime::OpenAi => "openai".to_owned(),
            Runtime::Cohere => "cohere".to_owned(),
        }
    }
}

pub enum EmbeddingRuntime<'a> {
    Cohere(cohere_runtime::CohereRuntime<'a>),
    OpenAi(openai_runtime::OpenAiRuntime<'a>),
    Ort(ort_runtime::OrtRuntime<'a>),
}

impl<'a> EmbeddingRuntime<'a> {
    pub fn new(
        runtime: &Runtime,
        logger: Option<&'a LoggerFn>,
        params: &'a str,
    ) -> Result<EmbeddingRuntime<'a>, anyhow::Error> {
        Ok(match runtime {
            Runtime::Ort => EmbeddingRuntime::Ort(OrtRuntime::new(
                logger.unwrap_or(&(default_logger as LoggerFn)),
                params,
            )?),
            Runtime::OpenAi => EmbeddingRuntime::OpenAi(OpenAiRuntime::new(
                logger.unwrap_or(&(default_logger as LoggerFn)),
                params,
            )?),
            Runtime::Cohere => EmbeddingRuntime::Cohere(CohereRuntime::new(
                logger.unwrap_or(&(default_logger as LoggerFn)),
                params,
            )?),
        })
    }

    pub async fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error> {
        match self {
            EmbeddingRuntime::Cohere(runtime) => runtime.process(model_name, inputs).await,
            EmbeddingRuntime::OpenAi(runtime) => runtime.process(model_name, inputs).await,
            EmbeddingRuntime::Ort(runtime) => runtime.process(model_name, inputs).await,
        }
    }

    pub async fn get_available_models(&self) -> (String, Vec<(String, bool)>) {
        match self {
            EmbeddingRuntime::Cohere(runtime) => runtime.get_available_models().await,
            EmbeddingRuntime::OpenAi(runtime) => runtime.get_available_models().await,
            EmbeddingRuntime::Ort(runtime) => runtime.get_available_models().await,
        }
    }
}

pub fn get_available_runtimes() -> Vec<String> {
    Runtime::iter().map(|e| e.to_string()).collect()
}

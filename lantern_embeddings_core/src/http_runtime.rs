use crate::runtime::EmbeddingResult;
use isahc::HttpClient;

pub trait IHTTPRuntime {
    fn get_client(&self) -> Result<HttpClient, anyhow::Error>;
    fn post_request(
        &self,
        endpoint: &str,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error>;
}

#[macro_export]
macro_rules! HTTPRuntime {
    ($a:ident) => {
        use crate::http_runtime::IHTTPRuntime;
        use crate::utils::post_with_retries;
        use core::time::Duration;
        use isahc::{config::RedirectPolicy, prelude::*, HttpClient};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::runtime::Runtime;
        use url::Url;

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
            ) -> Result<crate::runtime::EmbeddingResult, anyhow::Error> {
                let tokio_runtime = Runtime::new()?;
                let client = Arc::new(self.get_client()?);
                let mut tasks = Vec::new();
                let url = Url::parse(&self.base_url)?.join(endpoint)?.to_string();

                for request_body in self.chunk_inputs(model_name, inputs)? {
                    let client = client.clone();
                    let url = url.clone();
                    let task = tokio_runtime.spawn(async move {
                        let embedding_response = post_with_retries(
                            client,
                            url,
                            request_body,
                            Box::new($a::get_response),
                            5,
                        )
                        .await?;
                        Ok::<EmbeddingResult, anyhow::Error>(embedding_response)
                    });
                    tasks.push(task);
                }

                let processed_tokens = Arc::new(AtomicUsize::new(0));
                let processed_tokens_clone = processed_tokens.clone();
                let responses = tokio_runtime.block_on(async move {
                    let mut responses = Vec::with_capacity(inputs.len());
                    for task in tasks {
                        let embedding_response = task.await??;
                        processed_tokens_clone
                            .fetch_add(embedding_response.processed_tokens, Ordering::SeqCst);
                        responses.extend(embedding_response.embeddings);
                    }
                    Ok::<Vec<Vec<f32>>, anyhow::Error>(responses)
                })?;

                let processed_tokens = processed_tokens.load(Ordering::SeqCst);
                Ok(crate::runtime::EmbeddingResult {
                    processed_tokens,
                    embeddings: responses,
                })
            }
        }
    };
}

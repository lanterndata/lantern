use super::runtime::EmbeddingResult;

pub trait IHTTPRuntime {
    fn get_client(&self) -> Result<reqwest::Client, anyhow::Error>;
    fn post_request(
        &self,
        endpoint: &str,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> impl std::future::Future<Output = Result<EmbeddingResult, anyhow::Error>> + Send;
}

#[macro_export]
macro_rules! HTTPRuntime {
    ($a:ident) => {
        use super::http_runtime::IHTTPRuntime;
        use super::utils::post_with_retries;
        use core::time::Duration;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use url::Url;

        impl<'a> IHTTPRuntime for $a<'a> {
            fn get_client(&self) -> Result<reqwest::Client, anyhow::Error> {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(self.request_timeout))
                    .redirect(reqwest::redirect::Policy::limited(2));

                let mut headers = reqwest::header::HeaderMap::new();
                for header in &self.headers {
                    headers.insert(
                        reqwest::header::HeaderName::from_bytes(header.0.as_bytes())?,
                        reqwest::header::HeaderValue::from_str(&header.1)?,
                    );
                }

                Ok(client.default_headers(headers).build()?)
            }

            async fn post_request(
                &self,
                endpoint: &str,
                model_name: &str,
                inputs: &Vec<&str>,
            ) -> Result<super::runtime::EmbeddingResult, anyhow::Error> {
                let client = Arc::new(self.get_client()?);
                let url = Url::parse(&self.base_url)?.join(endpoint)?.to_string();

                let mut responses = Vec::with_capacity(inputs.len());
                let processed_tokens = Arc::new(AtomicUsize::new(0));
                let processed_tokens_clone = processed_tokens.clone();

                for request_body in self.chunk_inputs(model_name, inputs)? {
                    let client = client.clone();
                    let url = url.clone();
                    let embedding_response: super::runtime::EmbeddingResult =
                        post_with_retries(client, url, request_body, Box::new($a::get_response), 5)
                            .await?;
                    processed_tokens_clone
                        .fetch_add(embedding_response.processed_tokens, Ordering::SeqCst);
                    responses.extend(embedding_response.embeddings);
                }

                let processed_tokens = processed_tokens.load(Ordering::SeqCst);
                Ok(super::runtime::EmbeddingResult {
                    processed_tokens,
                    embeddings: responses,
                })
            }
        }
    };
}

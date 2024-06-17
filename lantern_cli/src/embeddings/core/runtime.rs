pub struct EmbeddingResult {
    pub embeddings: Vec<Vec<f32>>,
    pub processed_tokens: usize,
}

pub trait EmbeddingRuntimeT {
    fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> impl std::future::Future<Output = Result<EmbeddingResult, anyhow::Error>> + Send;
    fn get_available_models(
        &self,
    ) -> impl std::future::Future<Output = (String, Vec<(String, bool)>)> + Send;
}

pub struct EmbeddingResult {
    pub embeddings: Vec<Vec<f32>>,
    pub processed_tokens: usize,
}
pub trait EmbeddingRuntime {
    fn process(
        &self,
        model_name: &str,
        inputs: &Vec<&str>,
    ) -> Result<EmbeddingResult, anyhow::Error>;
    fn get_available_models(&self) -> (String, Vec<(String, bool)>);
}

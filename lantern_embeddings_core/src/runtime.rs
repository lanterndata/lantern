pub trait EmbeddingRuntime {
    fn process(&self, model_name: &str, inputs: &Vec<&str>)
        -> Result<Vec<Vec<f32>>, anyhow::Error>;
    fn get_available_models(&self) -> (String, Vec<(String, bool)>);
}

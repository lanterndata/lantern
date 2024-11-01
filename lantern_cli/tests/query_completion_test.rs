use lantern_cli::embeddings::core::{EmbeddingRuntime, Runtime};
use std::env;

static LLM_SYSTEM_PROMPT: &'static str = "You will be provided JSON with the following schema: {x: string}, answer to the message returning the x propery from the provided JSON object";

macro_rules! query_completion_test {
    ($($name:ident: $value:expr,)*) => {
    $(
        #[tokio::test]
        async fn $name() {
            let (runtime_name, model, query, expected, token_count) = $value;
            let api_token = match runtime_name {
                Runtime::OpenAi => env::var("OPENAI_TOKEN").unwrap_or("".to_owned()),
                _ => "".to_owned()
            };


            if api_token == "" {
                return;
            }

            let params = format!(r#"{{"api_token": "{api_token}", "system_prompt": "{LLM_SYSTEM_PROMPT}"}}"#);

            let runtime = EmbeddingRuntime::new(&runtime_name, None, &params).unwrap();
            let output = runtime.completion(
                model,
                query,
            ).await.unwrap();

            assert_eq!(output.message, expected);
            assert_eq!(output.processed_tokens, token_count);
        }
    )*
    }
}

macro_rules! query_completion_test_multiple {
    ($($name:ident: $value:expr,)*) => {
    $(
        #[tokio::test]
        async fn $name() {
            let (runtime_name, model, query1, query2, expected1, expected2, batch_size, token_count) = $value;
            let api_token = match runtime_name {
                Runtime::OpenAi => env::var("OPENAI_TOKEN").unwrap_or("".to_owned()),
                _ => "".to_owned()
            };

            if api_token == "" {
                return;
            }

            let mut inputs = Vec::with_capacity(batch_size);
            let mut expected_output = Vec::with_capacity(batch_size);

            for i in 0..batch_size {
                let (input, output) = if i % 2 == 0 {
                    (query1, expected1.clone())
                } else {
                    (query2, expected2.clone())
                };
                inputs.push(input);
                expected_output.push(output);
            }

            let params = format!(r#"{{"api_token": "{api_token}", "system_prompt": "{LLM_SYSTEM_PROMPT}"}}"#);

            let runtime = EmbeddingRuntime::new(&runtime_name, None, &params).unwrap();
            let output = runtime.batch_completion(
                model,
                &inputs,
            ).await.unwrap();

            let messages = output.messages;

            for i in 0..expected_output.len() {
                assert_eq!(messages[i], expected_output[i]);
            }
            assert_eq!(output.processed_tokens, token_count);
        }
    )*
    }
}

query_completion_test! {
  query_completion_openai: (Runtime::OpenAi, "gpt-4o-mini", r#"{"x": "working!"}"#, "working!", 49),
}

query_completion_test_multiple! {
  query_completion_openai_batch: (Runtime::OpenAi, "gpt-4o-mini", r#"{"x": "working!"}"#, r#"{"x": "working2!"}"#, "working!", "working2!", 2, 100),
}

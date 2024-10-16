// the suffix .stop is enforced by the PostgreSQL and is used to differentiate between the stopword frile
// from other optional full text search support files in the same directory
const USER_DEFINED_STOPWORDS_FILE: &str = "userdefined.stop";

use pgrx::prelude::*;
use std::fs::File;
use std::io::prelude::*;

extension_sql_file!(
    "./stemmer_api.sql",
    name = "text_to_stem_array_tsvector",
    // creates functions on lantern_extras schema
    requires = [lantern_extras]
);

fn stopwords_file_path() -> String {
    let base_dir = Spi::get_one::<String>("SELECT setting FROM pg_config WHERE name = 'SHAREDIR';")
        .expect("Failed to get SHAREDIR configuration paramter")
        .expect("SHAREDIR configuration parameter is NULL");

    format!("{}/tsearch_data/{}", base_dir, USER_DEFINED_STOPWORDS_FILE)
}

#[pg_extern]
fn set_user_stopwords(arr: Option<Vec<String>>) -> String {
    let file_path = stopwords_file_path();
    match File::create(&file_path) {
        Ok(mut file) => {
            for s in arr.unwrap_or(vec![]) {
                writeln!(file, "{}", s).unwrap();
            }
            file_path
        }
        Err(e) => match e.kind() {
            std::io::ErrorKind::PermissionDenied => {
                error!(
                    "Permission denied to create file: {} for writing custom stopwords",
                    file_path
                )
            }
            _ => {
                error!(
                    "Unknown error {} when creating file: {} for writing custom stopwords",
                    e.to_string(),
                    file_path
                )
            }
        },
    }
}

#[pg_extern]
fn get_user_stopwords() -> SetOfIterator<'static, String> {
    let file = match File::open(stopwords_file_path()) {
        Ok(file) => file,
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
            return SetOfIterator::new(std::iter::empty())
        }
        Err(e) => error!("Error opening file: {:?}", e),
    };
    let reader = std::io::BufReader::new(file);
    SetOfIterator::new(reader.lines().map(|line| line.unwrap()))
}

/*
 * plrust version in the comment:
    CREATE OR REPLACE FUNCTION text_to_stem_array_rust(input TEXT)
    RETURNS TEXT[]
    STRICT IMMUTABLE PARALLEL SAFE
    LANGUAGE plrust AS $$
    [dependencies]
        rust-stemmers = "1.2.0"
    [code]
        //  {git = "https://github.com/Ngalstyan4/rust-stemmers.git", branch = "narek/drop-unused-dependency"}
        use pgrx::prelude::*;
        use rust_stemmers::{Algorithm, Stemmer};
        let en_stemmer = Stemmer::create(Algorithm::English);
        let stop_words = [
                        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in",
                        "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the",
                        "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
                    ];

        let v = input.to_lowercase()
            .chars().map(|c| if !c.is_alphanumeric() { ' ' } else { c } ).collect::<String>()
            .split_whitespace()
            .map(|word| en_stemmer.stem(&word))
            .filter(|word| !stop_words.contains(&word.as_ref()))
            .map(|word| Some(word.to_string()))
            .collect::<Vec<Option<String>>>();
        Ok(Some(v))
    $$;
*/

#[pg_extern]
fn text_to_stem_array_rust(input: &str) -> Option<Vec<Option<String>>> {
    let en_stemmer = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);

    let stop_words = [
        "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
        "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
        "these", "they", "this", "to", "was", "will", "with",
    ];

    let v = input
        .to_lowercase()
        .chars()
        .map(|c| if !c.is_alphanumeric() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .map(|word| en_stemmer.stem(&word))
        .filter(|word| !stop_words.contains(&word.as_ref()))
        .map(|word| Some(word.to_string()))
        .collect::<Vec<Option<String>>>();
    Some(v)
}

// Unified itnerface for tokenization via rust or tsvector engines
extension_sql!(
    r#"
CREATE OR REPLACE FUNCTION text_to_stem_array(input TEXT, engine TEXT DEFAULT 'rust', tsvector_strategy REGCONFIG DEFAULT 'english')
RETURNS TEXT[]
STRICT IMMUTABLE PARALLEL SAFE
LANGUAGE plpgsql
AS $$
DECLARE
    result TEXT[];
BEGIN
    IF engine <> 'rust' AND engine <> 'tsvector' THEN
        RAISE EXCEPTION 'Invalid engine: %', engine;
    END IF;

    SELECT
        CASE
            WHEN engine = 'rust' THEN text_to_stem_array_rust(input)
            WHEN engine = 'tsvector' THEN text_to_stem_array_tsvector(input, tsvector_strategy)
            ELSE NULL
        END
    INTO result;
    RETURN result;
END
$$;
"#,
    name = "tokenizer_api",
    requires = ["text_to_stem_array_tsvector"],
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
pub mod tests {
    use crate::*;

    #[pg_test]
    fn test_text_to_stem_array() {
        let input = "The` quick? brown-fox@ jumps  over the (lazy) [(!!@)] dog.";
        let expected = vec![
            Some("quick".to_string()),
            Some("brown".to_string()),
            Some("fox".to_string()),
            Some("jump".to_string()),
            Some("over".to_string()),
            Some("lazi".to_string()),
            Some("dog".to_string()),
        ];
        assert_eq!(stemmers::text_to_stem_array_rust(input), Some(expected));
    }

    // Note: set_user_stopwords modifies a file on the file system, so it does not respect the
    // usual transaction boundaries in postgres. This causes issues if we use the API in more than
    // one pg_test, since pgrx runs all queries in parallel(source: https://github.com/pgcentralfoundation/pgrx/blob/develop/cargo-pgrx/README.md)
    // so, we need to ensure that the tests that use set_user_stopwords run in a single test
    // function sequentially.
    #[pg_test]
    fn custom_stem_stopwords() -> spi::Result<()> {
        Spi::run("SELECT set_user_stopwords(ARRAY['how'])").unwrap();
        let r = Spi::get_one::<String>("SELECT * FROM get_user_stopwords() LIMIT 1").unwrap();
        assert_eq!(r, Some("how".to_string()));
        Spi::run("SELECT set_user_stopwords(ARRAY[]::TEXT[])").unwrap();
        let r = Spi::get_one::<i64>("SELECT count(*) FROM get_user_stopwords()").unwrap();
        assert!(r.unwrap() == 0);

        Spi::run("SELECT set_user_stopwords(ARRAY['what', 'where', 'when', 'how'])").unwrap();
        let mut v =
            Spi::get_one::<Vec<String>>("SELECT array_agg(words) FROM get_user_stopwords() words")
                .unwrap()
                .unwrap();
        v.sort();
        assert_eq!(v, vec!["how", "what", "when", "where"]);

        // check that the function is not STRICT, and will remove all stopwords when called with
        // NULL
        Spi::run("SELECT set_user_stopwords(NULL)").unwrap();
        let r = Spi::get_one::<i64>("SELECT count(*) FROM get_user_stopwords()")
            .unwrap()
            .unwrap();
        assert!(r == 0);

        // let's setup a custom dictionary
        Spi::run(
            r#"
CREATE TEXT SEARCH DICTIONARY custom_english_stem (
    TEMPLATE = snowball,
    Language = 'english',
    StopWords = 'userdefined' -- the name of lantern's custom stopwords file
);

CREATE TEXT SEARCH CONFIGURATION custom_english ( COPY = english );

ALTER TEXT SEARCH CONFIGURATION custom_english
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart,
                       word, hword, hword_part
    WITH custom_english_stem;
        "#,
        )
        .unwrap();

        Spi::run("SELECT set_user_stopwords(ARRAY['the', 'over'])").unwrap();
        let input = "The quick brown fox jumps over the lazy dog.";
        let expected_simple = vec![
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
        ];
        let expected = vec!["quick", "brown", "fox", "jump", "lazi", "dog"];
        let result = Spi::get_one_with_args::<Vec<&str>>(
            "SELECT text_to_stem_array($1, 'tsvector')",
            vec![(PgBuiltInOids::TEXTOID.oid(), input.into_datum())],
        )
        .unwrap();
        assert_eq!(result.unwrap(), expected);

        // ensure that one can modify the stopwords after creating the configuration
        Spi::run("SELECT set_user_stopwords(ARRAY['the', 'over', 'quick'])").unwrap();
        let mut expected = expected.clone();
        expected.remove(0);
        let result = Spi::get_one_with_args::<Vec<&str>>(
            "SELECT text_to_stem_array($1, 'tsvector', tsvector_strategy => 'custom_english')",
            vec![(PgBuiltInOids::TEXTOID.oid(), input.into_datum())],
        )
        .unwrap()
        .unwrap();

        assert_eq!(result, expected);

        let result = Spi::get_one_with_args::<Vec<&str>>(
            "SELECT text_to_stem_array($1, 'tsvector', tsvector_strategy => 'simple')",
            vec![(PgBuiltInOids::TEXTOID.oid(), input.into_datum())],
        )
        .unwrap()
        .unwrap();

        assert_eq!(result, expected_simple);

        Ok(())
    }

    #[pg_test]
    fn test_text_to_stem_array_rust_engine() {
        let input = "The quick brown fox jumps over the lazy dog.";
        let expected = vec!["quick", "brown", "fox", "jump", "over", "lazi", "dog"];
        let result = Spi::get_one_with_args::<Vec<&str>>(
            "SELECT text_to_stem_array($1, 'rust')",
            vec![(PgBuiltInOids::TEXTOID.oid(), input.into_datum())],
        )
        .unwrap();
        assert_eq!(result, Some(expected));
    }

    #[pg_test]
    fn test_text_to_stem_array_tsvector_engine() {
        let input = "The quick brown fox jumps over the lazy dog.";
        let expected = vec!["quick", "brown", "fox", "jump", "lazi", "dog"];
        let result = Spi::get_one_with_args::<Vec<&str>>(
            "SELECT text_to_stem_array($1, 'tsvector')",
            vec![(PgBuiltInOids::TEXTOID.oid(), input.into_datum())],
        )
        .unwrap();
        assert_eq!(result, Some(expected));
    }
}

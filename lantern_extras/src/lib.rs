use core::ffi::CStr;
use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};

pgrx::pg_module_magic!();
pub mod dotvecs;
pub mod embeddings;
pub mod external_index;

pub static OPENAI_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_AZURE_API_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_AZURE_ENTRA_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static COHERE_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    GucRegistry::define_string_guc(
        "lantern_extras.openai_token",
        "OpenAI API token.",
        "Used when generating embeddings with OpenAI models",
        &OPENAI_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.openai_azure_api_token",
        "Azure API token.",
        "Used when generating embeddings with Azure OpenAI models",
        &OPENAI_AZURE_API_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.openai_azure_entra_token",
        "Azure Entra token.",
        "Used when generating embeddings with Azure OpenAI models",
        &OPENAI_AZURE_API_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.cohere_token",
        "Cohere API token.",
        "Used when generating embeddings with Cohere models",
        &COHERE_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
}
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}

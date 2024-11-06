use core::ffi::CStr;
use pgrx::{bgworkers::*, prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};
use std::time::Duration;

pgrx::pg_module_magic!();
pub mod bloom;
pub mod bm25_agg;
pub mod bm25_api;
pub mod daemon;
pub mod dotvecs;
pub mod embeddings;
pub mod external_index;
pub mod stemmers;

// this will be deprecated and removed on upcoming releases
pub static OPENAI_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static LLM_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static LLM_DEPLOYMENT_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_AZURE_ENTRA_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static ENABLE_DAEMON: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static DAEMON_DATABASES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static BM25_DEFAULT_K1: GucSetting<f64> = GucSetting::<f64>::new(1.2);
pub static BM25_DEFAULT_B: GucSetting<f64> = GucSetting::<f64>::new(0.75);
pub static BM25_DEFAULT_APPROXIMATION_THRESHHOLD: GucSetting<i32> = GucSetting::<i32>::new(8000);

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Lantern Daemon")
        .set_function("lantern_daemon_worker")
        .set_library("lantern_extras")
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    unsafe {
        use pg_sys::AsPgCStr;
        pg_sys::MarkGUCPrefixReserved("lantern_extras".as_pg_cstr());
    }

    GucRegistry::define_string_guc(
        "lantern_extras.llm_token",
        "LLM API token.",
        "Used when generating embeddings with OpenAI/Cohere/Llama models",
        &LLM_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.openai_token",
        "OpenAI API token.",
        "Used when generating embeddings with OpenAI models",
        &OPENAI_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.llm_deployment_url",
        "OpenAI Deployment URL.",
        "This can be set to any OpenAI compatible API url",
        &LLM_DEPLOYMENT_URL,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.openai_azure_entra_token",
        "Azure Entra token.",
        "Used when generating embeddings with Azure OpenAI models",
        &OPENAI_AZURE_ENTRA_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.daemon_databases",
        "Databases to watch",
        "Comma separated list of database names to which daemon will be connected",
        &DAEMON_DATABASES,
        GucContext::Sighup,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_bool_guc(
        "lantern_extras.enable_daemon",
        "Enable Lantern Daemon",
        "Flag to indicate if daemon is enabled or not",
        &ENABLE_DAEMON,
        GucContext::Sighup,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_float_guc(
        "lantern_extras.bm25_default_k1",
        "BM25 default k1",
        "BM25 default k1",
        &BM25_DEFAULT_K1,
        1.0,
        3.0,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_float_guc(
        "lantern_extras.bm25_default_b",
        "BM25 default b",
        "BM25 default b",
        &BM25_DEFAULT_B,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_int_guc(
        "lantern_extras.bm25_default_approximation_threshhold",
        "Term popularity threashold, after which approximation is used",
        "Term popularity threashold, after which approximation is used",
        &BM25_DEFAULT_APPROXIMATION_THRESHHOLD,
        5000,
        100_000,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn lantern_daemon_worker() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        if ENABLE_DAEMON.get() {
            daemon::start_daemon(true, false, false).unwrap();
        }
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries='lantern_extras'",
            "lantern_extras.daemon_databases='pgrx_tests'",
            "lantern_extras.enable_daemon=true",
        ]
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
pub mod tests {
    use crate::*;

    // note: this will not get to unwrap, since the failure in the result only represents
    // failures in the SPI machinery.
    // Postgres aborts the transaction and returns an error message to the client when the SPI
    // query fails. So, the rust interface as no Error representation for a failed query.
    // As a last resort we can ensure the test panics with the expected message.
    // https://www.postgresql.org/docs/current/spi.html
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    #[pg_test]
    #[should_panic(expected = "invalid configuration parameter name")]
    fn lantern_extras_prefix_reserved() {
        Spi::run("SET lantern_extras.aldkjalsdkj_invalid_param = 42").unwrap();

        error!("Managed to update a reserved-prefix variable. This should never be reached");
    }
}

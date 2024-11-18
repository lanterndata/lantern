use core::ffi::CStr;
use lantern_cli::{
    external_index::{
        cli::IndexServerArgs,
        server::{start_indexing_server, ServerContext},
    },
    logger::{LogLevel, Logger},
};
use pgrx::{bgworkers::*, pg_sys, prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

pgrx::pg_module_magic!();
pub mod bloom;
pub mod bm25_agg;
pub mod bm25_api;
pub mod daemon;
pub mod dotvecs;
pub mod embeddings;
pub mod stemmers;

// ensure lantern_extras schema is created
#[pg_schema]
mod lantern_extras {}

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
pub static ENABLE_INDEXING_SERVER: GucSetting<bool> = GucSetting::<bool>::new(true);

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

    BackgroundWorkerBuilder::new("Lantern Indexing Server")
        .set_function("lantern_indexing_worker")
        .set_library("lantern_extras")
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();

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
    GucRegistry::define_bool_guc(
        "lantern_extras.enable_indexing_server",
        "Enable Lantern Indexing Server",
        "Flag to indicate if local indexing server is enabled or not",
        &ENABLE_INDEXING_SERVER,
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
    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    unsafe {
        use pg_sys::AsPgCStr;
        pg_sys::MarkGUCPrefixReserved("lantern_extras".as_pg_cstr());
    }
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

#[pg_guard]
#[no_mangle]
pub extern "C" fn lantern_indexing_worker() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let data_dir = BackgroundWorker::transaction(|| {
        Spi::get_one::<&str>("SELECT setting::text FROM pg_settings WHERE name = 'data_directory'")
            .expect("Could not query from SPI")
            .expect("Could not get data data directory")
    });

    let tmp_dir = std::path::Path::new(data_dir)
        .join("ldb_indexes")
        .to_str()
        .expect("Could not concatinate tmp dir")
        .to_owned();

    std::fs::create_dir_all(&tmp_dir)
        .expect(&format!("Could not create tmp directory at {tmp_dir}"));

    let mut started = false;

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        /* Remove this code after the wait_latch bug is fixed in pgrx */
        let wakeup_flags = unsafe {
            let latch = pg_sys::WaitLatch(
                pg_sys::MyLatch,
                (pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as i32,
                0,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(pg_sys::MyLatch);
            latch
        };

        if (wakeup_flags & pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
            break;
        }
        /* ========================================================== */

        if BackgroundWorker::sighup_received() && !ENABLE_INDEXING_SERVER.get() {
            std::process::exit(1);
        }

        if ENABLE_INDEXING_SERVER.get() && !started {
            let tmp_dir = tmp_dir.to_owned();
            std::thread::spawn(move || {
                let context = Arc::new(RwLock::new(ServerContext::new()));
                start_indexing_server(
                    IndexServerArgs {
                        tmp_dir,
                        host: "127.0.0.1".to_owned(),
                        port: 8998,
                        status_port: 8999,
                        cert: None,
                        key: None,
                    },
                    Arc::new(Logger::new("Lantern Indexing Server", LogLevel::Debug)),
                    context,
                )
            });
            started = true;
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

    // note: this will not get to unwrap, since the failure in the result only represents
    // failures in the SPI machinery.
    // Postgres aborts the transaction and returns an error message to the client when the SPI
    // query fails. So, the rust interface has no Error representation for a failed query.
    // As a last resort we can ensure the test panics with the expected message.
    // https://www.postgresql.org/docs/current/spi.html

    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    #[pgrx::pg_test]
    #[should_panic(expected = "invalid configuration parameter name")]
    fn lantern_extras_prefix_reserved() {
        use pgrx::{error, Spi};
        Spi::run("SET lantern_extras.aldkjalsdkj_invalid_param = 42").unwrap();
        error!("Managed to update a reserved-prefix variable. This should never be reached");
    }
}

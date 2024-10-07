use core::ffi::CStr;
use pgrx::{bgworkers::*, prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pgrx::pg_module_magic!();
pub mod daemon;
pub mod dotvecs;
pub mod embeddings;
pub mod external_index;

pub static OPENAI_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_DEPLOYMENT_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_AZURE_API_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static OPENAI_AZURE_ENTRA_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static COHERE_TOKEN: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static ENABLE_DAEMON: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static DAEMON_DATABASES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Lantern Daemon")
        .set_function("lantern_daemon_worker")
        .set_library("lantern_extras")
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();

    GucRegistry::define_string_guc(
        "lantern_extras.openai_token",
        "OpenAI API token.",
        "Used when generating embeddings with OpenAI models",
        &OPENAI_TOKEN,
        GucContext::Userset,
        GucFlags::NO_SHOW_ALL,
    );
    GucRegistry::define_string_guc(
        "lantern_extras.openai_deployment_url",
        "OpenAI Deployment URL.",
        "Currently only Azure deployments of API version 2023-05-15 are supported",
        &OPENAI_DEPLOYMENT_URL,
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
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn lantern_daemon_worker() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    let mut cancellation_token = CancellationToken::new();
    let mut started = false;

    if ENABLE_DAEMON.get() {
        daemon::start_daemon(true, false, false, cancellation_token.clone()).unwrap();
        started = true;
    }

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        if BackgroundWorker::sighup_received() {
            if ENABLE_DAEMON.get() && !started {
                cancellation_token = CancellationToken::new();
                daemon::start_daemon(true, false, false, cancellation_token.clone()).unwrap();
                started = true;
            } else if !ENABLE_DAEMON.get() && started {
                cancellation_token.cancel();
                started = false;
            }
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
            "shared_preload_libraries='lantern_extras.so'",
            "lantern_extras.daemon_databases='pgrx_tests'",
            "lantern_extras.enable_daemon=true",
        ]
    }
}

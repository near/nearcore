use actix::{Actor, Arbiter, Context};
use near_o11y::reload_env_filter;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::runtime::Handle;

/// Runs LogConfigWatcher in a separate thread.
pub(crate) fn spawn_log_config_watcher(watched_path: PathBuf) {
    let log_config_arbiter = Arbiter::new();
    let log_config_arbiter_handle = log_config_arbiter.handle();
    LogConfigActor::start_in_arbiter(&log_config_arbiter_handle, move |_ctx| LogConfigActor {
        watched_path,
    });
}

/// Configures logging.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LogConfig {
    /// Comma-separated list of EnvFitler directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
}

/// Helper for running LogConfigWatcher in its own thread.
struct LogConfigActor {
    watched_path: PathBuf,
}

impl Actor for LogConfigActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        use tokio::signal::unix::{signal, SignalKind};
        let mut stream = signal(SignalKind::hangup()).unwrap();
        let handle = Handle::current();
        let _guard = handle.enter();
        futures::executor::block_on(async {
            loop {
                stream.recv().await;
                self.update();
            }
        });
    }
}

impl LogConfigActor {
    fn update(&self) {
        // Log to stdout, because otherwise these messages are about controlling logging.
        // If an issue with controlling logging occurs, and logging is disabled, the user may not be
        // able to enable logging.
        println!("Received SIGHUP, reloading logging config");
        match std::fs::read_to_string(&self.watched_path) {
            Ok(log_config_str) => {
                match serde_json::from_str::<LogConfig>(&log_config_str) {
                    Ok(log_config) => {
                        println!("Changing EnvFilter to {:?}", log_config);
                        if let Err(err) = reload_env_filter(
                            log_config.rust_log.as_deref(),
                            log_config.verbose_module.as_deref(),
                        ) {
                            println!("Failed to reload EnvFilter: {:?}", err);
                        }
                        // If file doesn't exist or isn't parse-able, the tail of this function will
                        // reset the config to `RUST_LOG`.
                        return;
                    }
                    Err(err) => {
                        println!("Ignoring the logging config change because failed to parse logging config file {}: {:?}", self.watched_path.display(), err);
                        return;
                    }
                }
            }
            Err(err) => {
                println!(
                    "Resetting EnvFilter, because failed to read logging config file {}: {:?}",
                    self.watched_path.display(),
                    err
                );
            }
        }
        // Reset EnvFilter to `RUST_LOG`.
        if let Err(err) = reload_env_filter(None, None) {
            println!("Failed to reload EnvFilter: {:?}", err);
        }
    }
}

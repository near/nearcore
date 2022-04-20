use actix::{Actor, Arbiter, Context};
use near_o11y::reload_env_filter;
use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver};
use std::time::Duration;
use tracing::info;

/// Runs LogConfigWatcher in a separate thread.
pub(crate) fn spawn_log_config_watcher(log_config_watcher: LogConfigWatcher) {
    let log_config_arbiter = Arbiter::new();
    let log_config_arbiter_handle = log_config_arbiter.handle();
    LogConfigActor::start_in_arbiter(&log_config_arbiter_handle, move |_ctx| LogConfigActor {
        watcher: log_config_watcher,
    });
}

pub(crate) struct LogConfigWatcher {
    _watcher: RecommendedWatcher,
    pub watched_path: Option<PathBuf>,
    pub rx: Receiver<DebouncedEvent>,
}

impl LogConfigWatcher {
    pub fn new(home_dir: &Path, log_config: Option<PathBuf>) -> Self {
        let (tx, rx) = channel();
        let mut watcher = watcher(tx, Duration::from_secs(60)).unwrap();
        let mut watched_path = None;
        if let Some(ref log_config) = log_config {
            info!(target: "neard", home_dir=home_dir.to_string_lossy().as_ref(), log_config=log_config.to_string_lossy().as_ref(), "Watching log config changes.");
            // Watch a directory changes, because the log config file may not exist,
            // and the events of creating and removing that file need to be handled.
            // `notify` library assumes that the given path exists.
            watcher.watch(home_dir, RecursiveMode::NonRecursive).unwrap();
            watched_path = Some(home_dir.join(log_config));
        } else {
            info!(target: "neard", "Not watching any files for log config changes.");
        }
        Self { _watcher: watcher, watched_path, rx }
    }

    /// Gets called when a write to the log config file is detected.
    /// Replaces env_filter according to the configuration in the log config file.
    fn handle_write_event(&mut self, path: PathBuf) {
        // Only processes events affecting the log config file.
        if Some(&path) != self.watched_path.as_ref() {
            return;
        }
        match std::fs::read_to_string(&path) {
            Ok(log_config_str) => match serde_json::from_str::<LogConfig>(&log_config_str) {
                Ok(log_config) => {
                    println!("Changing env_filter to {:?}", log_config);
                    if let Err(err) = reload_env_filter(
                        &log_config.rust_log,
                        log_config.verbose_module.is_some(),
                        &log_config.verbose_module,
                    ) {
                        println!("Failed to reload env_filter: {:?}", err);
                    }
                }
                Err(err) => {
                    println!("Failed to parse log config file {}: {:?}", path.display(), err);
                }
            },
            Err(err) => {
                println!("Failed to read log config file {}: {:?}", path.display(), err);
            }
        };
    }

    /// Gets called when the log config file gets removed.
    /// Resets env_filter to `RUST_LOG` but ignores `--verbose` command-line flag.
    fn handle_remove_event(&mut self, path: PathBuf) {
        // Only processes events affecting the log config file.
        if Some(&path) != self.watched_path.as_ref() {
            return;
        }
        println!("Resetting env_filter to RUST_LOG env variable.");
        if let Err(err) = reload_env_filter(&None, false, &None) {
            println!("Failed to reload env_filter: {:?}", err);
        }
    }

    /// Handles file modification events.
    pub fn update(&mut self, event: DebouncedEvent) {
        match event {
            DebouncedEvent::NoticeWrite(path) => self.handle_write_event(path),
            DebouncedEvent::NoticeRemove(path) => self.handle_remove_event(path),
            DebouncedEvent::Create(path) => self.handle_write_event(path),
            DebouncedEvent::Write(path) => self.handle_write_event(path),
            DebouncedEvent::Remove(path) => self.handle_remove_event(path),
            // Handle both creation and removal of files caused by renaming them.
            DebouncedEvent::Rename(from, to) => {
                self.handle_remove_event(from);
                self.handle_write_event(to);
            }
            _ => {}
        }
    }
}

/// Configures logging.
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LogConfig {
    /// Comma-separated list of env_filter directives.
    pub rust_log: Option<String>,
    /// Some("") enables global debug logging.
    /// Some("module") enables debug logging for "module".
    pub verbose_module: Option<String>,
}

/// Helper for running LogConfigWatcher in its own thread.
struct LogConfigActor {
    watcher: LogConfigWatcher,
}

impl Actor for LogConfigActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        loop {
            match self.watcher.rx.recv() {
                Ok(event) => {
                    self.watcher.update(event);
                }
                Err(err) => {
                    println!("Error while watching a log config file: {:?}", err);
                }
            }
        }
    }
}

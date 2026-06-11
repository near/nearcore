use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

// Owns signal handling for the whole `mirror run` lifetime, on its own runtime
// so it works both while run() is alive and during the rocksdb close wait after
// run()'s runtime is gone. The first SIGTERM/SIGINT requests a graceful drain
// via shutdown_token(); a second signal, or any signal after mark_run_finished(),
// exits immediately.
pub(crate) struct SignalHandler {
    _runtime: tokio::runtime::Runtime,
    shutdown: CancellationToken,
    run_finished: Arc<AtomicBool>,
}

impl SignalHandler {
    pub(crate) fn install() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let shutdown = CancellationToken::new();
        let run_finished = Arc::new(AtomicBool::new(false));
        #[cfg(unix)]
        runtime.spawn(handle_signals(shutdown.clone(), run_finished.clone()));
        Self { _runtime: runtime, shutdown, run_finished }
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub(crate) fn mark_run_finished(&self) {
        self.run_finished.store(true, Ordering::Relaxed);
    }
}

#[cfg(unix)]
async fn handle_signals(shutdown: CancellationToken, run_finished: Arc<AtomicBool>) {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut sigterm), Ok(mut sigint)) =
        (signal(SignalKind::terminate()), signal(SignalKind::interrupt()))
    else {
        tracing::warn!(target: "mirror", "could not install signal handlers, will exit without draining sent transactions");
        return;
    };
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
    if run_finished.load(Ordering::Relaxed) {
        tracing::warn!(target: "mirror", "got termination signal during shutdown, exiting immediately");
        std::process::exit(1);
    }
    tracing::info!(target: "mirror", "got termination signal, draining sent transactions before exiting");
    shutdown.cancel();
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = sigint.recv() => {}
    }
    tracing::warn!(target: "mirror", "got second termination signal, exiting immediately");
    std::process::exit(1);
}

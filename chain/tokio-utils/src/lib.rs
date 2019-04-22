use futures::future::IntoFuture;
use futures::stream::Stream;
use log::{debug, error};
use std::thread::JoinHandle;
use std::{panic, thread};
use tokio::prelude::Future;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_signal::unix::Signal;

struct InitializedState {
    thread: JoinHandle<()>,
    shutdown_tx: oneshot::Sender<()>,
}

pub struct ShutdownableThread {
    state: Option<InitializedState>,
}

impl ShutdownableThread {
    pub fn start<F>(future: F) -> ShutdownableThread
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mut runtime = Runtime::new().unwrap();
        let thread = thread::spawn(|| {
            runtime.spawn(future);
            let _ignored = runtime.block_on(shutdown_rx.map_err(drop).into_future());
            // TODO HACK: after shutdown all tokio::spawn calls will panic.
            // for now assume all code with tokio::spawn is exception safe
            //            panic::set_hook(Box::new(|_info| {}));
            let _ignored = runtime.shutdown_now().wait();
        });
        let state = Some(InitializedState { thread, shutdown_tx });
        ShutdownableThread { state }
    }

    pub fn shutdown(mut self) {
        if let Some(InitializedState { thread, shutdown_tx }) = self.state.take() {
            shutdown_tx.send(()).map_err(|_| error!("Error sending shutdown signal")).unwrap();
            thread.join().expect("Error joining child thread");
        }
    }

    pub fn join(mut self) {
        if let Some(InitializedState { thread, .. }) = self.state.take() {
            thread.join().expect("Error joining child thread");
        }
    }

    pub fn wait_sigint_and_shutdown(self) {
        tokio::run(
            Signal::new(tokio_signal::unix::SIGINT)
                .flatten_stream()
                .into_future()
                .map(drop)
                .map_err(|_| error!("Error listening to SIGINT")),
        );
        self.shutdown();
    }
}

impl Drop for ShutdownableThread {
    fn drop(&mut self) {
        // Ignore the panic from child thread
        if let Some(InitializedState { thread, shutdown_tx }) = self.state.take() {
            debug!("Trying to kill thread...");
            let _ = shutdown_tx.send(()).map_err(|_| error!("Error sending shutdown signal"));
            let _ = thread.join().map_err(|_| error!("Error joining child thread"));
            debug!("Killed thread");
        }
    }
}

/// Same as tokio::spawn, but catches panics.
/// The panic happens when tokio runtime is shut down.
///
pub fn spawn<F>(f: F)
where
    F: Future<Item = (), Error = ()> + Send + 'static,
{
    panic::set_hook(Box::new(|_info| {}));
    tokio::spawn(f);
    let _ = panic::take_hook();
}

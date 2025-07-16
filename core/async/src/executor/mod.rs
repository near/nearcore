pub mod envelope;
pub mod send;

use crate::executor::envelope::Envelope;
use crate::futures::DelayedActionRunner;
use crate::messaging;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct ExecutorHandle<T: messaging::Actor> {
    sender: mpsc::UnboundedSender<Envelope<T>>,
    cancel: CancellationToken,
}

pub struct ExecutorRuntime {
    _runtime: Arc<tokio::runtime::Runtime>,
}

impl<T: messaging::Actor> Clone for ExecutorHandle<T> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone(), cancel: self.cancel.clone() }
    }
}

struct ExecutorDelayedActionRunner<T: messaging::Actor> {
    sender: mpsc::UnboundedSender<Envelope<T>>,
    runtime_handle: tokio::runtime::Handle,
}

// impl<T: messaging::Actor> Clone for ExecutorDelayedActionRunner<T> {
//     fn clone(&self) -> Self {
//         Self {
//             sender: self.sender.clone(),
//             runtime_handle: self.runtime_handle.clone(),
//         }
//     }
// }

impl<T: messaging::Actor + 'static> DelayedActionRunner<T> for ExecutorDelayedActionRunner<T> {
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: near_time::Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    ) {
        let sender = self.sender.clone();
        let name = name.to_string();
        self.runtime_handle.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let envelope = Envelope::from_fn(name, move |actor, delayed_action_runner| {
                f(actor, delayed_action_runner)
            });
            sender.send(envelope).ok();
        });
    }
}

pub fn start_actor_in_runtime<T: messaging::Actor + Send + 'static>(
    runtime: Arc<tokio::runtime::Runtime>,
    mut logic: T,
) -> ExecutorHandle<T> {
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let mut delayed_action_runner = ExecutorDelayedActionRunner {
        sender: sender.clone(),
        runtime_handle: runtime.handle().clone(),
    };
    {
        let cancel = cancel.clone();
        // Note: With the way we're using the original Actix framework, we don't properly manage the
        // lifetime of the runtime. We just start the actor and forget it. So, have the event loop
        // prevent the dropping of the runtime until we fix all of the usage code.
        let runtime_clone = runtime.clone();
        runtime.spawn(async move {
            let _runtime = runtime_clone;
            logic.start_actor(&mut delayed_action_runner);
            loop {
                tokio::select! {
                    Some(envelope) = receiver.recv() => {
                        tracing::info!("Actor handling message: {}", envelope.describe());
                        envelope.handle_by(&mut logic, &mut delayed_action_runner);
                    },
                    _ = cancel.cancelled() => {
                        break;
                    }
                }
            }
        });
    }
    ExecutorHandle { sender, cancel }
}

pub fn start_actor_with_new_runtime<T: messaging::Actor + Send + 'static>(
    logic: T,
) -> (ExecutorRuntime, ExecutorHandle<T>) {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime"),
    );
    (ExecutorRuntime { _runtime: runtime.clone() }, start_actor_in_runtime(runtime, logic))
}

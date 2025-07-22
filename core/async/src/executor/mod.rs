pub mod envelope;
pub mod send;
pub mod sync;

use crate::executor::envelope::Envelope;
use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct ExecutorHandle<T: messaging::Actor> {
    sender: mpsc::UnboundedSender<Envelope<T>>,
    runtime: Arc<tokio::runtime::Runtime>,
    cancel: CancellationToken,
}

pub struct ExecutorRuntime {
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Deref for ExecutorRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl<T: messaging::Actor> Clone for ExecutorHandle<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            runtime: self.runtime.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl<T: messaging::Actor> FutureSpawner for ExecutorHandle<T> {
    fn spawn_boxed(&self, _description: &'static str, f: futures::future::BoxFuture<'static, ()>) {
        // TODO: spawning in this way makes the future untrackable for debuggability.
        self.runtime.spawn(f);
    }
}

struct ExecutorDelayedActionRunner<T: messaging::Actor> {
    sender: mpsc::UnboundedSender<Envelope<T>>,
    runtime_handle: tokio::runtime::Handle,
}

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

impl ExecutorRuntime {
    pub fn new() -> Self {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("Failed to create Tokio runtime"),
        );
        ExecutorRuntime { runtime }
    }

    pub fn construct_actor_in_runtime<T: messaging::Actor + Send + 'static>(
        &self,
        logic_constructor: impl FnOnce(ExecutorHandle<T>) -> T,
    ) -> ExecutorHandle<T> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let mut delayed_action_runner = ExecutorDelayedActionRunner {
            sender: sender.clone(),
            runtime_handle: self.runtime.handle().clone(),
        };
        let handle =
            ExecutorHandle { sender, runtime: self.runtime.clone(), cancel: cancel.clone() };
        let logic = logic_constructor(handle.clone());
        {
            self.runtime.spawn(async move {
                let mut logic = ActorAutoDrop { actor: logic };
                logic.actor.start_actor(&mut delayed_action_runner);
                loop {
                    tokio::select! {
                        Some(envelope) = receiver.recv() => {
                            tracing::info!("Actor handling message: {}", envelope.describe());
                            envelope.handle_by(&mut logic.actor, &mut delayed_action_runner);
                        },
                        _ = cancel.cancelled() => {
                            break;
                        }
                    }
                }
            });
        }
        handle
    }

    pub fn start_actor_in_runtime<T: messaging::Actor + Send + 'static>(
        &self,
        logic: T,
    ) -> ExecutorHandle<T> {
        self.construct_actor_in_runtime(move |_| logic)
    }

    pub fn stop_only_instance_test_only(self) {
        Arc::into_inner(self.runtime).expect("Not the only instance").shutdown_background();
    }
}

pub(crate) struct ActorAutoDrop<T: messaging::Actor> {
    pub(crate) actor: T,
}

impl<T: messaging::Actor> Drop for ActorAutoDrop<T> {
    fn drop(&mut self) {
        tracing::debug!(target: "actor", "Dropping actor {}", std::any::type_name::<T>());
        self.actor.stop_actor();
    }
}

pub fn start_actor_with_new_runtime<T: messaging::Actor + Send>(
    logic: T,
) -> (ExecutorRuntime, ExecutorHandle<T>) {
    let runtime = ExecutorRuntime::new();
    let handle = runtime.start_actor_in_runtime(logic);
    (runtime, handle)
}

impl<T: messaging::Actor> ExecutorHandle<T> {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl<T: messaging::Actor> DelayedActionRunner<T> for ExecutorHandle<T> {
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: near_time::Duration,
        f: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    ) {
        let mut delayed_action_runner = ExecutorDelayedActionRunner {
            sender: self.sender.clone(),
            runtime_handle: self.runtime.handle().clone(),
        };
        delayed_action_runner.run_later_boxed(name, dur, f);
    }
}

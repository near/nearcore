use std::sync::Arc;

use tokio::sync::mpsc;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::Actor;
use crate::tokio::sender::MySender;

pub struct RuntimeHandle<A> {
    sender: MySender<A>,
    runtime: Arc<tokio::runtime::Runtime>,
    // TODO: Add cancellation logic
}

impl<A> Clone for RuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone(), runtime: self.runtime.clone() }
    }
}

impl<A> RuntimeHandle<A>
where
    A: 'static,
{
    pub fn sender(&self) -> Box<MySender<A>> {
        Box::new(self.sender.clone())
    }

    pub fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }
}

impl<A> FutureSpawner for RuntimeHandle<A> {
    fn spawn_boxed(&self, _description: &'static str, f: futures::future::BoxFuture<'static, ()>) {
        self.runtime.spawn(f);
    }
}

impl<A> DelayedActionRunner<A> for RuntimeHandle<A>
where
    A: 'static,
{
    fn run_later_boxed(
        &mut self,
        _name: &str,
        dur: near_time::Duration,
        f: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send + 'static>,
    ) {
        let sender = self.sender.clone();
        self.runtime.spawn(async move {
            tokio::time::sleep(dur.unsigned_abs()).await;
            let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| f(actor, ctx);
            sender.send(Box::new(function)).ok();
        });
    }
}

pub fn construct_actor_with_tokio_runtime<A>(mut actor: A) -> RuntimeHandle<A>
where
    A: Actor + Send + Sized + 'static,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let (sender, mut receiver) = mpsc::unbounded_channel::<
        Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>,
    >();

    let runtime_handle = RuntimeHandle { sender, runtime: Arc::new(runtime) };

    // Spawn the actor in the runtime
    let runtime = runtime_handle.runtime.clone();
    let mut runtime_handle_clone = runtime_handle.clone();
    runtime.spawn(async move {
        // TODO: Handle cancellation logic
        actor.start_actor(&mut runtime_handle_clone);
        loop {
            tokio::select! {
                Some(function) = receiver.recv() => {
                    function(&mut actor, &mut runtime_handle_clone);
                },
                else => break,
            }
        }
    });

    runtime_handle
}

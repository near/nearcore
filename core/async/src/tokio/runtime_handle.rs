use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::futures::{DelayedActionRunner, FutureSpawner};
use crate::messaging::{Actor, CanSend, CanSendAsync};
use crate::tokio::traits::HandlerWithContext;

pub struct TokioRuntimeHandle<A> {
    sender: mpsc::UnboundedSender<Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send>>,
    runtime: tokio::runtime::Handle,
    cancel: CancellationToken,
}

impl<A> Clone for TokioRuntimeHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            runtime: self.runtime.clone(),
            cancel: self.cancel.clone(),
        }
    }
}

impl<A> TokioRuntimeHandle<A>
where
    A: 'static,
{
    pub fn sender(&self) -> Arc<TokioRuntimeHandle<A>> {
        Arc::new(self.clone())
    }

    pub fn future_spawner(&self) -> Box<dyn FutureSpawner> {
        Box::new(self.clone())
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl<A> FutureSpawner for TokioRuntimeHandle<A> {
    fn spawn_boxed(&self, _description: &'static str, f: futures::future::BoxFuture<'static, ()>) {
        self.runtime.spawn(f);
    }
}

impl<A> DelayedActionRunner<A> for TokioRuntimeHandle<A>
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

impl<A, M> CanSend<M> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M> + 'static,
    M: Send + 'static,
{
    fn send(&self, message: M) {
        let function =
            |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| actor.handle(message, ctx);
        // TODO: Figure out what to do with the result
        self.sender.send(Box::new(function)).unwrap();
    }
}

impl<A, M, R> CanSendAsync<M, R> for TokioRuntimeHandle<A>
where
    A: HandlerWithContext<M, R> + 'static,
    M: Send + 'static,
    R: Send + 'static,
{
    fn send_async(&self, message: M) -> BoxFuture<'static, R> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let future = async move {
            // TODO: Figure out what to do with the result. Should be fine to unwrap here
            receiver.await.unwrap()
        };
        let function = move |actor: &mut A, ctx: &mut dyn DelayedActionRunner<A>| {
            let result = actor.handle(message, ctx);
            sender.send(result).ok();
        };
        self.sender.send(Box::new(function)).unwrap();
        future.boxed()
    }
}

pub fn construct_actor_with_tokio_runtime<A>(
    mut actor: A,
) -> (tokio::runtime::Runtime, TokioRuntimeHandle<A>)
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

    let cancel = CancellationToken::new();
    let runtime_handle =
        TokioRuntimeHandle { sender, runtime: runtime.handle().clone(), cancel: cancel.clone() };

    // Spawn the actor in the runtime
    let mut runtime_handle_clone = runtime_handle.clone();
    runtime.spawn(async move {
        actor.start_actor(&mut runtime_handle_clone);
        loop {
            tokio::select! {
                Some(function) = receiver.recv() => {
                    function(&mut actor, &mut runtime_handle_clone);
                },
                _ = cancel.cancelled() => break,
            }
        }
    });

    (runtime, runtime_handle)
}

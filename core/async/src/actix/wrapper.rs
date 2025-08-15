use std::ops::{Deref, DerefMut};

use actix::SyncArbiter;

use crate::futures::DelayedActionRunner;
use crate::messaging;

/// Compatibility layer for actix messages.
impl<T: actix::Message> messaging::Message for T {}

/// Wrapper on top of a generic actor to make it implement actix::Actor trait. The wrapped actor
/// should implement the Handler trait for all the messages it would like to handle.
/// ActixWrapper is then used to create an actix actor that implements the CanSend trait.
pub struct ActixWrapper<T> {
    actor: T,
}

impl<T> ActixWrapper<T> {
    pub fn new(actor: T) -> Self {
        Self { actor }
    }
}

impl<T> actix::Actor for ActixWrapper<T>
where
    T: messaging::Actor + Unpin + 'static,
{
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.actor.start_actor(ctx);
    }
}

// Implementing Deref and DerefMut for the wrapped actor to allow access to the inner struct
// This is required for implementing DelayedActionRunner<T> for actix::Context<Outer>
impl<T> Deref for ActixWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.actor
    }
}

impl<T> DerefMut for ActixWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.actor
    }
}

impl<M, T> actix::Handler<M> for ActixWrapper<T>
where
    Self: actix::Actor,
    Self::Context: DelayedActionRunner<T>,
    T: messaging::HandlerWithContext<M, M::Result>,
    M: actix::Message,
    <M as actix::Message>::Result: actix::dev::MessageResponse<ActixWrapper<T>, M> + Send,
{
    type Result = M::Result;
    fn handle(&mut self, msg: M, ctx: &mut Self::Context) -> Self::Result {
        self.actor.handle(msg, ctx)
    }
}

pub struct SyncActixWrapper<T> {
    actor: T,
}

impl<T> SyncActixWrapper<T> {
    pub fn new(actor: T) -> Self {
        Self { actor }
    }
}

impl<T> actix::Actor for SyncActixWrapper<T>
where
    T: Unpin + 'static,
{
    type Context = actix::SyncContext<Self>;
}

impl<M, T> actix::Handler<M> for SyncActixWrapper<T>
where
    Self: actix::Actor,
    T: messaging::Handler<M, M::Result>,
    M: actix::Message,
    <M as actix::Message>::Result: actix::dev::MessageResponse<SyncActixWrapper<T>, M> + Send,
{
    type Result = M::Result;
    fn handle(&mut self, msg: M, _ctx: &mut Self::Context) -> Self::Result {
        self.actor.handle(msg)
    }
}

/// Spawns the actor returned by the factory function in a SyncArbiter,
/// after wrapping it in a SyncActixWrapper.
/// This is useful for actors that need to run in a separate thread pool.
pub fn spawn_sync_actix_actor<T, F>(
    num_threads: usize,
    factory: F,
) -> actix::Addr<SyncActixWrapper<T>>
where
    T: Unpin + Send + 'static,
    F: Fn() -> T + Sync + Send + 'static,
{
    SyncArbiter::start(num_threads, move || {
        let actor = factory();
        SyncActixWrapper::new(actor)
    })
}

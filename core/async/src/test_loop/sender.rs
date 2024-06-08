use std::any::type_name;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::futures::DelayedActionRunner;
use crate::messaging::{Actor, CanSend, HandlerWithContext, MessageWithCallback};
use crate::time::Duration;

use super::data::{TestLoopData, TestLoopDataHandle};
use super::DelaySender;

/// TestLoopSender implements the CanSend methods for an actor that can Handle them. This is
/// similar to our pattern of having an ActixWarpper around an actor to send messages to it.
///
/// ```
/// let actor = TestActor::new();
/// let adapter = LateBoundSender::new();
///
/// let sender: TestLoopSender<TestActor> = data.register_actor(actor, Some(adapter));
///
/// // We can now send messages to the actor using the sender and adapter.
/// sender.send(TestMessage {});
/// adapter.send(TestMessage {});
/// ```
///
/// For the purposes of testloop, we keep a copy of the delay sender that is used to schedule
/// callbacks on the testloop to execute either the actor.handle() function or the
/// DelayedActionRunner.run_later_boxed() function.
pub struct TestLoopSender<A>
where
    A: 'static,
{
    actor_handle: TestLoopDataHandle<A>,
    pending_events_sender: DelaySender,
    shutting_down: Arc<AtomicBool>,
    sender_delay: Duration,
}

impl<A> Clone for TestLoopSender<A> {
    fn clone(&self) -> Self {
        Self {
            actor_handle: self.actor_handle.clone(),
            pending_events_sender: self.pending_events_sender.clone(),
            shutting_down: self.shutting_down.clone(),
            sender_delay: self.sender_delay,
        }
    }
}

/// `DelayedActionRunner` that schedules the action to be run later by the TestLoop event loop.
impl<A> DelayedActionRunner<A> for TestLoopSender<A>
where
    A: 'static,
{
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: Duration,
        f: Box<dyn FnOnce(&mut A, &mut dyn DelayedActionRunner<A>) + Send + 'static>,
    ) {
        if self.shutting_down.load(Ordering::Relaxed) {
            return;
        }

        let mut this = self.clone();
        let callback = move |data: &mut TestLoopData| {
            let actor = data.get_mut(&this.actor_handle);
            f(actor, &mut this);
        };
        self.pending_events_sender.send_with_delay(
            format!("DelayedActionRunner {:?}", name),
            Box::new(callback),
            dur,
        );
    }
}

impl<M, A> CanSend<M> for TestLoopSender<A>
where
    M: actix::Message + Send + 'static,
    A: Actor + HandlerWithContext<M> + 'static,
{
    fn send(&self, msg: M) {
        let mut this = self.clone();
        let callback = move |data: &mut TestLoopData| {
            let actor = data.get_mut(&this.actor_handle);
            actor.handle(msg, &mut this);
        };
        self.pending_events_sender.send_with_delay(
            format!("Message {:?}", type_name::<M>()),
            Box::new(callback),
            self.sender_delay,
        );
    }
}

impl<M, R, A> CanSend<MessageWithCallback<M, R>> for TestLoopSender<A>
where
    M: actix::Message<Result = R> + Send + 'static,
    A: Actor + HandlerWithContext<M> + 'static,
    R: 'static,
{
    fn send(&self, msg: MessageWithCallback<M, R>) {
        let mut this = self.clone();
        let callback = move |data: &mut TestLoopData| {
            let MessageWithCallback { message: msg, callback } = msg;
            let actor = data.get_mut(&this.actor_handle);
            let result = actor.handle(msg, &mut this);
            callback(Ok(result));
        };
        self.pending_events_sender.send_with_delay(
            format!("Message {:?}", type_name::<M>()),
            Box::new(callback),
            self.sender_delay,
        );
    }
}

impl<A> TestLoopSender<A>
where
    A: Actor + 'static,
{
    pub fn new(
        actor_handle: TestLoopDataHandle<A>,
        pending_events_sender: DelaySender,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        Self { actor_handle, pending_events_sender, shutting_down, sender_delay: Duration::ZERO }
    }

    /// Returns a new TestLoopSender which sends messages with the given delay.
    pub fn with_delay(self, delay: Duration) -> Self {
        Self { sender_delay: delay, ..self }
    }

    pub fn actor_handle(&self) -> TestLoopDataHandle<A> {
        self.actor_handle.clone()
    }
}

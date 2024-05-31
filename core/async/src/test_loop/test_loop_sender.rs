use std::any::type_name;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::futures::DelayedActionRunner;
use crate::messaging::{Actor, CanSend, HandlerWithContext, MessageWithCallback};
use crate::time::Duration;

use super::delay_sender::DelaySender;
use super::event_handler::LoopEventHandler;

/// CallbackEvent for testloop is a simple event with a single callback which gets executed
/// This is very versatile and we can potentially have anything as a callback. For example, for the case of Senders
/// and Handlers, we can have a simple implementation of the CanSend function as a callback calling the Handle function
/// of the actor.
/// For DelayedActionRunner, we can have a callback that runs the function passed to run_later_boxed.
pub struct CallbackEvent {
    description: String,
    callback: Box<dyn FnOnce() + Send>,
}

impl Debug for CallbackEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackEvent").field("description", &self.description).finish()
    }
}

/// Handler for CallbackEvent, include in testloop event enum to handle CallbackEvent
pub fn callback_event_handler() -> LoopEventHandler<(), CallbackEvent> {
    LoopEventHandler::new(|event: CallbackEvent, _| Ok((event.callback)()))
}

/// TestLoopSender implements the CanSend methods for an actor that can Handle them. This is similar to our pattern
/// of having an ActixWarpper around an actor to send messages to it. For the purposes of testloop, we keep a copy of
/// the DelaySender<CallbackEvent> that is used to schedule callbacks on the testloop to execute either the
/// actor.handle() function or the DelayedActionRunner.run_later_boxed() function.
pub struct TestLoopSender<A> {
    actor: Arc<Mutex<A>>,
    pending_events_sender: DelaySender<CallbackEvent>,
    shutting_down: Arc<AtomicBool>,
}

// Since testloop is single-threaded, we can safely implement Send and Sync for TestLoopSender
// This was specifically required as rust was complaining the traits Sync/Send are not implemented for actor A
unsafe impl<A> Send for TestLoopSender<A> {}
unsafe impl<A> Sync for TestLoopSender<A> {}

impl<A> Clone for TestLoopSender<A> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            pending_events_sender: self.pending_events_sender.clone(),
            shutting_down: self.shutting_down.clone(),
        }
    }
}

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
        let callback = move || f(&mut this.clone().actor.lock().unwrap(), &mut this);
        self.pending_events_sender.send_with_delay(
            CallbackEvent {
                description: format!("DelayedActionRunner {:?}", name),
                callback: Box::new(callback),
            },
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
        let this = self.clone();
        let callback = move || {
            this.actor.lock().unwrap().handle(msg, &mut this.clone());
        };
        self.pending_events_sender.send_with_delay(
            CallbackEvent {
                description: format!("Message {:?}", type_name::<M>()),
                callback: Box::new(callback),
            },
            Duration::ZERO,
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
        let this = self.clone();
        let callback = move || {
            let MessageWithCallback { message: msg, callback } = msg;
            let result = this.actor.lock().unwrap().handle(msg, &mut this.clone());
            callback(Ok(result));
        };
        self.pending_events_sender.send_with_delay(
            CallbackEvent {
                description: format!("Message {:?}", type_name::<M>()),
                callback: Box::new(callback),
            },
            Duration::ZERO,
        );
    }
}

impl<A> TestLoopSender<A>
where
    A: Actor + 'static,
{
    pub fn new(
        actor: A,
        pending_events_sender: DelaySender<CallbackEvent>,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        Self { actor: Arc::new(Mutex::new(actor)), pending_events_sender, shutting_down }
    }

    pub fn actor(&self) -> MutexGuard<A> {
        self.actor.lock().unwrap()
    }

    pub fn queue_start_actor_event(&mut self) {
        let this = self.clone();
        let callback = move || this.actor.lock().unwrap().start_actor(&mut this.clone());
        self.pending_events_sender.send_with_delay(
            CallbackEvent {
                description: format!("StartActor {:?}", type_name::<A>()),
                callback: Box::new(callback),
            },
            Duration::ZERO,
        );
    }
}

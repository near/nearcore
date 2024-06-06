use super::{delay_sender::DelaySender, event_handler::LoopEventHandler, TestLoop};
use crate::futures::{AsyncComputationSpawner, DelayedActionRunner};
use crate::test_loop::event_handler::TryIntoOrSelf;
use crate::{futures::FutureSpawner, messaging::CanSend};
use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use near_time::Duration;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;

// Support for futures in TestLoop.
//
// There are two key features this file provides for TestLoop:
//
//   1. A general way to spawn futures and have the TestLoop drive the futures.
//      To support this, add () to the Data, add Arc<TestLoopTask> as an Event,
//      and add DriveFutures as a handler. Finally, pass
//      DelaySender<Arc<TestLoopTask>> as the &dyn FutureSpawner to any
//      component that needs to spawn futures.
//
//      This causes any futures spawned during the test to end up as an event
//      (an Arc<TestLoopTask>) in the test loop. The event will eventually be
//      executed by the DriveFutures handler, which will drive the future
//      until it is either suspended or completed. If suspended, then the waker
//      of the future (called when the future is ready to resume) will place
//      the Arc<TestLoopTask> event back into the test loop to be executed
//      again.
//
//   2. A way to send a message to the TestLoop and expect a response as a
//      future, which will resolve whenever the TestLoop handles the message.
//      To support this, use MessageWithCallback<Request, Response> as the
//      event type, and in the handler, call (event.responder)(result)
//      (possibly asynchronously) to complete the future.
//
//      This is needed to support the AsyncSender interface, which is required
//      by some components as they expect a response to each message. The way
//      this is implemented is by implementing a conversion from
//      DelaySender<MessageWithCallback<Request, Response>> to
//      AsyncSender<Request, Response>.

/// A message, plus a response callback. This should be used as the event type
/// when testing an Actix component that's expected to return a result.
///
/// The response is used to complete the future that is returned by
/// our `AsyncSender::send_async` implementation.

pub struct TestLoopTask {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    sender: DelaySender<Arc<TestLoopTask>>,
    description: String,
}

impl ArcWake for TestLoopTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = arc_self.clone();
        arc_self.sender.send(clone);
    }
}

impl Debug for TestLoopTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Task").field(&self.description).finish()
    }
}

/// Drives any Arc<TestLoopTask> events (futures spawned by our implementation
/// of FutureSpawner) that are remaining in the loop.
pub fn drive_futures() -> LoopEventHandler<(), Arc<TestLoopTask>> {
    LoopEventHandler::new_simple(|task: Arc<TestLoopTask>, _| {
        // The following is copied from the Rust async book.
        // Take the future, and if it has not yet completed (is still Some),
        // poll it in an attempt to complete it.
        let mut future_slot = task.future.lock().unwrap();
        if let Some(mut future) = future_slot.take() {
            let waker = waker_ref(&task);
            let context = &mut Context::from_waker(&*waker);
            if future.as_mut().poll(context).is_pending() {
                // We're still not done processing the future, so put it
                // back in its task to be run again in the future.
                *future_slot = Some(future);
            }
        }
    })
}

/// A DelaySender<Arc<TestLoopTask>> is a FutureSpawner that can be used to
/// spawn futures into the test loop. We give it a convenient alias.
pub type TestLoopFutureSpawner = DelaySender<Arc<TestLoopTask>>;

impl FutureSpawner for TestLoopFutureSpawner {
    fn spawn_boxed(&self, description: &str, f: BoxFuture<'static, ()>) {
        let task = Arc::new(TestLoopTask {
            future: Mutex::new(Some(f)),
            sender: self.clone(),
            description: description.to_string(),
        });
        self.send(task);
    }
}

/// Represents an action that was scheduled to run later, by using
/// `DelayedActionRunner::run_later`.
pub struct TestLoopDelayedActionEvent<T> {
    name: String,
    action: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
}

impl<T> Debug for TestLoopDelayedActionEvent<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DelayedAction").field(&self.name).finish()
    }
}

/// An event handler that handles only `TestLoopDelayedActionEvent`s, by
/// running the action encapsulated in the event.
pub fn drive_delayed_action_runners<T>(
    sender: DelaySender<TestLoopDelayedActionEvent<T>>,
    shutting_down: Arc<AtomicBool>,
) -> LoopEventHandler<T, TestLoopDelayedActionEvent<T>> {
    LoopEventHandler::new_simple(move |event: TestLoopDelayedActionEvent<T>, data: &mut T| {
        let mut runner = TestLoopDelayedActionRunner {
            sender: sender.clone(),
            shutting_down: shutting_down.clone(),
        };
        (event.action)(data, &mut runner);
    })
}

/// `DelayedActionRunner` that schedules the action to be run later by the
/// TestLoop event loop.
pub struct TestLoopDelayedActionRunner<T> {
    pub(crate) sender: DelaySender<TestLoopDelayedActionEvent<T>>,
    pub(crate) shutting_down: Arc<AtomicBool>,
}

impl<T> DelayedActionRunner<T> for TestLoopDelayedActionRunner<T> {
    fn run_later_boxed(
        &mut self,
        name: &str,
        dur: Duration,
        action: Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>,
    ) {
        if self.shutting_down.load(Ordering::Relaxed) {
            return;
        }
        self.sender.send_with_delay(
            TestLoopDelayedActionEvent { name: name.to_string(), action },
            dur.try_into().unwrap(),
        );
    }
}

impl<Data: 'static, Event: Debug + Send + 'static> TestLoop<Data, Event> {
    /// Shorthand for registering this frequently used handler.
    pub fn register_delayed_action_handler<T>(&mut self)
    where
        T: 'static,
        Data: AsMut<T>,
        Event: TryIntoOrSelf<TestLoopDelayedActionEvent<T>>
            + From<TestLoopDelayedActionEvent<T>>
            + 'static,
    {
        self.register_handler(
            drive_delayed_action_runners::<T>(self.sender().narrow(), self.shutting_down()).widen(),
        );
    }
}

impl<Data: 'static, Event: Debug + Send + 'static> TestLoop<Vec<Data>, (usize, Event)> {
    /// Shorthand for registering this frequently used handler for a multi-instance test.
    pub fn register_delayed_action_handler_for_index<T>(&mut self, idx: usize)
    where
        T: 'static,
        Data: AsMut<T>,
        Event: TryIntoOrSelf<TestLoopDelayedActionEvent<T>>
            + From<TestLoopDelayedActionEvent<T>>
            + 'static,
    {
        self.register_handler(
            drive_delayed_action_runners::<T>(
                self.sender().for_index(idx).narrow(),
                self.shutting_down(),
            )
            .widen()
            .for_index(idx),
        );
    }
}

/// An event that represents async computation. See async_computation_spawner() in DelaySender.
pub struct TestLoopAsyncComputationEvent {
    name: String,
    f: Box<dyn FnOnce() + Send>,
}

impl Debug for TestLoopAsyncComputationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AsyncComputation").field(&self.name).finish()
    }
}

/// AsyncComputationSpawner that spawns the computation in the TestLoop.
pub struct TestLoopAsyncComputationSpawner {
    pub(crate) sender: DelaySender<TestLoopAsyncComputationEvent>,
    pub(crate) artificial_delay: Box<dyn Fn(&str) -> Duration + Send + Sync>,
}

impl AsyncComputationSpawner for TestLoopAsyncComputationSpawner {
    fn spawn_boxed(&self, name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sender.send_with_delay(
            TestLoopAsyncComputationEvent { name: name.to_string(), f },
            (self.artificial_delay)(name),
        );
    }
}

pub fn drive_async_computations() -> LoopEventHandler<(), TestLoopAsyncComputationEvent> {
    LoopEventHandler::new_simple(|event: TestLoopAsyncComputationEvent, _data: &mut ()| {
        (event.f)();
    })
}

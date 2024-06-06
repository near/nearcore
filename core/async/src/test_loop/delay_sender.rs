use crate::break_apart::BreakApart;
use crate::messaging;
use crate::messaging::{IntoMultiSender, IntoSender};
use crate::test_loop::futures::{
    TestLoopAsyncComputationEvent, TestLoopAsyncComputationSpawner, TestLoopDelayedActionEvent,
    TestLoopDelayedActionRunner,
};
use crate::time;
use crate::time::Duration;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use super::futures::{TestLoopFutureSpawner, TestLoopTask};

/// Interface to send an event with a delay (in virtual time). It can be
/// converted to a Sender for any message type that can be converted into
/// the event type, so that a DelaySender given by the test loop may be passed
/// to production code that expects a Sender.
pub struct DelaySender<Event>(Arc<dyn Fn(Event, time::Duration) + Send + Sync>);

impl<Message, Event: From<Message> + 'static> messaging::CanSend<Message> for DelaySender<Event> {
    fn send(&self, message: Message) {
        self.send_with_delay(message.into(), time::Duration::ZERO);
    }
}

impl<Event> DelaySender<Event> {
    pub fn new(inner: impl Fn(Event, time::Duration) + Send + Sync + 'static) -> Self {
        Self(Arc::new(inner))
    }

    pub fn send_with_delay(&self, event: Event, delay: time::Duration) {
        self.0(event, delay);
    }

    pub fn with_additional_delay(&self, delay: time::Duration) -> DelaySender<Event>
    where
        Event: 'static,
    {
        let f = self.0.clone();
        Self(Arc::new(move |event, other_delay| f(event, delay + other_delay)))
    }

    pub fn narrow<InnerEvent>(self) -> DelaySender<InnerEvent>
    where
        Event: From<InnerEvent> + 'static,
    {
        DelaySender::<InnerEvent>::new(move |event, delay| {
            self.send_with_delay(event.into(), delay)
        })
    }

    /// A shortcut for a common use case, where we use an enum message to
    /// represent all the possible messages that a multisender may be used to
    /// send.
    ///
    /// This assumes that S is a multisender with the derive
    /// `#[derive(MultiSendMessage, ...)]`, which creates the enum
    /// `MyMultiSenderMessage` (where `MyMultiSender` is the name of the struct
    /// being derived from).
    ///
    /// To use, first include in the test loop event enum a case for
    /// `MyMultiSenderMessage`. Then, call this function to get a multisender,
    /// like
    /// `builder.wrapped_multi_sender<MyMultiSenderMessage, MyMultiSender>()`.
    pub fn into_wrapped_multi_sender<M: 'static, S: 'static>(self) -> S
    where
        Self: IntoSender<M>,
        BreakApart<M>: IntoMultiSender<S>,
    {
        self.into_sender().break_apart().into_multi_sender()
    }

    pub fn into_delayed_action_runner<InnerData>(
        self,
        shutting_down: Arc<AtomicBool>,
    ) -> TestLoopDelayedActionRunner<InnerData>
    where
        Event: From<TestLoopDelayedActionEvent<InnerData>> + 'static,
    {
        TestLoopDelayedActionRunner { sender: self.narrow(), shutting_down }
    }

    /// Returns a FutureSpawner that can be used to spawn futures into the loop.
    pub fn into_future_spawner(self) -> TestLoopFutureSpawner
    where
        Event: From<Arc<TestLoopTask>> + 'static,
    {
        self.narrow()
    }

    /// Returns an AsyncComputationSpawner that can be used to spawn async computation into the
    /// loop. The `artificial_delay` allows the test to determine an artificial delay that the
    /// computation should take, based on the name of the computation.
    pub fn into_async_computation_spawner(
        self,
        artificial_delay: impl Fn(&str) -> Duration + Send + Sync + 'static,
    ) -> TestLoopAsyncComputationSpawner
    where
        Event: From<TestLoopAsyncComputationEvent> + 'static,
    {
        TestLoopAsyncComputationSpawner {
            sender: self.narrow(),
            artificial_delay: Arc::new(artificial_delay),
        }
    }
}

impl<Event: 'static> DelaySender<(usize, Event)> {
    /// Converts a multi-instance sender to a single-instance sender.
    pub fn for_index(self, index: usize) -> DelaySender<Event> {
        DelaySender::new(move |event, delay| {
            self.send_with_delay((index, event), delay);
        })
    }
}

/// Custom implementation because #derive wouldn't work if Event does not Clone.
impl<Event> Clone for DelaySender<Event> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

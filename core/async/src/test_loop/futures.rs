use std::sync::{Arc, Mutex};
use std::task::Context;

use futures::future::BoxFuture;
use futures::task::{waker_ref, ArcWake};
use near_time::Duration;

use crate::futures::{AsyncComputationSpawner, FutureSpawner};

use super::data::TestLoopData;
use super::PendingEventsSender;

/// Support for futures in TestLoop.
///
/// There are two key features this file provides for TestLoop:
///
///   1. A general way to spawn futures and have the TestLoop drive the futures.
///      To support this, pass test_loop.future_spawner() the &dyn FutureSpawner
///      to any component that needs to spawn futures.
///
///      This causes any futures spawned during the test to end up as an callback in the
///      test loop. The event will eventually be executed by the drive_futures function,
///      which will drive the future until it is either suspended or completed. If suspended,
///      then the waker of the future (called when the future is ready to resume) will place
///      the event back into the test loop to be executed again.
///
///   2. A way to send a message to the TestLoop and expect a response as a
///      future, which will resolve whenever the TestLoop handles the message.
///      To support this, use MessageWithCallback<Request, Response> as the
///      event type, and in the handler, call (event.responder)(result)
///      (possibly asynchronously) to complete the future.
///
///      This is needed to support the AsyncSender interface, which is required
///      by some components as they expect a response to each message. The way
///      this is implemented is by implementing a conversion from
///      DelaySender<MessageWithCallback<Request, Response>> to
///      AsyncSender<Request, Response>.

/// A DelaySender is a FutureSpawner that can be used to
/// spawn futures into the test loop. We give it a convenient alias.
pub type TestLoopFututeSpawner = PendingEventsSender;

impl FutureSpawner for TestLoopFututeSpawner {
    fn spawn_boxed(&self, description: &str, f: BoxFuture<'static, ()>) {
        let task = Arc::new(FutureTask {
            future: Mutex::new(Some(f)),
            sender: self.clone(),
            description: description.to_string(),
        });
        let callback = move |_: &mut TestLoopData| {
            drive_futures(&task);
        };
        self.send(format!("FutureSpawn({})", description), Box::new(callback));
    }
}

struct FutureTask {
    future: Mutex<Option<BoxFuture<'static, ()>>>,
    sender: PendingEventsSender,
    description: String,
}

impl ArcWake for FutureTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = arc_self.clone();
        arc_self.sender.send(
            format!("FutureTask({})", arc_self.description),
            Box::new(move |_: &mut TestLoopData| drive_futures(&clone)),
        );
    }
}

fn drive_futures(task: &Arc<FutureTask>) {
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
}

/// AsyncComputationSpawner that spawns the computation in the TestLoop.
pub struct TestLoopAsyncComputationSpawner {
    sender: PendingEventsSender,
    artificial_delay: Box<dyn Fn(&str) -> Duration + Send + Sync>,
}

impl TestLoopAsyncComputationSpawner {
    pub fn new(
        sender: PendingEventsSender,
        artificial_delay: impl Fn(&str) -> Duration + Send + Sync + 'static,
    ) -> Self {
        Self { sender, artificial_delay: Box::new(artificial_delay) }
    }
}

impl AsyncComputationSpawner for TestLoopAsyncComputationSpawner {
    fn spawn_boxed(&self, name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sender.send_with_delay(
            format!("AsyncComputation({})", name),
            Box::new(move |_| f()),
            (self.artificial_delay)(name),
        );
    }
}

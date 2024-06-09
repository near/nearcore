use std::any::{type_name, Any};
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::messaging::{Actor, LateBoundSender};

use super::sender::TestLoopSender;
use super::PendingEventsSender;

/// TestLoopData is the container for all data that is stored and accessed by the test loop.
///
/// TestLoopData is used to mainly register actors, which can be accessed using a handle during
/// the execution of the TestLoop.
///
/// ```
/// let mut data = TestLoopData::new(pending_events_sender, shutting_down);
///
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
/// We have the ability to register data of any type, and then access it using a handle. This is
/// useful if we would like to have some arbitrary callback event in testloop to access this data.
/// ```
/// let mut data = TestLoopData::new(pending_events_sender, shutting_down);
/// let handle: TestLoopDataHandle<usize> = data.register_data(42);
/// assert_eq!(data.get(&handle), 42);
/// ```
pub struct TestLoopData {
    // Container of the data. We store it as a vec of Any so that we can store any type of data.
    data: Vec<Box<dyn Any>>,
    // Sender to send events to the test loop. Used mainly for registering actors.
    pending_events_sender: PendingEventsSender,
    // Atomic bool to check if the test loop is shutting down. Used mainly for registering actors.
    shutting_down: Arc<AtomicBool>,
}

impl TestLoopData {
    pub fn new(pending_events_sender: PendingEventsSender, shutting_down: Arc<AtomicBool>) -> Self {
        Self { data: Vec::new(), pending_events_sender, shutting_down }
    }

    /// Function to register data of any type in the TestLoopData.
    /// Returns a handler to the data that can be used to access the data later.
    pub fn register_data<T>(&mut self, data: T) -> TestLoopDataHandle<T> {
        let id = self.data.len();
        self.data.push(Box::new(data));
        TestLoopDataHandle::new(id)
    }

    /// Function to register an actor in the TestLoopData.
    /// Additionally schedules the start event for the actor on testloop.
    /// Returns a TestLoopSender<Actor> that can be used to send messages to the actor.
    pub fn register_actor_for_index<A>(
        &mut self,
        index: usize,
        actor: A,
        adapter: Option<Arc<LateBoundSender<TestLoopSender<A>>>>,
    ) -> TestLoopSender<A>
    where
        A: Actor + 'static,
    {
        let actor_handle = self.register_data(actor);
        let sender = TestLoopSender::new(
            actor_handle,
            self.pending_events_sender.clone().for_index(index),
            self.shutting_down.clone(),
        );
        self.queue_start_actor_event(sender.clone());
        if let Some(adapter) = adapter {
            adapter.bind(sender.clone());
        }
        sender
    }

    fn queue_start_actor_event<A>(&self, mut sender: TestLoopSender<A>)
    where
        A: Actor + 'static,
    {
        let callback = move |data: &mut TestLoopData| {
            let actor = data.get_mut(&sender.actor_handle());
            actor.start_actor(&mut sender);
        };
        self.pending_events_sender
            .send(format!("StartActor({:?})", type_name::<A>()), Box::new(callback));
    }

    /// Function to get reference to the data stored in TestLoopData.
    pub fn get<T>(&self, handle: &TestLoopDataHandle<T>) -> &T {
        self.data[handle.id].downcast_ref().unwrap()
    }

    /// Function to get mutable reference to the data stored in TestLoopData.
    pub fn get_mut<T>(&mut self, handle: &TestLoopDataHandle<T>) -> &mut T {
        self.data[handle.id].downcast_mut().unwrap()
    }
}

/// This is a handle to the data stored in TestLoopData.
/// test_loop_data.get(&handle) will return the data stored in TestLoopData.
/// test_loop_data.get_mut(&handle) will return a mutable reference to the data stored in TestLoopData.
pub struct TestLoopDataHandle<T>
where
    T: 'static,
{
    // This is an index into the data vector in TestLoopData.
    id: usize,
    // Saving the type info here as fn(T) to implicitly implement Send + Sync.
    _phantom: PhantomData<fn(T)>,
}

impl<T> Clone for TestLoopDataHandle<T> {
    fn clone(&self) -> Self {
        Self { id: self.id, _phantom: PhantomData }
    }
}

impl<T> TestLoopDataHandle<T> {
    fn new(id: usize) -> Self {
        Self { id, _phantom: PhantomData }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use crate::test_loop::data::TestLoopData;
    use crate::test_loop::PendingEventsSender;

    #[derive(Debug, PartialEq)]
    struct TestData {
        pub value: usize,
    }

    #[test]
    fn test_register_data() {
        let mut data =
            TestLoopData::new(PendingEventsSender::new(|_| {}), Arc::new(AtomicBool::new(false)));
        let test_data = TestData { value: 42 };
        let handle = data.register_data(test_data);
        assert_eq!(data.get(&handle), &TestData { value: 42 });

        data.get_mut(&handle).value = 43;
        assert_eq!(data.get(&handle), &TestData { value: 43 });
    }
}

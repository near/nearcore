use std::any::{Any, type_name};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::messaging::{Actor, LateBoundSender};

use super::pending_events_sender::RawPendingEventsSender;
use super::sender::TestLoopSender;

/// TestLoopData is the container for all data that is stored and accessed by the test loop.
///
/// TestLoopData is used to mainly register actors, which can be accessed using a handle during
/// the execution of the TestLoop.
///
/// ```rust, ignore
/// let mut data = TestLoopData::new(raw_pending_events_sender, shutting_down);
///
/// let actor = TestActor::new();
/// let adapter = LateBoundSender::new();
///
/// let sender: TestLoopSender<TestActor> = data.register_actor("client1", actor, Some(adapter));
///
/// // We can now send messages to the actor using the sender and adapter.
/// sender.send(TestMessage {});
/// adapter.send(TestMessage {});
/// ```
///
/// We have the ability to register data of any type, and then access it using a handle. This is
/// useful if we would like to have some arbitrary callback event in testloop to access this data.
///
/// ```rust, ignore
/// let mut data = TestLoopData::new(raw_pending_events_sender, shutting_down);
/// let handle: TestLoopDataHandle<usize> = data.register_data(42);
/// assert_eq!(data.get(&handle), 42);
/// ```
///
/// Note that the handler from one TestLoopData cannot be used to access data from another.
///
pub struct TestLoopData {
    // Container of the data. We store it as a vec of Any so that we can store any type of data.
    data: Vec<Box<dyn Any>>,
    // Sender to send events to the test loop. Used mainly for registering actors.
    raw_pending_events_sender: RawPendingEventsSender,
    // Atomic bool to check if the test loop is shutting down. Used mainly for registering actors.
    shutting_down: Arc<AtomicBool>,
}

impl TestLoopData {
    pub(crate) fn new(
        raw_pending_events_sender: RawPendingEventsSender,
        shutting_down: Arc<AtomicBool>,
    ) -> Self {
        Self { data: Vec::new(), raw_pending_events_sender, shutting_down }
    }

    /// Function to register data of any type in the TestLoopData.
    /// Returns a handler to the data that can be used to access the data later.
    pub fn register_data<T>(&mut self, data: T) -> TestLoopDataHandle<T> {
        let id = self.data.len();
        self.data.push(Box::new(data));
        TestLoopDataHandle::new(id)
    }

    /// Function to register an actor in the TestLoopData.
    /// We provide an identifier which is used to group events from the same client.
    /// Usually the identifier is the account_id of the client.
    /// This function additionally schedules the start event for the actor on testloop.
    /// Returns a TestLoopSender<Actor> that can be used to send messages to the actor.
    pub fn register_actor<A>(
        &mut self,
        identifier: &str,
        actor: A,
        adapter: Option<Arc<LateBoundSender<TestLoopSender<A>>>>,
    ) -> TestLoopSender<A>
    where
        A: Actor + 'static,
    {
        let actor_handle = self.register_data(actor);
        let sender = TestLoopSender::new(
            actor_handle,
            self.raw_pending_events_sender.for_identifier(identifier),
            self.shutting_down.clone(),
        );
        self.queue_start_actor_event(identifier, sender.clone());
        if let Some(adapter) = adapter {
            adapter.bind(sender.clone());
        }
        sender
    }

    // Helper function to queue the start actor event on the test loop while registering an actor.
    fn queue_start_actor_event<A>(&self, identifier: &str, mut sender: TestLoopSender<A>)
    where
        A: Actor + 'static,
    {
        let callback = move |data: &mut TestLoopData| {
            let actor = data.get_mut(&sender.actor_handle());
            actor.start_actor(&mut sender);
        };
        self.raw_pending_events_sender
            .for_identifier(identifier)
            .send(format!("StartActor({:?})", type_name::<A>()), Box::new(callback));
    }

    /// Function to get reference to the data stored in TestLoopData.
    pub fn get<T>(&self, handle: &TestLoopDataHandle<T>) -> &T {
        self.data
            .get(handle.id)
            .expect("Handle id out of bounds. Does handle belong to this TestLoopData?")
            .downcast_ref()
            .expect("Handle type mismatched. Does handle belong to this TestLoopData?")
    }

    /// Function to get mutable reference to the data stored in TestLoopData.
    pub fn get_mut<T>(&mut self, handle: &TestLoopDataHandle<T>) -> &mut T {
        self.data
            .get_mut(handle.id)
            .expect("Handle id out of bounds. Does handle belong to this TestLoopData?")
            .downcast_mut()
            .expect("Handle type mismatched. Does handle belong to this TestLoopData?")
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
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use crate::test_loop::data::TestLoopData;
    use crate::test_loop::pending_events_sender::RawPendingEventsSender;

    #[derive(Debug, PartialEq)]
    struct TestData {
        pub value: usize,
    }

    #[test]
    fn test_register_data() {
        let mut data = TestLoopData::new(
            RawPendingEventsSender::new(|_| {}),
            Arc::new(AtomicBool::new(false)),
        );
        let test_data = TestData { value: 42 };
        let handle = data.register_data(test_data);
        assert_eq!(data.get(&handle), &TestData { value: 42 });

        data.get_mut(&handle).value = 43;
        assert_eq!(data.get(&handle), &TestData { value: 43 });
    }
}

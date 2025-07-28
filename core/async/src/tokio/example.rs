use crate::messaging::{Actor, CanSend, CanSendAsync};
use crate::tokio::sender::TokioSender;
use crate::tokio::traits::Handler;

// Sync message
pub struct MySyncMessage {
    pub value: u32,
}

// Async Message
pub struct MyAsyncMessage {
    pub value: u32,
}

// Async message response
pub struct MyAsyncMessageResponse {
    pub response: String,
}

pub struct MyActor {}

impl Actor for MyActor {}

impl Handler<MySyncMessage> for MyActor {
    fn handle(&mut self, _msg: MySyncMessage) -> () {
        // Handle the sync message
    }
}

impl Handler<MyAsyncMessage, MyAsyncMessageResponse> for MyActor {
    fn handle(&mut self, _msg: MyAsyncMessage) -> MyAsyncMessageResponse {
        // Handle the async message
        MyAsyncMessageResponse { response: "Async response".into() }
    }
}

/// This is what we would use instead of MultiSenderFrom, MultiSendMessage etc.
pub trait CanSendMessagesTrait:
    CanSend<MySyncMessage> + CanSendAsync<MyAsyncMessage, MyAsyncMessageResponse>
{
}

// This is the equivalent for type PartialWitnessSenderForNetwork etc.
pub type MyAdapter = Box<dyn CanSendMessagesTrait>;

impl CanSendMessagesTrait for TokioSender<MyActor> {}

#[cfg(test)]
mod test {
    use crate::tokio::example::{MyActor, MyAdapter, MyAsyncMessage, MySyncMessage};
    use crate::tokio::runtime_handle::construct_actor_with_tokio_runtime;

    #[test]
    fn test_fn() {
        let actor = MyActor {};
        let executor_handle = construct_actor_with_tokio_runtime(actor);
        let sender: MyAdapter = executor_handle.sender();
        sender.send(MySyncMessage { value: 42 });
        let future = sender.send_async(MyAsyncMessage { value: 42 });
        let response = futures::executor::block_on(future);
        assert_eq!(response.response, "Async response");
        // TODO: Figure out shutdown logic for the runtime and executor_handle
    }
}

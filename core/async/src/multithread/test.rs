use crate::ActorSystem;
use crate::messaging::{Actor, CanSend, CanSendAsync, Handler, Message};
use futures::executor::block_on;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[test]
fn test_multithread_actor_basic() {
    struct MyActor;

    impl Actor for MyActor {}

    #[derive(Debug)]
    struct MessageA(String);
    #[derive(Debug)]
    struct MessageB(i32);
    impl Message for MessageA {}
    impl Message for MessageB {}

    impl Handler<MessageA> for MyActor {
        fn handle(&mut self, msg: MessageA) {
            println!("Received MessageA: {}", msg.0);
        }
    }

    impl Handler<MessageB, i32> for MyActor {
        fn handle(&mut self, msg: MessageB) -> i32 {
            println!("Received MessageB: {}", msg.0);
            msg.0 * 2
        }
    }

    let actor_system = ActorSystem::new();
    let handle = actor_system.spawn_multithread_actor(4, || MyActor);
    handle.send(MessageA("Hello".to_string()));
    let result = handle.send_async(MessageB(42));
    let result = futures::executor::block_on(result).unwrap();
    assert_eq!(result, 84);
    actor_system.stop();
}

#[test]
fn test_multithread_actor_multithreading() {
    struct MyActor;

    impl Actor for MyActor {}

    #[derive(Debug, Clone)]
    struct MessageA(Arc<Semaphore>, Arc<Semaphore>);
    impl Message for MessageA {}

    impl Handler<MessageA> for MyActor {
        fn handle(&mut self, msg: MessageA) {
            msg.0.add_permits(1);
            let _ = block_on(msg.1.acquire()).unwrap();
        }
    }

    let actor_system = ActorSystem::new();
    let handle = actor_system.spawn_multithread_actor(16, || MyActor);
    let msg = MessageA(Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0)));
    for _ in 0..16 {
        let msg = msg.clone();
        handle.send(msg);
    }
    let _ = block_on(msg.0.acquire_many(16)).unwrap();
    msg.1.add_permits(16);
    actor_system.stop();
}

#[test]
fn test_multithread_actor_stopping() {
    struct MyActor {
        _stopped: OwnedSemaphorePermit,
    }

    impl Actor for MyActor {}

    let actor_system = ActorSystem::new();
    let stopped = Arc::new(Semaphore::new(24));
    for _ in 0..3 {
        let stopped = stopped.clone();
        actor_system.spawn_multithread_actor(8, move || MyActor {
            _stopped: block_on(stopped.clone().acquire_owned()).unwrap(),
        });
    }
    actor_system.stop();
    let _ = block_on(stopped.acquire_many(24)).unwrap();
}

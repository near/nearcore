use crate::ActorSystem;
use crate::instrumentation::all_actor_instrumentations_view;
use crate::instrumentation::test_utils::get_total_times;
use crate::messaging::{Actor, CanSend, CanSendAsync, Handler};
use futures::executor::block_on;
use near_time::Clock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[test]
fn test_multithread_actor_basic() {
    struct MyActor;

    impl Actor for MyActor {}

    #[derive(Debug)]
    struct MessageA(String);
    #[derive(Debug)]
    struct MessageB(i32);

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

#[test]
fn test_instrumentation() {
    struct MyActor;
    impl Actor for MyActor {}

    #[derive(Debug)]
    struct MessageA {
        delay: Duration,
    }

    impl Handler<MessageA> for MyActor {
        fn handle(&mut self, msg: MessageA) {
            std::thread::sleep(msg.delay);
        }
    }

    let num_threads = 2;
    let actor_system = ActorSystem::new();
    let handle = actor_system.spawn_multithread_actor(num_threads, || MyActor);

    let delay_a = Duration::from_millis(100);
    let delay_b = Duration::from_millis(200);

    for _ in 0..num_threads {
        handle.send(MessageA { delay: delay_a });
    }
    // Since we sent `num_threads` messages with `delay_a`, the minimum expected
    // dequeue time for the next message is `delay_a`.
    handle.send(MessageA { delay: delay_b });

    // Retry up to 10 times, waiting 200ms each time, until we find a thread with windows
    // that has recorded expected events.
    let mut success = false;
    let expected_processing_time_ns = (delay_a * num_threads as u32 + delay_b).as_nanos() as u64;
    let expected_dequeue_time_ns = delay_a.as_nanos() as u64;
    let clock = Clock::real();
    for _ in 0..10 {
        // Add up all the processing and dequeue times recorded in the windows of the actor threads.
        let views = all_actor_instrumentations_view(&clock);
        let (total_processing_time_ns, total_dequeue_time_ns) = get_total_times("MyActor", &views);
        if total_processing_time_ns >= expected_processing_time_ns
            && total_dequeue_time_ns >= expected_dequeue_time_ns
        {
            success = true;
            break;
        }

        std::thread::sleep(Duration::from_millis(200));
    }
    actor_system.stop();
    assert!(
        success,
        "Did not find expected processing and dequeue times ({}, {}) in instrumentation data",
        expected_processing_time_ns, expected_dequeue_time_ns
    );
}

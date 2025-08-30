use crate::ActorSystem;
use crate::futures::{DelayedActionRunner, DelayedActionRunnerExt, FutureSpawnerExt};
use crate::messaging::{Actor, CanSend, CanSendAsync, Handler, Message};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn test_tokio_actor_basic() {
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
    let handle = actor_system.spawn_tokio_actor(MyActor);
    handle.send(MessageA("Hello".to_string()));
    let result = handle.send_async(MessageB(42));
    let result = result.await.unwrap();
    assert_eq!(result, 84);
    actor_system.stop();
}

#[tokio::test]
async fn test_tokio_actor_futures_delayed_actions_and_lifetime() {
    struct MyActor {
        started: Arc<AtomicUsize>,
        stopped: Arc<AtomicUsize>,
        wrapped: Arc<AtomicUsize>,
        delayed_action_executed: Arc<AtomicUsize>,
    }

    impl Actor for MyActor {
        fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
            self.started.fetch_add(1, Ordering::Relaxed);
            ctx.run_later("test", time::Duration::milliseconds(100), move |actor, ctx| {
                actor.delayed_action_executed.fetch_add(1, Ordering::Relaxed);
                ctx.run_later("test2", time::Duration::milliseconds(100), move |actor, _ctx| {
                    actor.delayed_action_executed.fetch_add(1, Ordering::Relaxed);
                });
            });
        }

        fn wrap_handler<M, R>(
            &mut self,
            msg: M,
            ctx: &mut dyn DelayedActionRunner<Self>,
            f: impl FnOnce(&mut Self, M, &mut dyn DelayedActionRunner<Self>) -> R,
        ) -> R {
            assert_eq!(self.started.load(Ordering::Relaxed), 1);
            assert_eq!(self.stopped.load(Ordering::Relaxed), 0);
            self.wrapped.fetch_add(1, Ordering::Relaxed);
            f(self, msg, ctx)
        }

        fn stop_actor(&mut self) {
            self.stopped.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Debug)]
    struct MessageA(String);
    impl Message for MessageA {}

    impl Handler<MessageA> for MyActor {
        fn handle(&mut self, msg: MessageA) {
            println!("Received MessageA: {}", msg.0);
        }
    }

    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let wrapped = Arc::new(AtomicUsize::new(0));
    let delayed_action_executed = Arc::new(AtomicUsize::new(0));
    let future_executed = Arc::new(AtomicUsize::new(0));
    let actor_system = ActorSystem::new();
    let handle = actor_system.spawn_tokio_actor(MyActor {
        started: started.clone(),
        stopped: stopped.clone(),
        wrapped: wrapped.clone(),
        delayed_action_executed: delayed_action_executed.clone(),
    });
    handle.send_async(MessageA("Hello".to_string())).await.unwrap();
    handle.send_async(MessageA("World".to_string())).await.unwrap();
    handle.spawn("test_future", {
        let future_executed = future_executed.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            future_executed.fetch_add(1, Ordering::Relaxed);
        }
    });
    handle.clone().run_later("test3", time::Duration::milliseconds(100), move |actor, ctx| {
        actor.delayed_action_executed.fetch_add(1, Ordering::Relaxed);
        ctx.run_later("test4", time::Duration::milliseconds(100), move |actor, _ctx| {
            actor.delayed_action_executed.fetch_add(1, Ordering::Relaxed);
        });
    });
    assert_eq!(started.load(Ordering::Relaxed), 1);
    assert_eq!(wrapped.load(Ordering::Relaxed), 2);
    assert_eq!(stopped.load(Ordering::Relaxed), 0);
    assert_eq!(delayed_action_executed.load(Ordering::Relaxed), 0);
    assert_eq!(future_executed.load(Ordering::Relaxed), 0);
    // Wait a bit for the delayed actions and future to execute.
    for _ in 0..10 {
        if delayed_action_executed.load(Ordering::Relaxed) != 4
            || future_executed.load(Ordering::Relaxed) != 1
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(delayed_action_executed.load(Ordering::Relaxed), 4);
    assert_eq!(future_executed.load(Ordering::Relaxed), 1);

    // Spawn a recurrent delayed action.
    fn recurrent(actor: &mut MyActor, ctx: &mut dyn DelayedActionRunner<MyActor>) {
        actor.delayed_action_executed.fetch_add(1, Ordering::Relaxed);
        ctx.run_later("recurrent", time::Duration::milliseconds(100), recurrent);
    }
    handle.clone().run_later("recurrent", time::Duration::milliseconds(100), recurrent);

    // Spawn a loop in a future.
    handle.spawn("loop", {
        let future_executed = future_executed.clone();
        async move {
            loop {
                future_executed.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    });
    // Wait a bit for the recurrent delayed action to execute a few times.
    for _ in 0..10 {
        if delayed_action_executed.load(Ordering::Relaxed) < 7
            || future_executed.load(Ordering::Relaxed) < 5
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert!(delayed_action_executed.load(Ordering::Relaxed) >= 7);
    assert!(future_executed.load(Ordering::Relaxed) >= 5);

    // Stop the actor system; this should call stop_actor exactly once. Also, recurrent
    // delayed actions should not execute after the actor is stopped.
    actor_system.stop();
    for _ in 0..10 {
        if stopped.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(stopped.load(Ordering::Relaxed), 1);
    let delayed_action_count = delayed_action_executed.load(Ordering::Relaxed);
    let future_count = future_executed.load(Ordering::Relaxed);
    // Wait a bit to make sure no more recurrent delayed actions are executed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(delayed_action_executed.load(Ordering::Relaxed), delayed_action_count);
    assert_eq!(future_executed.load(Ordering::Relaxed), future_count);
}

#[tokio::test]
async fn test_tokio_runtime_shutdown() {
    #[derive(Clone)]
    struct DropToIncrement {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for DropToIncrement {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct MyActor {
        _dropped: DropToIncrement,
    }

    impl Actor for MyActor {}

    #[derive(Debug)]
    struct MessageA;
    impl Message for MessageA {}
    impl Handler<MessageA> for MyActor {
        fn handle(&mut self, _msg: MessageA) {
            println!("Received MessageA");
        }
    }

    let actor_system = ActorSystem::new();
    let dropped = DropToIncrement { dropped: Arc::new(AtomicUsize::new(0)) };
    let handle1 = actor_system.spawn_tokio_actor(MyActor { _dropped: dropped.clone() });
    let handle2 = actor_system.spawn_tokio_actor(MyActor { _dropped: dropped.clone() });
    actor_system.spawn_tokio_actor(MyActor { _dropped: dropped.clone() });

    // Dropping the handle does not drop the actor.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(dropped.dropped.load(Ordering::Relaxed), 0);

    // Spawn a future that never finishes. This should not prevent the runtime from shutting down.
    handle1.spawn("test", {
        let dropped = dropped.clone();
        async move {
            let _dropped = dropped;
            std::future::pending::<()>().await;
        }
    });

    // Spawn a delayed action that never triggers. This should also not prevent the runtime from shutting down.
    handle2.clone().run_later("test", time::Duration::days(10000), {
        let dropped = dropped.clone();
        move |_actor, _ctx| {
            let _dropped = dropped;
        }
    });

    // Check the count again.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(dropped.dropped.load(Ordering::Relaxed), 0);

    // Stop the first actor; this should drop the actor and the future on the runtime.
    handle1.stop();
    for _ in 0..10 {
        if dropped.dropped.load(Ordering::Relaxed) != 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(dropped.dropped.load(Ordering::Relaxed), 2);

    // Stop the actor system; this should drop the remaining actors and the delayed action.
    actor_system.stop();
    for _ in 0..10 {
        if dropped.dropped.load(Ordering::Relaxed) != 5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(dropped.dropped.load(Ordering::Relaxed), 5);

    // It's OK to still send messages after actor shutdown; they just won't be processed.
    handle1.send(MessageA); // Should not crash.
    handle2.spawn("", async move {}); // Should not crash.
    handle1.clone().run_later("", time::Duration::milliseconds(100), |_, _| {}); // Should not crash.
    handle2.send_async(MessageA).await.unwrap_err(); // Should return error.
}

#[tokio::test]
async fn test_tokio_builder() {
    struct MyActor;

    impl Actor for MyActor {}

    #[derive(Debug)]
    struct MessageA(u64);
    impl Message for MessageA {}

    impl Handler<MessageA, u64> for MyActor {
        fn handle(&mut self, msg: MessageA) -> u64 {
            msg.0 + 1
        }
    }

    let counter = Arc::new(AtomicUsize::new(0));

    let actor_system = ActorSystem::new();
    let builder = actor_system.new_tokio_builder();
    let handle = builder.handle();
    let response_fut = handle.send_async(MessageA(12));
    handle.spawn("test", {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    handle.clone().run_later("test", time::Duration::milliseconds(100), {
        let counter = counter.clone();
        move |_, _| {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    builder.spawn_tokio_actor(MyActor);
    assert_eq!(response_fut.await.unwrap(), 13);
    for _ in 0..10 {
        if counter.load(Ordering::Relaxed) != 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(counter.load(Ordering::Relaxed), 2);
    assert_eq!(handle.send_async(MessageA(100)).await.unwrap(), 101);
    actor_system.stop();
}

use crate::ActorSystem;
use crate::futures::{DelayedActionRunnerExt, FutureSpawnerExt};
use crate::messaging::CanSend;
use crate::messaging::{Actor, Handler, Message};
use crate::tokio::TokioRuntimeHandle;
use rand::Rng;
use time::Duration;

pub fn spawn_actors_for_testing_instrumentation(actor_system: ActorSystem) {
    spawn_long_running_task_actor(actor_system.clone());
    spawn_very_high_message_count_actor(actor_system.clone());
    spawn_periodic_futures_actor(actor_system);
}

struct LongRunningTaskActor;

impl Actor for LongRunningTaskActor {
    fn start_actor(&mut self, ctx: &mut dyn crate::futures::DelayedActionRunner<Self>) {
        self.run_task(ctx);
    }
}

impl LongRunningTaskActor {
    fn run_task(&self, ctx: &mut dyn crate::futures::DelayedActionRunner<Self>) {
        let mut rng = rand::thread_rng();
        let duration = std::time::Duration::from_millis(rng.gen_range(3000..=15000));
        std::thread::sleep(duration);
        let next_duration = Duration::milliseconds(rng.gen_range(100..=1000));
        ctx.run_later("long task", next_duration, |act, ctx| act.run_task(ctx));
    }
}

fn spawn_long_running_task_actor(actor_system: ActorSystem) {
    actor_system.spawn_tokio_actor(LongRunningTaskActor);
}

struct VeryHighMessageCountActor {
    handle: TokioRuntimeHandle<Self>,
}

#[derive(Debug)]
struct DummyMessage;

impl Message for DummyMessage {}

impl Actor for VeryHighMessageCountActor {}

impl Handler<DummyMessage> for VeryHighMessageCountActor {
    fn handle(&mut self, _msg: DummyMessage) {
        std::thread::sleep(std::time::Duration::from_millis(1));
        self.handle.send(DummyMessage);
    }
}

fn spawn_very_high_message_count_actor(actor_system: ActorSystem) {
    let builder = actor_system.new_tokio_builder();
    let handle = builder.handle();
    builder.spawn_tokio_actor(VeryHighMessageCountActor { handle: handle.clone() });
    for _ in 0..100_000 {
        handle.send(DummyMessage);
    }
}

fn spawn_periodic_futures_actor(actor_system: ActorSystem) {
    let spawner = actor_system.new_future_spawner();
    spawner.spawn("timer 1", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });
    spawner.spawn("timer 2", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(130));
        loop {
            interval.tick().await;
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    });
}

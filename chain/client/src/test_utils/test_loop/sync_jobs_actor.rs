use crate::client_actor::{SyncJobsSenderForClient, SyncJobsSenderForClientMessage};
use crate::sync_jobs_actor::SyncJobsActor;
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopDelayedActionRunner;
use near_async::v2::{self, LoopData, LoopStream};

pub fn forward_messages_from_client_to_sync_jobs_actor(
    mut ctx: TestLoopDelayedActionRunner<SyncJobsActor>,
) -> LoopEventHandler<SyncJobsActor, SyncJobsSenderForClientMessage> {
    LoopEventHandler::new_simple(move |msg, sync_jobs_actor: &mut SyncJobsActor| match msg {
        SyncJobsSenderForClientMessage::_apply_state_parts(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_block_catch_up(msg) => {
            sync_jobs_actor.handle(msg);
        }
        SyncJobsSenderForClientMessage::_resharding(msg) => {
            sync_jobs_actor.handle_resharding_request(msg, &mut ctx);
        }
        SyncJobsSenderForClientMessage::_load_memtrie(msg) => {
            sync_jobs_actor.handle(msg);
        }
    })
}

#[derive(Clone)]
pub struct LoopSyncJobsActorBuilder {
    pub from_client_stream: LoopStream<SyncJobsSenderForClientMessage>,
    pub from_client: SyncJobsSenderForClient,
}

#[derive(Clone)]
pub struct LoopSyncJobsActor {
    pub actor: LoopData<SyncJobsActor>,
    pub from_client: SyncJobsSenderForClient,
    pub delayed_action_runner: TestLoopDelayedActionRunner<SyncJobsActor>,
}

pub fn loop_sync_jobs_actor_builder(test: &mut v2::TestLoop) -> LoopSyncJobsActorBuilder {
    let from_client_stream = test.new_stream();
    let from_client = from_client_stream.wrapped_multi_sender();
    LoopSyncJobsActorBuilder { from_client_stream, from_client }
}

impl LoopSyncJobsActorBuilder {
    pub fn build(self, test: &mut v2::TestLoop, actor: SyncJobsActor) -> LoopSyncJobsActor {
        let actor = test.add_data(actor);
        let delayed_action_runner = test.new_delayed_actions_runner(actor);
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_messages_from_client_to_sync_jobs_actor(delayed_action_runner.clone()),
        );
        LoopSyncJobsActor { actor, from_client: self.from_client, delayed_action_runner }
    }
}

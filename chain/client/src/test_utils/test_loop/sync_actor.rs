use crate::sync::adapter::SyncActorHandler;
use crate::sync::sync_actor::SyncActor;
use crate::SyncMessage;
use near_async::messaging::Sender;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::v2::{self, LoopData, LoopStream};
use near_network::state_sync::StateSyncResponse;
use near_network::types::PeerManagerMessageRequest;
use near_primitives::shard_layout::ShardUId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type TestSyncActors = Arc<Mutex<HashMap<ShardUId, SyncActor>>>;

pub fn forward_sync_actor_messages_from_client(
) -> LoopEventHandler<TestSyncActors, (ShardUId, SyncMessage)> {
    LoopEventHandler::new_simple(|(shard_uid, msg), sync_actors: &mut TestSyncActors| {
        sync_actors
            .lock()
            .unwrap()
            .get_mut(&shard_uid)
            .expect("No such ShardUId for sync actor")
            .handle_client_sync_message(msg);
    })
}

pub fn forward_sync_actor_messages_from_network(
) -> LoopEventHandler<TestSyncActors, (ShardUId, StateSyncResponse)> {
    LoopEventHandler::new_simple(|(shard_uid, msg), sync_actors: &mut TestSyncActors| {
        sync_actors
            .lock()
            .unwrap()
            .get_mut(&shard_uid)
            .expect("No such ShardUId for sync actor")
            .handle_network_sync_message(msg);
    })
}

#[derive(Clone)]
#[must_use = "Builder should be used to build; otherwise events would not be handled"]
pub struct LoopSyncActorBuilder {
    pub sync_actors: TestSyncActors,
    pub from_client_stream: LoopStream<(ShardUId, SyncMessage)>,
    pub from_client: Sender<(ShardUId, SyncMessage)>,
    pub from_network_stream: LoopStream<(ShardUId, StateSyncResponse)>,
    pub from_network: Sender<(ShardUId, StateSyncResponse)>,
}

#[derive(Clone)]
pub struct LoopSyncActor {
    pub actor: LoopData<TestSyncActors>,
    pub from_client: Sender<(ShardUId, SyncMessage)>,
    pub from_network: Sender<(ShardUId, StateSyncResponse)>,
}

pub fn loop_sync_actor_builder(test: &mut v2::TestLoop) -> LoopSyncActorBuilder {
    let sync_actors = Arc::new(Mutex::new(HashMap::new()));
    let from_client_stream = test.new_stream();
    let from_client = from_client_stream.sender();
    let from_network_stream = test.new_stream();
    let from_network = from_network_stream.sender();
    LoopSyncActorBuilder {
        sync_actors,
        from_client_stream,
        from_client,
        from_network_stream,
        from_network,
    }
}

impl LoopSyncActorBuilder {
    pub fn sync_actor_maker(
        &self,
    ) -> Arc<
        dyn Fn(ShardUId, Sender<SyncMessage>, Sender<PeerManagerMessageRequest>) -> SyncActorHandler
            + Send
            + Sync,
    > {
        let sync_actors = self.sync_actors.clone();
        let sender_for_client = self.from_client.clone();
        let sender_for_network = self.from_network.clone();
        Arc::new(move |shard_uid, client_sender, network_sender| {
            let sender_for_client = sender_for_client.clone();
            let sender_for_network = sender_for_network.clone();
            let sender_for_client: Sender<SyncMessage> =
                Sender::from_fn(move |msg| sender_for_client.send((shard_uid, msg)));
            let sender_for_network: Sender<StateSyncResponse> =
                Sender::from_fn(move |msg| sender_for_network.send((shard_uid, msg)));
            let sync_actor = SyncActor::new(shard_uid, client_sender, network_sender);
            assert!(
                sync_actors.lock().unwrap().insert(shard_uid, sync_actor).is_none(),
                "Sync actor for shard {} already created!",
                shard_uid
            );

            let sync_actors = sync_actors.clone();
            SyncActorHandler {
                client_sender: sender_for_client,
                network_sender: sender_for_network,
                shutdown: Mutex::new(Box::new(move || {
                    sync_actors.lock().unwrap().remove(&shard_uid);
                })),
            }
        })
    }

    pub fn build(self, test: &mut v2::TestLoop) -> LoopSyncActor {
        let actor = test.add_data(self.sync_actors);
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_sync_actor_messages_from_client(),
        );
        self.from_network_stream.handle1_legacy(
            test,
            actor,
            forward_sync_actor_messages_from_network(),
        );
        LoopSyncActor { actor, from_client: self.from_client, from_network: self.from_network }
    }
}

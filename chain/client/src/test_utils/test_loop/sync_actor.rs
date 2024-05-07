use crate::sync::adapter::SyncActorHandler;
use crate::sync::sync_actor::SyncActor;
use crate::SyncMessage;
use near_async::messaging::{IntoSender, Sender};
use near_async::test_loop::delay_sender::DelaySender;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_network::state_sync::StateSyncResponse;
use near_network::types::PeerManagerMessageRequest;
use near_primitives::shard_layout::ShardUId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type TestSyncActors = Arc<Mutex<HashMap<ShardUId, SyncActor>>>;

pub fn test_loop_sync_actor_maker<E>(
    sender: DelaySender<E>,
    sync_actors: TestSyncActors,
) -> Arc<
    dyn Fn(ShardUId, Sender<SyncMessage>, Sender<PeerManagerMessageRequest>) -> SyncActorHandler
        + Send
        + Sync,
>
where
    E: From<(ShardUId, SyncMessage)> + From<(ShardUId, StateSyncResponse)> + 'static,
{
    Arc::new(move |shard_uid, client_sender, network_sender| {
        let sender_for_client: Sender<(ShardUId, SyncMessage)> = sender.clone().into_sender();
        let sender_for_network: Sender<(ShardUId, StateSyncResponse)> =
            sender.clone().into_sender();
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

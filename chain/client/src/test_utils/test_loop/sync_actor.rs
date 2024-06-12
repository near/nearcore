use crate::sync::adapter::SyncActorHandler;
use crate::sync::sync_actor::SyncActor;
use crate::SyncMessage;
use near_async::messaging::{IntoSender, LateBoundSender, Sender};
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::delay_sender::DelaySender as DelaySenderOld;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::DelaySender;
use near_network::state_sync::StateSyncResponse;
use near_network::types::PeerManagerMessageRequest;
use near_primitives::shard_layout::ShardUId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn test_loop_sync_actor_maker(
    sender: DelaySender,
) -> Arc<
    dyn Fn(ShardUId, Sender<SyncMessage>, Sender<PeerManagerMessageRequest>) -> SyncActorHandler
        + Send
        + Sync,
> {
    // This is a closure that will be called by SyncAdapter to create SyncActor.
    // Since we don't have too much control over when the closure is called, we need to use the CallbackEvent
    // to register the SyncActor in the TestLoopData.
    // TestLoop and TestLoopData can not cross the closure boundary and be moved while the PendingEventsSender can.
    Arc::new(move |shard_uid, client_sender, network_sender| {
        let sync_actor = SyncActor::new(shard_uid, client_sender, network_sender);
        let sync_actor_adapter = LateBoundSender::new();
        let sync_actor_adapter_clone = sync_actor_adapter.clone();
        let callback = move |data: &mut TestLoopData| {
            data.register_actor(sync_actor, Some(sync_actor_adapter));
        };
        sender.send(format!("Register SyncActor {:?}", shard_uid), Box::new(callback));
        SyncActorHandler {
            client_sender: sync_actor_adapter_clone.as_sender(),
            network_sender: sync_actor_adapter_clone.as_sender(),
            shutdown: Mutex::new(Box::new(move || {})),
        }
    })
}

pub type TestSyncActors = Arc<Mutex<HashMap<ShardUId, SyncActor>>>;

pub fn test_loop_sync_actor_maker_old<E>(
    sender: DelaySenderOld<E>,
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

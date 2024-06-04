use crate::state_snapshot_actor::{
    StateSnapshotActor, StateSnapshotSenderForClient, StateSnapshotSenderForClientMessage,
    StateSnapshotSenderForStateSnapshot, StateSnapshotSenderForStateSnapshotMessage,
};
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::v2::{self, LoopStream};

pub fn forward_state_snapshot_messages_from_state_snapshot(
) -> LoopEventHandler<StateSnapshotActor, StateSnapshotSenderForStateSnapshotMessage> {
    LoopEventHandler::new_simple(|msg, actor: &mut StateSnapshotActor| match msg {
        StateSnapshotSenderForStateSnapshotMessage::_create_snapshot(msg) => actor.handle(msg),
    })
}

pub fn forward_state_snapshot_messages_from_client(
) -> LoopEventHandler<StateSnapshotActor, StateSnapshotSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, actor: &mut StateSnapshotActor| match msg {
        StateSnapshotSenderForClientMessage::_0(msg) => actor.handle(msg),
    })
}

pub struct LoopStateSnapshotActorBuilder {
    pub from_state_snapshot_stream: LoopStream<StateSnapshotSenderForStateSnapshotMessage>,
    pub from_state_snapshot: StateSnapshotSenderForStateSnapshot,
    pub from_client_stream: LoopStream<StateSnapshotSenderForClientMessage>,
    pub from_client: StateSnapshotSenderForClient,
}

pub struct LoopStateSnapshotActor {
    pub actor: v2::LoopData<StateSnapshotActor>,
    pub from_state_snapshot: StateSnapshotSenderForStateSnapshot,
    pub from_client: StateSnapshotSenderForClient,
}

pub fn loop_state_snapshot_actor_builder(test: &mut v2::TestLoop) -> LoopStateSnapshotActorBuilder {
    let from_state_snapshot = test.new_stream();
    let from_state_snapshot_multi_sender = from_state_snapshot.wrapped_multi_sender();
    let from_client = test.new_stream();
    let from_client_multi_sender = from_client.wrapped_multi_sender();
    LoopStateSnapshotActorBuilder {
        from_state_snapshot_stream: from_state_snapshot,
        from_state_snapshot: from_state_snapshot_multi_sender,
        from_client_stream: from_client,
        from_client: from_client_multi_sender,
    }
}

impl LoopStateSnapshotActorBuilder {
    pub fn build(
        self,
        test: &mut v2::TestLoop,
        actor: StateSnapshotActor,
    ) -> LoopStateSnapshotActor {
        let actor = test.add_data(actor);
        self.from_state_snapshot_stream.handle1_legacy(
            test,
            actor,
            forward_state_snapshot_messages_from_state_snapshot(),
        );
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_state_snapshot_messages_from_client(),
        );
        LoopStateSnapshotActor {
            actor,
            from_state_snapshot: self.from_state_snapshot,
            from_client: self.from_client,
        }
    }
}

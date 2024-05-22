use crate::state_snapshot_actor::{
    StateSnapshotActor, StateSnapshotSenderForClientMessage,
    StateSnapshotSenderForStateSnapshotMessage,
};
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;

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

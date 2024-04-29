use crate::state_snapshot_actor::{
    StateSnapshotActions, StateSnapshotSenderForClientMessage,
    StateSnapshotSenderForStateSnapshotMessage,
};
use near_async::test_loop::event_handler::LoopEventHandler;

pub fn forward_state_snapshot_messages_from_state_snapshot(
) -> LoopEventHandler<StateSnapshotActions, StateSnapshotSenderForStateSnapshotMessage> {
    LoopEventHandler::new_simple(|msg, actor: &mut StateSnapshotActions| match msg {
        StateSnapshotSenderForStateSnapshotMessage::_create_snapshot(msg) => {
            actor.handle_create_snapshot_request(msg)
        }
    })
}

pub fn forward_state_snapshot_messages_from_client(
) -> LoopEventHandler<StateSnapshotActions, StateSnapshotSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, actor: &mut StateSnapshotActions| match msg {
        StateSnapshotSenderForClientMessage::_0(msg) => {
            actor.handle_delete_and_maybe_create_snapshot_request(msg)
        }
    })
}

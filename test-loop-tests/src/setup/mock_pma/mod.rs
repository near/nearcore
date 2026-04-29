//! Legacy mock `PeerManagerActor` and surrounding glue.
//!
//! Everything in this directory exists to keep the mock-using tests
//! compiling until they migrate to the real-PMA path. The directory
//! is deleted wholesale once migration completes.

pub mod delayed_senders;
pub mod drop_condition_bridge;
pub mod peer_manager_actor;

pub use peer_manager_actor::{
    HandlerResult, TestLoopNetworkSharedState, TestLoopPeerManagerActor, UnreachableActor,
};

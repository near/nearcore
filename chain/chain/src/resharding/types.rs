use near_async::{Message, messaging::Sender};

use super::event_type::ReshardingSplitShardParams;

/// Request to schedule a resharding task. The resharding actor will wait till the resharding
/// block is finalized before starting resharding.
#[derive(Message, Clone, Debug)]
pub struct ScheduleResharding {
    pub split_shard_event: ReshardingSplitShardParams,
}

/// A multi-sender for the FlatStorageResharder post processing API.
///
/// This is meant to be used to send messages to handle the post processing tasks needed for
/// resharding the flat storage. An example is splitting a shard.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ReshardingSender {
    pub schedule_resharding_sender: Sender<ScheduleResharding>,
}

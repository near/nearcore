use std::hash::Hash;
use std::collections::HashMap;
use super::message::MessageWeakRef;

/// Messages that approve a Group that satisfy certain criteria.
/// Examples:
/// * endorsements of representative message(s) of epoch X.
/// * promises of kickout(s) of epoch X.
#[derive(Debug)]
pub struct GroupApprovals<T: Hash> {
    /// Approved message hash -> pair:
    /// * Approved message
    /// * approval hash ->
    pub approvals_by_message: HashMap<u64, (MessageWeakRef<T>, HashMap<u64, MessageWeakRef<T>>)>,
}

#[derive(Debug)]
pub struct GroupApprovalPerEpoch<T: Hash> {

}

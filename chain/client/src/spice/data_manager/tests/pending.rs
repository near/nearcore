use super::*;
use near_crypto::Signature;
use near_primitives::spice::partial_data::{
    SpiceDataIdentifier, SpiceDataPart, SpicePartialData, testonly_create_spice_partial_data,
};
use std::num::NonZeroUsize;

fn message(block_hash: CryptoHash, parts: Vec<SpiceDataPart>) -> SpicePartialData {
    testonly_create_spice_partial_data(
        SpiceDataIdentifier::ReceiptProof {
            block_hash,
            from_shard_id: ShardId::new(0),
            to_shard_id: ShardId::new(1),
        },
        SpiceDataCommitment {
            hash: CryptoHash::default(),
            root: CryptoHash::default(),
            encoded_length: 1,
        },
        parts,
        Signature::default(),
        account("producer"),
    )
}

#[test]
fn empty_message_is_refused_and_not_buffered() {
    let mut pending = PendingPartialData::new(NonZeroUsize::new(10).unwrap());
    let block_hash = hash(b"block");
    let part = SpiceDataPart { part_ord: 0, part: Box::new([0]), merkle_proof: Vec::new() };
    let full = message(block_hash, vec![part]);

    assert_matches!(
        pending.insert(message(block_hash, Vec::new())),
        Err(SenderFault::EmptyMessage)
    );
    pending.insert(full.clone()).unwrap();
    assert_eq!(pending.take(&block_hash), vec![full]);
}

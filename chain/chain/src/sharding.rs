use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, ShardId};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;

pub fn shuffle_receipt_proofs<ReceiptProofType>(
    receipt_proofs: &mut Vec<ReceiptProofType>,
    block_hash: &CryptoHash,
) {
    let mut slice = [0u8; 32];
    slice.copy_from_slice(block_hash.as_ref());
    let mut rng: ChaCha20Rng = SeedableRng::from_seed(slice);
    receipt_proofs.shuffle(&mut rng);
}

pub fn cares_about_shard_this_or_next_epoch(
    account_id: Option<&AccountId>,
    parent_hash: &CryptoHash,
    shard_id: ShardId,
    is_me: bool,
    shard_tracker: &ShardTracker,
) -> bool {
    // TODO(robin-near): I think we only need the shard_tracker if is_me is false.
    shard_tracker.care_about_shard(account_id, parent_hash, shard_id, is_me)
        || shard_tracker.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
}

#[cfg(test)]
mod tests {
    use crate::sharding::shuffle_receipt_proofs;
    use near_primitives::hash::CryptoHash;

    #[test]
    pub fn receipt_randomness_reproducibility() {
        // Sanity check that the receipt shuffling implementation does not change.
        let mut receipt_proofs = vec![0, 1, 2, 3, 4, 5, 6];
        shuffle_receipt_proofs(&mut receipt_proofs, &CryptoHash::hash_bytes(&[1, 2, 3, 4, 5]));
        assert_eq!(receipt_proofs, vec![2, 3, 1, 4, 0, 5, 6],);
    }
}

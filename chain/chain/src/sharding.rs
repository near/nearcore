use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha20Rng;

/// Gets salt for shuffling receipts grouped by **source shards** before
/// processing them in the target shard.
pub fn get_receipts_shuffle_salt<'a>(
    epoch_manager: &dyn EpochManagerAdapter,
    block: &'a Block,
) -> Result<&'a CryptoHash, EpochError> {
    let protocol_version = epoch_manager.get_epoch_protocol_version(&block.header().epoch_id())?;
    if ProtocolFeature::BlockHeightForReceiptId.enabled(protocol_version) {
        Ok(block.header().prev_hash())
    } else {
        Ok(block.hash())
    }
}

pub fn shuffle_receipt_proofs<ReceiptProofType>(
    receipt_proofs: &mut Vec<ReceiptProofType>,
    shuffle_salt: &CryptoHash,
) {
    let mut slice = [0u8; 32];
    slice.copy_from_slice(shuffle_salt.as_ref());
    let mut rng: ChaCha20Rng = SeedableRng::from_seed(slice);
    receipt_proofs.shuffle(&mut rng);
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

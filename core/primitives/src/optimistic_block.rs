use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;

use crate::block::BlockHeader;
use crate::hash::{hash, CryptoHash};
use crate::merkle::combine_hash;
use crate::types::BlockHeight;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq)]
pub struct OptimisticBlockInner {
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    // Data to confirm the correctness of randomness beacon output
    pub random_value: CryptoHash,
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
}

/// An optimistic block is independent of specific chunks and can be generated
/// and distributed immediately after the previous block is processed.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq)]
#[borsh(init=init)]
pub struct OptimisticBlock {
    pub prev_block_hash: CryptoHash,
    pub inner: OptimisticBlockInner,
    /// Signature of the block producer.
    pub signature: Signature,
    #[borsh(skip)]
    pub hash: CryptoHash,
}

impl OptimisticBlock {
    #[cfg(feature = "clock")]
    pub fn produce(
        prev_block_header: &BlockHeader,
        height: BlockHeight,
        signer: &crate::validator_signer::ValidatorSigner,
        clock: near_time::Clock,
        sandbox_delta_time: Option<near_time::Duration>,
    ) -> Self {
        let prev_block_hash = *prev_block_header.hash();
        let (vrf_value, vrf_proof) =
            signer.compute_vrf_with_proof(prev_block_header.random_value().as_ref());
        let random_value = hash(vrf_value.0.as_ref());

        let now = clock.now_utc().unix_timestamp_nanos() as u64;
        #[cfg(feature = "sandbox")]
        let now = now + sandbox_delta_time.unwrap().whole_nanoseconds() as u64;
        #[cfg(not(feature = "sandbox"))]
        debug_assert!(sandbox_delta_time.is_none());
        let time = if now <= prev_block_header.raw_timestamp() {
            prev_block_header.raw_timestamp() + 1
        } else {
            now
        };

        let inner = OptimisticBlockInner {
            block_height: height,
            block_timestamp: time,
            random_value,
            vrf_value,
            vrf_proof,
        };

        let inner_hash = hash(&borsh::to_vec(&inner).expect("Failed to serialize"));
        let hash = combine_hash(&inner_hash, &prev_block_hash);
        let signature = signer.sign_bytes(hash.as_ref());

        Self { prev_block_hash, inner, signature, hash }
    }

    /// Recompute the hash after deserialization.
    pub fn init(&mut self) {
        let inner_hash = hash(&borsh::to_vec(&self.inner).expect("Failed to serialize"));
        self.hash = combine_hash(&inner_hash, &self.prev_block_hash);
    }
}

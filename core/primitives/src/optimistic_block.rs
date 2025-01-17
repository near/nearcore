use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;

use crate::block::BlockHeader;
use crate::hash::{hash, CryptoHash};
use crate::types::BlockHeight;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq)]
pub struct OptimisticBlock {
    pub block_height: BlockHeight,
    pub prev_block_hash: CryptoHash,
    pub block_timestamp: u64,

    // Data to confirm the correctness of randomness beacon output
    pub random_value: CryptoHash,
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,

    /// Signature of the block producer.
    pub signature: Signature,
    pub hash: CryptoHash,
}

impl OptimisticBlock {
    #[cfg(feature = "clock")]
    pub fn new(
        prev: &BlockHeader,
        height: BlockHeight,
        signer: &crate::validator_signer::ValidatorSigner,
        clock: near_time::Clock,
        sandbox_delta_time: Option<near_time::Duration>,
    ) -> Self {
        let prev_block_hash = *prev.hash();
        let (vrf_value, vrf_proof) = signer.compute_vrf_with_proof(prev.random_value().as_ref());
        let random_value = hash(vrf_value.0.as_ref());

        let now = clock.now_utc().unix_timestamp_nanos() as u64;
        #[cfg(feature = "sandbox")]
        let now = now + sandbox_delta_time.unwrap().whole_nanoseconds() as u64;
        #[cfg(not(feature = "sandbox"))]
        debug_assert!(sandbox_delta_time.is_none());
        let time = if now <= prev.raw_timestamp() { prev.raw_timestamp() + 1 } else { now };

        Self {
            block_height: height,
            prev_block_hash,
            block_timestamp: time,
            random_value,
            vrf_value,
            vrf_proof,
            signature: Default::default(),
            hash: Default::default(),
        }
    }

    pub fn produce_optimistic(
        prev: &BlockHeader,
        height: BlockHeight,
        signer: &crate::validator_signer::ValidatorSigner,
        clock: near_time::Clock,
        sandbox_delta_time: Option<near_time::Duration>,
    ) -> Self {
        let mut optimistic_block = Self::new(prev, height, signer, clock, sandbox_delta_time);
        optimistic_block.hash_and_sign(signer);
        optimistic_block
    }

    pub fn hash_and_sign(&mut self, signer: &crate::validator_signer::ValidatorSigner) {
        self.signature = Default::default();
        self.hash = Default::default();
        let hash = hash(&borsh::to_vec(self).expect("Failed to serialize"));
        let signature = signer.sign_bytes(hash.as_ref());
        self.signature = signature;
        self.hash = hash;
    }
}

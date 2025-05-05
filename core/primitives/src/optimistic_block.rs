#[cfg(feature = "clock")]
use crate::block::BlockHeader;
use crate::hash::{CryptoHash, hash};
use crate::types::{BlockHeight, SignatureDifferentiator};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "rand")]
use near_crypto::InMemorySigner;
use near_crypto::Signature;
#[cfg(feature = "rand")]
use near_primitives_core::types::AccountId;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;
#[cfg(feature = "rand")]
use std::str::FromStr;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, ProtocolSchema)]
pub struct OptimisticBlockInner {
    pub prev_block_hash: CryptoHash,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    // Data to confirm the correctness of randomness beacon output
    pub random_value: CryptoHash,
    pub vrf_value: near_crypto::vrf::Value,
    pub vrf_proof: near_crypto::vrf::Proof,
    signature_differentiator: SignatureDifferentiator,
}

#[cfg(feature = "test_features")]
pub enum OptimisticBlockAdvType {
    Normal,
    InvalidVrfValue,
    InvalidVrfProof,
    InvalidRandomValue,
    InvalidTimestamp(u64),
    InvalidPrevBlockHash,
    InvalidSignature,
}

/// An optimistic block is independent of specific chunks and can be generated
/// and distributed immediately after the previous block is processed.
/// This block is shared with the validators and used to optimistically process
/// chunks before they get included in the block.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq, PartialEq, ProtocolSchema)]
#[borsh(init=init)]
pub struct OptimisticBlock {
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
        now: u64,
        sandbox_delta_time: Option<near_time::Duration>,
    ) -> Self {
        use crate::utils::get_block_metadata;
        let prev_block_hash = *prev_block_header.hash();
        let (time, vrf_value, vrf_proof, random_value) =
            get_block_metadata(prev_block_header, signer, now, sandbox_delta_time);

        let inner = OptimisticBlockInner {
            prev_block_hash,
            block_height: height,
            block_timestamp: time,
            random_value,
            vrf_value,
            vrf_proof,
            signature_differentiator: "OptimisticBlock".to_owned(),
        };

        let hash = hash(&borsh::to_vec(&inner).expect("Failed to serialize"));
        let signature = signer.sign_bytes(hash.as_ref());

        Self { inner, signature, hash }
    }

    #[cfg(all(feature = "clock", feature = "test_features"))]
    pub fn adv_produce(
        prev_block_header: &BlockHeader,
        height: BlockHeight,
        signer: &crate::validator_signer::ValidatorSigner,
        now: u64,
        sandbox_delta_time: Option<near_time::Duration>,
        adv_type: OptimisticBlockAdvType,
    ) -> Self {
        let original = Self::produce(prev_block_header, height, signer, now, sandbox_delta_time);
        Self::alter(&original, signer, adv_type)
    }

    #[cfg(all(feature = "clock", feature = "test_features"))]
    pub fn alter(
        original: &OptimisticBlock,
        signer: &crate::validator_signer::ValidatorSigner,
        adv_type: OptimisticBlockAdvType,
    ) -> Self {
        let mut inner = original.inner.clone();
        match adv_type {
            OptimisticBlockAdvType::Normal => {}
            OptimisticBlockAdvType::InvalidVrfValue => {
                inner.vrf_value.0[0] = !inner.vrf_value.0[0];
            }
            OptimisticBlockAdvType::InvalidVrfProof => {
                inner.vrf_proof.0[0] = !inner.vrf_proof.0[0];
            }
            OptimisticBlockAdvType::InvalidRandomValue => {
                inner.random_value.0[0] = !inner.random_value.0[0];
            }
            OptimisticBlockAdvType::InvalidTimestamp(ts) => {
                inner.block_timestamp = ts;
            }
            OptimisticBlockAdvType::InvalidPrevBlockHash => {
                inner.prev_block_hash.0[0] = !inner.prev_block_hash.0[0];
            }
            _ => {}
        }

        let hash = hash(&borsh::to_vec(&inner).expect("Failed to serialize"));
        let signature = if let OptimisticBlockAdvType::InvalidSignature = adv_type {
            signer.sign_bytes(CryptoHash::default().as_ref())
        } else {
            signer.sign_bytes(hash.as_ref())
        };

        Self { inner, signature, hash }
    }

    /// Recompute the hash after deserialization.
    pub fn init(&mut self) {
        self.hash = hash(&borsh::to_vec(&self.inner).expect("Failed to serialize"));
    }

    pub fn height(&self) -> BlockHeight {
        self.inner.block_height
    }

    pub fn hash(&self) -> &CryptoHash {
        &self.hash
    }

    pub fn prev_block_hash(&self) -> &CryptoHash {
        &self.inner.prev_block_hash
    }

    pub fn block_timestamp(&self) -> u64 {
        self.inner.block_timestamp
    }

    pub fn random_value(&self) -> &CryptoHash {
        &self.inner.random_value
    }

    #[cfg(feature = "rand")]
    pub fn new_dummy(height: BlockHeight, prev_hash: CryptoHash) -> Self {
        let signer = InMemorySigner::test_signer(&AccountId::from_str("test".into()).unwrap());
        let (vrf_value, vrf_proof) = signer.compute_vrf_with_proof(Default::default());
        Self {
            inner: OptimisticBlockInner {
                block_height: height,
                prev_block_hash: prev_hash,
                block_timestamp: 0,
                random_value: Default::default(),
                vrf_value,
                vrf_proof,
                signature_differentiator: "test".to_string(),
            },
            signature: Default::default(),
            hash: Default::default(),
        }
    }
}

/// Optimistic block fields which are enough to define unique context for
/// applying chunks in that block. Thus hash of this struct can be used to
/// cache *valid* optimistic blocks.
///
/// This struct is created just so that we can conveniently derive and use
/// `borsh` serialization for it.
#[derive(BorshSerialize)]
pub struct OptimisticBlockKeySource {
    pub height: BlockHeight,
    pub prev_block_hash: CryptoHash,
    pub block_timestamp: u64,
    pub random_seed: CryptoHash,
}

#[derive(Debug, Clone, strum::AsRefStr)]
pub enum BlockToApply {
    Normal(CryptoHash),
    Optimistic(BlockHeight),
}

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
pub struct CachedShardUpdateKey(CryptoHash);

impl CachedShardUpdateKey {
    /// Explicit constructor to minimize the risk of using hashes of other
    /// entities accidentally.
    pub fn new(hash: CryptoHash) -> Self {
        Self(hash)
    }
}

use std::fmt::Debug;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use super::hash::CryptoHash;
use super::serialize::{Decode, Encode};
use super::signer::BlockSigner;
use super::types::PartialSignature;

/// Trait that abstracts ``Header"
pub trait SignedHeader:
    Debug + Clone + Encode + Decode + Send + Sync + Eq + Serialize + DeserializeOwned + 'static
{
    /// Returns hash of the block body.
    fn block_hash(&self) -> CryptoHash;

    /// Returns block index.
    fn index(&self) -> u64;

    /// Returns hash of parent block.
    fn parent_hash(&self) -> CryptoHash;
}

/// Trait that abstracts a ``Block", Is used for both beacon-chain blocks
/// and shard-chain blocks.
pub trait SignedBlock:
    Debug + Clone + Encode + Decode + Send + Sync + Eq + Serialize + DeserializeOwned + 'static
{
    type SignedHeader: SignedHeader;

    /// Returns signed header for given block.
    fn header(&self) -> Self::SignedHeader;

    /// Returns index of given block.
    fn index(&self) -> u64;

    /// Returns hash of the block body.
    fn block_hash(&self) -> CryptoHash;

    /// Signs this block with given signer and returns part of multi signature.
    fn sign(&self, signer: Arc<BlockSigner>) -> PartialSignature {
        signer.bls_sign(self.block_hash().as_ref())
    }

    /// Add signature to multi sign.
    fn add_signature(&mut self, signature: &PartialSignature, authority_id: usize);

    /// Returns stake weight of given block signers.
    fn weight(&self) -> u128;
}

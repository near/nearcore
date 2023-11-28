use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::sharding::{EncodedShardChunk, ShardChunk, ShardChunkHeader};
use crate::types::AccountId;
use crate::validator_signer::ValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use std::sync::Arc;
use arbitrary::{Arbitrary, Unstructured};

/// Serialized TrieNodeWithSize or state value.
pub type TrieValue = std::sync::Arc<[u8]>;

#[derive(BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, Eq, PartialEq)]
/// TODO (#8984): consider supporting format containing trie values only for
/// state part boundaries and storing state items for state part range.
pub enum PartialState {
    /// State represented by the set of unique trie values (`RawTrieNodeWithSize`s and state values).
    TrieValues(Vec<TrieValue>),
}

impl Arbitrary<'_> for PartialState {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let mut trie_values = Vec::new();
        for _ in 0..u.arbitrary::<u8>()? {
            let trie_value: Arc<[u8]> = Arc::from(Vec::<u8>::arbitrary(u)?.into_boxed_slice());
            trie_values.push(trie_value);
        }

        Ok(PartialState::TrieValues(trie_values))
    }
}

impl PartialState {
    pub fn len(&self) -> usize {
        let Self::TrieValues(values) = self;
        values.len()
    }
}

/// Double signed block.
#[derive(BorshSerialize, BorshDeserialize, Arbitrary, PartialEq, Eq, Clone, Debug)]
pub struct BlockDoubleSign {
    pub left_block_header: Vec<u8>,
    pub right_block_header: Vec<u8>,
}

impl std::fmt::Display for BlockDoubleSign {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

/// Invalid chunk (body of the chunk doesn't match proofs or invalid encoding).
#[derive(BorshSerialize, BorshDeserialize, Arbitrary, PartialEq, Eq, Clone, Debug)]
pub struct ChunkProofs {
    /// Encoded block header that contains invalid chunk.
    pub block_header: Vec<u8>,
    /// Merkle proof of inclusion of this chunk.
    pub merkle_proof: MerklePath,
    /// Invalid chunk in an encoded form or in a decoded form.
    pub chunk: MaybeEncodedShardChunk,
}

/// Either `EncodedShardChunk` or `ShardChunk`. Used for `ChunkProofs`.
/// `Decoded` is used to avoid re-encoding an already decoded chunk to construct a challenge.
/// `Encoded` is still needed in case a challenge challenges an invalid encoded chunk that can't be
/// decoded.
#[derive(BorshSerialize, BorshDeserialize, Arbitrary, PartialEq, Eq, Clone, Debug)]
pub enum MaybeEncodedShardChunk {
    Encoded(EncodedShardChunk),
    Decoded(ShardChunk),
}

/// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
#[derive(BorshSerialize, BorshDeserialize, Arbitrary, PartialEq, Eq, Clone, Debug)]
pub struct ChunkState {
    /// Encoded prev block header.
    pub prev_block_header: Vec<u8>,
    /// Encoded block header that contains invalid chunnk.
    pub block_header: Vec<u8>,
    /// Merkle proof in inclusion of prev chunk.
    pub prev_merkle_proof: MerklePath,
    /// Previous chunk that contains transactions.
    pub prev_chunk: ShardChunk,
    /// Merkle proof of inclusion of this chunk.
    pub merkle_proof: MerklePath,
    /// Invalid chunk header.
    pub chunk_header: ShardChunkHeader,
    /// Partial state that was affected by transactions of given chunk.
    pub partial_state: PartialState,
}

#[derive(BorshSerialize, BorshDeserialize, Arbitrary, PartialEq, Eq, Clone, Debug)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
pub enum ChallengeBody {
    BlockDoubleSign(BlockDoubleSign),
    ChunkProofs(ChunkProofs),
    ChunkState(ChunkState),
}

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
#[borsh(init=init)]
pub struct Challenge {
    pub body: ChallengeBody,
    pub account_id: AccountId,
    pub signature: Signature,

    #[borsh(skip)]
    pub hash: CryptoHash,
}

impl Arbitrary<'_> for Challenge {
    fn arbitrary(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<Self> {
        let body = ChallengeBody::arbitrary(u)?;
        let account_id = AccountId::arbitrary(u)?;
        let signature = Signature::arbitrary(u)?;
        let hash = CryptoHash::arbitrary(u)?;
        Ok(Challenge { body, account_id, signature, hash })
    }
}

impl Challenge {
    pub fn init(&mut self) {
        self.hash = CryptoHash::hash_borsh(&self.body);
    }

    pub fn produce(body: ChallengeBody, signer: &dyn ValidatorSigner) -> Self {
        let (hash, signature) = signer.sign_challenge(&body);
        Self { body, account_id: signer.validator_id().clone(), signature, hash }
    }
}

pub type Challenges = Vec<Challenge>;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct SlashedValidator {
    pub account_id: AccountId,
    pub is_double_sign: bool,
}

impl SlashedValidator {
    pub fn new(account_id: AccountId, is_double_sign: bool) -> Self {
        SlashedValidator { account_id, is_double_sign }
    }
}

/// Result of checking challenge, contains which accounts to slash.
/// If challenge is invalid this is sender, otherwise author of chunk (and possibly other participants that signed invalid blocks).
pub type ChallengesResult = Vec<SlashedValidator>;

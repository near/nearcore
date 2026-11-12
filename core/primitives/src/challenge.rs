use crate::hash::CryptoHash;
use crate::merkle::MerklePath;
use crate::receipt::{Receipt, ReceiptEnum};
use crate::sharding::{EncodedShardChunk, ShardChunk, ShardChunkHeader};
use crate::state::PartialState;
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_primitives_core::types::BlockHeight;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::Debug;
use std::io::{Error, ErrorKind, Read};

/// Double signed block.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct BlockDoubleSign {
    pub left_block_header: Vec<u8>,
    pub right_block_header: Vec<u8>,
}

/// Invalid chunk (body of the chunk doesn't match proofs or invalid encoding).
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct ChunkProofs {
    /// Encoded block header that contains invalid chunk.
    pub block_header: Vec<u8>,
    /// Merkle proof of inclusion of this chunk.
    pub merkle_proof: MerklePath,
    /// Invalid chunk in an encoded form or in a decoded form.
    pub chunk: Box<MaybeEncodedShardChunk>,
}

/// Either `EncodedShardChunk` or `ShardChunk`. Used for `ChunkProofs`.
/// `Decoded` is used to avoid re-encoding an already decoded chunk to construct a challenge.
/// `Encoded` is still needed in case a challenge challenges an invalid encoded chunk that can't be
/// decoded.
#[allow(clippy::large_enum_variant)] // both variants are large
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum MaybeEncodedShardChunk {
    Encoded(EncodedShardChunk) = 0,
    Decoded(ShardChunk) = 1,
}

/// Doesn't match post-{state root, outgoing receipts, gas used, etc} results after applying previous chunk.
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct ChunkState {
    /// Encoded prev block header.
    pub prev_block_header: Vec<u8>,
    /// Block height.
    /// TODO: block header is likely to be needed if we ever want to support
    /// challenges fully.
    pub block_height: BlockHeight,
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

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
// TODO(#1313): Use Box
#[allow(clippy::large_enum_variant)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ChallengeBody {
    BlockDoubleSign(BlockDoubleSign) = 0,
    ChunkProofs(ChunkProofs) = 1,
    ChunkState(ChunkState) = 2,
}

impl ChallengeBody {
    /// Whether the body holds a chunk with an ECC or an `ActionReceiptV3`, which the previous
    /// release fails to decode.
    fn holds_ecc_or_receipt_v3(&self) -> bool {
        let chunk = match self {
            Self::BlockDoubleSign(_) => return false,
            Self::ChunkProofs(proofs) => match proofs.chunk.as_ref() {
                MaybeEncodedShardChunk::Decoded(chunk) => chunk,
                MaybeEncodedShardChunk::Encoded(_) => return false,
            },
            Self::ChunkState(state) => &state.prev_chunk,
        };
        chunk.to_transactions().iter().any(|tx| tx.as_unsigned().is_some())
            || chunk.prev_outgoing_receipts().iter().any(is_action_receipt_v3)
    }
}

/// Exhaustive, so that a new receipt variant has to be classified here.
fn is_action_receipt_v3(receipt: &Receipt) -> bool {
    match receipt.receipt() {
        ReceiptEnum::ActionV3(_) | ReceiptEnum::PromiseYieldV3(_) => true,
        ReceiptEnum::Action(_)
        | ReceiptEnum::Data(_)
        | ReceiptEnum::PromiseYield(_)
        | ReceiptEnum::PromiseResume(_)
        | ReceiptEnum::GlobalContractDistribution(_)
        | ReceiptEnum::ActionV2(_)
        | ReceiptEnum::PromiseYieldV2(_) => false,
    }
}

#[derive(BorshSerialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct Challenge {
    pub body: ChallengeBody,
    pub account_id: AccountId,
    pub signature: Signature,

    #[borsh(skip)]
    pub hash: CryptoHash,
}

impl Challenge {
    pub fn init(&mut self) {
        self.hash = CryptoHash::hash_borsh(&self.body);
    }
}

/// Refuses a challenge holding an ECC or an `ActionReceiptV3`, which the previous release cannot
/// decode, so both releases drop a block carrying one without a protocol version check.
impl BorshDeserialize for Challenge {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let body = ChallengeBody::deserialize_reader(reader)?;
        if body.holds_ecc_or_receipt_v3() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "challenge holds an ECC or a V3 receipt",
            ));
        }
        let account_id = AccountId::deserialize_reader(reader)?;
        let signature = Signature::deserialize_reader(reader)?;
        let mut challenge = Self { body, account_id, signature, hash: CryptoHash::default() };
        challenge.init();
        Ok(challenge)
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SlashedValidator {
    pub account_id: AccountId,
    pub is_double_sign: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receipt::{ActionReceiptV3, AuthorizationContext, ReceiptV0};
    use crate::sharding::ShardChunkHeader;
    use crate::transaction::{EccTransaction, SignedTransaction, TransactionEnvelope};
    use crate::version::PROTOCOL_VERSION;
    use near_crypto::KeyType;
    use near_primitives_core::types::{Balance, ShardId};

    fn chunk_with(transactions: Vec<TransactionEnvelope>) -> ShardChunk {
        chunk_with_receipts(transactions, vec![])
    }

    fn chunk_with_receipts(
        transactions: Vec<TransactionEnvelope>,
        receipts: Vec<Receipt>,
    ) -> ShardChunk {
        let hash = CryptoHash::default();
        let header = ShardChunkHeader::new_dummy(1, ShardId::new(0), hash, PROTOCOL_VERSION);
        ShardChunk::new(header, transactions, receipts)
    }

    fn action_receipt_v3() -> Receipt {
        let account_id: AccountId = "wallet.near".parse().unwrap();
        Receipt::V0(ReceiptV0 {
            predecessor_id: account_id.clone(),
            receiver_id: account_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::ActionV3(ActionReceiptV3 {
                signer_id: account_id,
                refund_to: None,
                authorization_context: AuthorizationContext::ExternalContractCall,
                gas_price: Balance::ZERO,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![],
            }),
        })
    }

    fn receipt_v3_chunk() -> ShardChunk {
        // Signed transactions only, so the receipt is the only reason to refuse.
        let refund = Receipt::new_balance_refund(&"alice.near".parse().unwrap(), Balance::ZERO);
        chunk_with_receipts(signed_txs(), vec![refund, action_receipt_v3()])
    }

    fn signed_txs() -> Vec<TransactionEnvelope> {
        vec![SignedTransaction::empty(CryptoHash::default()).into()]
    }

    fn ecc_txs() -> Vec<TransactionEnvelope> {
        let ecc = EccTransaction::new("wallet.near".parse().unwrap(), vec![1, 2, 3]);
        // A signed transaction next to the ECC, so the check cannot look only at the first one.
        signed_txs().into_iter().chain([ecc.into()]).collect()
    }

    fn challenge(body: ChallengeBody) -> Challenge {
        let account_id = "alice.near".parse().unwrap();
        let signature = Signature::empty(KeyType::ED25519);
        let mut challenge = Challenge { body, account_id, signature, hash: CryptoHash::default() };
        challenge.init();
        challenge
    }

    fn chunk_proofs(chunk: ShardChunk) -> Challenge {
        challenge(ChallengeBody::ChunkProofs(ChunkProofs {
            block_header: vec![1],
            merkle_proof: vec![],
            chunk: Box::new(MaybeEncodedShardChunk::Decoded(chunk)),
        }))
    }

    fn chunk_state(prev_chunk: ShardChunk) -> Challenge {
        let chunk_header = ShardChunkHeader::new_dummy(
            2,
            ShardId::new(0),
            CryptoHash::default(),
            PROTOCOL_VERSION,
        );
        challenge(ChallengeBody::ChunkState(ChunkState {
            prev_block_header: vec![1],
            block_height: 2,
            prev_merkle_proof: vec![],
            prev_chunk,
            merkle_proof: vec![],
            chunk_header,
            partial_state: PartialState::default(),
        }))
    }

    fn assert_refused(challenge: &Challenge) {
        let bytes = borsh::to_vec(challenge).unwrap();
        let err = Challenge::try_from_slice(&bytes).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("challenge holds an ECC or a V3 receipt"), "{err}");
    }

    fn assert_decodes(challenge: &Challenge) {
        let bytes = borsh::to_vec(challenge).unwrap();
        let decoded = Challenge::try_from_slice(&bytes).unwrap();
        assert_eq!(&decoded, challenge);
        assert_eq!(decoded.hash, CryptoHash::hash_borsh(&challenge.body));
    }

    #[test]
    fn test_chunk_proofs_with_ecc_refused() {
        assert_refused(&chunk_proofs(chunk_with(ecc_txs())));
    }

    #[test]
    fn test_chunk_state_with_ecc_refused() {
        assert_refused(&chunk_state(chunk_with(ecc_txs())));
    }

    #[test]
    fn test_chunk_proofs_with_receipt_v3_refused() {
        assert_refused(&chunk_proofs(receipt_v3_chunk()));
    }

    #[test]
    fn test_chunk_state_with_receipt_v3_refused() {
        assert_refused(&chunk_state(receipt_v3_chunk()));
    }

    #[test]
    fn test_chunk_state_with_older_receipts_decodes() {
        let refund = Receipt::new_balance_refund(&"alice.near".parse().unwrap(), Balance::ZERO);
        assert_decodes(&chunk_state(chunk_with_receipts(signed_txs(), vec![refund])));
    }

    #[test]
    fn test_chunk_proofs_with_signed_txs_decodes() {
        assert_decodes(&chunk_proofs(chunk_with(signed_txs())));
    }

    #[test]
    fn test_chunk_state_with_signed_txs_decodes() {
        assert_decodes(&chunk_state(chunk_with(signed_txs())));
    }

    #[test]
    fn test_block_double_sign_decodes() {
        let body = BlockDoubleSign { left_block_header: vec![1], right_block_header: vec![2] };
        assert_decodes(&challenge(ChallengeBody::BlockDoubleSign(body)));
    }
}

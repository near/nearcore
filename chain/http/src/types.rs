use std::collections::HashMap;

use near_protos::serde::b64_format as protos_b64_format;
use primitives::beacon::{BeaconBlockHeader, SignedBeaconBlockHeader};
use primitives::chain::{ReceiptBlock, ShardBlock, ShardBlockHeader, SignedShardBlock};
use primitives::crypto::aggregate_signature::BlsPublicKey;
use primitives::crypto::group_signature::GroupSignature;
use primitives::crypto::signature::{bs58_serializer, PublicKey};
use primitives::hash::{bs58_format, CryptoHash};
use primitives::transaction::{
    FinalTransactionResult, LogEntry, ReceiptTransaction, SignedTransaction, TransactionResult,
};
use primitives::types::{
    AccountId, AuthorityStake, Balance, BlockIndex, MerkleHash, Nonce, ShardId,
};

/// RPC errors that JSONRPC and HTTP that APIs can use to return meaningful type of errors.
pub enum RPCError {
    /// Error if input arguments don't match the expected types or format.
    BadRequest(String),
    /// Method for JSON RPC not found.
    MethodNotFound(String),
    /// Item / account not found.
    NotFound,
    /// Service is not available.
    ServiceUnavailable(String),
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountRequest {
    pub account_id: AccountId,
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountResponse {
    pub account_id: AccountId,
    pub amount: Balance,
    pub stake: Balance,
    pub nonce: Nonce,
    pub public_keys: Vec<PublicKey>,
    #[serde(with = "bs58_format")]
    pub code_hash: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct CallViewFunctionRequest {
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CallViewFunctionResponse {
    pub result: Vec<u8>,
    pub logs: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct ViewStateRequest {
    pub contract_account_id: AccountId,
}

#[derive(Serialize, Deserialize)]
pub struct ViewStateResponse {
    pub contract_account_id: AccountId,
    pub values: HashMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AuthorityProposalResponse {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "bs58_serializer")]
    pub bls_public_key: BlsPublicKey,
    pub amount: Balance,
}

impl From<AuthorityStake> for AuthorityProposalResponse {
    fn from(proposal: AuthorityStake) -> Self {
        AuthorityProposalResponse {
            account_id: proposal.account_id,
            public_key: proposal.public_key,
            bls_public_key: proposal.bls_public_key,
            amount: proposal.amount,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct BeaconBlockHeaderResponse {
    #[serde(with = "bs58_format")]
    pub parent_hash: CryptoHash,
    pub index: BlockIndex,
    pub authority_proposal: Vec<AuthorityProposalResponse>,
    #[serde(with = "bs58_format")]
    pub shard_block_hash: CryptoHash,
}

impl From<BeaconBlockHeader> for BeaconBlockHeaderResponse {
    fn from(header: BeaconBlockHeader) -> Self {
        let authority_proposal =
            header.authority_proposal.into_iter().map(std::convert::Into::into).collect();
        BeaconBlockHeaderResponse {
            parent_hash: header.parent_hash,
            index: header.index,
            authority_proposal,
            shard_block_hash: header.shard_block_hash,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedBeaconBlockResponse {
    pub header: BeaconBlockHeaderResponse,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl From<SignedBeaconBlockHeader> for SignedBeaconBlockResponse {
    fn from(header: SignedBeaconBlockHeader) -> Self {
        SignedBeaconBlockResponse {
            header: header.body.into(),
            hash: header.hash,
            signature: header.signature,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ShardBlockHeaderResponse {
    #[serde(with = "bs58_format")]
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: BlockIndex,
    #[serde(with = "bs58_format")]
    pub merkle_root_state: MerkleHash,
}

impl From<ShardBlockHeader> for ShardBlockHeaderResponse {
    fn from(header: ShardBlockHeader) -> Self {
        ShardBlockHeaderResponse {
            parent_hash: header.parent_hash,
            shard_id: header.shard_id,
            index: header.index,
            merkle_root_state: header.merkle_root_state,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ShardBlockResponse {
    pub header: ShardBlockHeaderResponse,
    pub transactions: Vec<SignedTransactionResponse>,
    pub receipts: Vec<ReceiptBlock>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedTransactionResponse {
    #[serde(with = "protos_b64_format")]
    pub body: near_protos::signed_transaction::SignedTransaction,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ReceiptResponse {
    #[serde(with = "protos_b64_format")]
    pub body: near_protos::receipt::ReceiptTransaction,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

impl From<SignedTransaction> for SignedTransactionResponse {
    fn from(transaction: SignedTransaction) -> Self {
        let hash = transaction.get_hash();
        Self { body: transaction.into(), hash }
    }
}

impl From<ShardBlock> for ShardBlockResponse {
    fn from(block: ShardBlock) -> Self {
        let transactions =
            block.transactions.into_iter().map(SignedTransactionResponse::from).collect();
        ShardBlockResponse { header: block.header.into(), transactions, receipts: block.receipts }
    }
}

impl From<ReceiptTransaction> for ReceiptResponse {
    fn from(receipt: ReceiptTransaction) -> Self {
        let hash = receipt.nonce;
        Self { body: receipt.into(), hash }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedShardBlockResponse {
    pub body: ShardBlockResponse,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

impl From<SignedShardBlock> for SignedShardBlockResponse {
    fn from(block: SignedShardBlock) -> Self {
        SignedShardBlockResponse { body: block.body.into(), hash: block.hash }
    }
}

#[derive(Serialize, Deserialize)]
pub struct GetBlockByHashRequest {
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct GetBlocksByIndexRequest {
    pub start: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct SignedShardBlocksResponse {
    pub blocks: Vec<SignedShardBlockResponse>,
}

#[derive(Serialize, Deserialize)]
pub struct SignedBeaconBlocksResponse {
    pub blocks: Vec<SignedBeaconBlockResponse>,
}

#[derive(Serialize, Deserialize)]
pub struct GetTransactionRequest {
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionResultResponse {
    pub result: TransactionResult,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionFinalResultResponse {
    /// Final result of given transaction, including it's receipts.
    pub result: FinalTransactionResult,
}

#[derive(Serialize, Deserialize)]
pub struct SubmitTransactionResponse {
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionInfoResponse {
    pub transaction: SignedTransactionResponse,
    pub block_index: BlockIndex,
    pub result: TransactionResult,
}

#[derive(Serialize, Deserialize)]
pub struct ReceiptInfoResponse {
    pub receipt: ReceiptResponse,
    pub block_index: BlockIndex,
    pub result: TransactionResult,
}

#[derive(Serialize, Deserialize)]
pub struct SubmitTransactionRequest {
    #[serde(with = "protos_b64_format")]
    pub transaction: near_protos::signed_transaction::SignedTransaction,
}

#[derive(Serialize, Deserialize)]
pub struct HealthzResponse {
    #[serde(with = "bs58_format")]
    pub genesis_hash: CryptoHash,
    pub latest_block_index: BlockIndex,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Vec<serde_json::Value>,
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcResponseError {
    code: i64,
    message: String,
    data: serde_json::Value,
}

impl From<RPCError> for JsonRpcResponseError {
    fn from(error: RPCError) -> JsonRpcResponseError {
        let (code, message) = match error {
            RPCError::BadRequest(msg) => (-32602, format!("Bad request: {}", msg)),
            RPCError::MethodNotFound(msg) => (-32601, format!("Method not found: {}", msg)),
            RPCError::NotFound => (-30000, format!("Not found")),
            RPCError::ServiceUnavailable(msg) => (-32603, format!("Service unavailable: {}", msg)),
        };
        JsonRpcResponseError { code, message, data: serde_json::Value::Null }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcResponseError>,
    pub id: String,
}

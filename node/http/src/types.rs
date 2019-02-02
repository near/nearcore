use std::collections::HashMap;

use beacon::types::{BeaconBlock, BeaconBlockHeader, SignedBeaconBlock};
use near_protos::serde::b64_format as protos_b64_format;
use primitives::hash::{bs58_format, CryptoHash};
use primitives::signature::{bs58_pub_key_format, PublicKey};
use primitives::types::{
    AccountId, AuthorityStake, Balance, GroupSignature, MerkleHash, ShardId
};
use chain::{ShardBlock, ShardBlockHeader, SignedShardBlock, ReceiptBlock};
use transaction::{
    FinalTransactionResult, SignedTransaction, TransactionResult,
};

#[derive(Serialize, Deserialize)]
pub struct ViewAccountRequest {
    pub account_id: AccountId,
}

#[derive(Serialize, Deserialize)]
pub struct ViewAccountResponse {
    pub account_id: AccountId,
    pub amount: Balance,
    pub stake: Balance,
    pub nonce: u64,
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
    #[serde(with = "bs58_pub_key_format")]
    pub public_key: PublicKey,
    pub amount: u64,
}

impl From<AuthorityStake> for AuthorityProposalResponse {
    fn from(proposal: AuthorityStake) -> Self {
        AuthorityProposalResponse {
            account_id: proposal.account_id,
            public_key: proposal.public_key,
            amount: proposal.amount,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct BeaconBlockHeaderResponse {
    #[serde(with = "bs58_format")]
    pub parent_hash: CryptoHash,
    pub index: u64,
    pub authority_proposal: Vec<AuthorityProposalResponse>,
    #[serde(with = "bs58_format")]
    pub shard_block_hash: CryptoHash,
}

impl From<BeaconBlockHeader> for BeaconBlockHeaderResponse {
    fn from(header: BeaconBlockHeader) -> Self {
        let authority_proposal = header.authority_proposal.into_iter()
            .map(|x| x.into())
            .collect();
        BeaconBlockHeaderResponse {
            parent_hash: header.shard_block_hash,
            index: header.index,
            authority_proposal,
            shard_block_hash: header.shard_block_hash,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct BeaconBlockResponse {
    pub header: BeaconBlockHeaderResponse,
}

impl From<BeaconBlock> for BeaconBlockResponse {
    fn from(block: BeaconBlock) -> Self {
        BeaconBlockResponse {
            header: block.header.into()
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedBeaconBlockResponse {
    pub body: BeaconBlockResponse,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl From<SignedBeaconBlock> for SignedBeaconBlockResponse {
    fn from(block: SignedBeaconBlock) -> Self {
        SignedBeaconBlockResponse {
            body: block.body.into(),
            hash: block.hash,
            signature: block.signature,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ShardBlockHeaderResponse {
    #[serde(with = "bs58_format")]
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: u64,
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

impl From<SignedTransaction> for SignedTransactionResponse {
    fn from(transaction: SignedTransaction) -> Self {
        Self {
            body: transaction.clone().into(),
            hash: transaction.get_hash(),
        }
    }
}

impl From<ShardBlock> for ShardBlockResponse {
    fn from(block: ShardBlock) -> Self {
        let transactions = block.transactions.into_iter()
            .map(SignedTransactionResponse::from)
            .collect();
        ShardBlockResponse {
            header: block.header.into(),
            transactions,
            receipts: block.receipts,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedShardBlockResponse {
    pub body: ShardBlockResponse,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
    pub signature: GroupSignature,
}

impl From<SignedShardBlock> for SignedShardBlockResponse {
    fn from(block: SignedShardBlock) -> Self {
        SignedShardBlockResponse {
            body: block.body.into(),
            hash: block.hash,
            signature: block.signature,
        }
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
    pub blocks: Vec<SignedShardBlockResponse>
}

#[derive(Serialize, Deserialize)]
pub struct GetTransactionRequest {
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash
}

#[derive(Serialize, Deserialize)]
pub struct TransactionResultResponse {
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
    pub block_index: u64,
    pub result: TransactionResult,
}

#[derive(Serialize, Deserialize)]
pub struct SubmitTransactionRequest {
    #[serde(with = "protos_b64_format")]
    pub transaction: near_protos::signed_transaction::SignedTransaction,
}

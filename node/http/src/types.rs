use std::collections::HashMap;

use beacon::authority::AuthorityStake;
use beacon::types::{BeaconBlock, BeaconBlockHeader, SignedBeaconBlock};
use primitives::hash::{bs58_format, CryptoHash};
use primitives::signature::{bs58_pub_key_format, PublicKey};
use primitives::types::{
    AccountId, AuthorityMask, Balance, MerkleHash, ShardId, Transaction,
    TransactionBody,
};
use shard::{ShardBlock, ShardBlockHeader, SignedShardBlock};

#[derive(Serialize, Deserialize)]
pub struct SendMoneyRequest {
    pub nonce: u64,
    pub originator: AccountId,
    pub receiver_account_id: AccountId,
    pub amount: Balance,
}

#[derive(Serialize, Deserialize)]
pub struct StakeRequest {
    pub nonce: u64,
    pub originator: AccountId,
    pub amount: Balance,
}

#[derive(Serialize, Deserialize)]
pub struct DeployContractRequest {
    pub nonce: u64,
    pub originator: AccountId,
    pub contract_account_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
    #[serde(with = "bs58_pub_key_format")]
    pub public_key: PublicKey,
}

#[derive(Serialize, Deserialize)]
pub struct CreateAccountRequest {
    pub nonce: u64,
    pub originator: AccountId,
    pub new_account_id: AccountId,
    pub amount: u64,
    #[serde(with = "bs58_pub_key_format")]
    pub public_key: PublicKey
}

#[derive(Serialize, Deserialize)]
pub struct SwapKeyRequest {
    pub nonce: u64,
    pub account: AccountId,
    #[serde(with = "bs58_pub_key_format")]
    pub current_key: PublicKey,
    #[serde(with = "bs58_pub_key_format")]
    pub new_key: PublicKey,
}

#[derive(Serialize, Deserialize)]
pub struct ScheduleFunctionCallRequest {
    pub nonce: u64,
    pub originator: AccountId,
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<u8>,
    pub amount: Balance,
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
    pub nonce: u64,
    #[serde(with = "bs58_format")]
    pub code_hash: CryptoHash,
}

#[derive(Serialize, Deserialize)]
pub struct CallViewFunctionRequest {
    pub originator: AccountId,
    pub contract_account_id: AccountId,
    pub method_name: String,
    pub args: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CallViewFunctionResponse {
    pub result: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PreparedTransactionBodyResponse {
    pub body: TransactionBody,
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
    // TODO(#298): should have a format for MultiSignature
    pub signature: Vec<String>,
    pub authority_mask: AuthorityMask,
}

impl From<SignedBeaconBlock> for SignedBeaconBlockResponse {
    fn from(block: SignedBeaconBlock) -> Self {
        let signature = block.signature.iter()
            .map(String::from)
            .collect();
        SignedBeaconBlockResponse {
            body: block.body.into(),
            hash: block.hash,
            signature,
            authority_mask: block.authority_mask,
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
    // TODO(#301): should have a bs58 format for TransactionResponse
    pub transactions: Vec<Transaction>,
    // TODO(#301): should have a bs58 format for TransactionResponse
    pub new_receipts: Vec<Transaction>,
}

impl From<ShardBlock> for ShardBlockResponse {
    fn from(block: ShardBlock) -> Self {
        ShardBlockResponse {
            header: block.header.into(),
            transactions: block.transactions,
            new_receipts: block.new_receipts,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SignedShardBlockResponse {
    pub body: ShardBlockResponse,
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    // TODO(#298): should have a format for MultiSignature
    pub signature: Vec<String>,
}

impl From<SignedShardBlock> for SignedShardBlockResponse {
    fn from(block: SignedShardBlock) -> Self {
        let signature = block.signature.iter()
            .map(String::from)
            .collect();
        SignedShardBlockResponse {
            body: block.body.into(),
            hash: block.hash,
            authority_mask: block.authority_mask,
            signature,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct GetBlockByHashRequest {
    #[serde(with = "bs58_format")]
    pub hash: CryptoHash,
}

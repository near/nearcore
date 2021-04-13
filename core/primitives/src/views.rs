//! This module defines "stable" internal API to view internal data using view_client.
//!
//! These types should only change when we cannot avoid this. Thus, when the counterpart internal
//! type gets changed, the view should preserve the old shape and only re-map the necessary bits
//! from the source structure in the relevant `From<SourceStruct>` impl.
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use near_crypto::{PublicKey, Signature};

use crate::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use crate::block::{Block, BlockHeader};
use crate::block_header::{
    BlockHeaderInnerLite, BlockHeaderInnerRest, BlockHeaderInnerRestV2, BlockHeaderV1,
    BlockHeaderV2,
};
#[cfg(feature = "protocol_feature_block_header_v3")]
use crate::block_header::{BlockHeaderInnerRestV3, BlockHeaderV3};
use crate::challenge::{Challenge, ChallengesResult};
use crate::contract::ContractCode;
use crate::errors::TxExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::merkle::MerklePath;
use crate::receipt::{ActionReceipt, DataReceipt, DataReceiver, Receipt, ReceiptEnum};
use crate::serialize::{
    base64_format, from_base64, option_base64_format, option_u128_dec_format, to_base64,
    u128_dec_format, u64_dec_format,
};
#[cfg(not(feature = "protocol_feature_block_header_v3"))]
use crate::sharding::ShardChunkHeaderV2;
use crate::sharding::{ChunkHash, ShardChunk, ShardChunkHeader, ShardChunkHeaderInner};
#[cfg(feature = "protocol_feature_block_header_v3")]
use crate::sharding::{ShardChunkHeaderInnerV2, ShardChunkHeaderV3};
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, ExecutionOutcome, ExecutionOutcomeWithIdAndProof, ExecutionStatus,
    FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use crate::types::{
    AccountId, AccountWithPublicKey, Balance, BlockHeight, CompiledContractCache, EpochHeight,
    EpochId, FunctionArgs, Gas, Nonce, NumBlocks, ShardId, StateChangeCause, StateChangeKind,
    StateChangeValue, StateChangeWithCause, StateChangesRequest, StateRoot, StorageUsage, StoreKey,
    StoreValue, ValidatorKickoutReason,
};
use crate::version::{ProtocolVersion, Version};
use validator_stake_view::ValidatorStakeView;

/// A view of the account
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub locked: Balance,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    /// TODO(2271): deprecated.
    #[serde(default)]
    pub storage_paid_at: BlockHeight,
}

/// A view of the contract code.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct ContractCodeView {
    #[serde(rename = "code_base64", with = "base64_format")]
    pub code: Vec<u8>,
    pub hash: CryptoHash,
}

/// State for the view call.
#[derive(Debug)]
pub struct ViewApplyState {
    /// Currently building block height.
    pub block_height: BlockHeight,
    /// Prev block hash
    pub prev_block_hash: CryptoHash,
    /// Currently building block hash
    pub block_hash: CryptoHash,
    /// Current epoch id
    pub epoch_id: EpochId,
    /// Current epoch height
    pub epoch_height: EpochHeight,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// Current Protocol version when we apply the state transition
    pub current_protocol_version: ProtocolVersion,
    /// Cache for compiled contracts.
    pub cache: Option<Arc<dyn CompiledContractCache>>,
    /// EVM chain ID
    #[cfg(feature = "protocol_feature_evm")]
    pub evm_chain_id: u64,
}

impl From<&Account> for AccountView {
    fn from(account: &Account) -> Self {
        AccountView {
            amount: account.amount(),
            locked: account.locked(),
            code_hash: account.code_hash(),
            storage_usage: account.storage_usage(),
            storage_paid_at: 0,
        }
    }
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        (&account).into()
    }
}

impl From<&AccountView> for Account {
    fn from(view: &AccountView) -> Self {
        Account::new(view.amount, view.locked, view.code_hash, view.storage_usage)
    }
}

impl From<AccountView> for Account {
    fn from(view: AccountView) -> Self {
        (&view).into()
    }
}

impl From<ContractCode> for ContractCodeView {
    fn from(contract_code: ContractCode) -> Self {
        ContractCodeView { code: contract_code.code, hash: contract_code.hash }
    }
}

impl From<ContractCodeView> for ContractCode {
    fn from(contract_code: ContractCodeView) -> Self {
        ContractCode { code: contract_code.code, hash: contract_code.hash }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub enum AccessKeyPermissionView {
    FunctionCall {
        #[serde(with = "option_u128_dec_format")]
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<String>,
    },
    FullAccess,
}

impl From<AccessKeyPermission> for AccessKeyPermissionView {
    fn from(permission: AccessKeyPermission) -> Self {
        match permission {
            AccessKeyPermission::FunctionCall(func_call) => AccessKeyPermissionView::FunctionCall {
                allowance: func_call.allowance,
                receiver_id: func_call.receiver_id,
                method_names: func_call.method_names,
            },
            AccessKeyPermission::FullAccess => AccessKeyPermissionView::FullAccess,
        }
    }
}

impl From<AccessKeyPermissionView> for AccessKeyPermission {
    fn from(view: AccessKeyPermissionView) -> Self {
        match view {
            AccessKeyPermissionView::FunctionCall { allowance, receiver_id, method_names } => {
                AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance,
                    receiver_id,
                    method_names,
                })
            }
            AccessKeyPermissionView::FullAccess => AccessKeyPermission::FullAccess,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccessKeyView {
    pub nonce: Nonce,
    pub permission: AccessKeyPermissionView,
}

impl From<AccessKey> for AccessKeyView {
    fn from(access_key: AccessKey) -> Self {
        Self { nonce: access_key.nonce, permission: access_key.permission.into() }
    }
}

impl From<AccessKeyView> for AccessKey {
    fn from(view: AccessKeyView) -> Self {
        Self { nonce: view.nonce, permission: view.permission.into() }
    }
}

/// Set of serialized TrieNodes that are encoded in base64. Represent proof of inclusion of some TrieNode in the MerkleTrie.
pub type TrieProofPath = Vec<String>;

/// Item of the state, key and value are serialized in base64 and proof for inclusion of given state item.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct StateItem {
    pub key: String,
    pub value: String,
    pub proof: TrieProofPath,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ViewStateResult {
    pub values: Vec<StateItem>,
    pub proof: TrieProofPath,
}

#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default,
)]
pub struct CallResult {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct QueryError {
    pub error: String,
    pub logs: Vec<String>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AccessKeyInfoView {
    pub public_key: PublicKey,
    pub access_key: AccessKeyView,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct AccessKeyList {
    pub keys: Vec<AccessKeyInfoView>,
}

impl std::iter::FromIterator<AccessKeyInfoView> for AccessKeyList {
    fn from_iter<I: IntoIterator<Item = AccessKeyInfoView>>(iter: I) -> Self {
        Self { keys: iter.into_iter().collect() }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, Clone)]
pub enum QueryResponseKind {
    ViewAccount(AccountView),
    ViewCode(ContractCodeView),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    AccessKey(AccessKeyView),
    AccessKeyList(AccessKeyList),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "request_type", rename_all = "snake_case")]
pub enum QueryRequest {
    ViewAccount {
        account_id: AccountId,
    },
    ViewCode {
        account_id: AccountId,
    },
    ViewState {
        account_id: AccountId,
        #[serde(rename = "prefix_base64", with = "base64_format")]
        prefix: StoreKey,
    },
    ViewAccessKey {
        account_id: AccountId,
        public_key: PublicKey,
    },
    ViewAccessKeyList {
        account_id: AccountId,
    },
    CallFunction {
        account_id: AccountId,
        method_name: String,
        #[serde(rename = "args_base64", with = "base64_format")]
        args: FunctionArgs,
    },
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq, Clone)]
pub struct QueryResponse {
    pub kind: QueryResponseKind,
    pub block_height: BlockHeight,
    pub block_hash: CryptoHash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockHeight,
    pub latest_state_root: CryptoHash,
    pub latest_block_time: DateTime<Utc>,
    pub syncing: bool,
}

// TODO: add more information to ValidatorInfo
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ValidatorInfo {
    pub account_id: AccountId,
    pub is_slashed: bool,
}

// TODO: add more information to status.
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusResponse {
    /// Binary version.
    pub version: Version,
    /// Unique chain id.
    pub chain_id: String,
    /// Currently active protocol version.
    pub protocol_version: u32,
    /// Latest protocol version that this client supports.
    pub latest_protocol_version: u32,
    /// Address for RPC server.
    pub rpc_addr: String,
    /// Current epoch validators.
    pub validators: Vec<ValidatorInfo>,
    /// Sync status of the node.
    pub sync_info: StatusSyncInfo,
    /// Validator id of the node
    pub validator_account_id: Option<AccountId>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChallengeView {
    // TODO: decide how to represent challenges in json.
}

impl From<Challenge> for ChallengeView {
    fn from(_challenge: Challenge) -> Self {
        Self {}
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockHeaderView {
    pub height: BlockHeight,
    #[cfg(feature = "protocol_feature_block_header_v3")]
    pub prev_height: Option<BlockHeight>,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub chunk_receipts_root: CryptoHash,
    pub chunk_headers_root: CryptoHash,
    pub chunk_tx_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub chunks_included: u64,
    pub challenges_root: CryptoHash,
    /// Legacy json number. Should not be used.
    pub timestamp: u64,
    #[serde(with = "u64_dec_format")]
    pub timestamp_nanosec: u64,
    pub random_value: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub chunk_mask: Vec<bool>,
    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,
    #[cfg(feature = "protocol_feature_block_header_v3")]
    pub block_ordinal: Option<NumBlocks>,
    /// TODO(2271): deprecated.
    #[serde(with = "u128_dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "u128_dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "u128_dec_format")]
    pub total_supply: Balance,
    pub challenges_result: ChallengesResult,
    pub last_final_block: CryptoHash,
    pub last_ds_final_block: CryptoHash,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
    #[cfg(feature = "protocol_feature_block_header_v3")]
    pub epoch_sync_data_hash: Option<CryptoHash>,
    pub approvals: Vec<Option<Signature>>,
    pub signature: Signature,
    pub latest_protocol_version: ProtocolVersion,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(header: BlockHeader) -> Self {
        Self {
            height: header.height(),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            prev_height: header.prev_height(),
            epoch_id: header.epoch_id().0,
            next_epoch_id: header.next_epoch_id().0,
            hash: header.hash().clone(),
            prev_hash: header.prev_hash().clone(),
            prev_state_root: header.prev_state_root().clone(),
            chunk_receipts_root: header.chunk_receipts_root().clone(),
            chunk_headers_root: header.chunk_headers_root().clone(),
            chunk_tx_root: header.chunk_tx_root().clone(),
            chunks_included: header.chunks_included(),
            challenges_root: header.challenges_root().clone(),
            outcome_root: header.outcome_root().clone(),
            timestamp: header.raw_timestamp(),
            timestamp_nanosec: header.raw_timestamp(),
            random_value: header.random_value().clone(),
            validator_proposals: header.validator_proposals().map(Into::into).collect(),
            chunk_mask: header.chunk_mask().to_vec(),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            block_ordinal: if header.block_ordinal() != 0 {
                Some(header.block_ordinal())
            } else {
                None
            },
            gas_price: header.gas_price(),
            rent_paid: 0,
            validator_reward: 0,
            total_supply: header.total_supply(),
            challenges_result: header.challenges_result().clone(),
            last_final_block: header.last_final_block().clone(),
            last_ds_final_block: header.last_ds_final_block().clone(),
            next_bp_hash: header.next_bp_hash().clone(),
            block_merkle_root: header.block_merkle_root().clone(),
            #[cfg(feature = "protocol_feature_block_header_v3")]
            epoch_sync_data_hash: header.epoch_sync_data_hash(),
            approvals: header.approvals().to_vec(),
            signature: header.signature().clone(),
            latest_protocol_version: header.latest_protocol_version(),
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(view: BlockHeaderView) -> Self {
        let inner_lite = BlockHeaderInnerLite {
            height: view.height,
            epoch_id: EpochId(view.epoch_id),
            next_epoch_id: EpochId(view.next_epoch_id),
            prev_state_root: view.prev_state_root,
            outcome_root: view.outcome_root,
            timestamp: view.timestamp,
            next_bp_hash: view.next_bp_hash,
            block_merkle_root: view.block_merkle_root,
        };
        #[cfg(not(feature = "protocol_feature_block_header_v3"))]
        let last_header_v2_version = None;
        #[cfg(feature = "protocol_feature_block_header_v3")]
        let last_header_v2_version =
            Some(crate::version::ProtocolFeature::BlockHeaderV3.protocol_version() - 1);
        if view.latest_protocol_version <= 29 {
            #[cfg(feature = "protocol_feature_block_header_v3")]
            let validator_proposals = view
                .validator_proposals
                .into_iter()
                .map(|v| v.into_validator_stake().into_v1())
                .collect();
            #[cfg(not(feature = "protocol_feature_block_header_v3"))]
            let validator_proposals =
                view.validator_proposals.into_iter().map(Into::into).collect();
            let mut header = BlockHeaderV1 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRest {
                    chunk_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    chunks_included: view.chunks_included,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    validator_proposals,
                    chunk_mask: view.chunk_mask,
                    gas_price: view.gas_price,
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV1(Box::new(header))
        } else if last_header_v2_version.is_none()
            || view.latest_protocol_version <= last_header_v2_version.unwrap()
        {
            #[cfg(feature = "protocol_feature_block_header_v3")]
            let validator_proposals = view
                .validator_proposals
                .into_iter()
                .map(|v| v.into_validator_stake().into_v1())
                .collect();
            #[cfg(not(feature = "protocol_feature_block_header_v3"))]
            let validator_proposals =
                view.validator_proposals.into_iter().map(Into::into).collect();
            let mut header = BlockHeaderV2 {
                prev_hash: view.prev_hash,
                inner_lite,
                inner_rest: BlockHeaderInnerRestV2 {
                    chunk_receipts_root: view.chunk_receipts_root,
                    chunk_headers_root: view.chunk_headers_root,
                    chunk_tx_root: view.chunk_tx_root,
                    challenges_root: view.challenges_root,
                    random_value: view.random_value,
                    validator_proposals,
                    chunk_mask: view.chunk_mask,
                    gas_price: view.gas_price,
                    total_supply: view.total_supply,
                    challenges_result: view.challenges_result,
                    last_final_block: view.last_final_block,
                    last_ds_final_block: view.last_ds_final_block,
                    approvals: view.approvals.clone(),
                    latest_protocol_version: view.latest_protocol_version,
                },
                signature: view.signature,
                hash: CryptoHash::default(),
            };
            header.init();
            BlockHeader::BlockHeaderV2(Box::new(header))
        } else {
            #[cfg(not(feature = "protocol_feature_block_header_v3"))]
            unreachable!();
            #[cfg(feature = "protocol_feature_block_header_v3")]
            {
                let mut header = BlockHeaderV3 {
                    prev_hash: view.prev_hash,
                    inner_lite,
                    inner_rest: BlockHeaderInnerRestV3 {
                        chunk_receipts_root: view.chunk_receipts_root,
                        chunk_headers_root: view.chunk_headers_root,
                        chunk_tx_root: view.chunk_tx_root,
                        challenges_root: view.challenges_root,
                        random_value: view.random_value,
                        validator_proposals: view
                            .validator_proposals
                            .into_iter()
                            .map(Into::into)
                            .collect(),
                        chunk_mask: view.chunk_mask,
                        gas_price: view.gas_price,
                        block_ordinal: match view.block_ordinal {
                            Some(value) => value,
                            None => 0,
                        },
                        total_supply: view.total_supply,
                        challenges_result: view.challenges_result,
                        last_final_block: view.last_final_block,
                        last_ds_final_block: view.last_ds_final_block,
                        prev_height: view.prev_height.unwrap_or_default(),
                        epoch_sync_data_hash: view.epoch_sync_data_hash,
                        approvals: view.approvals.clone(),
                        latest_protocol_version: view.latest_protocol_version,
                    },
                    signature: view.signature,
                    hash: CryptoHash::default(),
                };
                header.init();
                BlockHeader::BlockHeaderV3(Box::new(header))
            }
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct BlockHeaderInnerLiteView {
    pub height: BlockHeight,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub outcome_root: CryptoHash,
    /// Legacy json number. Should not be used.
    pub timestamp: u64,
    #[serde(with = "u64_dec_format")]
    pub timestamp_nanosec: u64,
    pub next_bp_hash: CryptoHash,
    pub block_merkle_root: CryptoHash,
}

impl From<BlockHeader> for BlockHeaderInnerLiteView {
    fn from(header: BlockHeader) -> Self {
        match header {
            BlockHeader::BlockHeaderV1(header) => BlockHeaderInnerLiteView {
                height: header.inner_lite.height,
                epoch_id: header.inner_lite.epoch_id.0,
                next_epoch_id: header.inner_lite.next_epoch_id.0,
                prev_state_root: header.inner_lite.prev_state_root,
                outcome_root: header.inner_lite.outcome_root,
                timestamp: header.inner_lite.timestamp,
                timestamp_nanosec: header.inner_lite.timestamp,
                next_bp_hash: header.inner_lite.next_bp_hash,
                block_merkle_root: header.inner_lite.block_merkle_root,
            },
            BlockHeader::BlockHeaderV2(header) => BlockHeaderInnerLiteView {
                height: header.inner_lite.height,
                epoch_id: header.inner_lite.epoch_id.0,
                next_epoch_id: header.inner_lite.next_epoch_id.0,
                prev_state_root: header.inner_lite.prev_state_root,
                outcome_root: header.inner_lite.outcome_root,
                timestamp: header.inner_lite.timestamp,
                timestamp_nanosec: header.inner_lite.timestamp,
                next_bp_hash: header.inner_lite.next_bp_hash,
                block_merkle_root: header.inner_lite.block_merkle_root,
            },
            #[cfg(feature = "protocol_feature_block_header_v3")]
            BlockHeader::BlockHeaderV3(header) => BlockHeaderInnerLiteView {
                height: header.inner_lite.height,
                epoch_id: header.inner_lite.epoch_id.0,
                next_epoch_id: header.inner_lite.next_epoch_id.0,
                prev_state_root: header.inner_lite.prev_state_root,
                outcome_root: header.inner_lite.outcome_root,
                timestamp: header.inner_lite.timestamp,
                timestamp_nanosec: header.inner_lite.timestamp,
                next_bp_hash: header.inner_lite.next_bp_hash,
                block_merkle_root: header.inner_lite.block_merkle_root,
            },
        }
    }
}

impl From<BlockHeaderInnerLiteView> for BlockHeaderInnerLite {
    fn from(view: BlockHeaderInnerLiteView) -> Self {
        BlockHeaderInnerLite {
            height: view.height,
            epoch_id: EpochId(view.epoch_id),
            next_epoch_id: EpochId(view.next_epoch_id),
            prev_state_root: view.prev_state_root,
            outcome_root: view.outcome_root,
            timestamp: view.timestamp_nanosec,
            next_bp_hash: view.next_bp_hash,
            block_merkle_root: view.block_merkle_root,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkHeaderView {
    pub chunk_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub outcome_root: CryptoHash,
    pub prev_state_root: StateRoot,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockHeight,
    pub height_included: BlockHeight,
    pub shard_id: ShardId,
    pub gas_used: Gas,
    pub gas_limit: Gas,
    /// TODO(2271): deprecated.
    #[serde(with = "u128_dec_format")]
    pub rent_paid: Balance,
    /// TODO(2271): deprecated.
    #[serde(with = "u128_dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "u128_dec_format")]
    pub balance_burnt: Balance,
    pub outgoing_receipts_root: CryptoHash,
    pub tx_root: CryptoHash,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub signature: Signature,
}

impl From<ShardChunkHeader> for ChunkHeaderView {
    fn from(chunk: ShardChunkHeader) -> Self {
        let hash = chunk.chunk_hash();
        let signature = chunk.signature().clone();
        let height_included = chunk.height_included();
        let inner = chunk.take_inner();
        ChunkHeaderView {
            chunk_hash: hash.0,
            prev_block_hash: *inner.prev_block_hash(),
            outcome_root: *inner.outcome_root(),
            prev_state_root: *inner.prev_state_root(),
            encoded_merkle_root: *inner.encoded_merkle_root(),
            encoded_length: inner.encoded_length(),
            height_created: inner.height_created(),
            height_included,
            shard_id: inner.shard_id(),
            gas_used: inner.gas_used(),
            gas_limit: inner.gas_limit(),
            rent_paid: 0,
            validator_reward: 0,
            balance_burnt: inner.balance_burnt(),
            outgoing_receipts_root: *inner.outgoing_receipts_root(),
            tx_root: *inner.tx_root(),
            validator_proposals: inner.validator_proposals().map(Into::into).collect(),
            signature,
        }
    }
}

#[cfg(feature = "protocol_feature_block_header_v3")]
impl From<ChunkHeaderView> for ShardChunkHeader {
    fn from(view: ChunkHeaderView) -> Self {
        let mut header = ShardChunkHeaderV3 {
            inner: ShardChunkHeaderInner::V2(ShardChunkHeaderInnerV2 {
                prev_block_hash: view.prev_block_hash,
                prev_state_root: view.prev_state_root,
                outcome_root: view.outcome_root,
                encoded_merkle_root: view.encoded_merkle_root,
                encoded_length: view.encoded_length,
                height_created: view.height_created,
                shard_id: view.shard_id,
                gas_used: view.gas_used,
                gas_limit: view.gas_limit,
                balance_burnt: view.balance_burnt,
                outgoing_receipts_root: view.outgoing_receipts_root,
                tx_root: view.tx_root,
                validator_proposals: view.validator_proposals.into_iter().map(Into::into).collect(),
            }),
            height_included: view.height_included,
            signature: view.signature,
            hash: ChunkHash::default(),
        };
        header.init();
        ShardChunkHeader::V3(header)
    }
}

#[cfg(not(feature = "protocol_feature_block_header_v3"))]
impl From<ChunkHeaderView> for ShardChunkHeader {
    fn from(view: ChunkHeaderView) -> Self {
        let mut header = ShardChunkHeaderV2 {
            inner: ShardChunkHeaderInner {
                prev_block_hash: view.prev_block_hash,
                prev_state_root: view.prev_state_root,
                outcome_root: view.outcome_root,
                encoded_merkle_root: view.encoded_merkle_root,
                encoded_length: view.encoded_length,
                height_created: view.height_created,
                shard_id: view.shard_id,
                gas_used: view.gas_used,
                gas_limit: view.gas_limit,
                balance_burnt: view.balance_burnt,
                outgoing_receipts_root: view.outgoing_receipts_root,
                tx_root: view.tx_root,
                validator_proposals: view.validator_proposals.into_iter().map(Into::into).collect(),
            },
            height_included: view.height_included,
            signature: view.signature,
            hash: ChunkHash::default(),
        };
        header.init();
        ShardChunkHeader::V2(header)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockView {
    pub author: AccountId,
    pub header: BlockHeaderView,
    pub chunks: Vec<ChunkHeaderView>,
}

impl BlockView {
    pub fn from_author_block(author: AccountId, block: Block) -> Self {
        BlockView {
            author,
            header: block.header().clone().into(),
            chunks: block.chunks().iter().cloned().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkView {
    pub author: AccountId,
    pub header: ChunkHeaderView,
    pub transactions: Vec<SignedTransactionView>,
    pub receipts: Vec<ReceiptView>,
}

impl ChunkView {
    pub fn from_author_chunk(author: AccountId, chunk: ShardChunk) -> Self {
        match chunk {
            ShardChunk::V1(chunk) => Self {
                author,
                header: ShardChunkHeader::V1(chunk.header).into(),
                transactions: chunk.transactions.into_iter().map(Into::into).collect(),
                receipts: chunk.receipts.into_iter().map(Into::into).collect(),
            },
            ShardChunk::V2(chunk) => Self {
                author,
                header: chunk.header.into(),
                transactions: chunk.transactions.into_iter().map(Into::into).collect(),
                receipts: chunk.receipts.into_iter().map(Into::into).collect(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub enum ActionView {
    CreateAccount,
    DeployContract {
        code: String,
    },
    FunctionCall {
        method_name: String,
        args: String,
        gas: Gas,
        #[serde(with = "u128_dec_format")]
        deposit: Balance,
    },
    Transfer {
        #[serde(with = "u128_dec_format")]
        deposit: Balance,
    },
    Stake {
        #[serde(with = "u128_dec_format")]
        stake: Balance,
        public_key: PublicKey,
    },
    AddKey {
        public_key: PublicKey,
        access_key: AccessKeyView,
    },
    DeleteKey {
        public_key: PublicKey,
    },
    DeleteAccount {
        beneficiary_id: AccountId,
    },
}

impl From<Action> for ActionView {
    fn from(action: Action) -> Self {
        match action {
            Action::CreateAccount(_) => ActionView::CreateAccount,
            Action::DeployContract(action) => {
                ActionView::DeployContract { code: to_base64(&hash(&action.code)) }
            }
            Action::FunctionCall(action) => ActionView::FunctionCall {
                method_name: action.method_name,
                args: to_base64(&action.args),
                gas: action.gas,
                deposit: action.deposit,
            },
            Action::Transfer(action) => ActionView::Transfer { deposit: action.deposit },
            Action::Stake(action) => {
                ActionView::Stake { stake: action.stake, public_key: action.public_key }
            }
            Action::AddKey(action) => ActionView::AddKey {
                public_key: action.public_key,
                access_key: action.access_key.into(),
            },
            Action::DeleteKey(action) => ActionView::DeleteKey { public_key: action.public_key },
            Action::DeleteAccount(action) => {
                ActionView::DeleteAccount { beneficiary_id: action.beneficiary_id }
            }
        }
    }
}

impl TryFrom<ActionView> for Action {
    type Error = Box<dyn std::error::Error>;

    fn try_from(action_view: ActionView) -> Result<Self, Self::Error> {
        Ok(match action_view {
            ActionView::CreateAccount => Action::CreateAccount(CreateAccountAction {}),
            ActionView::DeployContract { code } => {
                Action::DeployContract(DeployContractAction { code: from_base64(&code)? })
            }
            ActionView::FunctionCall { method_name, args, gas, deposit } => {
                Action::FunctionCall(FunctionCallAction {
                    method_name,
                    args: from_base64(&args)?,
                    gas,
                    deposit,
                })
            }
            ActionView::Transfer { deposit } => Action::Transfer(TransferAction { deposit }),
            ActionView::Stake { stake, public_key } => {
                Action::Stake(StakeAction { stake, public_key })
            }
            ActionView::AddKey { public_key, access_key } => {
                Action::AddKey(AddKeyAction { public_key, access_key: access_key.into() })
            }
            ActionView::DeleteKey { public_key } => {
                Action::DeleteKey(DeleteKeyAction { public_key })
            }
            ActionView::DeleteAccount { beneficiary_id } => {
                Action::DeleteAccount(DeleteAccountAction { beneficiary_id })
            }
        })
    }
}

#[derive(Serialize, Deserialize, Debug, BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone)]
pub struct SignedTransactionView {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: Nonce,
    pub receiver_id: AccountId,
    pub actions: Vec<ActionView>,
    pub signature: Signature,
    pub hash: CryptoHash,
}

impl From<SignedTransaction> for SignedTransactionView {
    fn from(signed_tx: SignedTransaction) -> Self {
        let hash = signed_tx.get_hash();
        SignedTransactionView {
            signer_id: signed_tx.transaction.signer_id,
            public_key: signed_tx.transaction.public_key,
            nonce: signed_tx.transaction.nonce,
            receiver_id: signed_tx.transaction.receiver_id,
            actions: signed_tx
                .transaction
                .actions
                .into_iter()
                .map(|action| action.into())
                .collect(),
            signature: signed_tx.signature,
            hash,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum FinalExecutionStatus {
    /// The execution has not yet started.
    NotStarted,
    /// The execution has started and still going.
    Started,
    /// The execution has failed with the given error.
    Failure(TxExecutionError),
    /// The execution has succeeded and returned some value or an empty vec encoded in base64.
    SuccessValue(String),
}

impl fmt::Debug for FinalExecutionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FinalExecutionStatus::NotStarted => f.write_str("NotStarted"),
            FinalExecutionStatus::Started => f.write_str("Started"),
            FinalExecutionStatus::Failure(e) => f.write_fmt(format_args!("Failure({:?})", e)),
            FinalExecutionStatus::SuccessValue(v) => f.write_fmt(format_args!(
                "SuccessValue({})",
                logging::pretty_utf8(&from_base64(&v).unwrap())
            )),
        }
    }
}

impl Default for FinalExecutionStatus {
    fn default() -> Self {
        FinalExecutionStatus::NotStarted
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum ServerError {
    TxExecutionError(TxExecutionError),
    Timeout,
    Closed,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ExecutionStatusView {
    /// The execution is pending or unknown.
    Unknown,
    /// The execution has failed.
    Failure(TxExecutionError),
    /// The final action succeeded and returned some value or an empty vec encoded in base64.
    SuccessValue(String),
    /// The final action of the receipt returned a promise or the signed transaction was converted
    /// to a receipt. Contains the receipt_id of the generated receipt.
    SuccessReceiptId(CryptoHash),
}

impl fmt::Debug for ExecutionStatusView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionStatusView::Unknown => f.write_str("Unknown"),
            ExecutionStatusView::Failure(e) => f.write_fmt(format_args!("Failure({:?})", e)),
            ExecutionStatusView::SuccessValue(v) => f.write_fmt(format_args!(
                "SuccessValue({})",
                logging::pretty_utf8(&from_base64(&v).unwrap())
            )),
            ExecutionStatusView::SuccessReceiptId(receipt_id) => {
                f.write_fmt(format_args!("SuccessReceiptId({})", receipt_id))
            }
        }
    }
}

impl From<ExecutionStatus> for ExecutionStatusView {
    fn from(outcome: ExecutionStatus) -> Self {
        match outcome {
            ExecutionStatus::Unknown => ExecutionStatusView::Unknown,
            ExecutionStatus::Failure(e) => ExecutionStatusView::Failure(e),
            ExecutionStatus::SuccessValue(v) => ExecutionStatusView::SuccessValue(to_base64(&v)),
            ExecutionStatus::SuccessReceiptId(receipt_id) => {
                ExecutionStatusView::SuccessReceiptId(receipt_id)
            }
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcomeView {
    /// Logs from this transaction or receipt.
    pub logs: Vec<String>,
    /// Receipt IDs generated by this transaction or receipt.
    pub receipt_ids: Vec<CryptoHash>,
    /// The amount of the gas burnt by the given transaction or receipt.
    pub gas_burnt: Gas,
    /// The amount of tokens burnt corresponding to the burnt gas amount.
    /// This value doesn't always equal to the `gas_burnt` multiplied by the gas price, because
    /// the prepaid gas price might be lower than the actual gas price and it creates a deficit.
    #[serde(with = "u128_dec_format")]
    pub tokens_burnt: Balance,
    /// The id of the account on which the execution happens. For transaction this is signer_id,
    /// for receipt this is receiver_id.
    pub executor_id: AccountId,
    /// Execution status. Contains the result in case of successful execution.
    pub status: ExecutionStatusView,
}

impl From<ExecutionOutcome> for ExecutionOutcomeView {
    fn from(outcome: ExecutionOutcome) -> Self {
        Self {
            logs: outcome.logs,
            receipt_ids: outcome.receipt_ids,
            gas_burnt: outcome.gas_burnt,
            tokens_burnt: outcome.tokens_burnt,
            executor_id: outcome.executor_id,
            status: outcome.status.into(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ExecutionOutcomeWithIdView {
    pub proof: MerklePath,
    pub block_hash: CryptoHash,
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
}

impl From<ExecutionOutcomeWithIdAndProof> for ExecutionOutcomeWithIdView {
    fn from(outcome_with_id_and_proof: ExecutionOutcomeWithIdAndProof) -> Self {
        Self {
            proof: outcome_with_id_and_proof.proof,
            block_hash: outcome_with_id_and_proof.block_hash,
            id: outcome_with_id_and_proof.outcome_with_id.id,
            outcome: outcome_with_id_and_proof.outcome_with_id.outcome.into(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum FinalExecutionOutcomeViewEnum {
    FinalExecutionOutcome(FinalExecutionOutcomeView),
    FinalExecutionOutcomeWithReceipt(FinalExecutionOutcomeWithReceiptView),
}

/// Final execution outcome of the transaction and all of subsequent the receipts.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FinalExecutionOutcomeView {
    /// Execution status. Contains the result in case of successful execution.
    pub status: FinalExecutionStatus,
    /// Signed Transaction
    pub transaction: SignedTransactionView,
    /// The execution outcome of the signed transaction.
    pub transaction_outcome: ExecutionOutcomeWithIdView,
    /// The execution outcome of receipts.
    pub receipts_outcome: Vec<ExecutionOutcomeWithIdView>,
}

impl fmt::Debug for FinalExecutionOutcomeView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FinalExecutionOutcome")
            .field("status", &self.status)
            .field("transaction", &self.transaction)
            .field("transaction_outcome", &self.transaction_outcome)
            .field(
                "receipts_outcome",
                &format_args!("{}", logging::pretty_vec(&self.receipts_outcome)),
            )
            .finish()
    }
}

/// Final execution outcome of the transaction and all of subsequent the receipts. Also includes
/// the generated receipt.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct FinalExecutionOutcomeWithReceiptView {
    /// Final outcome view without receipts
    #[serde(flatten)]
    pub final_outcome: FinalExecutionOutcomeView,
    /// Receipts generated from the transaction
    pub receipts: Vec<ReceiptView>,
}

impl From<FinalExecutionOutcomeWithReceiptView> for FinalExecutionOutcomeView {
    fn from(final_outcome_view: FinalExecutionOutcomeWithReceiptView) -> Self {
        final_outcome_view.final_outcome
    }
}

#[cfg(feature = "protocol_feature_block_header_v3")]
pub mod validator_stake_view {
    use crate::types::validator_stake::ValidatorStake;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::types::AccountId;
    use serde::{Deserialize, Serialize};

    pub use super::ValidatorStakeViewV1;

    #[derive(
        BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Eq, PartialEq,
    )]
    #[serde(tag = "validator_stake_struct_version")]
    pub enum ValidatorStakeView {
        V1(ValidatorStakeViewV1),
    }

    impl ValidatorStakeView {
        pub fn into_validator_stake(self) -> ValidatorStake {
            self.into()
        }

        #[inline]
        pub fn take_account_id(self) -> AccountId {
            match self {
                Self::V1(v1) => v1.account_id,
            }
        }

        #[inline]
        pub fn account_id(&self) -> &AccountId {
            match self {
                Self::V1(v1) => &v1.account_id,
            }
        }
    }

    impl From<ValidatorStake> for ValidatorStakeView {
        fn from(stake: ValidatorStake) -> Self {
            match stake {
                ValidatorStake::V1(v1) => Self::V1(ValidatorStakeViewV1 {
                    account_id: v1.account_id,
                    public_key: v1.public_key,
                    stake: v1.stake,
                }),
            }
        }
    }

    impl From<ValidatorStakeView> for ValidatorStake {
        fn from(view: ValidatorStakeView) -> Self {
            match view {
                ValidatorStakeView::V1(v1) => Self::new(v1.account_id, v1.public_key, v1.stake),
            }
        }
    }
}

#[cfg(not(feature = "protocol_feature_block_header_v3"))]
pub mod validator_stake_view {
    use crate::types::validator_stake::ValidatorStake;
    use near_primitives_core::types::AccountId;

    pub use super::ValidatorStakeViewV1;
    pub type ValidatorStakeView = ValidatorStakeViewV1;

    impl ValidatorStakeView {
        #[inline]
        pub fn take_account_id(self) -> AccountId {
            self.account_id
        }

        #[inline]
        pub fn account_id(&self) -> &AccountId {
            &self.account_id
        }
    }

    impl From<ValidatorStake> for ValidatorStakeView {
        fn from(stake: ValidatorStake) -> Self {
            Self { account_id: stake.account_id, public_key: stake.public_key, stake: stake.stake }
        }
    }

    impl From<ValidatorStakeView> for ValidatorStake {
        fn from(view: ValidatorStakeView) -> Self {
            Self { account_id: view.account_id, public_key: view.public_key, stake: view.stake }
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ValidatorStakeViewV1 {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

    pub receipt: ReceiptEnumView,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DataReceiverView {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ReceiptEnumView {
    Action {
        signer_id: AccountId,
        signer_public_key: PublicKey,
        #[serde(with = "u128_dec_format")]
        gas_price: Balance,
        output_data_receivers: Vec<DataReceiverView>,
        input_data_ids: Vec<CryptoHash>,
        actions: Vec<ActionView>,
    },
    Data {
        data_id: CryptoHash,
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    },
}

impl From<Receipt> for ReceiptView {
    fn from(receipt: Receipt) -> Self {
        ReceiptView {
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id,
            receipt: match receipt.receipt {
                ReceiptEnum::Action(action_receipt) => ReceiptEnumView::Action {
                    signer_id: action_receipt.signer_id,
                    signer_public_key: action_receipt.signer_public_key,
                    gas_price: action_receipt.gas_price,
                    output_data_receivers: action_receipt
                        .output_data_receivers
                        .into_iter()
                        .map(|data_receiver| DataReceiverView {
                            data_id: data_receiver.data_id,
                            receiver_id: data_receiver.receiver_id,
                        })
                        .collect(),
                    input_data_ids: action_receipt
                        .input_data_ids
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    actions: action_receipt.actions.into_iter().map(Into::into).collect(),
                },
                ReceiptEnum::Data(data_receipt) => {
                    ReceiptEnumView::Data { data_id: data_receipt.data_id, data: data_receipt.data }
                }
            },
        }
    }
}

impl TryFrom<ReceiptView> for Receipt {
    type Error = Box<dyn std::error::Error>;

    fn try_from(receipt_view: ReceiptView) -> Result<Self, Self::Error> {
        Ok(Receipt {
            predecessor_id: receipt_view.predecessor_id,
            receiver_id: receipt_view.receiver_id,
            receipt_id: receipt_view.receipt_id,
            receipt: match receipt_view.receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    gas_price,
                    output_data_receivers,
                    input_data_ids,
                    actions,
                } => ReceiptEnum::Action(ActionReceipt {
                    signer_id,
                    signer_public_key,
                    gas_price,
                    output_data_receivers: output_data_receivers
                        .into_iter()
                        .map(|data_receiver_view| DataReceiver {
                            data_id: data_receiver_view.data_id,
                            receiver_id: data_receiver_view.receiver_id,
                        })
                        .collect(),
                    input_data_ids: input_data_ids.into_iter().map(Into::into).collect(),
                    actions: actions
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, _>>()?,
                }),
                ReceiptEnumView::Data { data_id, data } => {
                    ReceiptEnum::Data(DataReceipt { data_id, data })
                }
            },
        })
    }
}

/// Information about this epoch validators and next epoch validators
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct EpochValidatorInfo {
    /// Validators for the current epoch
    pub current_validators: Vec<CurrentEpochValidatorInfo>,
    /// Validators for the next epoch
    pub next_validators: Vec<NextEpochValidatorInfo>,
    /// Fishermen for the current epoch
    pub current_fishermen: Vec<ValidatorStakeView>,
    /// Fishermen for the next epoch
    pub next_fishermen: Vec<ValidatorStakeView>,
    /// Proposals in the current epoch
    pub current_proposals: Vec<ValidatorStakeView>,
    /// Kickout in the previous epoch
    pub prev_epoch_kickout: Vec<ValidatorKickoutView>,
    /// Epoch start block height
    pub epoch_start_height: BlockHeight,
    /// Epoch height
    pub epoch_height: EpochHeight,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ValidatorKickoutView {
    pub account_id: AccountId,
    pub reason: ValidatorKickoutReason,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct CurrentEpochValidatorInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub is_slashed: bool,
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
    pub shards: Vec<ShardId>,
    pub num_produced_blocks: NumBlocks,
    pub num_expected_blocks: NumBlocks,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct NextEpochValidatorInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
    pub shards: Vec<ShardId>,
}

#[derive(Serialize, PartialEq, Eq, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct LightClientBlockView {
    pub prev_block_hash: CryptoHash,
    pub next_block_inner_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
    pub inner_rest_hash: CryptoHash,
    pub next_bps: Option<Vec<ValidatorStakeView>>,
    pub approvals_after_next: Vec<Option<Signature>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct LightClientBlockLiteView {
    pub prev_block_hash: CryptoHash,
    pub inner_rest_hash: CryptoHash,
    pub inner_lite: BlockHeaderInnerLiteView,
}

impl From<BlockHeader> for LightClientBlockLiteView {
    fn from(header: BlockHeader) -> Self {
        Self {
            prev_block_hash: header.prev_hash().clone(),
            inner_rest_hash: hash(&header.inner_rest_bytes()),
            inner_lite: header.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GasPriceView {
    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,
}

/// It is a [serializable view] of [`StateChangesRequest`].
///
/// [serializable view]: ./index.html
/// [`StateChangesRequest`]: ../types/struct.StateChangesRequest.html
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "changes_type", rename_all = "snake_case")]
pub enum StateChangesRequestView {
    AccountChanges {
        account_ids: Vec<AccountId>,
    },
    SingleAccessKeyChanges {
        keys: Vec<AccountWithPublicKey>,
    },
    AllAccessKeyChanges {
        account_ids: Vec<AccountId>,
    },
    ContractCodeChanges {
        account_ids: Vec<AccountId>,
    },
    DataChanges {
        account_ids: Vec<AccountId>,
        #[serde(rename = "key_prefix_base64", with = "base64_format")]
        key_prefix: StoreKey,
    },
}

impl From<StateChangesRequestView> for StateChangesRequest {
    fn from(request: StateChangesRequestView) -> Self {
        match request {
            StateChangesRequestView::AccountChanges { account_ids } => {
                Self::AccountChanges { account_ids }
            }
            StateChangesRequestView::SingleAccessKeyChanges { keys } => {
                Self::SingleAccessKeyChanges { keys }
            }
            StateChangesRequestView::AllAccessKeyChanges { account_ids } => {
                Self::AllAccessKeyChanges { account_ids }
            }
            StateChangesRequestView::ContractCodeChanges { account_ids } => {
                Self::ContractCodeChanges { account_ids }
            }
            StateChangesRequestView::DataChanges { account_ids, key_prefix } => {
                Self::DataChanges { account_ids, key_prefix }
            }
        }
    }
}

/// It is a [serializable view] of [`StateChangeKind`].
///
/// [serializable view]: ./index.html
/// [`StateChangeKind`]: ../types/struct.StateChangeKind.html
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum StateChangeKindView {
    AccountTouched { account_id: AccountId },
    AccessKeyTouched { account_id: AccountId },
    DataTouched { account_id: AccountId },
    ContractCodeTouched { account_id: AccountId },
}

impl From<StateChangeKind> for StateChangeKindView {
    fn from(state_change_kind: StateChangeKind) -> Self {
        match state_change_kind {
            StateChangeKind::AccountTouched { account_id } => Self::AccountTouched { account_id },
            StateChangeKind::AccessKeyTouched { account_id } => {
                Self::AccessKeyTouched { account_id }
            }
            StateChangeKind::DataTouched { account_id } => Self::DataTouched { account_id },
            StateChangeKind::ContractCodeTouched { account_id } => {
                Self::ContractCodeTouched { account_id }
            }
        }
    }
}

pub type StateChangesKindsView = Vec<StateChangeKindView>;

/// See crate::types::StateChangeCause for details.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum StateChangeCauseView {
    NotWritableToDisk,
    InitialState,
    TransactionProcessing { tx_hash: CryptoHash },
    ActionReceiptProcessingStarted { receipt_hash: CryptoHash },
    ActionReceiptGasReward { receipt_hash: CryptoHash },
    ReceiptProcessing { receipt_hash: CryptoHash },
    PostponedReceipt { receipt_hash: CryptoHash },
    UpdatedDelayedReceipts,
    ValidatorAccountsUpdate,
}

impl From<StateChangeCause> for StateChangeCauseView {
    fn from(state_change_cause: StateChangeCause) -> Self {
        match state_change_cause {
            StateChangeCause::NotWritableToDisk => Self::NotWritableToDisk,
            StateChangeCause::InitialState => Self::InitialState,
            StateChangeCause::TransactionProcessing { tx_hash } => {
                Self::TransactionProcessing { tx_hash }
            }
            StateChangeCause::ActionReceiptProcessingStarted { receipt_hash } => {
                Self::ActionReceiptProcessingStarted { receipt_hash }
            }
            StateChangeCause::ActionReceiptGasReward { receipt_hash } => {
                Self::ActionReceiptGasReward { receipt_hash }
            }
            StateChangeCause::ReceiptProcessing { receipt_hash } => {
                Self::ReceiptProcessing { receipt_hash }
            }
            StateChangeCause::PostponedReceipt { receipt_hash } => {
                Self::PostponedReceipt { receipt_hash }
            }
            StateChangeCause::UpdatedDelayedReceipts => Self::UpdatedDelayedReceipts,
            StateChangeCause::ValidatorAccountsUpdate => Self::ValidatorAccountsUpdate,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "change")]
pub enum StateChangeValueView {
    AccountUpdate {
        account_id: AccountId,
        #[serde(flatten)]
        account: AccountView,
    },
    AccountDeletion {
        account_id: AccountId,
    },
    AccessKeyUpdate {
        account_id: AccountId,
        public_key: PublicKey,
        access_key: AccessKeyView,
    },
    AccessKeyDeletion {
        account_id: AccountId,
        public_key: PublicKey,
    },
    DataUpdate {
        account_id: AccountId,
        #[serde(rename = "key_base64", with = "base64_format")]
        key: StoreKey,
        #[serde(rename = "value_base64", with = "base64_format")]
        value: StoreValue,
    },
    DataDeletion {
        account_id: AccountId,
        #[serde(rename = "key_base64", with = "base64_format")]
        key: StoreKey,
    },
    ContractCodeUpdate {
        account_id: AccountId,
        #[serde(rename = "code_base64", with = "base64_format")]
        code: Vec<u8>,
    },
    ContractCodeDeletion {
        account_id: AccountId,
    },
}

impl From<StateChangeValue> for StateChangeValueView {
    fn from(state_change: StateChangeValue) -> Self {
        match state_change {
            StateChangeValue::AccountUpdate { account_id, account } => {
                Self::AccountUpdate { account_id, account: account.into() }
            }
            StateChangeValue::AccountDeletion { account_id } => {
                Self::AccountDeletion { account_id }
            }
            StateChangeValue::AccessKeyUpdate { account_id, public_key, access_key } => {
                Self::AccessKeyUpdate { account_id, public_key, access_key: access_key.into() }
            }
            StateChangeValue::AccessKeyDeletion { account_id, public_key } => {
                Self::AccessKeyDeletion { account_id, public_key }
            }
            StateChangeValue::DataUpdate { account_id, key, value } => {
                Self::DataUpdate { account_id, key, value }
            }
            StateChangeValue::DataDeletion { account_id, key } => {
                Self::DataDeletion { account_id, key }
            }
            StateChangeValue::ContractCodeUpdate { account_id, code } => {
                Self::ContractCodeUpdate { account_id, code }
            }
            StateChangeValue::ContractCodeDeletion { account_id } => {
                Self::ContractCodeDeletion { account_id }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StateChangeWithCauseView {
    pub cause: StateChangeCauseView,
    #[serde(flatten)]
    pub value: StateChangeValueView,
}

impl From<StateChangeWithCause> for StateChangeWithCauseView {
    fn from(state_change_with_cause: StateChangeWithCause) -> Self {
        let StateChangeWithCause { cause, value } = state_change_with_cause;
        Self { cause: cause.into(), value: value.into() }
    }
}

pub type StateChangesView = Vec<StateChangeWithCauseView>;

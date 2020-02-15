use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;

use chrono::{DateTime, Utc};

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};

use crate::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use crate::block::{Approval, Block, BlockHeader, BlockHeaderInnerLite, BlockHeaderInnerRest};
use crate::challenge::{Challenge, ChallengesResult};
use crate::errors::TxExecutionError;
use crate::hash::{hash, CryptoHash};
use crate::logging;
use crate::merkle::MerklePath;
use crate::receipt::{ActionReceipt, DataReceipt, DataReceiver, Receipt, ReceiptEnum};
use crate::serialize::{
    from_base64, option_base64_format, option_u128_dec_format, to_base64, u128_dec_format,
};
use crate::sharding::{ChunkHash, ShardChunk, ShardChunkHeader, ShardChunkHeaderInner};
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, ExecutionOutcome, ExecutionOutcomeWithIdAndProof, ExecutionStatus,
    FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use crate::types::{
    AccountId, Balance, BlockHeight, EpochId, FunctionArgs, Gas, Nonce, NumBlocks, ShardId,
    StateChanges, StateRoot, StorageUsage, StoreKey, ValidatorStake, Version,
};

/// A view of the account
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub locked: Balance,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    pub storage_paid_at: BlockHeight,
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        AccountView {
            amount: account.amount,
            locked: account.locked,
            code_hash: account.code_hash,
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,
        }
    }
}

impl From<AccountView> for Account {
    fn from(view: AccountView) -> Self {
        Self {
            amount: view.amount,
            locked: view.locked,
            code_hash: view.code_hash,
            storage_usage: view.storage_usage,
            storage_paid_at: view.storage_paid_at,
        }
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

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
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

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum QueryResponseKind {
    ViewAccount(AccountView),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    Error(QueryError),
    AccessKey(AccessKeyView),
    AccessKeyList(AccessKeyList),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(tag = "request_type", rename_all = "snake_case")]
pub enum QueryRequest {
    ViewAccount { account_id: AccountId },
    ViewState { account_id: AccountId, prefix: StoreKey },
    ViewAccessKey { account_id: AccountId, public_key: PublicKey },
    ViewAccessKeyList { account_id: AccountId },
    CallFunction { account_id: AccountId, method_name: String, args: FunctionArgs },
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct QueryResponse {
    #[serde(flatten)]
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
    /// Address for RPC server.
    pub rpc_addr: String,
    /// Current epoch validators.
    pub validators: Vec<ValidatorInfo>,
    /// Sync status of the node.
    pub sync_info: StatusSyncInfo,
}

impl TryFrom<QueryResponse> for AccountView {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response.kind {
            QueryResponseKind::ViewAccount(acc) => Ok(acc),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for ViewStateResult {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response.kind {
            QueryResponseKind::ViewState(vs) => Ok(vs),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for AccessKeyView {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(access_key),
            _ => Err("Invalid type of response".into()),
        }
    }
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
    pub timestamp: u64,
    pub score: u64,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub chunk_mask: Vec<bool>,
    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,
    #[serde(with = "u128_dec_format")]
    pub rent_paid: Balance,
    #[serde(with = "u128_dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "u128_dec_format")]
    pub total_supply: Balance,
    pub challenges_result: ChallengesResult,
    pub last_quorum_pre_vote: CryptoHash,
    pub last_quorum_pre_commit: CryptoHash,
    pub last_ds_final_block: CryptoHash,
    pub next_bp_hash: CryptoHash,
    pub approvals: Vec<(AccountId, CryptoHash, Option<CryptoHash>, BlockHeight, bool, Signature)>,
    pub signature: Signature,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(header: BlockHeader) -> Self {
        Self {
            height: header.inner_lite.height,
            epoch_id: header.inner_lite.epoch_id.0,
            next_epoch_id: header.inner_lite.next_epoch_id.0,
            hash: header.hash,
            prev_hash: header.prev_hash,
            prev_state_root: header.inner_lite.prev_state_root,
            chunk_receipts_root: header.inner_rest.chunk_receipts_root,
            chunk_headers_root: header.inner_rest.chunk_headers_root,
            chunk_tx_root: header.inner_rest.chunk_tx_root,
            chunks_included: header.inner_rest.chunks_included,
            challenges_root: header.inner_rest.challenges_root,
            outcome_root: header.inner_lite.outcome_root,
            timestamp: header.inner_lite.timestamp,
            score: header.inner_rest.score.to_num(),
            validator_proposals: header
                .inner_rest
                .validator_proposals
                .into_iter()
                .map(|v| v.into())
                .collect(),
            chunk_mask: header.inner_rest.chunk_mask,
            gas_price: header.inner_rest.gas_price,
            rent_paid: header.inner_rest.rent_paid,
            validator_reward: header.inner_rest.validator_reward,
            total_supply: header.inner_rest.total_supply,
            challenges_result: header.inner_rest.challenges_result,
            last_quorum_pre_vote: header.inner_rest.last_quorum_pre_vote,
            last_quorum_pre_commit: header.inner_rest.last_quorum_pre_commit,
            last_ds_final_block: header.inner_rest.last_ds_final_block,
            next_bp_hash: header.inner_lite.next_bp_hash,
            approvals: header
                .inner_rest
                .approvals
                .into_iter()
                .map(|x| {
                    (
                        x.account_id,
                        x.parent_hash,
                        x.reference_hash,
                        x.target_height,
                        x.is_endorsement,
                        x.signature,
                    )
                })
                .collect(),
            signature: header.signature,
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(view: BlockHeaderView) -> Self {
        let mut header = Self {
            prev_hash: view.prev_hash,
            inner_lite: BlockHeaderInnerLite {
                height: view.height,
                epoch_id: EpochId(view.epoch_id),
                next_epoch_id: EpochId(view.next_epoch_id),
                prev_state_root: view.prev_state_root,
                outcome_root: view.outcome_root,
                timestamp: view.timestamp,
                next_bp_hash: view.next_bp_hash,
            },
            inner_rest: BlockHeaderInnerRest {
                chunk_receipts_root: view.chunk_receipts_root,
                chunk_headers_root: view.chunk_headers_root,
                chunk_tx_root: view.chunk_tx_root,
                chunks_included: view.chunks_included,
                challenges_root: view.challenges_root,
                score: view.score.into(),
                validator_proposals: view
                    .validator_proposals
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
                chunk_mask: view.chunk_mask,
                gas_price: view.gas_price,
                total_supply: view.total_supply,
                challenges_result: view.challenges_result,
                rent_paid: view.rent_paid,
                validator_reward: view.validator_reward,
                last_quorum_pre_vote: view.last_quorum_pre_vote,
                last_quorum_pre_commit: view.last_quorum_pre_commit,
                last_ds_final_block: view.last_ds_final_block,
                approvals: view
                    .approvals
                    .into_iter()
                    .map(
                        |(
                            account_id,
                            parent_hash,
                            reference_hash,
                            target_height,
                            is_endorsement,
                            signature,
                        )| {
                            Approval {
                                account_id,
                                parent_hash,
                                reference_hash,
                                target_height,
                                is_endorsement,
                                signature,
                            }
                        },
                    )
                    .collect(),
            },
            signature: view.signature,
            hash: CryptoHash::default(),
        };
        header.init();
        header
    }
}

#[derive(Serialize, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct BlockHeaderInnerLiteView {
    pub height: BlockHeight,
    pub epoch_id: CryptoHash,
    pub next_epoch_id: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub timestamp: u64,
    pub next_bp_hash: CryptoHash,
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
    #[serde(with = "u128_dec_format")]
    pub rent_paid: Balance,
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
        ChunkHeaderView {
            chunk_hash: chunk.hash.0,
            prev_block_hash: chunk.inner.prev_block_hash,
            outcome_root: chunk.inner.outcome_root,
            prev_state_root: chunk.inner.prev_state_root,
            encoded_merkle_root: chunk.inner.encoded_merkle_root,
            encoded_length: chunk.inner.encoded_length,
            height_created: chunk.inner.height_created,
            height_included: chunk.height_included,
            shard_id: chunk.inner.shard_id,
            gas_used: chunk.inner.gas_used,
            gas_limit: chunk.inner.gas_limit,
            rent_paid: chunk.inner.rent_paid,
            validator_reward: chunk.inner.validator_reward,
            balance_burnt: chunk.inner.balance_burnt,
            outgoing_receipts_root: chunk.inner.outgoing_receipts_root,
            tx_root: chunk.inner.tx_root,
            validator_proposals: chunk
                .inner
                .validator_proposals
                .into_iter()
                .map(Into::into)
                .collect(),
            signature: chunk.signature,
        }
    }
}

impl From<ChunkHeaderView> for ShardChunkHeader {
    fn from(view: ChunkHeaderView) -> Self {
        let mut header = ShardChunkHeader {
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
                rent_paid: view.rent_paid,
                validator_reward: view.validator_reward,
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
        header
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
            header: block.header.into(),
            chunks: block.chunks.into_iter().map(Into::into).collect(),
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
        Self {
            author,
            header: chunk.header.into(),
            transactions: chunk.transactions.into_iter().map(Into::into).collect(),
            receipts: chunk.receipts.into_iter().map(Into::into).collect(),
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
    /// Execution status. Contains the result in case of successful execution.
    pub status: ExecutionStatusView,
    /// Logs from this transaction or receipt.
    pub logs: Vec<String>,
    /// Receipt IDs generated by this transaction or receipt.
    pub receipt_ids: Vec<CryptoHash>,
    /// The amount of the gas burnt by the given transaction or receipt.
    pub gas_burnt: Gas,
}

impl From<ExecutionOutcome> for ExecutionOutcomeView {
    fn from(outcome: ExecutionOutcome) -> Self {
        Self {
            status: outcome.status.into(),
            logs: outcome.logs,
            receipt_ids: outcome.receipt_ids,
            gas_burnt: outcome.gas_burnt,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ExecutionOutcomeWithIdView {
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
    pub proof: MerklePath,
    pub block_hash: CryptoHash,
}

impl From<ExecutionOutcomeWithIdAndProof> for ExecutionOutcomeWithIdView {
    fn from(outcome_with_id_and_proof: ExecutionOutcomeWithIdAndProof) -> Self {
        Self {
            id: outcome_with_id_and_proof.outcome_with_id.id,
            outcome: outcome_with_id_and_proof.outcome_with_id.outcome.into(),
            proof: outcome_with_id_and_proof.proof,
            block_hash: outcome_with_id_and_proof.block_hash,
        }
    }
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone)]
pub struct StateChangesView {
    pub changes: StateChanges,
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ValidatorStakeView {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub stake: Balance,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

    pub receipt: ReceiptEnumView,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataReceiverView {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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

#[derive(Serialize, Deserialize, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct LightClientApprovalView {
    pub parent_hash: CryptoHash,
    pub reference_hash: CryptoHash,
    pub signature: Signature,
}

#[derive(Serialize, Debug, Clone, BorshDeserialize, BorshSerialize)]
pub struct LightClientBlockView {
    pub inner_lite: BlockHeaderInnerLiteView,
    pub inner_rest_hash: CryptoHash,
    pub next_bps: Option<Vec<ValidatorStakeView>>,
    pub qv_hash: CryptoHash,
    pub future_inner_hashes: Vec<CryptoHash>,
    pub qv_approvals: Vec<Option<LightClientApprovalView>>,
    pub qc_approvals: Vec<Option<LightClientApprovalView>>,
    pub prev_hash: CryptoHash,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GasPriceView {
    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,
}

/// Different types of finality.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub enum Finality {
    #[serde(rename = "optimistic")]
    None,
    #[serde(rename = "near-final")]
    DoomSlug,
    #[serde(rename = "final")]
    NFG,
}

impl Default for Finality {
    fn default() -> Self {
        Finality::NFG
    }
}

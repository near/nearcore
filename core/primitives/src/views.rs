use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;

use chrono::{DateTime, Utc};

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};

use crate::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use crate::block::{Block, BlockHeader, BlockHeaderInner};
use crate::challenge::{Challenge, ChallengesResult};
use crate::errors::{ActionError, ExecutionError, InvalidAccessKeyError, InvalidTxError};
use crate::hash::CryptoHash;
use crate::logging;
use crate::receipt::{ActionReceipt, DataReceipt, DataReceiver, Receipt, ReceiptEnum};
use crate::serialize::{
    from_base64, option_base64_format, option_u128_dec_format, to_base64, u128_dec_format,
};
use crate::sharding::{ChunkHash, ShardChunk, ShardChunkHeader, ShardChunkHeaderInner};
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus,
    FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use crate::types::{
    AccountId, Balance, BlockIndex, EpochId, Gas, Nonce, ShardId, StateRoot, StorageUsage,
    ValidatorStake, Version,
};

/// A view of the account
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub locked: Balance,
    pub code_hash: CryptoHash,
    pub storage_usage: StorageUsage,
    pub storage_paid_at: BlockIndex,
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

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewStateResult {
    pub values: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CallResult {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryError {
    pub error: String,
    pub logs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccessKeyInfoView {
    pub public_key: PublicKey,
    pub access_key: AccessKeyView,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum QueryResponse {
    ViewAccount(AccountView),
    ViewState(ViewStateResult),
    CallResult(CallResult),
    Error(QueryError),
    AccessKey(Option<AccessKeyView>),
    AccessKeyList(Vec<AccessKeyInfoView>),
    Validators(EpochValidatorInfo),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHash,
    pub latest_block_height: BlockIndex,
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
        match query_response {
            QueryResponse::ViewAccount(acc) => Ok(acc),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for ViewStateResult {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::ViewState(vs) => Ok(vs),
            _ => Err("Invalid type of response".into()),
        }
    }
}

impl TryFrom<QueryResponse> for Option<AccessKeyView> {
    type Error = String;

    fn try_from(query_response: QueryResponse) -> Result<Self, Self::Error> {
        match query_response {
            QueryResponse::AccessKey(access_key) => Ok(access_key),
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
    pub height: BlockIndex,
    pub epoch_id: CryptoHash,
    pub hash: CryptoHash,
    pub prev_hash: CryptoHash,
    pub prev_state_root: CryptoHash,
    pub chunk_receipts_root: CryptoHash,
    pub chunk_headers_root: CryptoHash,
    pub chunk_tx_root: CryptoHash,
    pub outcome_root: CryptoHash,
    pub chunks_included: u64,
    pub timestamp: u64,
    pub approval_mask: Vec<bool>,
    pub approval_sigs: Vec<Signature>,
    #[serde(with = "u128_dec_format")]
    pub total_weight: u128,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub chunk_mask: Vec<bool>,
    pub gas_used: Gas,
    pub gas_limit: Gas,
    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,
    #[serde(with = "u128_dec_format")]
    pub rent_paid: Balance,
    #[serde(with = "u128_dec_format")]
    pub validator_reward: Balance,
    #[serde(with = "u128_dec_format")]
    pub total_supply: Balance,
    pub challenges_result: ChallengesResult,
    pub signature: Signature,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(header: BlockHeader) -> Self {
        Self {
            height: header.inner.height,
            epoch_id: header.inner.epoch_id.0,
            hash: header.hash,
            prev_hash: header.inner.prev_hash,
            prev_state_root: header.inner.prev_state_root,
            chunk_receipts_root: header.inner.chunk_receipts_root,
            chunk_headers_root: header.inner.chunk_headers_root,
            chunk_tx_root: header.inner.chunk_tx_root,
            chunks_included: header.inner.chunks_included,
            outcome_root: header.inner.outcome_root,
            timestamp: header.inner.timestamp,
            approval_mask: header.inner.approval_mask,
            approval_sigs: header.inner.approval_sigs,
            total_weight: header.inner.total_weight.to_num(),
            validator_proposals: header
                .inner
                .validator_proposals
                .into_iter()
                .map(|v| v.into())
                .collect(),
            chunk_mask: header.inner.chunk_mask,
            gas_used: header.inner.gas_used,
            gas_limit: header.inner.gas_limit,
            gas_price: header.inner.gas_price,
            rent_paid: header.inner.rent_paid,
            validator_reward: header.inner.validator_reward,
            total_supply: header.inner.total_supply,
            challenges_result: header.inner.challenges_result,
            signature: header.signature,
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(view: BlockHeaderView) -> Self {
        let mut header = Self {
            inner: BlockHeaderInner {
                height: view.height,
                epoch_id: EpochId(view.epoch_id),
                prev_hash: view.prev_hash,
                prev_state_root: view.prev_state_root,
                chunk_receipts_root: view.chunk_receipts_root,
                chunk_headers_root: view.chunk_headers_root,
                chunk_tx_root: view.chunk_tx_root,
                chunks_included: view.chunks_included,
                outcome_root: view.outcome_root,
                timestamp: view.timestamp,
                approval_mask: view.approval_mask,
                approval_sigs: view.approval_sigs,
                total_weight: view.total_weight.into(),
                validator_proposals: view
                    .validator_proposals
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
                chunk_mask: view.chunk_mask,
                gas_limit: view.gas_limit,
                gas_price: view.gas_price,
                gas_used: view.gas_used,
                total_supply: view.total_supply,
                challenges_result: view.challenges_result,
                rent_paid: view.rent_paid,
                validator_reward: view.validator_reward,
            },
            signature: view.signature,
            hash: CryptoHash::default(),
        };
        header.init();
        header
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkHeaderView {
    pub chunk_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub outcome_root: CryptoHash,
    pub prev_state_root_hash: CryptoHash,
    pub prev_state_num_parts: u64,
    pub encoded_merkle_root: CryptoHash,
    pub encoded_length: u64,
    pub height_created: BlockIndex,
    pub height_included: BlockIndex,
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
            prev_state_root_hash: chunk.inner.prev_state_root.hash,
            prev_state_num_parts: chunk.inner.prev_state_root.num_parts,
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
                prev_state_root: StateRoot {
                    hash: view.prev_state_root_hash,
                    num_parts: view.prev_state_num_parts,
                },
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
    pub header: BlockHeaderView,
    pub chunks: Vec<ChunkHeaderView>,
}

impl From<Block> for BlockView {
    fn from(block: Block) -> Self {
        BlockView {
            header: block.header.into(),
            chunks: block.chunks.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkView {
    pub header: ChunkHeaderView,
    pub transactions: Vec<SignedTransactionView>,
    pub receipts: Vec<ReceiptView>,
}

impl From<ShardChunk> for ChunkView {
    fn from(chunk: ShardChunk) -> Self {
        Self {
            header: chunk.header.into(),
            transactions: chunk.transactions.into_iter().map(Into::into).collect(),
            receipts: chunk.receipts.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
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
                ActionView::DeployContract { code: to_base64(&action.code) }
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

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedTransactionView {
    signer_id: AccountId,
    public_key: PublicKey,
    nonce: Nonce,
    receiver_id: AccountId,
    actions: Vec<ActionView>,
    signature: Signature,
    hash: CryptoHash,
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
    Failure(ExecutionErrorView),
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
pub struct ExecutionErrorView {
    pub error_message: String,
    pub error_type: String,
}

impl From<ExecutionError> for ExecutionErrorView {
    fn from(error: ExecutionError) -> Self {
        ExecutionErrorView {
            error_message: format!("{}", error),
            error_type: match &error {
                ExecutionError::Action(e) => match e {
                    ActionError::AccountAlreadyExists(_) => {
                        "ActionError::AccountAlreadyExists".to_string()
                    }
                    ActionError::AccountDoesNotExist(_, _) => {
                        "ActionError::AccountDoesNotExist".to_string()
                    }
                    ActionError::CreateAccountNotAllowed(_, _) => {
                        "ActionError::CreateAccountNotAllowed".to_string()
                    }
                    ActionError::ActorNoPermission(_, _, _) => {
                        "ActionError::ActorNoPermission".to_string()
                    }
                    ActionError::DeleteKeyDoesNotExist(_) => {
                        "ActionError::DeleteKeyDoesNotExist".to_string()
                    }
                    ActionError::AddKeyAlreadyExists(_) => {
                        "ActionError::AddKeyAlreadyExists".to_string()
                    }
                    ActionError::DeleteAccountStaking(_) => {
                        "ActionError::DeleteAccountStaking".to_string()
                    }
                    ActionError::DeleteAccountHasRent(_, _) => {
                        "ActionError::DeleteAccountHasRent".to_string()
                    }
                    ActionError::RentUnpaid(_, _) => "ActionError::RentUnpaid".to_string(),
                    ActionError::TriesToUnstake(_) => "ActionError::TriesToUnstake".to_string(),
                    ActionError::TriesToStake(_, _, _, _) => {
                        "ActionError::TriesToStake".to_string()
                    }
                    ActionError::FunctionCallError(_) => {
                        "ActionError::FunctionCallError".to_string()
                    }
                },
                ExecutionError::InvalidTx(e) => match e {
                    InvalidTxError::InvalidSigner(_) => "InvalidTxError::InvalidSigner".to_string(),
                    InvalidTxError::SignerDoesNotExist(_) => {
                        "InvalidTxError::SignerDoesNotExist".to_string()
                    }
                    InvalidTxError::InvalidAccessKey(e) => match e {
                        InvalidAccessKeyError::AccessKeyNotFound(_, _) => {
                            "InvalidTxError::InvalidAccessKey::AccessKeyNotFound".to_string()
                        }
                        InvalidAccessKeyError::ReceiverMismatch(_, _) => {
                            "InvalidTxError::InvalidAccessKey::ReceiverMismatch".to_string()
                        }
                        InvalidAccessKeyError::MethodNameMismatch(_) => {
                            "InvalidTxError::InvalidAccessKey::MethodNameMismatch".to_string()
                        }
                        InvalidAccessKeyError::ActionError => {
                            "InvalidTxError::InvalidAccessKey::ActionError".to_string()
                        }
                        InvalidAccessKeyError::NotEnoughAllowance(_, _, _, _) => {
                            "InvalidTxError::InvalidAccessKey::NotEnoughAllowance".to_string()
                        }
                    },
                    InvalidTxError::InvalidNonce(_, _) => {
                        "InvalidTxError::InvalidNonce".to_string()
                    }
                    InvalidTxError::InvalidReceiver(_) => {
                        "InvalidTxError::InvalidReceiver".to_string()
                    }
                    InvalidTxError::InvalidSignature => {
                        "InvalidTxError::InvalidSignature".to_string()
                    }
                    InvalidTxError::NotEnoughBalance(_, _, _) => {
                        "InvalidTxError::NotEnoughBalance".to_string()
                    }
                    InvalidTxError::RentUnpaid(_, _) => "InvalidTxError::RentUnpaid".to_string(),
                    InvalidTxError::CostOverflow => "InvalidTxError::CostOverflow".to_string(),
                    InvalidTxError::InvalidChain => "InvalidTxError::InvalidChain".to_string(),
                    InvalidTxError::Expired => "InvalidTxError::Expired".to_string(),
                },
            },
        }
    }
}

impl From<ActionError> for ExecutionErrorView {
    fn from(error: ActionError) -> Self {
        ExecutionError::Action(error).into()
    }
}

impl From<InvalidTxError> for ExecutionErrorView {
    fn from(error: InvalidTxError) -> Self {
        ExecutionError::InvalidTx(error).into()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum ExecutionStatusView {
    /// The execution is pending or unknown.
    Unknown,
    /// The execution has failed.
    Failure(ExecutionErrorView),
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
            ExecutionStatus::Failure(e) => ExecutionStatusView::Failure(e.into()),
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
}

impl From<ExecutionOutcomeWithId> for ExecutionOutcomeWithIdView {
    fn from(outcome_with_id: ExecutionOutcomeWithId) -> Self {
        Self { id: outcome_with_id.id, outcome: outcome_with_id.outcome.into() }
    }
}

/// Final execution outcome of the transaction and all of subsequent the receipts.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FinalExecutionOutcomeView {
    /// Execution status. Contains the result in case of successful execution.
    pub status: FinalExecutionStatus,
    /// The execution outcome of the signed transaction.
    pub transaction: ExecutionOutcomeWithIdView,
    /// The execution outcome of receipts.
    pub receipts: Vec<ExecutionOutcomeWithIdView>,
}

impl fmt::Debug for FinalExecutionOutcomeView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FinalExecutionOutcome")
            .field("status", &self.status)
            .field("transaction", &self.transaction)
            .field("receipts", &format_args!("{}", logging::pretty_vec(&self.receipts)))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ValidatorStakeView {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
}

impl From<ValidatorStake> for ValidatorStakeView {
    fn from(stake: ValidatorStake) -> Self {
        Self { account_id: stake.account_id, public_key: stake.public_key, amount: stake.amount }
    }
}

impl From<ValidatorStakeView> for ValidatorStake {
    fn from(view: ValidatorStakeView) -> Self {
        Self { account_id: view.account_id, public_key: view.public_key, amount: view.amount }
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
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct EpochValidatorInfo {
    /// Validators for the current epoch
    pub current_validators: Vec<ValidatorStakeView>,
    /// Validators for the next epoch
    pub next_validators: Vec<ValidatorStakeView>,
    /// Proposals in the current epoch
    pub current_proposals: Vec<ValidatorStakeView>,
}

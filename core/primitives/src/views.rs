use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use near_crypto::{PublicKey, Signature};

use crate::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use crate::block::{Block, BlockHeader, BlockHeaderInner};
use crate::hash::CryptoHash;
use crate::logging;
use crate::receipt::{ActionReceipt, DataReceipt, DataReceiver, Receipt, ReceiptEnum};
use crate::serialize::{
    from_base, from_base64, option_base64_format, option_u128_dec_format, to_base, to_base64,
    u128_dec_format,
};
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, LogEntry, SignedTransaction, StakeAction,
    TransactionLog, TransactionResult, TransactionStatus, TransferAction,
};
use crate::types::{
    AccountId, Balance, BlockIndex, Gas, Nonce, StorageUsage, ValidatorStake, Version,
};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CryptoHashView(pub Vec<u8>);

impl Serialize for CryptoHashView {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for CryptoHashView {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let view = from_base(&s)
            .map(|v| CryptoHashView(v))
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        if let Err(err) = CryptoHash::try_from(view.0.clone()) {
            return Err(serde::de::Error::custom(err.to_string()));
        }
        Ok(view)
    }
}

impl From<CryptoHash> for CryptoHashView {
    fn from(hash: CryptoHash) -> Self {
        CryptoHashView(hash.as_ref().to_vec())
    }
}

impl From<CryptoHashView> for CryptoHash {
    fn from(view: CryptoHashView) -> Self {
        CryptoHash::try_from(view.0).expect("Failed to convert CryptoHashView to CryptoHash")
    }
}

/// A view of the account
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct AccountView {
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub staked: Balance,
    pub code_hash: CryptoHashView,
    pub storage_usage: StorageUsage,
    pub storage_paid_at: BlockIndex,
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        AccountView {
            amount: account.amount,
            staked: account.staked,
            code_hash: account.code_hash.into(),
            storage_usage: account.storage_usage,
            storage_paid_at: account.storage_paid_at,
        }
    }
}

impl From<AccountView> for Account {
    fn from(view: AccountView) -> Self {
        Self {
            amount: view.amount,
            staked: view.staked,
            code_hash: view.code_hash.into(),
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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusSyncInfo {
    pub latest_block_hash: CryptoHashView,
    pub latest_block_height: BlockIndex,
    pub latest_state_root: CryptoHashView,
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
pub struct BlockHeaderView {
    pub height: BlockIndex,
    pub epoch_hash: CryptoHashView,
    pub hash: CryptoHashView,
    pub prev_hash: CryptoHashView,
    pub prev_state_root: CryptoHashView,
    pub tx_root: CryptoHashView,
    pub timestamp: u64,
    pub approval_mask: Vec<bool>,
    pub approval_sigs: Vec<Signature>,
    pub total_weight: u64,
    pub validator_proposals: Vec<ValidatorStakeView>,
    pub signature: Signature,
}

impl From<BlockHeader> for BlockHeaderView {
    fn from(header: BlockHeader) -> Self {
        Self {
            height: header.inner.height,
            epoch_hash: header.inner.epoch_hash.into(),
            hash: header.hash.into(),
            prev_hash: header.inner.prev_hash.into(),
            prev_state_root: header.inner.prev_state_root.into(),
            tx_root: header.inner.tx_root.into(),
            timestamp: header.inner.timestamp,
            approval_mask: header.inner.approval_mask,
            approval_sigs: header
                .inner
                .approval_sigs
                .into_iter()
                .map(|signature| signature.into())
                .collect(),
            total_weight: header.inner.total_weight.to_num(),
            validator_proposals: header
                .inner
                .validator_proposals
                .into_iter()
                .map(|v| v.into())
                .collect(),
            signature: header.signature.into(),
        }
    }
}

impl From<BlockHeaderView> for BlockHeader {
    fn from(view: BlockHeaderView) -> Self {
        let mut header = Self {
            inner: BlockHeaderInner {
                height: view.height,
                epoch_hash: view.epoch_hash.into(),
                prev_hash: view.prev_hash.into(),
                prev_state_root: view.prev_state_root.into(),
                tx_root: view.tx_root.into(),
                timestamp: view.timestamp,
                approval_mask: view.approval_mask,
                approval_sigs: view
                    .approval_sigs
                    .into_iter()
                    .map(|signature| signature.into())
                    .collect(),
                total_weight: view.total_weight.into(),
                validator_proposals: view
                    .validator_proposals
                    .into_iter()
                    .map(|v| v.into())
                    .collect(),
            },
            signature: view.signature.into(),
            hash: CryptoHash::default(),
        };
        header.init();
        header
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockView {
    pub header: BlockHeaderView,
    pub transactions: Vec<SignedTransactionView>,
}

impl From<Block> for BlockView {
    fn from(block: Block) -> Self {
        BlockView {
            header: block.header.into(),
            transactions: block.transactions.into_iter().map(|tx| tx.into()).collect(),
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
                ActionView::Stake { stake: action.stake, public_key: action.public_key.into() }
            }
            Action::AddKey(action) => ActionView::AddKey {
                public_key: action.public_key.into(),
                access_key: action.access_key.into(),
            },
            Action::DeleteKey(action) => {
                ActionView::DeleteKey { public_key: action.public_key.into() }
            }
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
                Action::Stake(StakeAction { stake, public_key: public_key.into() })
            }
            ActionView::AddKey { public_key, access_key } => Action::AddKey(AddKeyAction {
                public_key: public_key.into(),
                access_key: access_key.into(),
            }),
            ActionView::DeleteKey { public_key } => {
                Action::DeleteKey(DeleteKeyAction { public_key: public_key.into() })
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
    hash: CryptoHashView,
}

impl From<SignedTransaction> for SignedTransactionView {
    fn from(signed_tx: SignedTransaction) -> Self {
        let hash = signed_tx.get_hash().into();
        SignedTransactionView {
            signer_id: signed_tx.transaction.signer_id,
            public_key: signed_tx.transaction.public_key.into(),
            nonce: signed_tx.transaction.nonce,
            receiver_id: signed_tx.transaction.receiver_id,
            actions: signed_tx
                .transaction
                .actions
                .into_iter()
                .map(|action| action.into())
                .collect(),
            signature: signed_tx.signature.into(),
            hash,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum FinalTransactionStatus {
    Unknown,
    Started,
    Failed,
    Completed,
}

impl Default for FinalTransactionStatus {
    fn default() -> Self {
        FinalTransactionStatus::Unknown
    }
}

impl FinalTransactionStatus {
    pub fn to_code(&self) -> u64 {
        match self {
            FinalTransactionStatus::Completed => 0,
            FinalTransactionStatus::Failed => 1,
            FinalTransactionStatus::Started => 2,
            FinalTransactionStatus::Unknown => std::u64::MAX,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionResultView {
    pub status: TransactionStatus,
    pub logs: Vec<LogEntry>,
    pub receipts: Vec<CryptoHashView>,
    pub result: Option<String>,
}

impl From<TransactionResult> for TransactionResultView {
    fn from(result: TransactionResult) -> Self {
        Self {
            status: result.status,
            logs: result.logs,
            receipts: result.receipts.into_iter().map(|h| h.into()).collect(),
            result: result.result.map(|v| to_base64(&v)),
        }
    }
}

impl From<TransactionResultView> for TransactionResult {
    fn from(view: TransactionResultView) -> Self {
        Self {
            status: view.status,
            logs: view.logs,
            receipts: view.receipts.into_iter().map(|h| h.into()).collect(),
            result: view.result.map(|v| from_base64(&v).unwrap()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionLogView {
    pub hash: CryptoHashView,
    pub result: TransactionResultView,
}

impl From<TransactionLog> for TransactionLogView {
    fn from(log: TransactionLog) -> Self {
        Self { hash: log.hash.into(), result: log.result.into() }
    }
}

/// Result of transaction and all of subsequent the receipts.
#[derive(Serialize, Deserialize)]
pub struct FinalTransactionResult {
    /// Status of the whole transaction and it's receipts.
    pub status: FinalTransactionStatus,
    /// Transaction results.
    pub transactions: Vec<TransactionLogView>,
}

impl fmt::Debug for FinalTransactionResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FinalTransactionResult")
            .field("status", &self.status)
            .field("transactions", &format_args!("{}", logging::pretty_vec(&self.transactions)))
            .finish()
    }
}

impl FinalTransactionResult {
    pub fn final_log(&self) -> String {
        let mut logs = vec![];
        for transaction in &self.transactions {
            for line in &transaction.result.logs {
                logs.push(line.clone());
            }
        }
        logs.join("\n")
    }

    pub fn last_result(&self) -> String {
        for transaction in self.transactions.iter().rev() {
            if let Some(r) = &transaction.result.result {
                return r.clone();
            }
        }
        "".to_string()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ValidatorStakeView {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
}

impl From<ValidatorStake> for ValidatorStakeView {
    fn from(stake: ValidatorStake) -> Self {
        Self {
            account_id: stake.account_id,
            public_key: stake.public_key.into(),
            amount: stake.amount,
        }
    }
}

impl From<ValidatorStakeView> for ValidatorStake {
    fn from(view: ValidatorStakeView) -> Self {
        Self {
            account_id: view.account_id,
            public_key: view.public_key.into(),
            amount: view.amount,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHashView,

    pub receipt: ReceiptEnumView,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataReceiverView {
    pub data_id: CryptoHashView,
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
        input_data_ids: Vec<CryptoHashView>,
        actions: Vec<ActionView>,
    },
    Data {
        data_id: CryptoHashView,
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    },
}

impl From<Receipt> for ReceiptView {
    fn from(receipt: Receipt) -> Self {
        ReceiptView {
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id.into(),
            receipt: match receipt.receipt {
                ReceiptEnum::Action(action_receipt) => ReceiptEnumView::Action {
                    signer_id: action_receipt.signer_id,
                    signer_public_key: action_receipt.signer_public_key.into(),
                    gas_price: action_receipt.gas_price,
                    output_data_receivers: action_receipt
                        .output_data_receivers
                        .into_iter()
                        .map(|data_receiver| DataReceiverView {
                            data_id: data_receiver.data_id.into(),
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
                ReceiptEnum::Data(data_receipt) => ReceiptEnumView::Data {
                    data_id: data_receipt.data_id.into(),
                    data: data_receipt.data,
                },
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
            receipt_id: receipt_view.receipt_id.into(),
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
                    signer_public_key: signer_public_key.into(),
                    gas_price,
                    output_data_receivers: output_data_receivers
                        .into_iter()
                        .map(|data_receiver_view| DataReceiver {
                            data_id: data_receiver_view.data_id.into(),
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
                    ReceiptEnum::Data(DataReceipt { data_id: data_id.into(), data })
                }
            },
        })
    }
}

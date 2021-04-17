use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance, EpochId, Gas, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

use crate::hash::CryptoHash;
use near_rpc_error_macro::RpcError;
use near_vm_errors::{CompilationError, FunctionCallErrorSer, MethodResolveError, VMLogicError};

/// Error returned in the ExecutionOutcome in case of failure
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub enum TxExecutionError {
    /// An error happened during Acton execution
    ActionError(ActionError),
    /// An error happened during Transaction execution
    InvalidTxError(InvalidTxError),
}

impl Display for TxExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            TxExecutionError::ActionError(e) => write!(f, "{}", e),
            TxExecutionError::InvalidTxError(e) => write!(f, "{}", e),
        }
    }
}

impl From<ActionError> for TxExecutionError {
    fn from(error: ActionError) -> Self {
        TxExecutionError::ActionError(error)
    }
}

impl From<InvalidTxError> for TxExecutionError {
    fn from(error: InvalidTxError) -> Self {
        TxExecutionError::InvalidTxError(error)
    }
}

/// Error returned from `Runtime::apply`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// An unexpected integer overflow occurred. The likely issue is an invalid state or the transition.
    UnexpectedIntegerOverflow,
    /// An error happened during TX verification and account charging. It's likely the chunk is invalid.
    /// and should be challenged.
    InvalidTxError(InvalidTxError),
    /// Unexpected error which is typically related to the node storage corruption.
    /// It's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// An error happens if `check_balance` fails, which is likely an indication of an invalid state.
    BalanceMismatchError(BalanceMismatchError),
    /// The incoming receipt didn't pass the validation, it's likely a malicious behaviour.
    ReceiptValidationError(ReceiptValidationError),
    /// Error when accessing validator information. Happens inside epoch manager.
    ValidatorError(EpochError),
}

/// Error used by `RuntimeExt`. This error has to be serializable, because it's transferred through
/// the `VMLogicError`, which isn't aware of internal Runtime errors.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExternalError {
    /// Unexpected error which is typically related to the node storage corruption.
    /// It's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// Error when accessing validator information. Happens inside epoch manager.
    ValidatorError(EpochError),
}

impl From<ExternalError> for VMLogicError {
    fn from(err: ExternalError) -> Self {
        VMLogicError::ExternalError(err.try_to_vec().expect("Borsh serialize cannot fail"))
    }
}

/// Internal
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Key-value db internal failure
    StorageInternalError,
    /// Storage is PartialStorage and requested a missing trie node
    TrieNodeMissing,
    /// Either invalid state or key-value db is corrupted.
    /// For PartialStorage it cannot be corrupted.
    /// Error message is unreliable and for debugging purposes only. It's also probably ok to
    /// panic in every place that produces this error.
    /// We can check if db is corrupted by verifying everything in the state trie.
    StorageInconsistentState(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// An error happened during TX execution
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub enum InvalidTxError {
    /// Happens if a wrong AccessKey used or AccessKey has not enough permissions
    InvalidAccessKeyError(InvalidAccessKeyError),
    /// TX signer_id is not in a valid format or not satisfy requirements see `near_runtime_utils::utils::is_valid_account_id`
    InvalidSignerId { signer_id: AccountId },
    /// TX signer_id is not found in a storage
    SignerDoesNotExist { signer_id: AccountId },
    /// Transaction nonce must be account[access_key].nonce + 1
    InvalidNonce { tx_nonce: Nonce, ak_nonce: Nonce },
    /// Transaction nonce is larger than the upper bound given by the block height
    NonceTooLarge { tx_nonce: Nonce, upper_bound: Nonce },
    /// TX receiver_id is not in a valid format or not satisfy requirements see `near_runtime_utils::is_valid_account_id`
    InvalidReceiverId { receiver_id: AccountId },
    /// TX signature is not valid
    InvalidSignature,
    /// Account does not have enough balance to cover TX cost
    NotEnoughBalance {
        signer_id: AccountId,
        #[serde(with = "u128_dec_format")]
        balance: Balance,
        #[serde(with = "u128_dec_format")]
        cost: Balance,
    },
    /// Signer account doesn't have enough balance after transaction.
    LackBalanceForState {
        /// An account which doesn't have enough balance to cover storage.
        signer_id: AccountId,
        /// Required balance to cover the state.
        #[serde(with = "u128_dec_format")]
        amount: Balance,
    },
    /// An integer overflow occurred during transaction cost estimation.
    CostOverflow,
    /// Transaction parent block hash doesn't belong to the current chain
    InvalidChain,
    /// Transaction has expired
    Expired,
    /// An error occurred while validating actions of a Transaction.
    ActionsValidation(ActionsValidationError),
    /// The size of serialized transaction exceeded the limit.
    #[cfg(feature = "protocol_feature_tx_size_limit")]
    TransactionSizeExceeded { size: u64, limit: u64 },
}

#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub enum InvalidAccessKeyError {
    /// The access key identified by the `public_key` doesn't exist for the account
    AccessKeyNotFound { account_id: AccountId, public_key: PublicKey },
    /// Transaction `receiver_id` doesn't match the access key receiver_id
    ReceiverMismatch { tx_receiver: AccountId, ak_receiver: AccountId },
    /// Transaction method name isn't allowed by the access key
    MethodNameMismatch { method_name: String },
    /// Transaction requires a full permission access key.
    RequiresFullAccess,
    /// Access Key does not have enough allowance to cover transaction cost
    NotEnoughAllowance {
        account_id: AccountId,
        public_key: PublicKey,
        #[serde(with = "u128_dec_format")]
        allowance: Balance,
        #[serde(with = "u128_dec_format")]
        cost: Balance,
    },
    /// Having a deposit with a function call action is not allowed with a function call access key.
    DepositWithFunctionCall,
}

/// Describes the error for validating a list of actions.
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, RpcError,
)]
pub enum ActionsValidationError {
    /// The delete action must be a final aciton in transaction
    DeleteActionMustBeFinal,
    /// The total prepaid gas (for all given actions) exceeded the limit.
    TotalPrepaidGasExceeded { total_prepaid_gas: Gas, limit: Gas },
    /// The number of actions exceeded the given limit.
    TotalNumberOfActionsExceeded { total_number_of_actions: u64, limit: u64 },
    /// The total number of bytes of the method names exceeded the limit in a Add Key action.
    AddKeyMethodNamesNumberOfBytesExceeded { total_number_of_bytes: u64, limit: u64 },
    /// The length of some method name exceeded the limit in a Add Key action.
    AddKeyMethodNameLengthExceeded { length: u64, limit: u64 },
    /// Integer overflow during a compute.
    IntegerOverflow,
    /// Invalid account ID.
    InvalidAccountId { account_id: AccountId },
    /// The size of the contract code exceeded the limit in a DeployContract action.
    ContractSizeExceeded { size: u64, limit: u64 },
    /// The length of the method name exceeded the limit in a Function Call action.
    FunctionCallMethodNameLengthExceeded { length: u64, limit: u64 },
    /// The length of the arguments exceeded the limit in a Function Call action.
    FunctionCallArgumentsLengthExceeded { length: u64, limit: u64 },
    /// An attempt to stake with a public key that is not convertible to ristretto.
    UnsuitableStakingKey { public_key: PublicKey },
    /// The attached amount of gas in a FunctionCall action has to be a positive number.
    FunctionCallZeroAttachedGas,
}

/// Describes the error for validating a receipt.
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, RpcError,
)]
pub enum ReceiptValidationError {
    /// The `predecessor_id` of a Receipt is not valid.
    InvalidPredecessorId { account_id: AccountId },
    /// The `receiver_id` of a Receipt is not valid.
    InvalidReceiverId { account_id: AccountId },
    /// The `signer_id` of an ActionReceipt is not valid.
    InvalidSignerId { account_id: AccountId },
    /// The `receiver_id` of a DataReceiver within an ActionReceipt is not valid.
    InvalidDataReceiverId { account_id: AccountId },
    /// The length of the returned data exceeded the limit in a DataReceipt.
    ReturnedValueLengthExceeded { length: u64, limit: u64 },
    /// The number of input data dependencies exceeds the limit in an ActionReceipt.
    NumberInputDataDependenciesExceeded { number_of_input_data_dependencies: u64, limit: u64 },
    /// An error occurred while validating actions of an ActionReceipt.
    ActionsValidation(ActionsValidationError),
}

impl Display for ReceiptValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ReceiptValidationError::InvalidPredecessorId { account_id } => {
                write!(f, "The predecessor_id `{}` of a Receipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidReceiverId { account_id } => {
                write!(f, "The receiver_id `{}` of a Receipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidSignerId { account_id } => {
                write!(f, "The signer_id `{}` of an ActionReceipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidDataReceiverId { account_id } => write!(
                f,
                "The receiver_id `{}` of a DataReceiver within an ActionReceipt is not valid.",
                account_id
            ),
            ReceiptValidationError::ReturnedValueLengthExceeded { length, limit } => write!(
                f,
                "The length of the returned data {} exceeded the limit {} in a DataReceipt",
                length, limit
            ),
            ReceiptValidationError::NumberInputDataDependenciesExceeded { number_of_input_data_dependencies, limit } => write!(
                f,
                "The number of input data dependencies {} exceeded the limit {} in an ActionReceipt",
                number_of_input_data_dependencies, limit
            ),
            ReceiptValidationError::ActionsValidation(e) => write!(f, "{}", e),
        }
    }
}

impl Display for ActionsValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ActionsValidationError::DeleteActionMustBeFinal => {
                write!(f, "The delete action must be the last action in transaction")
            }
            ActionsValidationError::TotalPrepaidGasExceeded { total_prepaid_gas, limit } => {
                write!(f, "The total prepaid gas {} exceeds the limit {}", total_prepaid_gas, limit)
            }
            ActionsValidationError::TotalNumberOfActionsExceeded {total_number_of_actions, limit } => {
                write!(
                    f,
                    "The total number of actions {} exceeds the limit {}",
                    total_number_of_actions, limit
                )
            }
            ActionsValidationError::AddKeyMethodNamesNumberOfBytesExceeded { total_number_of_bytes, limit } => write!(
                f,
                "The total number of bytes in allowed method names {} exceeds the maximum allowed number {} in a AddKey action",
                total_number_of_bytes, limit
            ),
            ActionsValidationError::AddKeyMethodNameLengthExceeded { length, limit } => write!(
                f,
                "The length of some method name {} exceeds the maximum allowed length {} in a AddKey action",
                length, limit
            ),
            ActionsValidationError::IntegerOverflow => write!(
                f,
                "Integer overflow during a compute",
            ),
            ActionsValidationError::InvalidAccountId { account_id } => write!(
                f,
                "Invalid account ID `{}`",
                account_id
            ),
            ActionsValidationError::ContractSizeExceeded { size, limit } => write!(
                f,
                "The length of the contract size {} exceeds the maximum allowed size {} in a DeployContract action",
                size, limit
            ),
            ActionsValidationError::FunctionCallMethodNameLengthExceeded { length, limit } => write!(
                f,
                "The length of the method name {} exceeds the maximum allowed length {} in a FunctionCall action",
                length, limit
            ),
            ActionsValidationError::FunctionCallArgumentsLengthExceeded { length, limit } => write!(
                f,
                "The length of the arguments {} exceeds the maximum allowed length {} in a FunctionCall action",
                length, limit
            ),
            ActionsValidationError::UnsuitableStakingKey { public_key } => write!(
                f,
                "The staking key must be ristretto compatible ED25519 key. {} is provided instead.",
                public_key,
            ),
            ActionsValidationError::FunctionCallZeroAttachedGas => write!(
                f,
                "The attached amount of gas in a FunctionCall action has to be a positive number",
            ),
        }
    }
}

/// An error happened during Acton execution
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub struct ActionError {
    /// Index of the failed action in the transaction.
    /// Action index is not defined if ActionError.kind is `ActionErrorKind::LackBalanceForState`
    pub index: Option<u64>,
    /// The kind of ActionError happened
    pub kind: ActionErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq, RpcError)]
pub enum ContractCallError {
    MethodResolveError(MethodResolveError),
    CompilationError(CompilationError),
    ExecutionError { msg: String },
}

impl From<ContractCallError> for FunctionCallErrorSer {
    fn from(e: ContractCallError) -> Self {
        match e {
            ContractCallError::CompilationError(e) => FunctionCallErrorSer::CompilationError(e),
            ContractCallError::MethodResolveError(e) => FunctionCallErrorSer::MethodResolveError(e),
            ContractCallError::ExecutionError { msg } => FunctionCallErrorSer::ExecutionError(msg),
        }
    }
}

impl From<FunctionCallErrorSer> for ContractCallError {
    fn from(e: FunctionCallErrorSer) -> Self {
        match e {
            FunctionCallErrorSer::CompilationError(e) => ContractCallError::CompilationError(e),
            FunctionCallErrorSer::MethodResolveError(e) => ContractCallError::MethodResolveError(e),
            FunctionCallErrorSer::ExecutionError(msg) => ContractCallError::ExecutionError { msg },
            FunctionCallErrorSer::LinkError { msg } => ContractCallError::ExecutionError { msg },
            FunctionCallErrorSer::WasmUnknownError => {
                ContractCallError::ExecutionError { msg: "unknown error".to_string() }
            }
            FunctionCallErrorSer::EvmError(e) => {
                ContractCallError::ExecutionError { msg: format!("EVM: {:?}", e) }
            }
            FunctionCallErrorSer::WasmTrap(e) => {
                ContractCallError::ExecutionError { msg: format!("WASM: {:?}", e) }
            }
            FunctionCallErrorSer::HostError(e) => {
                ContractCallError::ExecutionError { msg: format!("Host: {:?}", e) }
            }
        }
    }
}

#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub enum ActionErrorKind {
    /// Happens when CreateAccount action tries to create an account with account_id which is already exists in the storage
    AccountAlreadyExists { account_id: AccountId },
    /// Happens when TX receiver_id doesn't exist (but action is not Action::CreateAccount)
    AccountDoesNotExist { account_id: AccountId },
    /// A top-level account ID can only be created by registrar.
    CreateAccountOnlyByRegistrar {
        account_id: AccountId,
        registrar_account_id: AccountId,
        predecessor_id: AccountId,
    },
    /// A newly created account must be under a namespace of the creator account
    CreateAccountNotAllowed { account_id: AccountId, predecessor_id: AccountId },
    /// Administrative actions like `DeployContract`, `Stake`, `AddKey`, `DeleteKey`. can be proceed only if sender=receiver
    /// or the first TX action is a `CreateAccount` action
    ActorNoPermission { account_id: AccountId, actor_id: AccountId },
    /// Account tries to remove an access key that doesn't exist
    DeleteKeyDoesNotExist { account_id: AccountId, public_key: PublicKey },
    /// The public key is already used for an existing access key
    AddKeyAlreadyExists { account_id: AccountId, public_key: PublicKey },
    /// Account is staking and can not be deleted
    DeleteAccountStaking { account_id: AccountId },
    /// ActionReceipt can't be completed, because the remaining balance will not be enough to cover storage.
    LackBalanceForState {
        /// An account which needs balance
        account_id: AccountId,
        /// Balance required to complete an action.
        #[serde(with = "u128_dec_format")]
        amount: Balance,
    },
    /// Account is not yet staked, but tries to unstake
    TriesToUnstake { account_id: AccountId },
    /// The account doesn't have enough balance to increase the stake.
    TriesToStake {
        account_id: AccountId,
        #[serde(with = "u128_dec_format")]
        stake: Balance,
        #[serde(with = "u128_dec_format")]
        locked: Balance,
        #[serde(with = "u128_dec_format")]
        balance: Balance,
    },
    InsufficientStake {
        account_id: AccountId,
        #[serde(with = "u128_dec_format")]
        stake: Balance,
        #[serde(with = "u128_dec_format")]
        minimum_stake: Balance,
    },
    /// An error occurred during a `FunctionCall` Action, parameter is debug message.
    FunctionCallError(FunctionCallErrorSer),
    /// Error occurs when a new `ActionReceipt` created by the `FunctionCall` action fails
    /// receipt validation.
    NewReceiptValidationError(ReceiptValidationError),
    /// Error occurs when a `CreateAccount` action is called on hex-characters account of length 64.
    /// See implicit account creation NEP: https://github.com/nearprotocol/NEPs/pull/71
    OnlyImplicitAccountCreationAllowed { account_id: AccountId },
    /// Delete account whose state is large is temporarily banned.
    DeleteAccountWithLargeState { account_id: AccountId },
}

impl From<ActionErrorKind> for ActionError {
    fn from(e: ActionErrorKind) -> ActionError {
        ActionError { index: None, kind: e }
    }
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            InvalidTxError::InvalidSignerId { signer_id } => {
                write!(f, "Invalid signer account ID {:?} according to requirements", signer_id)
            }
            InvalidTxError::SignerDoesNotExist { signer_id } => {
                write!(f, "Signer {:?} does not exist", signer_id)
            }
            InvalidTxError::InvalidAccessKeyError(access_key_error) => {
                Display::fmt(&access_key_error, f)
            }
            InvalidTxError::InvalidNonce { tx_nonce, ak_nonce } => write!(
                f,
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                tx_nonce, ak_nonce
            ),
            InvalidTxError::InvalidReceiverId { receiver_id } => {
                write!(f, "Invalid receiver account ID {:?} according to requirements", receiver_id)
            }
            InvalidTxError::InvalidSignature => {
                write!(f, "Transaction is not signed with the given public key")
            }
            InvalidTxError::NotEnoughBalance { signer_id, balance, cost } => write!(
                f,
                "Sender {:?} does not have enough balance {} for operation costing {}",
                signer_id, balance, cost
            ),
            InvalidTxError::LackBalanceForState { signer_id, amount } => {
                write!(f, "Failed to execute, because the account {:?} wouldn't have enough balance to cover storage, required to have {} yoctoNEAR more", signer_id, amount)
            }
            InvalidTxError::CostOverflow => {
                write!(f, "Transaction gas or balance cost is too high")
            }
            InvalidTxError::InvalidChain => {
                write!(f, "Transaction parent block hash doesn't belong to the current chain")
            }
            InvalidTxError::Expired => {
                write!(f, "Transaction has expired")
            }
            InvalidTxError::ActionsValidation(error) => {
                write!(f, "Transaction actions validation error: {}", error)
            }
            InvalidTxError::NonceTooLarge { tx_nonce, upper_bound } => {
                write!(
                    f,
                    "Transaction nonce {} must be smaller than the access key nonce upper bound {}",
                    tx_nonce, upper_bound
                )
            }
            #[cfg(feature = "protocol_feature_tx_size_limit")]
            InvalidTxError::TransactionSizeExceeded { size, limit } => {
                write!(f, "Size of serialized transaction {} exceeded the limit {}", size, limit)
            }
        }
    }
}

impl From<InvalidAccessKeyError> for InvalidTxError {
    fn from(error: InvalidAccessKeyError) -> Self {
        InvalidTxError::InvalidAccessKeyError(error)
    }
}

impl Display for InvalidAccessKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            InvalidAccessKeyError::AccessKeyNotFound { account_id, public_key } => write!(
                f,
                "Signer {:?} doesn't have access key with the given public_key {}",
                account_id, public_key
            ),
            InvalidAccessKeyError::ReceiverMismatch { tx_receiver, ak_receiver } => write!(
                f,
                "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                tx_receiver, ak_receiver
            ),
            InvalidAccessKeyError::MethodNameMismatch { method_name } => write!(
                f,
                "Transaction method name {:?} isn't allowed by the access key",
                method_name
            ),
            InvalidAccessKeyError::RequiresFullAccess => write!(
                f,
                "The transaction contains more then one action, but it was signed \
                 with an access key which allows transaction to apply only one specific action. \
                 To apply more then one actions TX must be signed with a full access key"
            ),
            InvalidAccessKeyError::NotEnoughAllowance {
                account_id,
                public_key,
                allowance,
                cost,
            } => write!(
                f,
                "Access Key {:?}:{} does not have enough balance {} for transaction costing {}",
                account_id, public_key, allowance, cost
            ),
            InvalidAccessKeyError::DepositWithFunctionCall => {
                write!(f, "Having a deposit with a function call action is not allowed with a function call access key.")
            }
        }
    }
}

/// Happens when the input balance doesn't match the output balance in Runtime apply.
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
pub struct BalanceMismatchError {
    // Input balances
    #[serde(with = "u128_dec_format")]
    pub incoming_validator_rewards: Balance,
    #[serde(with = "u128_dec_format")]
    pub initial_accounts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub incoming_receipts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub processed_delayed_receipts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub initial_postponed_receipts_balance: Balance,
    // Output balances
    #[serde(with = "u128_dec_format")]
    pub final_accounts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub outgoing_receipts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub new_delayed_receipts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub final_postponed_receipts_balance: Balance,
    #[serde(with = "u128_dec_format")]
    pub tx_burnt_amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub slashed_burnt_amount: Balance,
    #[serde(with = "u128_dec_format")]
    pub other_burnt_amount: Balance,
}

impl Display for BalanceMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        // Using saturating add to avoid overflow in display
        let initial_balance = self
            .incoming_validator_rewards
            .saturating_add(self.initial_accounts_balance)
            .saturating_add(self.incoming_receipts_balance)
            .saturating_add(self.processed_delayed_receipts_balance)
            .saturating_add(self.initial_postponed_receipts_balance);
        let final_balance = self
            .final_accounts_balance
            .saturating_add(self.outgoing_receipts_balance)
            .saturating_add(self.new_delayed_receipts_balance)
            .saturating_add(self.final_postponed_receipts_balance)
            .saturating_add(self.tx_burnt_amount)
            .saturating_add(self.slashed_burnt_amount)
            .saturating_add(self.other_burnt_amount);
        write!(
            f,
            "Balance Mismatch Error. The input balance {} doesn't match output balance {}\n\
             Inputs:\n\
             \tIncoming validator rewards sum: {}\n\
             \tInitial accounts balance sum: {}\n\
             \tIncoming receipts balance sum: {}\n\
             \tProcessed delayed receipts balance sum: {}\n\
             \tInitial postponed receipts balance sum: {}\n\
             Outputs:\n\
             \tFinal accounts balance sum: {}\n\
             \tOutgoing receipts balance sum: {}\n\
             \tNew delayed receipts balance sum: {}\n\
             \tFinal postponed receipts balance sum: {}\n\
             \tTx fees burnt amount: {}\n\
             \tSlashed amount: {}\n\
             \tOther burnt amount: {}",
            initial_balance,
            final_balance,
            self.incoming_validator_rewards,
            self.initial_accounts_balance,
            self.incoming_receipts_balance,
            self.processed_delayed_receipts_balance,
            self.initial_postponed_receipts_balance,
            self.final_accounts_balance,
            self.outgoing_receipts_balance,
            self.new_delayed_receipts_balance,
            self.final_postponed_receipts_balance,
            self.tx_burnt_amount,
            self.slashed_burnt_amount,
            self.other_burnt_amount,
        )
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct IntegerOverflowError;

impl From<IntegerOverflowError> for InvalidTxError {
    fn from(_: IntegerOverflowError) -> Self {
        InvalidTxError::CostOverflow
    }
}

impl From<IntegerOverflowError> for RuntimeError {
    fn from(_: IntegerOverflowError) -> Self {
        RuntimeError::UnexpectedIntegerOverflow
    }
}

impl From<StorageError> for RuntimeError {
    fn from(e: StorageError) -> Self {
        RuntimeError::StorageError(e)
    }
}

impl From<BalanceMismatchError> for RuntimeError {
    fn from(e: BalanceMismatchError) -> Self {
        RuntimeError::BalanceMismatchError(e)
    }
}

impl From<InvalidTxError> for RuntimeError {
    fn from(e: InvalidTxError) -> Self {
        RuntimeError::InvalidTxError(e)
    }
}

impl From<EpochError> for RuntimeError {
    fn from(e: EpochError) -> Self {
        RuntimeError::ValidatorError(e)
    }
}

impl Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Action #{}: {}", self.index.unwrap_or_default(), self.kind)
    }
}

impl Display for ActionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ActionErrorKind::AccountAlreadyExists { account_id } => {
                write!(f, "Can't create a new account {:?}, because it already exists", account_id)
            }
            ActionErrorKind::AccountDoesNotExist { account_id } => write!(
                f,
                "Can't complete the action because account {:?} doesn't exist",
                account_id
            ),
            ActionErrorKind::ActorNoPermission { actor_id, account_id } => write!(
                f,
                "Actor {:?} doesn't have permission to account {:?} to complete the action",
                actor_id, account_id
            ),
            ActionErrorKind::LackBalanceForState { account_id, amount } => write!(
                f,
                "The account {} wouldn't have enough balance to cover storage, required to have {} yoctoNEAR more",
                account_id, amount
            ),
            ActionErrorKind::TriesToUnstake { account_id } => {
                write!(f, "Account {:?} is not yet staked, but tries to unstake", account_id)
            }
            ActionErrorKind::TriesToStake { account_id, stake, locked, balance } => write!(
                f,
                "Account {:?} tries to stake {}, but has staked {} and only has {}",
                account_id, stake, locked, balance
            ),
            ActionErrorKind::CreateAccountOnlyByRegistrar { account_id, registrar_account_id, predecessor_id } => write!(
                f,
                "A top-level account ID {:?} can't be created by {:?}, short top-level account IDs can only be created by {:?}",
                account_id, predecessor_id, registrar_account_id,
            ),
            ActionErrorKind::CreateAccountNotAllowed { account_id, predecessor_id } => write!(
                f,
                "A sub-account ID {:?} can't be created by account {:?}",
                account_id, predecessor_id,
            ),
            ActionErrorKind::DeleteKeyDoesNotExist { account_id, .. } => write!(
                f,
                "Account {:?} tries to remove an access key that doesn't exist",
                account_id
            ),
            ActionErrorKind::AddKeyAlreadyExists { public_key, .. } => write!(
                f,
                "The public key {:?} is already used for an existing access key",
                public_key
            ),
            ActionErrorKind::DeleteAccountStaking { account_id } => {
                write!(f, "Account {:?} is staking and can not be deleted", account_id)
            }
            ActionErrorKind::FunctionCallError(s) => write!(f, "{:?}", s),
            ActionErrorKind::NewReceiptValidationError(e) => {
                write!(f, "An new action receipt created during a FunctionCall is not valid: {}", e)
            }
            ActionErrorKind::InsufficientStake { account_id, stake, minimum_stake } => write!(f, "Account {} tries to stake {} but minimum required stake is {}", account_id, stake, minimum_stake),
            ActionErrorKind::OnlyImplicitAccountCreationAllowed { account_id } => write!(f, "CreateAccount action is called on hex-characters account of length 64 {}", account_id),
            ActionErrorKind::DeleteAccountWithLargeState { account_id } => write!(f, "The state of account {} is too large and therefore cannot be deleted", account_id),
        }
    }
}

#[derive(Eq, PartialEq, BorshSerialize, BorshDeserialize, Clone)]
pub enum EpochError {
    /// Error calculating threshold from given stakes for given number of seats.
    /// Only should happened if calling code doesn't check for integer value of stake > number of seats.
    ThresholdError { stake_sum: Balance, num_seats: u64 },
    /// Requesting validators for an epoch that wasn't computed yet.
    EpochOutOfBounds(EpochId),
    /// Missing block hash in the storage (means there is some structural issue).
    MissingBlock(CryptoHash),
    /// Error due to IO (DB read/write, serialization, etc.).
    IOErr(String),
    /// Given account ID is not a validator in the given epoch ID.
    NotAValidator(AccountId, EpochId),
}

impl std::error::Error for EpochError {}

impl Display for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::ThresholdError { stake_sum, num_seats } => write!(
                f,
                "Total stake {} must be higher than the number of seats {}",
                stake_sum, num_seats
            ),
            EpochError::EpochOutOfBounds(epoch_id) => {
                write!(f, "Epoch {:?} is out of bounds", epoch_id)
            }
            EpochError::MissingBlock(hash) => write!(f, "Missing block {}", hash),
            EpochError::IOErr(err) => write!(f, "IO: {}", err),
            EpochError::NotAValidator(account_id, epoch_id) => {
                write!(f, "{} is not a validator in epoch {:?}", account_id, epoch_id)
            }
        }
    }
}

impl Debug for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::ThresholdError { stake_sum, num_seats } => {
                write!(f, "ThresholdError({}, {})", stake_sum, num_seats)
            }
            EpochError::EpochOutOfBounds(epoch_id) => write!(f, "EpochOutOfBounds({:?})", epoch_id),
            EpochError::MissingBlock(hash) => write!(f, "MissingBlock({})", hash),
            EpochError::IOErr(err) => write!(f, "IOErr({})", err),
            EpochError::NotAValidator(account_id, epoch_id) => {
                write!(f, "NotAValidator({}, {:?})", account_id, epoch_id)
            }
        }
    }
}

impl From<std::io::Error> for EpochError {
    fn from(error: std::io::Error) -> Self {
        EpochError::IOErr(error.to_string())
    }
}

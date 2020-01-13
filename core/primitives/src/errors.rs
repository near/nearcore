use crate::types::{AccountId, Balance, Gas, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use std::fmt::Display;

/// Internal
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// External
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum InvalidTxError {
    InvalidSigner(AccountId),
    SignerDoesNotExist(AccountId),
    InvalidAccessKey(InvalidAccessKeyError),
    InvalidNonce(Nonce, Nonce),
    InvalidReceiver(AccountId),
    InvalidSignature,
    NotEnoughBalance(AccountId, Balance, Balance),
    RentUnpaid(AccountId, Balance),
    CostOverflow,
    InvalidChain,
    Expired,
    /// An error occurred while validating actions of a Transaction.
    ActionsValidation(ActionsValidationError),
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum InvalidAccessKeyError {
    AccessKeyNotFound(AccountId, PublicKey),
    ReceiverMismatch(AccountId, AccountId),
    MethodNameMismatch(String),
    ActionError,
    NotEnoughAllowance(AccountId, PublicKey, Balance, Balance),
    /// Having a deposit with a function call action is not allowed with a function call access key.  
    DepositWithFunctionCall,
}

/// Describes the error for validating a list of actions.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ActionsValidationError {
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
}

/// Describes the error for validating a receipt.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ActionsValidationError::TotalPrepaidGasExceeded{ total_prepaid_gas, limit } => {
                write!(f, "The total prepaid gas {} exceeds the limit {}", total_prepaid_gas, limit)
            }
            ActionsValidationError::TotalNumberOfActionsExceeded{total_number_of_actions, limit} => {
                write!(
                    f,
                    "The total number of actions {} exceeds the limit {}",
                    total_number_of_actions, limit
                )
            }
            ActionsValidationError::AddKeyMethodNamesNumberOfBytesExceeded{total_number_of_bytes, limit} => write!(
                f,
                "The total number of bytes in allowed method names {} exceeds the maximum allowed number {} in a AddKey action",
                total_number_of_bytes, limit
            ),
            ActionsValidationError::AddKeyMethodNameLengthExceeded{length, limit} => write!(
                f,
                "The length of some method name {} exceeds the maximum allowed length {} in a AddKey action",
                length, limit
            ),
            ActionsValidationError::IntegerOverflow => write!(
                f,
                "Integer overflow during a compute",
            ),
            ActionsValidationError::InvalidAccountId{account_id} => write!(
                f,
                "Invalid account ID `{}`",
                account_id
            ),
            ActionsValidationError::ContractSizeExceeded{ size, limit} => write!(
                f,
                "The length of the contract size {} exceeds the maximum allowed size {} in a DeployContract action",
                size, limit
            ),
            ActionsValidationError::FunctionCallMethodNameLengthExceeded{length, limit} => write!(
                f,
                "The length of the method name {} exceeds the maximum allowed length {} in a FunctionCall action",
                length, limit
            ),
            ActionsValidationError::FunctionCallArgumentsLengthExceeded{length, limit} => write!(
                f,
                "The length of the arguments {} exceeds the maximum allowed length {} in a FunctionCall action",
                length, limit
            ),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ActionError {
    AccountAlreadyExists(AccountId),
    AccountDoesNotExist(String, AccountId),
    CreateAccountNotAllowed(AccountId, AccountId),
    ActorNoPermission(AccountId, AccountId, String),
    DeleteKeyDoesNotExist(AccountId),
    AddKeyAlreadyExists(PublicKey),
    DeleteAccountStaking(AccountId),
    DeleteAccountHasRent(AccountId, Balance),
    RentUnpaid(AccountId, Balance),
    TriesToUnstake(AccountId),
    TriesToStake(AccountId, Balance, Balance, Balance),
    FunctionCallError(String), // TODO type
    /// Error occurs when a new `ActionReceipt` created by the `FunctionCall` action fails
    /// receipt validation.
    NewReceiptValidationError(ReceiptValidationError),
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            InvalidTxError::InvalidSigner(signer_id) => {
                write!(f, "Invalid signer account ID {:?} according to requirements", signer_id)
            }
            InvalidTxError::SignerDoesNotExist(signer_id) => {
                write!(f, "Signer {:?} does not exist", signer_id)
            }
            InvalidTxError::InvalidAccessKey(access_key_error) => access_key_error.fmt(f),
            InvalidTxError::InvalidNonce(tx_nonce, ak_nonce) => write!(
                f,
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                tx_nonce, ak_nonce
            ),
            InvalidTxError::InvalidReceiver(receiver_id) => {
                write!(f, "Invalid receiver account ID {:?} according to requirements", receiver_id)
            }
            InvalidTxError::InvalidSignature => {
                write!(f, "Transaction is not signed with the given public key")
            }
            InvalidTxError::NotEnoughBalance(signer_id, balance, cost) => write!(
                f,
                "Sender {:?} does not have enough balance {} for operation costing {}",
                signer_id, balance, cost
            ),
            InvalidTxError::RentUnpaid(signer_id, amount) => {
                write!(f, "Failed to execute, because the account {:?} wouldn't have enough to pay required rent {}", signer_id, amount)
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
        }
    }
}

impl From<InvalidAccessKeyError> for InvalidTxError {
    fn from(error: InvalidAccessKeyError) -> Self {
        InvalidTxError::InvalidAccessKey(error)
    }
}

impl Display for InvalidAccessKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            InvalidAccessKeyError::AccessKeyNotFound(account_id, public_key) => write!(
                f,
                "Signer {:?} doesn't have access key with the given public_key {}",
                account_id, public_key
            ),
            InvalidAccessKeyError::ReceiverMismatch(tx_receiver, ak_receiver) => write!(
                f,
                "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                tx_receiver, ak_receiver
            ),
            InvalidAccessKeyError::MethodNameMismatch(method_name) => write!(
                f,
                "Transaction method name {:?} isn't allowed by the access key",
                method_name
            ),
            InvalidAccessKeyError::ActionError => {
                write!(f, "The used access key requires exactly one FunctionCall action")
            }
            InvalidAccessKeyError::NotEnoughAllowance(account_id, public_key, allowance, cost) => {
                write!(
                    f,
                    "Access Key {:?}:{} does not have enough balance {} for transaction costing {}",
                    account_id, public_key, allowance, cost
                )
            }
            InvalidAccessKeyError::DepositWithFunctionCall => {
                write!(f, "Having a deposit with a function call action is not allowed with a function call access key.")
            }
        }
    }
}

/// Happens when the input balance doesn't match the output balance in Runtime apply.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct BalanceMismatchError {
    // Input balances
    pub incoming_validator_rewards: Balance,
    pub initial_accounts_balance: Balance,
    pub incoming_receipts_balance: Balance,
    pub processed_delayed_receipts_balance: Balance,
    pub initial_postponed_receipts_balance: Balance,
    // Output balances
    pub final_accounts_balance: Balance,
    pub outgoing_receipts_balance: Balance,
    pub new_delayed_receipts_balance: Balance,
    pub final_postponed_receipts_balance: Balance,
    pub total_rent_paid: Balance,
    pub total_validator_reward: Balance,
    pub total_balance_burnt: Balance,
    pub total_balance_slashed: Balance,
}

impl Display for BalanceMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
            .saturating_add(self.total_rent_paid)
            .saturating_add(self.total_validator_reward)
            .saturating_add(self.total_balance_burnt)
            .saturating_add(self.total_balance_slashed);
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
             \tTotal rent paid: {}\n\
             \tTotal validators reward: {}\n\
             \tTotal balance burnt: {}\n\
             \tTotal balance slashed: {}",
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
            self.total_rent_paid,
            self.total_validator_reward,
            self.total_balance_burnt,
            self.total_balance_slashed,
        )
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct IntegerOverflowError;

/// Error returned from `Runtime::apply`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    UnexpectedIntegerOverflow,
    InvalidTxError(InvalidTxError),
    StorageError(StorageError),
    BalanceMismatch(BalanceMismatchError),
}

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
        RuntimeError::BalanceMismatch(e)
    }
}

impl From<InvalidTxError> for RuntimeError {
    fn from(e: InvalidTxError) -> Self {
        RuntimeError::InvalidTxError(e)
    }
}

impl Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ActionError::AccountAlreadyExists(account_id) => {
                write!(f, "Can't create a new account {:?}, because it already exists", account_id)
            }
            ActionError::AccountDoesNotExist(action, account_id) => write!(
                f,
                "Can't complete the action {:?}, because account {:?} doesn't exist",
                action, account_id
            ),
            ActionError::ActorNoPermission(actor_id, account_id, action) => write!(
                f,
                "Actor {:?} doesn't have permission to account {:?} to complete the action {:?}",
                actor_id, account_id, action
            ),
            ActionError::RentUnpaid(account_id, amount) => write!(
                f,
                "The account {} wouldn't have enough balance to pay required rent {}",
                account_id, amount
            ),
            ActionError::TriesToUnstake(account_id) => {
                write!(f, "Account {:?} is not yet staked, but tries to unstake", account_id)
            }
            ActionError::TriesToStake(account_id, stake, staked, balance) => write!(
                f,
                "Account {:?} tries to stake {}, but has staked {} and only has {}",
                account_id, stake, staked, balance
            ),
            ActionError::CreateAccountNotAllowed(account_id, predecessor_id) => write!(
                f,
                "The new account_id {:?} can't be created by {:?}",
                account_id, predecessor_id
            ),
            ActionError::DeleteKeyDoesNotExist(account_id) => write!(
                f,
                "Account {:?} tries to remove an access key that doesn't exist",
                account_id
            ),
            ActionError::AddKeyAlreadyExists(public_key) => write!(
                f,
                "The public key {:?} is already used for an existing access key",
                public_key
            ),
            ActionError::DeleteAccountStaking(account_id) => {
                write!(f, "Account {:?} is staking and can not be deleted", account_id)
            }
            ActionError::DeleteAccountHasRent(account_id, balance) => write!(
                f,
                "Account {:?} can't be deleted. It has {}, which is enough to cover the rent",
                account_id, balance
            ),
            ActionError::FunctionCallError(s) => write!(f, "{}", s),
            ActionError::NewReceiptValidationError(e) => {
                write!(f, "An new action receipt created during a FunctionCall is not valid: {}", e)
            }
        }
    }
}

/// Error returned in the ExecutionOutcome in case of failure.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    Action(ActionError),
    InvalidTx(InvalidTxError),
}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ExecutionError::Action(e) => write!(f, "{}", e),
            ExecutionError::InvalidTx(e) => write!(f, "{}", e),
        }
    }
}

impl From<ActionError> for ExecutionError {
    fn from(error: ActionError) -> Self {
        ExecutionError::Action(error)
    }
}

impl From<InvalidTxError> for ExecutionError {
    fn from(error: InvalidTxError) -> Self {
        ExecutionError::InvalidTx(error)
    }
}

use crate::types::{AccountId, Balance, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use std::fmt::Display;

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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// External
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum InvalidTxError {
    InvalidAccessKey(InvalidAccessKeyError),
    InvalidSigner { signer_id: AccountId },
    SignerDoesNotExist { signer_id: AccountId },
    InvalidNonce { tx_nonce: Nonce, ak_nonce: Nonce },
    InvalidReceiver { receiver_id: AccountId },
    InvalidSignature,
    NotEnoughBalance { signer_id: AccountId, balance: Balance, cost: Balance },
    RentUnpaid { signer_id: AccountId, amount: Balance },
    CostOverflow,
    InvalidChain,
    Expired,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum InvalidAccessKeyError {
    AccessKeyNotFound {
        account_id: AccountId,
        public_key: PublicKey,
    },
    ReceiverMismatch {
        tx_receiver: AccountId,
        ak_receiver: AccountId,
    },
    MethodNameMismatch {
        method_name: String,
    },
    ActionError,
    NotEnoughAllowance {
        account_id: AccountId,
        public_key: PublicKey,
        allowance: Balance,
        cost: Balance,
    },
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ActionError {
    AccountAlreadyExists { account_id: AccountId },
    AccountDoesNotExist { action: String, account_id: AccountId },
    CreateAccountNotAllowed { account_id: AccountId, predecessor_id: AccountId },
    ActorNoPermission { account_id: AccountId, actor_id: AccountId, action: String },
    DeleteKeyDoesNotExist { account_id: AccountId },
    AddKeyAlreadyExists { public_key: PublicKey },
    DeleteAccountStaking { account_id: AccountId },
    DeleteAccountHasRent { account_id: AccountId, balance: Balance },
    RentUnpaid { account_id: AccountId, amount: Balance },
    TriesToUnstake { account_id: AccountId },
    TriesToStake { account_id: AccountId, stake: Balance, locked: Balance, balance: Balance },
    FunctionCallError(String), // TODO type
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            InvalidTxError::InvalidSigner{signer_id} => {
                write!(f, "Invalid signer account ID {:?} according to requirements", signer_id)
            }
            InvalidTxError::SignerDoesNotExist{signer_id} => {
                write!(f, "Signer {:?} does not exist", signer_id)
            }
            InvalidTxError::InvalidAccessKey(access_key_error) => access_key_error.fmt(f),
            InvalidTxError::InvalidNonce{tx_nonce, ak_nonce} => write!(
                f,
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                tx_nonce, ak_nonce
            ),
            InvalidTxError::InvalidReceiver{receiver_id} => {
                write!(f, "Invalid receiver account ID {:?} according to requirements", receiver_id)
            }
            InvalidTxError::InvalidSignature => {
                write!(f, "Transaction is not signed with the given public key")
            }
            InvalidTxError::NotEnoughBalance{signer_id, balance, cost} => write!(
                f,
                "Sender {:?} does not have enough balance {} for operation costing {}",
                signer_id, balance, cost
            ),
            InvalidTxError::RentUnpaid{ signer_id, amount} => {
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
            InvalidAccessKeyError::ActionError => {
                write!(f, "The used access key requires exactly one FunctionCall action")
            }
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
        }
    }
}

/// Happens when the input balance doesn't match the output balance in Runtime apply.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
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

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
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
            ActionError::AccountAlreadyExists { account_id } => {
                write!(f, "Can't create a new account {:?}, because it already exists", account_id)
            }
            ActionError::AccountDoesNotExist { action, account_id } => write!(
                f,
                "Can't complete the action {:?}, because account {:?} doesn't exist",
                action, account_id
            ),
            ActionError::ActorNoPermission { actor_id, account_id, action } => write!(
                f,
                "Actor {:?} doesn't have permission to account {:?} to complete the action {:?}",
                actor_id, account_id, action
            ),
            ActionError::RentUnpaid { account_id, amount } => write!(
                f,
                "The account {} wouldn't have enough balance to pay required rent {}",
                account_id, amount
            ),
            ActionError::TriesToUnstake { account_id } => {
                write!(f, "Account {:?} is not yet staked, but tries to unstake", account_id)
            }
            ActionError::TriesToStake { account_id, stake, locked, balance } => write!(
                f,
                "Account {:?} tries to stake {}, but has staked {} and only has {}",
                account_id, stake, locked, balance
            ),
            ActionError::CreateAccountNotAllowed { account_id, predecessor_id } => write!(
                f,
                "The new account_id {:?} can't be created by {:?}",
                account_id, predecessor_id
            ),
            ActionError::DeleteKeyDoesNotExist { account_id } => write!(
                f,
                "Account {:?} tries to remove an access key that doesn't exist",
                account_id
            ),
            ActionError::AddKeyAlreadyExists { public_key } => write!(
                f,
                "The public key {:?} is already used for an existing access key",
                public_key
            ),
            ActionError::DeleteAccountStaking { account_id } => {
                write!(f, "Account {:?} is staking and can not be deleted", account_id)
            }
            ActionError::DeleteAccountHasRent { account_id, balance } => write!(
                f,
                "Account {:?} can't be deleted. It has {}, which is enough to cover the rent",
                account_id, balance
            ),
            ActionError::FunctionCallError(s) => write!(f, "{}", s),
        }
    }
}

/// Error returned in the ExecutionOutcome in case of failure.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
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

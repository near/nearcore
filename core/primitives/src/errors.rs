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

/// Internal
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct GasOverflowError;
/// Internal
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct BalanceOverflowError;

/// External
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
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
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum InvalidAccessKeyError {
    AccessKeyNotFound(AccountId, PublicKey),
    ReceiverMismatch(AccountId, AccountId),
    MethodNameMismatch(String),
    ActionError,
    NotEnoughAllowance(AccountId, PublicKey, Balance, Balance),
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
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
        }
    }
}

impl From<InvalidAccessKeyError> for InvalidTxError {
    fn from(error: InvalidAccessKeyError) -> Self {
        InvalidTxError::InvalidAccessKey(error)
    }
}

impl From<GasOverflowError> for InvalidTxError {
    fn from(_: GasOverflowError) -> Self {
        InvalidTxError::CostOverflow
    }
}

impl From<BalanceOverflowError> for InvalidTxError {
    fn from(_: BalanceOverflowError) -> Self {
        InvalidTxError::CostOverflow
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidTxErrorOrStorageError {
    InvalidTxError(InvalidTxError),
    StorageError(StorageError),
}

impl From<StorageError> for InvalidTxErrorOrStorageError {
    fn from(e: StorageError) -> Self {
        InvalidTxErrorOrStorageError::StorageError(e)
    }
}

impl<T> From<T> for InvalidTxErrorOrStorageError
where
    T: Into<InvalidTxError>,
{
    fn from(e: T) -> Self {
        InvalidTxErrorOrStorageError::InvalidTxError(e.into())
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
        }
    }
}

/// Error returned in the ExecutionOutcome in case of failure.
/// Currently it's can only be an ActionError, but potentially there can be more errors from
/// Runtime. That's why we use ExecutionError instead of ActionError directly.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    Action(ActionError),
}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            ExecutionError::Action(e) => write!(f, "{}", e),
        }
    }
}

impl From<ActionError> for ExecutionError {
    fn from(error: ActionError) -> Self {
        ExecutionError::Action(error)
    }
}

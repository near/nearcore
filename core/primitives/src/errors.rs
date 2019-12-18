use crate::hash::CryptoHash;
use crate::types::{AccountId, Balance, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
// TODO: looks like in this crate we're trying to avoid inner deps, but I can't find a better solution so far
pub use near_vm_errors::VMError;

use std::fmt::Display;

/// Internal
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Deserialize, Serialize)]
#[serde(tag = "type")]
pub struct InvalidTxError {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: Nonce,
    pub receiver_id: AccountId,
    pub block_hash: CryptoHash,
    pub kind: InvalidTxErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Deserialize, Serialize)]
pub enum InvalidTxErrorKind {
    InvalidAccessKey(InvalidAccessKeyErrorKind),
    Action(ActionError),
    InvalidSigner,
    SignerDoesNotExist,
    InvalidNonce {
        access_key_nonce: Nonce,
    },
    InvalidReceiver,
    NotEnoughBalance {
        balance: Balance,
        cost: Balance,
    },
    RentUnpaid {
        amount: Balance,
    },
    CostOverflow,
    InvalidChain,
    InvalidSignature,
    Expired,
    AccessKeyNotFound,
    /// For this action the sender is required to be equal to the receiver
    OwnershipError,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum InvalidAccessKeyErrorKind {
    ReceiverMismatch {
        tx_receiver_id: AccountId,
        ak_receiver_id: AccountId,
    },
    MethodNameMismatch {
        method_name: String,
    },
    ActionError,
    NotEnoughAllowance {
        signer_id: AccountId,
        public_key: PublicKey,
        allowed: Balance,
        cost: Balance,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Deserialize, Serialize)]
pub struct ActionError {
    // position of a failed action in the transaction/receipt
    index: u64,
    kind: ActionErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Deserialize, Serialize)]
pub enum ActionErrorKind {
    AccountAlreadyExists,
    AccountDoesNotExist { action: String },
    CreateAccountNotAllowed,
    ActorNoPermission { actor_id, action: String },
    DeleteKeyDoesNotExist { public_key: PublicKey },
    AddKeyAlreadyExists { public_key: PublicKey },
    DeleteAccountStaking,
    DeleteAccountHasRent { amount: Balance },
    TriesToUnstake,
    TriesToStake { stake: Balance, staked: Balance, amount: Balance },
    FunctionCall(VMError),
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self.kind {
            InvalidTxErrorKind::InvalidAccessKey(access_key_error) =>
            match access_key_error {
                InvalidAccessKeyErrorKind::ReceiverMismatch { tx_receiver_id, ak_receiver_id } => write!(
                    f,
                    "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                    tx_receiver_id, ak_receiver_id
                ),
                InvalidAccessKeyErrorKind::MethodNameMismatch { method_name } => write!(
                    f,
                    "Transaction method name {:?} isn't allowed by the access key",
                    method_name
                ),
                InvalidAccessKeyErrorKind::ActionError => {
                    write!(f, "The used access key requires exactly one FunctionCall action")
                }
                InvalidAccessKeyErrorKind::NotEnoughAllowance { signer_id, public_key, allowed, cost } => {
                    write!(
                        f,
                        "Access Key {:?}:{} does not have enough balance {} for transaction costing {}",
                        signer_id, public_key, allowed, cost
                    )
                }
            },
            InvalidTxErrorKind::Action(action) => {
                match action.kind {
                    ActionErrorKind::AccountAlreadyExists => {
                        write!(f, "Can't create a new account {:?}, because it already exists", self.receiver_id)
                    }
                    ActionErrorKind::AccountDoesNotExist { action } => write!(
                        f,
                        "Can't complete the action {:?}, because account {:?} doesn't exist",
                        action, self.receiver_id
                    ),
                    ActionErrorKind::ActorNoPermission { actor_id, action } => write!(
                        f,
                        "Actor {:?} doesn't have permission to account {:?} to complete the action {:?}",
                        actor_id, self.receiver_id, action
                    ),
                    ActionErrorKind::TriesToUnstake => {
                        write!(f, "Account {:?} is not yet staked, but tries to unstake", self.receiver_id)
                    }
                    ActionErrorKind::TriesToStake {stake, staked, amount } => write!(
                        f,
                        "Account {:?} tries to stake {}, but has staked {} and only has {}",
                        self.receiver_id, stake, staked, amount
                    ),
                    ActionErrorKind::CreateAccountNotAllowed => write!(
                        f,
                        "The new account_id {:?} can't be created by {:?}",
                        self.receiver_id, self.signer_id
                    ),
                    ActionErrorKind::DeleteKeyDoesNotExist {public_key} => write!(
                        f,
                        "Account {:?} tries to remove an access key {:?} that doesn't exist",
                        self.receiver_id, public_key
                    ),
                    ActionErrorKind::AddKeyAlreadyExists => write!(
                        f,
                        "The public key {:?} for account {:?} is already used for an existing access key",
                        self.public_key, self.receiver_id
                    ),
                    ActionErrorKind::DeleteAccountStaking => {
                        write!(f, "Account {:?} is staking and can not be deleted", self.receiver_id)
                    }
                    ActionErrorKind::DeleteAccountHasRent { receiver_id, amount } => write!(
                        f,
                        "Account {:?} can't be deleted. It has {}, which is enough to cover the rent",
                        receiver_id, amount
                    ),
                    ActionErrorKind::FunctionCall(s) => write!(f, "FunctionCall action error: {}", s),
                }
            }
            InvalidTxErrorKind::InvalidSigner => {
                write!(f, "Invalid signer account ID {:?} according to requirements", self.signer_id)
            }
            InvalidTxErrorKind::SignerDoesNotExist => {
                write!(f, "Signer {:?} does not exist", self.signer_id)
            }
            InvalidTxErrorKind::InvalidNonce{access_key_nonce} => write!(
                f,
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                self.nonce, access_key_nonce
            ),
            InvalidTxErrorKind::InvalidReceiver => {
                write!(f, "Invalid receiver account ID {:?} according to requirements", self.receiver_id)
            }

            InvalidTxErrorKind::NotEnoughBalance{balance, cost} => write!(
                f,
                "Sender {:?} does not have enough balance {} for operation costing {}",
                self.signer_id, balance, cost
            ),
            InvalidTxErrorKind::RentUnpaid{amount} => {
                write!(f, "Failed to execute, because the account {:?} wouldn't have enough to pay required rent {}", self.signer_id, amount)
            }
            InvalidTxErrorKind::CostOverflow => {
                write!(f, "Transaction gas or balance cost is too high")
            }
            InvalidTxErrorKind::InvalidChain => {
                write!(f, "Transaction parent block hash doesn't belong to the current chain")
            }
            InvalidTxErrorKind::InvalidSignature => {
                write!(f, "Transaction is not signed with the given public key")
            }
            InvalidTxErrorKind::Expired => {
                write!(f, "Transaction has expired")
            }
            InvalidTxErrorKind::AccessKeyNotFound => write!(
                f,
                "Signer {:?} doesn't have access key with the given public_key {}",
                self.signer_id, self.public_key
            )
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
    InvalidTxErrorKind(InvalidTxErrorKind),
    StorageError(StorageError),
    BalanceMismatch(BalanceMismatchError),
}

impl From<IntegerOverflowError> for InvalidTxErrorKind {
    fn from(_: IntegerOverflowError) -> Self {
        InvalidTxErrorKind::CostOverflow
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

impl From<InvalidTxErrorKind> for RuntimeError {
    fn from(e: InvalidTxErrorKind) -> Self {
        RuntimeError::InvalidTxErrorKind(e)
    }
}

/// Error returned in the ExecutionOutcome in case of failure.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub enum ExecutionError {
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

impl From<InvalidTxErrorKind> for ExecutionError {
    fn from(error: InvalidTxErrorKind) -> Self {
        ExecutionError::InvalidTx(error)
    }
}

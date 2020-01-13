use crate::serialize::u128_dec_format;
use crate::types::{AccountId, Balance, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use near_rpc_error_macro::RpcError;
use near_vm_errors::VMError;

/// Error returned in the ExecutionOutcome in case of failure
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
#[rpc_error_variant = "TxExecutionError"]
pub enum TxExecutionError {
    /// An error happened during Acton execution
    ActionError(ActionError),
    /// An error happened during Transaction execution
    InvalidTxError(InvalidTxError),
}

impl Display for TxExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
    /// Unexpected error which is typically related to the node storage corruption.account
    /// That it's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// An error happens if `check_balance` fails, which is likely an indication of an invalid state.
    BalanceMismatch(BalanceMismatchError),
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// An error happened during TX execution
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
#[rpc_error_variant = "InvalidTxError"]
pub enum InvalidTxError {
    /// Happens if a wrong AccessKey used or AccessKey has not enough permissions
    InvalidAccessKey(InvalidAccessKeyError),
    /// TX signer_id is not in a valid format or not satisfy requirements see `near_core::primitives::utils::is_valid_account_id`
    InvalidSignerId { signer_id: AccountId },
    /// TX signer_id is not found in a storage
    SignerDoesNotExist { signer_id: AccountId },
    /// Transaction nonce must be account[access_key].nonce + 1
    InvalidNonce { tx_nonce: Nonce, ak_nonce: Nonce },
    /// TX receiver_id is not in a valid format or not satisfy requirements see `near_core::primitives::utils::is_valid_account_id`
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
    /// Signer account rent is unpaid
    RentUnpaid {
        /// An account which is required to pay the rent
        signer_id: AccountId,
        /// Required balance to cover the state rent
        #[serde(with = "u128_dec_format")]
        amount: Balance,
    },
    /// An integer overflow occurred during transaction cost estimation.
    CostOverflow,
    /// Transaction parent block hash doesn't belong to the current chain
    InvalidChain,
    /// Transaction has expired
    Expired,
}

#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
#[rpc_error_variant = "InvalidAccessKey"]
pub enum InvalidAccessKeyError {
    /// The access key identified by the public_key doesn't exist for the account
    AccessKeyNotFound { account_id: AccountId, public_key: PublicKey },
    /// Transaction receiver_id doesn't match the access key receiver_id
    ReceiverMismatch { tx_receiver: AccountId, ak_receiver: AccountId },
    /// Transaction method name isn't allowed by the access key
    MethodNameMismatch { method_name: String },
    /// The used access key requires exactly one FunctionCall action
    ActionError,
    /// Access Key does not have enough allowance to cover transaction cost
    NotEnoughAllowance {
        account_id: AccountId,
        public_key: PublicKey,
        #[serde(with = "u128_dec_format")]
        allowance: Balance,
        #[serde(with = "u128_dec_format")]
        cost: Balance,
    },
}

/// An error happened during Acton execution
#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
#[rpc_error_variant = "ActionError"]
pub struct ActionError {
    pub index: Option<u64>,
    pub kind: ActionErrorKind,
}

#[derive(
    BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize, RpcError,
)]
#[rpc_error_variant = "ActionError"]
pub enum ActionErrorKind {
    /// Happens when CreateAccount action tries to create an account with account_id which is already exists in the storage
    AccountAlreadyExists { account_id: AccountId },
    /// Happens when TX receiver_id doesn't exist (but action is not Action::CreateAccount)
    AccountDoesNotExist { account_id: AccountId },
    /// A newly created account must be under a namespace of the creator account
    CreateAccountNotAllowed { account_id: AccountId, predecessor_id: AccountId },
    /// Administrative actions like DeployContract, Stake, AddKey, DeleteKey. can be proceed only if sender=receiver
    /// or the first TX action is a CreateAccount action
    ActorNoPermission { account_id: AccountId, actor_id: AccountId },
    /// Account tries to remove an access key that doesn't exist
    DeleteKeyDoesNotExist { account_id: AccountId, public_key: PublicKey },
    /// The public key is already used for an existing access key
    AddKeyAlreadyExists { account_id: AccountId, public_key: PublicKey },
    /// Account is staking and can not be deleted
    DeleteAccountStaking { account_id: AccountId },
    /// Have to cover rent before delete an account
    DeleteAccountHasRent {
        account_id: AccountId,
        #[serde(with = "u128_dec_format")]
        balance: Balance,
    },
    /// ActionReceipt can't be completed, because the remaining balance will not be enough to pay rent.
    RentUnpaid {
        /// An account which is required to pay the rent
        account_id: AccountId,
        /// Rent due to pay.
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
    /// An error occurred during a FunctionCall Action.
    FunctionCall(VMError),
}

impl From<ActionErrorKind> for ActionError {
    fn from(e: ActionErrorKind) -> ActionError {
        ActionError { index: None, kind: e }
    }
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            InvalidTxError::InvalidSignerId{signer_id} => {
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
            InvalidTxError::InvalidReceiverId{receiver_id} => {
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
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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
    pub total_rent_paid: Balance,
    #[serde(with = "u128_dec_format")]
    pub total_validator_reward: Balance,
    #[serde(with = "u128_dec_format")]
    pub total_balance_burnt: Balance,
    #[serde(with = "u128_dec_format")]
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

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
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
        write!(f, "Action #{}: {}", self.index.unwrap_or_default(), self.kind)
    }
}

impl Display for ActionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
            ActionErrorKind::RentUnpaid { account_id, amount } => write!(
                f,
                "The account {} wouldn't have enough balance to pay required rent {}",
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
            ActionErrorKind::CreateAccountNotAllowed { account_id, predecessor_id } => write!(
                f,
                "The new account_id {:?} can't be created by {:?}",
                account_id, predecessor_id
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
            ActionErrorKind::DeleteAccountHasRent { account_id, balance } => write!(
                f,
                "Account {:?} can't be deleted. It has {}, which is enough to cover the rent",
                account_id, balance
            ),
            ActionErrorKind::FunctionCall(s) => write!(f, "{}", s),
        }
    }
}

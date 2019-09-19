use crate::types::{AccountId, Balance, BlockIndex, Gas, PublicKey, StorageUsage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
/// Context for the contract execution.
pub struct VMContext {
    /// The account id of the current contract that we are executing.
    pub current_account_id: AccountId,
    /// The account id of that signed the original transaction that led to this
    /// execution.
    pub signer_account_id: AccountId,
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    /// The public key that was used to sign the original transaction that led to
    /// this execution.
    pub signer_account_pk: PublicKey,
    /// If this execution is the result of cross-contract call or a callback then
    /// predecessor is the account that called it.
    /// If this execution is the result of direct execution of transaction then it
    /// is equal to `signer_account_id`.
    pub predecessor_account_id: AccountId,
    #[serde(with = "crate::serde_with::bytes_as_str")]
    /// The input to the contract call.
    pub input: Vec<u8>,
    /// The current block index.
    pub block_index: BlockIndex,
    /// The current block timestamp.
    pub block_timestamp: u64,

    /// The balance attached to the given account. Excludes the `attached_deposit` that was
    /// attached to the transaction.
    pub account_balance: Balance,
    /// The account's storage usage before the contract execution
    pub storage_usage: StorageUsage,
    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    pub attached_deposit: Balance,
    /// The gas attached to the call that can be used to pay for the gas fees.
    pub prepaid_gas: Gas,
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
    /// Whether the execution should not charge any costs.
    pub free_of_charge: bool,
    /// How many `DataReceipt`'s should receive this execution result. This should be empty if
    /// this function call is a part of a batch and it is not the last action.
    pub output_data_receivers: Vec<AccountId>,
}

use crate::types::PublicKey;
use near_primitives_core::serialize::u64_dec_format;
use near_primitives_core::types::{
    AccountId, Balance, BlockHeight, EpochHeight, Gas, StorageUsage,
};
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
    /// The input to the contract call.
    /// Encoded as base64 string to be able to pass input in borsh binary format.
    #[serde(with = "crate::serde_with::bytes_as_base64")]
    pub input: Vec<u8>,
    /// The current block height.
    // TODO #1903 rename to `block_height`
    pub block_index: BlockHeight,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    #[serde(with = "u64_dec_format")]
    pub block_timestamp: u64,
    /// The current epoch height.
    pub epoch_height: EpochHeight,

    /// The balance attached to the given account. Excludes the `attached_deposit` that was
    /// attached to the transaction.
    #[serde(with = "crate::serde_with::u128_dec_format_compatible")]
    pub account_balance: Balance,
    /// The balance of locked tokens on the given account.
    #[serde(with = "crate::serde_with::u128_dec_format_compatible")]
    pub account_locked_balance: Balance,
    /// The account's storage usage before the contract execution
    pub storage_usage: StorageUsage,
    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    #[serde(with = "crate::serde_with::u128_dec_format_compatible")]
    pub attached_deposit: Balance,
    /// The gas attached to the call that can be used to pay for the gas fees.
    pub prepaid_gas: Gas,
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
    /// Whether the execution should not charge any costs.
    pub is_view: bool,
    /// How many `DataReceipt`'s should receive this execution result. This should be empty if
    /// this function call is a part of a batch and it is not the last action.
    pub output_data_receivers: Vec<AccountId>,
}

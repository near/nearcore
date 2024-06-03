use super::types::PublicKey;
use near_primitives_core::config::ViewConfig;
use near_primitives_core::types::{
    AccountId, Balance, BlockHeight, EpochHeight, Gas, StorageUsage,
};

#[derive(Clone)]
/// Context for the contract execution.
pub struct VMContext {
    /// The account id of the current contract that we are executing.
    pub current_account_id: AccountId,
    /// The account id of that signed the original transaction that led to this
    /// execution.
    pub signer_account_id: AccountId,
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
    pub input: Vec<u8>,
    /// The current block height.
    pub block_height: BlockHeight,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// The current epoch height.
    pub epoch_height: EpochHeight,

    /// The balance attached to the given account. Excludes the `attached_deposit` that was
    /// attached to the transaction.
    pub account_balance: Balance,
    /// The balance of locked tokens on the given account.
    pub account_locked_balance: Balance,
    /// The account's storage usage before the contract execution
    pub storage_usage: StorageUsage,
    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    pub attached_deposit: Balance,
    /// The gas attached to the call that can be used to pay for the gas fees.
    pub prepaid_gas: Gas,
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
    /// If Some, it means that execution is made in a view mode and defines its configuration.
    /// View mode means that only read-only operations are allowed.
    /// See <https://nomicon.io/Proposals/0018-view-change-method.html> for more details.
    pub view_config: Option<ViewConfig>,
    /// How many `DataReceipt`'s should receive this execution result. This should be empty if
    /// this function call is a part of a batch and it is not the last action.
    pub output_data_receivers: Vec<AccountId>,
}

impl VMContext {
    pub fn is_view(&self) -> bool {
        self.view_config.is_some()
    }
}

use super::GasCounter;
use super::types::{PromiseResult, PublicKey};
use near_parameters::vm::{Config, LimitConfig};
use near_primitives_core::account::AccountContract;
use near_primitives_core::config::ViewConfig;
use near_primitives_core::types::{
    AccountId, Balance, BlockHeight, EpochHeight, Gas, StorageUsage,
};
use std::cmp::min;
use std::rc::Rc;

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
    /// Where balance refunds after failure should go. Usually the same as
    /// `predecessor_account_id` but may have been changed by the predecessor
    /// via host function `promise_set_refund_to`.
    pub refund_to_account_id: AccountId,
    /// The input to the contract call.
    /// Encoded as base64 string to be able to pass input in borsh binary format.
    pub input: Rc<[u8]>,
    /// If this method execution is invoked directly as a callback by one or more contract calls
    /// the results of the methods that made the callback are stored in this collection.
    pub promise_results: std::sync::Arc<[PromiseResult]>,
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
    /// The account's current contract code
    pub account_contract: AccountContract,
    /// The balance that was attached to the call that will be immediately deposited before the
    /// contract execution starts.
    pub attached_deposit: Balance,
    /// The gas attached to the call that can be used to pay for the gas fees.
    pub prepaid_gas: Gas,
    /// Initial seed for randomness
    pub random_seed: Vec<u8>,
    /// How this execution was initiated, which determines its gas limits and
    /// which host functions are available.
    pub execution_mode: ExecutionMode,
    /// How many `DataReceipt`'s should receive this execution result. This should be empty if
    /// this function call is a part of a batch and it is not the last action.
    pub output_data_receivers: Vec<AccountId>,
}

impl VMContext {
    pub fn is_view(&self) -> bool {
        self.execution_mode.is_view()
    }

    pub fn is_external(&self) -> bool {
        self.execution_mode.is_external()
    }

    /// Make a gas counter based on the configuration in this VMContext.
    ///
    /// Meant for use in tests only.
    pub fn make_gas_counter(&self, config: &Config) -> GasCounter {
        let balance = self.account_balance.saturating_add(self.attached_deposit);
        let GasLimits { max_gas_burnt, prepaid_gas } =
            self.execution_mode.gas_limits(&config.limit_config, self.prepaid_gas, balance);
        GasCounter::new(
            config.ext_costs.clone(),
            max_gas_burnt,
            config.regular_op_cost,
            prepaid_gas,
            self.is_view(),
        )
    }
}

/// How the current contract execution was initiated.
#[derive(Clone, Debug)]
pub enum ExecutionMode {
    /// Execution of a receipt: a transaction or a cross-contract call.
    Internal,
    /// Execution of an external contract call, authorized and paid for by the
    /// contract itself.
    ///
    /// There is no prepaid gas: the contract pays for all the gas the call
    /// uses from its balance, both the gas it burns and the gas of the
    /// promises it creates. The balance takes the place of prepaid gas, so the
    /// call fails with `GasExceeded` if it uses more gas than the contract can
    /// pay for.
    External {
        /// Gas price at which the contract pays for gas.
        gas_price: Balance,
    },
    /// Read-only execution of a view call. Defines the view configuration.
    /// See <https://github.com/near/NEPs/pull/18> for more details.
    View(ViewConfig),
}

impl ExecutionMode {
    pub fn is_view(&self) -> bool {
        matches!(self, Self::View(_))
    }

    pub fn is_external(&self) -> bool {
        matches!(self, Self::External { .. })
    }

    /// The gas limits of an execution in this mode, where `prepaid_gas` is
    /// the gas attached to the function call and `balance` is the balance of
    /// the contract, including the attached deposit.
    pub fn gas_limits(
        &self,
        limit_config: &LimitConfig,
        prepaid_gas: Gas,
        balance: Balance,
    ) -> GasLimits {
        match self {
            Self::Internal => GasLimits { max_gas_burnt: limit_config.max_gas_burnt, prepaid_gas },
            // There is no prepaid gas in an external call; the contract pays for
            // the gas with its balance, up to the limits set by the protocol.
            Self::External { gas_price } => GasLimits {
                max_gas_burnt: limit_config.max_gas_burnt_external,
                prepaid_gas: min(
                    limit_config.max_total_prepaid_gas,
                    affordable_gas(balance, *gas_price),
                ),
            },
            // There is no real prepaid gas in view mode; the per-call budget is
            // `max_gas_burnt`. See `GasCounter::new` for why it is bounded.
            Self::View(ViewConfig { max_gas_burnt }) => {
                GasLimits { max_gas_burnt: *max_gas_burnt, prepaid_gas: *max_gas_burnt }
            }
        }
    }
}

/// The amount of gas that `balance` pays for at `gas_price`.
pub(crate) fn affordable_gas(balance: Balance, gas_price: Balance) -> Gas {
    let gas = balance
        .as_yoctonear()
        .checked_div(gas_price.as_yoctonear())
        .map_or(u64::MAX, |gas| u64::try_from(gas).unwrap_or(u64::MAX));
    Gas::from_gas(gas)
}

/// Gas limits of a single contract execution, used to build a `GasCounter`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GasLimits {
    /// Max gas that can be burnt, excluding gas attached to promises.
    pub max_gas_burnt: Gas,
    /// Max gas that can be used: burnt gas plus gas attached to promises.
    pub prepaid_gas: Gas,
}

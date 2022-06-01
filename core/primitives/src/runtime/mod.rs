pub use near_primitives_core::runtime::fees;
pub use near_primitives_core::runtime::*;

use crate::account::Account;
use crate::runtime::config::RuntimeConfig;
use crate::types::Balance;

pub mod apply_state;
pub mod config;
pub mod config_store;
pub mod migration_data;
pub mod parameter_table;

/// Checks if given account has enough balance for storage stake, and returns:
///  - None if account has enough balance,
///  - Some(insufficient_balance) if account doesn't have enough and how much need to be added,
///  - Err(message) if account has invalid storage usage or amount/locked.
///
/// Read details of state staking
/// <https://nomicon.io/Economics/README.html#state-stake>.
pub fn get_insufficient_storage_stake(
    account: &Account,
    runtime_config: &RuntimeConfig,
) -> Result<Option<Balance>, String> {
    let required_amount = Balance::from(account.storage_usage())
        .checked_mul(runtime_config.storage_amount_per_byte)
        .ok_or_else(|| {
            format!("Account's storage_usage {} overflows multiplication", account.storage_usage())
        })?;
    let available_amount = account.amount().checked_add(account.locked()).ok_or_else(|| {
        format!(
            "Account's amount {} and locked {} overflow addition",
            account.amount(),
            account.locked()
        )
    })?;
    if available_amount >= required_amount {
        Ok(None)
    } else {
        Ok(Some(required_amount - available_amount))
    }
}

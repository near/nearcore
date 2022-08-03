//! Collection of feature specific tests

#[cfg(feature = "protocol_feature_account_id_in_function_call_permission")]
mod account_id_in_function_call_permission;
#[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
mod fix_contract_loading_cost;
mod restore_receipts_after_fix;
mod storage_usage_fix;

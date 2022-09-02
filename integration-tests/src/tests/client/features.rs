//! Collection of feature specific tests

#[cfg(feature = "protocol_feature_account_id_in_function_call_permission")]
mod account_id_in_function_call_permission;
mod cap_max_gas_price;
#[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
mod fix_contract_loading_cost;
mod fix_storage_usage;
mod lower_storage_key_limit;
mod restore_receipts_after_fix_apply_chunks;
mod wasmer2;

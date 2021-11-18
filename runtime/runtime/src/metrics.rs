use near_metrics::{try_create_int_counter, IntCounter};
use once_cell::sync::Lazy;

pub static ACTION_CREATE_ACCOUNT_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_create_account_total",
        "The number of CreateAccount actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_DEPLOY_CONTRACT_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_deploy_contract_total",
        "The number of DeployContract actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_FUNCTION_CALL_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_function_call_total",
        "The number of FunctionCall actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_TRANSFER_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_transfer_total",
        "The number of Transfer actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_STAKE_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_stake_total",
        "The number of stake actions called since starting this node",
    )
    .unwrap()
});
#[cfg(feature = "protocol_feature_chunk_only_producers")]
pub static ACTION_STAKE_CHUNK_ONLY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_stake_chunk_only_total",
        "The number of chunk-only stake actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_ADD_KEY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_add_key_total",
        "The number of AddKey actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_DELETE_KEY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_delete_key_total",
        "The number of DeleteKey actions called since starting this node",
    )
    .unwrap()
});
pub static ACTION_DELETE_ACCOUNT_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_action_delete_account_total",
        "The number of DeleteAccount actions called since starting this node",
    )
    .unwrap()
});
pub static TRANSACTION_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_total",
        "The number of transactions processed since starting this node",
    )
    .unwrap()
});
pub static TRANSACTION_PROCESSED_SUCCESSFULLY_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_successfully_total",
        "The number of transactions processed successfully since starting this node",
    )
    .unwrap()
});
pub static TRANSACTION_PROCESSED_FAILED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_transaction_processed_failed_total",
        "The number of transactions processed and failed since starting this node",
    )
    .unwrap()
});

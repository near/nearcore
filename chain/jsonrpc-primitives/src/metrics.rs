use near_metrics::IntCounterVec;
use once_cell::sync::Lazy;

pub static RPC_UNREACHABLE_ERROR_COUNT: Lazy<IntCounterVec> = Lazy::new(|| {
    near_metrics::try_create_int_counter_vec(
        "near_rpc_unreachable_errors_total",
        "Total count of Unreachable RPC errors returned, by target error enum",
        &["target_error_enum"],
    )
    .unwrap()
});

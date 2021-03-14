use lazy_static::lazy_static;
use near_metrics::IntCounterVec;

lazy_static! {
    pub static ref RPC_UNREACHABLE_ERROR_COUNT: near_metrics::Result<IntCounterVec> =
        near_metrics::try_create_int_counter_vec(
            "near_rpc_unreachable_errors_total",
            "Total count of Unreachable RPC errors returned, by target error enum",
            &["target_error_enum"]
        );
}

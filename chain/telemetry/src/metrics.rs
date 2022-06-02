use once_cell::sync::Lazy;

pub(crate) static TELEMETRY_RESULT: Lazy<near_metrics::IntCounterVec> = Lazy::new(|| {
    near_metrics::try_create_int_counter_vec(
        "near_telemetry_result",
        "Count of 'ok' or 'failed' results of uploading telemetry data",
        &["success"],
    )
    .unwrap()
});

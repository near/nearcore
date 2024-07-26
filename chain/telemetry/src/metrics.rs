use std::sync::LazyLock;

pub(crate) static TELEMETRY_RESULT: LazyLock<near_o11y::metrics::IntCounterVec> =
    LazyLock::new(|| {
        near_o11y::metrics::try_create_int_counter_vec(
            "near_telemetry_result",
            "Count of 'ok' or 'failed' results of uploading telemetry data",
            &["success"],
        )
        .unwrap()
    });

use near_metrics::exponential_buckets;
use once_cell::sync::Lazy;

pub static PARTIAL_ENCODED_CHUNK_REQUEST_PROCESSING_TIME: Lazy<near_metrics::HistogramVec> =
    Lazy::new(|| {
        near_metrics::try_create_histogram_vec(
            "near_partial_encoded_chunk_request_processing_time",
            concat!(
                "Time taken to prepare responses to partial encoded chuck ",
                "requests.  The ‘method’ key describes how we tried to fulfil ",
                "the request and ‘success’ describes whether we managed to ",
                "create a response (‘ok’) or not (‘failed’).  Note that ",
                "success does not mean that we managed to send the response ",
                "over network; the count is taken before we attempt to send ",
                "the data out."
            ),
            &["method", "success"],
            Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
        )
        .unwrap()
    });

pub static DISTRIBUTE_ENCODED_CHUNK_TIME: Lazy<near_metrics::Histogram> = Lazy::new(|| {
    near_metrics::try_create_histogram_with_buckets(
        "near_distribute_encoded_chunk_time",
        "Time to distribute data about a produced chunk",
        exponential_buckets(0.001, 2.0, 16).unwrap(),
    )
    .unwrap()
});

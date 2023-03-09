use near_o11y::metrics::{exponential_buckets, try_create_histogram, Counter, Histogram};
use once_cell::sync::Lazy;

pub static PARTIAL_ENCODED_CHUNK_REQUEST_PROCESSING_TIME: Lazy<near_o11y::metrics::HistogramVec> =
    Lazy::new(|| {
        near_o11y::metrics::try_create_histogram_vec(
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

pub static DISTRIBUTE_ENCODED_CHUNK_TIME: Lazy<near_o11y::metrics::HistogramVec> =
    Lazy::new(|| {
        near_o11y::metrics::try_create_histogram_vec(
            "near_distribute_encoded_chunk_time",
            concat!(
                "Time to distribute data about a produced chunk: Preparation of network messages ",
                "and passing it to peer manager",
            ),
            &["shard_id"],
            Some(exponential_buckets(0.001, 2.0, 16).unwrap()),
        )
        .unwrap()
    });

pub(crate) static PARTIAL_ENCODED_CHUNK_RESPONSE_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
            "near_partial_encoded_chunk_response_delay",
            "Delay between when a partial encoded chunk response is sent from PeerActor and when it is received by ShardsManagerActor",
        )
        .unwrap()
});

pub static PARTIAL_ENCODED_CHUNK_FORWARD_CACHED_WITHOUT_HEADER: Lazy<Counter> = Lazy::new(|| {
    near_o11y::metrics::try_create_counter(
        "near_partial_encoded_chunk_forward_cached_without_header",
        concat!(
            "Number of times we received a valid partial encoded chunk forward without having the corresponding chunk header so we cached it"
        ),
    )
    .unwrap()
});

pub static PARTIAL_ENCODED_CHUNK_FORWARD_CACHED_WITHOUT_PREV_BLOCK: Lazy<Counter> = Lazy::new(
    || {
        near_o11y::metrics::try_create_counter(
        "near_partial_encoded_chunk_forward_cached_without_prev_block",
        concat!(
            "Number of times we received a partial encoded chunk forward without having the previous block to fully validate it, so we cached it"
        ),
    )
    .unwrap()
    },
);

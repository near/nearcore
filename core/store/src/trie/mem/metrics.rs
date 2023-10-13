use near_o11y::metrics::{try_create_int_gauge_vec, IntGaugeVec};
use once_cell::sync::Lazy;

pub static MEM_TRIE_NUM_ROOTS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_mem_trie_num_roots",
        "Number of trie roots currently active in the in-memory trie",
        &["shard_uid"],
    )
    .unwrap()
});

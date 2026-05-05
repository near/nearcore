use near_primitives::types::BlockHeightDelta;

/// Maximum number of receipts admitted to the pending-compile queue in a
/// single chunk. Receipts beyond this cap spill to the delayed-receipt
/// queue and re-attempt admission in subsequent chunks.
pub const COMPILE_QUEUE_ADMISSION_CAP: usize = 20;

/// Maximum number of blocks an admitted receipt may reside in the queue
/// before it is evicted with a `CompileQueueExpired` failure outcome.
/// An entry pushed at height `H` may be advanced through height `H +
/// COMPILE_QUEUE_TTL_BLOCKS` and is evicted at the start of `H +
/// COMPILE_QUEUE_TTL_BLOCKS + 1`.
pub const COMPILE_QUEUE_TTL_BLOCKS: BlockHeightDelta = 5;

/// Maximum number of TTL evictions performed in a single chunk. Bounds
/// per-chunk apply work spent on eviction.
pub const COMPILE_QUEUE_EVICTION_CAP: usize = 20;

pub mod chunk_executor_actor;
pub mod chunk_executor_coordinator;
pub mod chunk_validator_actor;
pub mod data_distributor_actor;
pub mod per_shard_executor;
pub mod per_shard_spawner;
pub mod persist;
pub mod timer;

#[cfg(test)]
mod tests;

/// Prototype toggle: when `true`, run the experimental per-shard SPICE
/// chunk-execution subsystem (`ChunkExecutorCoordinator` + `PerShardExecutor`)
/// instead of the monolithic `ChunkExecutorActor`. A plain global const (not a
/// config field) keeps the prototype's diff small.
///
/// It is `false` unless the `protocol_feature_spice` build is active, so non-SPICE
/// builds and tests (Linux, Linux Nightly) are entirely unaffected — only the
/// SPICE build exercises the new path.
#[cfg(feature = "protocol_feature_spice")]
pub const SPICE_PER_SHARD_EXECUTOR: bool = true;
#[cfg(not(feature = "protocol_feature_spice"))]
pub const SPICE_PER_SHARD_EXECUTOR: bool = false;

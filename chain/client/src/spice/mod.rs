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
/// The per-shard subsystem is a SPICE-only concern: every wiring site is also
/// guarded by `cfg!(feature = "protocol_feature_spice")`, so non-SPICE builds and
/// tests (Linux, Linux Nightly) never activate it regardless of this const.
pub const SPICE_PER_SHARD_EXECUTOR: bool = true;

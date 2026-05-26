pub mod chunk_executor_actor;
pub mod chunk_executor_coordinator;
pub mod chunk_validator_actor;
pub mod data_distributor_actor;
pub mod timer;

#[cfg(test)]
mod tests;

/// Prototype toggle: when `true`, run the experimental per-shard SPICE
/// chunk-execution subsystem (`ChunkExecutorCoordinator` + `PerShardExecutor`)
/// instead of the monolithic `ChunkExecutorActor`. A plain global const (not a
/// config field) keeps the prototype's diff small; flip to `true` to exercise
/// the new path.
pub const SPICE_PER_SHARD_EXECUTOR: bool = false;

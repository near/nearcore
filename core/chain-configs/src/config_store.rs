use std::collections::BTreeMap;
use near_primitives::types::EpochHeight;
use std::sync::Arc;
use crate::ChainConfig;

/// Stores chain config for each epoch where it was updated.
#[derive(Debug)]
pub struct ChainConfigStore {
    /// Maps epoch to the config.
    store: BTreeMap<EpochHeight, Arc<ChainConfig>>,
}

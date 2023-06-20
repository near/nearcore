/// Stores chain config for each epoch where it was updated.
#[derive(Debug)]
pub struct ChainConfigStore {
    /// Maps epoch to the config.
    store: BTreeMap<EpochHeight, Arc<ChainConfig>>,
}

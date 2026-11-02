use crate::utils::ReaderHandles;
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_historical_reader::bootstrap_range;
use near_primitives::types::BlockHeight;
use near_store::genesis::initialize_genesis_state;
use std::path::Path;

#[derive(clap::Parser)]
pub(crate) struct BootstrapCmd {
    /// First block height to download (inclusive).
    #[clap(long)]
    start_height: BlockHeight,
    /// Last block height to download. The walk runs to the end of the batch this height
    /// falls in, so the store covers at least it.
    #[clap(long)]
    end_height: BlockHeight,
    /// Leave `DBCol::State` empty, so the store answers every query but a state one.
    #[clap(long)]
    skip_state: bool,
}

impl BootstrapCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let handles = ReaderHandles::open(home_dir, genesis_validation)?;

        // Nothing else writes the genesis state roots into a store the bootstrap builds, and
        // the view client that serves it reads them at startup. Idempotent, so a store a node
        // already initialized keeps what it has.
        initialize_genesis_state(
            handles.store.clone(),
            &handles.near_config.genesis,
            Some(home_dir),
        );

        // TODO(cloud_archival): consider uploading the genesis block to cloud
        // (or constructing it locally from genesis.json in the reader) so the
        // genesis epoch can be bootstrapped too.
        let genesis_height = handles.near_config.genesis.config.genesis_height;
        anyhow::ensure!(
            self.start_height > genesis_height,
            "start_height ({}) must be > genesis_height ({}); the genesis block is not in cloud storage",
            self.start_height,
            genesis_height,
        );

        handles.runtime.block_on(bootstrap_range(
            &handles.store,
            &handles.cloud_storage,
            handles.shard_tracker.epoch_manager().as_ref(),
            &handles.shard_tracker,
            self.start_height,
            self.end_height,
            self.skip_state,
        ))?;

        Ok(())
    }
}

use crate::utils::ReaderHandles;
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_historical_reader::bootstrap_range;
use near_primitives::types::BlockHeight;
use std::path::Path;

#[derive(clap::Parser)]
pub(crate) struct BootstrapCmd {
    /// First block height to download (inclusive).
    #[clap(long)]
    start_height: BlockHeight,
    /// Last block height to download (inclusive).
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

        tracing::info!(
            start_height = self.start_height,
            end_height = self.end_height,
            blocks = self.end_height - self.start_height + 1,
            "bootstrap complete"
        );

        Ok(())
    }
}

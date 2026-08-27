use anyhow::Context;
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_historical_reader::bootstrap_range;
use near_epoch_manager::EpochManager;
use near_primitives::types::BlockHeight;
use near_store::{Mode, NodeStorage};
use std::path::Path;
use tokio::runtime::Runtime;

#[derive(clap::Parser)]
pub(crate) struct BootstrapReaderCmd {
    /// First block height to download (inclusive).
    #[clap(long)]
    start_height: BlockHeight,
    /// Last block height to download (inclusive).
    #[clap(long)]
    end_height: BlockHeight,
}

impl BootstrapReaderCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.start_height <= self.end_height,
            "start_height ({}) must be <= end_height ({})",
            self.start_height,
            self.end_height,
        );

        let near_config = nearcore::config::load_config(home_dir, genesis_validation)
            .context("failed to load config")?;

        // TODO(cloud_archival): consider uploading the genesis block to cloud
        // (or constructing it locally from genesis.json in the reader) so the
        // genesis epoch can be bootstrapped too.
        let genesis_height = near_config.genesis.config.genesis_height;
        anyhow::ensure!(
            self.start_height > genesis_height,
            "start_height ({}) must be > genesis_height ({}); the genesis block is not in cloud storage",
            self.start_height,
            genesis_height,
        );

        let cloud_storage_context = near_config
            .cloud_storage_context()
            .context("cloud_archival not configured in config.json")?;

        // Opening cloud storage and every retrieval below run on tokio, and this command
        // is not async. Multi-threaded on purpose: the retrievals are polled by a foreign
        // executor, so tokio's driver needs worker threads of its own to make progress.
        let tokio_runtime = Runtime::new().expect("failed to create the tokio runtime");
        let _runtime_guard = tokio_runtime.enter();

        let storage = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            Some(cloud_storage_context),
        )
        .open_in_mode(Mode::ReadWrite)
        .context("failed to open storage")?;

        let store = storage.get_hot_store();
        let cloud_storage =
            storage.get_cloud_storage().context("cloud storage not available")?.clone();

        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );

        bootstrap_range(
            &store,
            &cloud_storage,
            epoch_manager.as_ref(),
            self.start_height,
            self.end_height,
        )?;

        tracing::info!(
            start_height = self.start_height,
            end_height = self.end_height,
            blocks = self.end_height - self.start_height + 1,
            "bootstrap complete"
        );

        Ok(())
    }
}

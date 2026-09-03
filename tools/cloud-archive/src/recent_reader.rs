use crate::utils::{ReaderHandles, serve_store_while};
use anyhow::Context;
use near_async::time::Clock;
use near_chain_configs::GenesisValidationMode;
use near_client::archive::cloud_recent_reader::CloudArchivalRecentReader;
use std::path::Path;
use tokio::signal::ctrl_c;

#[derive(clap::Parser)]
pub(crate) struct FollowCmd {
    /// Copy from the bucket without answering queries. Removing the `rpc` section does not
    /// do this: the config falls back to its default address.
    #[clap(long)]
    no_serve: bool,
}

impl FollowCmd {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let handles = ReaderHandles::open(home_dir, genesis_validation)?;
        let reader_config = handles
            .near_config
            .config
            .cloud_archival
            .as_ref()
            .and_then(|cloud_archival| cloud_archival.reader.as_ref())
            .context("cloud_archival.reader is not configured in config.json")?;
        let reader = CloudArchivalRecentReader::new(
            Clock::real(),
            handles.store.clone(),
            handles.cloud_storage.clone(),
            handles.shard_tracker.epoch_manager().clone(),
            handles.shard_tracker.clone(),
            reader_config.polling_interval,
        );

        // The loop checks between polls, so a retrieval in flight still finishes.
        let interrupt = reader.clone();
        handles.runtime.spawn(async move {
            if ctrl_c().await.is_ok() {
                tracing::info!(target: "cloud_archival", "stopping after the poll in flight");
                interrupt.stop();
            }
        });

        let runtime = &handles.runtime;
        let follow = || {
            runtime.block_on(reader.cloud_archival_loop())?;
            tracing::info!(target: "cloud_archival", "follow stopped");
            Ok(())
        };
        if self.no_serve {
            return follow();
        }
        // One process and one store handle. Two cannot do it: the reading side opens a
        // snapshot fixed at open time, so it never sees what the follow loop appends.
        serve_store_while(home_dir, &handles.near_config, handles.store.clone(), runtime, follow)
    }
}

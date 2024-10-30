use near_chain_configs::GenesisValidationMode;
use near_store::{DBCol, NodeStorage};
use std::path::Path;

#[derive(clap::Subcommand)]
enum BlockMiscKeySelector {
    StateSnapshot,
}

#[derive(clap::Subcommand)]
enum ColumnSelector {
    BlockMisc {
        #[clap(subcommand)]
        key: BlockMiscKeySelector,
    },
}

#[derive(clap::Args)]
pub(crate) struct WriteCryptoHashCommand {
    #[clap(long)]
    hash: near_primitives::hash::CryptoHash,
    #[clap(subcommand)]
    column: ColumnSelector,
}

impl WriteCryptoHashCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(&home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );

        let storage = opener.open()?;
        let store = storage.get_hot_store();
        let mut store_update = store.store_update();

        match &self.column {
            ColumnSelector::BlockMisc { key } => match key {
                BlockMiscKeySelector::StateSnapshot => {
                    store_update.set_ser(
                        DBCol::BlockMisc,
                        near_store::STATE_SNAPSHOT_KEY,
                        &self.hash,
                    )?;
                }
            },
        }

        Ok(store_update.commit()?)
    }
}

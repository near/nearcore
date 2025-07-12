use std::path::PathBuf;

use near_chain_configs::GenesisValidationMode;
use near_store::DBCol;
use near_store::migrations::ordinal_inconsistency::{
    find_ordinal_inconsistencies, repair_ordinal_inconsistencies,
};

use crate::utils::get_user_confirmation;

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(crate) enum OrdinalInconsistencyCommand {
    Find(FindCommand),
    FindAndRepair(FindAndRepairCommand),
}

#[derive(clap::Args)]
pub(crate) struct FindCommand {
    #[clap(long, default_value_t = 100)]
    pub print_max_inconsistencies: usize,
}

#[derive(clap::Args)]
pub(crate) struct FindAndRepairCommand {
    #[clap(long, default_value_t = 100)]
    pub print_max_inconsistencies: usize,

    #[clap(long)]
    pub noconfirm: bool,
}

impl OrdinalInconsistencyCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mut near_config = nearcore::config::load_config(home, genesis_validation)?;
        let node_storage = nearcore::open_storage(home, &mut near_config)?;

        // Hot store is enough, cold store doesn't contain the affected columns.
        assert_eq!(DBCol::BlockHeight.is_cold(), false);
        assert_eq!(DBCol::BlockOrdinal.is_cold(), false);
        assert_eq!(DBCol::BlockMerkleTree.is_cold(), false);
        let store = node_storage.get_hot_store();

        match self {
            OrdinalInconsistencyCommand::Find(find_cmd) => {
                find_ordinal_inconsistencies(&store, find_cmd.print_max_inconsistencies).unwrap();
            }
            OrdinalInconsistencyCommand::FindAndRepair(scan_and_fix_cmd) => {
                let inconsistencies = find_ordinal_inconsistencies(
                    &store,
                    scan_and_fix_cmd.print_max_inconsistencies,
                )
                .unwrap();
                if inconsistencies.is_empty() {
                    return Ok(());
                }

                if !scan_and_fix_cmd.noconfirm {
                    if !get_user_confirmation(&format!("Continue with repair?")) {
                        println!("Cancelling...");
                        return Ok(());
                    }
                }
                repair_ordinal_inconsistencies(&store, &inconsistencies).unwrap();
            }
        }
        Ok(())
    }
}

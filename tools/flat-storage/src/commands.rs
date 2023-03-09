/// Tools for modifying flat storage - should be used only for experimentation & debugging.
use clap::Parser;
use near_chain::{
    flat_storage_creator::FlatStorageShardCreator, types::RuntimeAdapter, ChainStore,
    ChainStoreAccess,
};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{state::ValueRef, trie_key::trie_key_parsers::parse_account_id_from_raw_key};
use near_store::flat::store_helper::{self, get_flat_head, get_flat_storage_creation_status};
use near_store::{Mode, NodeStorage, ShardUId, Store, StoreOpener};
use nearcore::{load_config, NearConfig, NightshadeRuntime};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tqdm::tqdm;

#[derive(Parser)]
pub struct FlatStorageCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    /// View the current state of flat storage
    View,

    /// Reset the flat storage state (remove all the contents)
    Reset(ResetCmd),

    /// Init the flat storage state, by copying from trie
    Init(InitCmd),

    /// Verify flat storage state (it can take up to couple hours if flat storage is very large)
    Verify(VerifyCmd),

    /// Temporary command to set the store version (useful as long flat
    /// storage is enabled only during nightly with separate DB version).
    SetStoreVersion(SetStoreVersionCmd),
}

#[derive(Parser)]
pub struct SetStoreVersionCmd {
    version: u32,
}

#[derive(Parser)]
pub struct ResetCmd {
    shard_id: u64,
}

#[derive(Parser)]

pub struct InitCmd {
    shard_id: u64,

    #[clap(default_value = "3")]
    num_threads: usize,
}

#[derive(Parser)]
pub struct VerifyCmd {
    shard_id: u64,
}

impl FlatStorageCommand {
    fn get_db(
        opener: &StoreOpener,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        mode: Mode,
    ) -> (NodeStorage, Arc<NightshadeRuntime>, ChainStore, Store) {
        let node_storage = opener.open_in_mode(mode).unwrap();
        let hot_runtime = Arc::new(NightshadeRuntime::from_config(
            home_dir,
            node_storage.get_hot_store(),
            &near_config,
        ));
        let chain_store = ChainStore::new(node_storage.get_hot_store(), 0, false);
        let hot_store = node_storage.get_hot_store();
        (node_storage, hot_runtime, chain_store, hot_store)
    }

    pub fn run(&self, home_dir: &PathBuf) -> anyhow::Result<()> {
        let near_config =
            load_config(home_dir, near_chain_configs::GenesisValidationMode::Full).unwrap();
        let opener = NodeStorage::opener(home_dir, false, &near_config.config.store, None);

        match &self.subcmd {
            SubCommand::View => {
                let (_, hot_runtime, chain_store, hot_store) =
                    Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadOnly);
                let tip = chain_store.final_head().unwrap();
                let shards = hot_runtime.num_shards(&tip.epoch_id).unwrap();
                println!("DB version: {:?}", hot_store.get_db_version());
                println!("Current final tip @{:?} - shards: {:?}", tip.height, shards);

                for shard in 0..shards {
                    let head_hash = get_flat_head(&hot_store, shard);
                    if let Some(head_hash) = head_hash {
                        let head_header = chain_store.get_block_header(&head_hash);
                        let creation_status = get_flat_storage_creation_status(&hot_store, shard);
                        println!(
                            "Shard: {:?} - flat storage @{:?} Details: {:?}",
                            shard,
                            head_header.map(|header| header.height()),
                            creation_status
                        );
                    } else {
                        println!("Shard: {:?} - no flat storage.", shard)
                    }
                }
            }
            SubCommand::SetStoreVersion(set_version) => {
                let rw_storage = opener.open_in_mode(near_store::Mode::ReadWriteExisting).unwrap();
                let rw_store = rw_storage.get_hot_store();
                println!("Setting storage DB version to: {:?}", set_version.version);
                rw_store.set_db_version(set_version.version)?;
            }
            SubCommand::Reset(reset_cmd) => {
                let (_, rw_hot_runtime, rw_chain_store, _) = Self::get_db(
                    &opener,
                    home_dir,
                    &near_config,
                    near_store::Mode::ReadWriteExisting,
                );
                let tip = rw_chain_store.final_head().unwrap();

                // TODO: there should be a method that 'loads' the current flat storage state based on Storage.
                let shard_uid =
                    rw_hot_runtime.shard_id_to_uid(reset_cmd.shard_id, &tip.epoch_id)?;
                rw_hot_runtime.create_flat_storage_for_shard(shard_uid);

                rw_hot_runtime.remove_flat_storage_for_shard(reset_cmd.shard_id, &tip.epoch_id)?;
            }
            SubCommand::Init(init_cmd) => {
                let (_, rw_hot_runtime, rw_chain_store, rw_hot_store) = Self::get_db(
                    &opener,
                    home_dir,
                    &near_config,
                    near_store::Mode::ReadWriteExisting,
                );

                let tip = rw_chain_store.final_head().unwrap();

                let mut creator =
                    FlatStorageShardCreator::new(init_cmd.shard_id, tip.height - 1, rw_hot_runtime);
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(init_cmd.num_threads)
                    .build()
                    .unwrap();

                loop {
                    let status = creator.update_status(&rw_chain_store, &pool)?;
                    if status {
                        break;
                    }
                    let current_status =
                        get_flat_storage_creation_status(&rw_hot_store, init_cmd.shard_id);
                    println!("Status: {:?}", current_status);

                    std::thread::sleep(Duration::from_secs(1));
                }

                println!("Flat storage initialization finished.");
            }
            SubCommand::Verify(verify_cmd) => {
                let (_, hot_runtime, chain_store, hot_store) =
                    Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadOnly);

                let head_hash = get_flat_head(&hot_store, verify_cmd.shard_id).expect(&format!(
                    "Flat storage head missing for shard {:?}.",
                    verify_cmd.shard_id
                ));
                let block_header = chain_store.get_block_header(&head_hash).unwrap();
                let shard_layout = hot_runtime.get_shard_layout(block_header.epoch_id()).unwrap();

                println!(
                    "Verifying flat storage for shard {:?} - flat head @{:?} ({:?})",
                    verify_cmd.shard_id,
                    block_header.height(),
                    block_header.hash()
                );
                let chunk_extra = chain_store
                    .get_chunk_extra(
                        &head_hash,
                        &ShardUId::from_shard_id_and_layout(verify_cmd.shard_id, &shard_layout),
                    )
                    .unwrap();

                // The state root must be from AFTER applying the final block (that's why we're taking it from the chunk extra).
                let state_root = chunk_extra.state_root();

                println!("Verifying using the {:?} as state_root", state_root);
                let tip = chain_store.final_head().unwrap();

                let shard_uid = hot_runtime.shard_id_to_uid(verify_cmd.shard_id, &tip.epoch_id)?;
                hot_runtime.create_flat_storage_for_shard(shard_uid);

                let trie = hot_runtime
                    .get_view_trie_for_shard(verify_cmd.shard_id, &head_hash, *state_root)
                    .unwrap();

                let flat_state_entries_iter = store_helper::iter_flat_state_entries(
                    shard_layout,
                    verify_cmd.shard_id,
                    &hot_store,
                    None,
                    None,
                );

                // Trie iterator which skips all the 'delayed' keys - that don't contain the account_id as string.
                let trie_iter = trie.iter().unwrap().filter(|entry| {
                    let result_copy = &entry.clone().unwrap().0;
                    let result = &result_copy[..];
                    parse_account_id_from_raw_key(result).unwrap().is_some()
                });

                let mut verified = 0;
                let mut success = true;
                for (item_trie, item_flat) in
                    tqdm(std::iter::zip(trie_iter, flat_state_entries_iter))
                {
                    let value_ref = ValueRef::decode((*item_flat.1).try_into().unwrap());
                    verified += 1;

                    let item_trie = item_trie.unwrap();
                    if item_trie.0 != *item_flat.0 {
                        println!(
                            "Different keys {:?} in trie, {:?} in flat storage. ",
                            item_trie.0, item_flat.0
                        );
                        success = false;
                        break;
                    }
                    if item_trie.1.len() != value_ref.length as usize {
                        println!(
                            "Different ValueRef::length for key: {:?}  in trie: {:?} vs flat storage: {:?}",
                            item_trie.0,
                            item_trie.1.len(),
                            value_ref.length
                        );
                        success = false;
                        break;
                    }

                    if near_primitives::hash::hash(&item_trie.1) != value_ref.hash {
                        println!(
                            "Different ValueRef::hashfor key: {:?} in trie: {:?} vs flat storage: {:?}",
                            item_trie.0,
                            near_primitives::hash::hash(&item_trie.1),
                            value_ref.hash
                        );
                        success = false;
                        break;
                    }
                }
                if success {
                    println!("Success - verified {:?} nodes", verified);
                } else {
                    println!("FAILED - on node {:?}", verified);
                }
            }
        }

        Ok(())
    }
}

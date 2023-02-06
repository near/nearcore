use std::{path::PathBuf, sync::Arc, time::Duration};

use borsh::BorshDeserialize;
use clap::Parser;
use near_chain::{
    flat_storage_creator::FlatStorageShardCreator, types::RuntimeAdapter, ChainStore,
    ChainStoreAccess,
};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    hash,
    state::ValueRef,
    trie_key::{trie_key_parsers::parse_account_id_from_raw_key, TrieKey},
};
use near_store::{
    flat_state::{
        store_helper::{get_flat_head, get_flat_storage_creation_status},
        FlatStateFactory, FlatStorageCreationStatus,
    },
    NodeStorage, RawTrieNodeWithSize,
};
use nearcore::{load_config, NightshadeRuntime};
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

    SetStoreVersion(SetStoreVersionCmd),

    /// Reset the flat storage state (remove all the contents)
    Reset(ResetCmd),

    /// Init the flat storage state, by copying from trie
    Init(InitCmd),

    /// Verify flat storage state
    Verify(VerifyCmd),
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
}

#[derive(Parser)]
pub struct VerifyCmd {
    shard_id: u64,
}

impl FlatStorageCommand {
    pub fn run(&self, home_dir: &PathBuf) -> anyhow::Result<()> {
        let near_config =
            load_config(home_dir, near_chain_configs::GenesisValidationMode::Full).unwrap();
        let opener = NodeStorage::opener(home_dir, &near_config.config.store, None);

        let store = opener.open_in_mode(near_store::Mode::ReadOnly).unwrap();
        let hot_runtime =
            Arc::new(NightshadeRuntime::from_config(home_dir, store.get_hot_store(), &near_config));

        let chain_store = ChainStore::new(store.get_hot_store(), 0, false);

        let store = store.get_hot_store();

        match &self.subcmd {
            SubCommand::View => {
                let tip = chain_store.final_head().unwrap();
                let shards = hot_runtime.num_shards(&tip.epoch_id).unwrap();
                println!("DB version: {:?}", store.get_db_version());
                println!("Current final tip @{:?} - shards: {:?}", tip.height, shards);

                for shard in 0..shards {
                    let head_hash = get_flat_head(&store, shard);
                    if let Some(head_hash) = head_hash {
                        let head_header = chain_store.get_block_header(&head_hash);
                        let creation_status = get_flat_storage_creation_status(&store, shard);
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
                let tip = chain_store.final_head().unwrap();
                let rw_store = opener.open_in_mode(near_store::Mode::ReadWriteExisting)?;
                let hot_rw_runtime = Arc::new(NightshadeRuntime::from_config(
                    home_dir,
                    rw_store.get_hot_store(),
                    &near_config,
                ));

                // TODO: there should be a method that 'loads' the current flat storage state based on Storage.
                hot_rw_runtime.create_flat_storage_state_for_shard(
                    reset_cmd.shard_id,
                    tip.height,
                    &chain_store,
                );

                hot_rw_runtime
                    .remove_flat_storage_state_for_shard(reset_cmd.shard_id, &tip.epoch_id)?;
            }
            SubCommand::Init(init_cmd) => {
                let tip = chain_store.final_head().unwrap();
                let rw_store = opener.open_in_mode(near_store::Mode::ReadWriteExisting)?;
                let hot_rw_runtime = Arc::new(NightshadeRuntime::from_config(
                    home_dir,
                    rw_store.get_hot_store(),
                    &near_config,
                ));
                let rw_chain_store = ChainStore::new(rw_store.get_hot_store(), 0, false);

                let mut creator =
                    FlatStorageShardCreator::new(init_cmd.shard_id, tip.height, hot_rw_runtime);
                let num_threads = 3;
                let pool =
                    rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();

                loop {
                    creator.update_status(&rw_chain_store, &pool)?;
                    println!("Waiting for rayon ...");

                    if creator.remaining_state_parts.is_none() {
                        break;
                    }
                    std::thread::sleep(Duration::from_secs(1));
                }

                println!("Rayon threads are done now");
                /*{
                    let pool =
                    rayon::ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap();
                    creator.update_status(&rw_chain_store, &pool)?;
                }*/
            }
            SubCommand::Verify(verify_cmd) => {
                let head_hash = get_flat_head(&store, verify_cmd.shard_id).unwrap();
                let block_header = chain_store.get_block_header(&head_hash).unwrap();
                let block = chain_store.get_block(&head_hash).unwrap();
                println!(
                    "Verifying flat storage for shard {:?} - flat head @{:?} ({:?})",
                    verify_cmd.shard_id,
                    block_header.height(),
                    block_header.hash()
                );
                let chunks_collection = block.chunks();
                let shard_chunk_header =
                    chunks_collection.get(verify_cmd.shard_id as usize).unwrap();
                // TODO: this might be wrong..
                let state_root = shard_chunk_header.prev_state_root();

                println!("Verifying using the {:?} as state_root", state_root);
                let tip = chain_store.final_head().unwrap();

                hot_runtime.create_flat_storage_state_for_shard(
                    verify_cmd.shard_id,
                    tip.height,
                    &chain_store,
                );

                let trie = hot_runtime
                    .get_view_trie_for_shard(verify_cmd.shard_id, &head_hash, state_root)
                    .unwrap();

                let flat_state = hot_runtime
                    .flat_state_factory
                    .new_flat_state_for_shard(verify_cmd.shard_id, Some(head_hash), true)
                    .unwrap();

                let shard_layout = hot_runtime.get_shard_layout(block_header.epoch_id()).unwrap();
                let all_entries = flat_state.iter_flat_state_entries(&shard_layout, &[], &None);

                let trie_iter = trie.iter().unwrap().filter(|entry| {
                    let result_copy = &entry.clone().unwrap().0;
                    let result = &result_copy[..];
                    parse_account_id_from_raw_key(result).unwrap().is_some()
                });

                let mut verified = 0;
                let mut success = true;
                for (item_trie, item_flat) in tqdm(std::iter::zip(trie_iter, all_entries.iter())) {
                    let value_ref = ValueRef::decode((*item_flat.1).try_into().unwrap());
                    verified += 1;

                    let item_trie = item_trie.unwrap();
                    if item_trie.0 != *item_flat.0 {
                        println!(
                            "Different keys {:?} in trie, {:?} in flat. ",
                            item_trie.0, item_flat.0
                        );
                        success = false;
                        break;
                    }
                    if item_trie.1.len() != value_ref.length as usize {
                        println!(
                            "Wrong values for key: {:?} length trie: {:?} vs {:?}",
                            item_trie.0,
                            item_trie.1.len(),
                            value_ref.length
                        );
                        success = false;
                        break;
                    }

                    if near_primitives::hash::hash(&item_trie.1) != value_ref.hash {
                        println!(
                            "Wrong hash for key: {:?} length trie: {:?} vs {:?}",
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

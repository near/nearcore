use crate::resume_resharding::resume_resharding;
/// Tools for modifying flat storage - should be used only for experimentation & debugging.
use borsh::BorshDeserialize;
use clap::Parser;
use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::errors::EpochError;
use near_primitives::shard_layout::ShardVersion;
use near_primitives::state::FlatStateValue;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::flat_store::FlatStoreAdapter;
use near_store::flat::{
    FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorageStatus,
};
use near_store::trie::AccessOptions;
use near_store::{DBCol, Mode, NodeStorage, ShardUId, Store, StoreOpener};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt, load_config};
use std::{path::PathBuf, sync::Arc};
// cspell:ignore tqdm
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
    View(ViewCmd),

    /// Reset the flat storage state (remove all the contents)
    Reset(ResetCmd),

    /// Verify flat storage state (it can take up to couple hours if flat storage is very large)
    Verify(VerifyCmd),

    /// Temporary command to set the store version (useful as long flat
    /// storage is enabled only during nightly with separate DB version).
    SetStoreVersion(SetStoreVersionCmd),

    /// Construct and store trie in a separate directory from flat storage state for a given shard.
    /// The trie is constructed for the block height equal to flat_head
    ConstructTrieFromFlat(ConstructTriedFromFlatCmd),

    /// Move flat storage head.
    MoveFlatHead(MoveFlatHeadCmd),

    /// Resume an unfinished Flat storage resharding for a given shard.
    ResumeResharding(ResumeReshardingCmd),
}

#[derive(Parser)]
pub struct ViewCmd {
    #[clap(long)]
    shard_id: Option<ShardId>,
}

#[derive(Parser)]
pub struct ConstructTriedFromFlatCmd {
    #[clap(long)]
    shard_id: ShardId,
    /// Path to directory where the constructed trie would be stored. Note that there shouldn't be an
    /// existing DB in the path provided.
    #[clap(long)]
    write_store_path: PathBuf,
}

#[derive(Parser)]
pub struct SetStoreVersionCmd {
    version: u32,
}

#[derive(Parser)]
pub struct ResetCmd {
    shard_id: ShardId,
}

#[derive(Parser)]
pub struct InitCmd {
    shard_id: ShardId,

    #[clap(default_value = "3")]
    num_threads: usize,
}

#[derive(Parser)]
pub struct VerifyCmd {
    shard_id: ShardId,
}

#[derive(Parser)]
pub struct MigrateValueInliningCmd {
    #[clap(default_value = "16")]
    num_threads: usize,

    #[clap(default_value = "50000")]
    batch_size: usize,
}

#[derive(Parser)]
pub enum MoveFlatHeadMode {
    /// Moves head forward to specific height.
    Forward {
        #[clap(long)]
        new_flat_head_height: BlockHeight,
    },
    /// Moves head back by specific number of blocks.
    ///
    /// Note: it doesn't record deltas on the way and should be used only for replaying chain
    /// forward.
    Back {
        #[clap(long)]
        blocks: usize,
    },
}

#[derive(Parser)]
pub struct MoveFlatHeadCmd {
    #[clap(long)]
    shard_id: ShardId,
    #[clap(long)]
    version: ShardVersion,
    #[clap(subcommand)]
    mode: MoveFlatHeadMode,
}

#[derive(Parser)]
pub struct ResumeReshardingCmd {
    #[clap(long)]
    pub shard_id: ShardId,
}

fn print_delta(store: &FlatStoreAdapter, shard_uid: ShardUId, metadata: FlatStateDeltaMetadata) {
    let changes = store.get_delta(shard_uid, metadata.block.hash).unwrap().unwrap();
    println!("{:?}", FlatStateDelta { metadata, changes });
}

fn print_deltas(store: &FlatStoreAdapter, shard_uid: ShardUId) {
    let deltas_metadata = store.get_all_deltas_metadata(shard_uid).unwrap();
    let num_deltas = deltas_metadata.len();
    println!("Deltas: {}", num_deltas);

    if num_deltas <= 10 {
        for delta_metadata in deltas_metadata {
            print_delta(store, shard_uid, delta_metadata);
        }
    } else {
        let (first_deltas, last_deltas) = deltas_metadata.split_at(5);

        for delta_metadata in first_deltas {
            print_delta(store, shard_uid, *delta_metadata);
        }
        println!("... skipped {} deltas ...", num_deltas - 10);
        let (_, last_deltas) = last_deltas.split_at(last_deltas.len() - 5);
        for delta_metadata in last_deltas {
            print_delta(store, shard_uid, *delta_metadata);
        }
    }
}

impl FlatStorageCommand {
    fn get_db(
        opener: &StoreOpener,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        mode: Mode,
    ) -> (NodeStorage, Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, ChainStore, Store) {
        let node_storage = opener.open_in_mode(mode).unwrap();
        let epoch_manager = EpochManager::new_arc_handle(
            node_storage.get_hot_store(),
            &near_config.genesis.config,
            Some(home_dir.as_path()),
        );
        let hot_runtime = NightshadeRuntime::from_config(
            home_dir,
            node_storage.get_hot_store(),
            &near_config,
            epoch_manager.clone(),
        )
        .expect("could not create transaction runtime");
        let chain_store = ChainStore::new(node_storage.get_hot_store(), false, 100);
        let hot_store = node_storage.get_hot_store();
        (node_storage, epoch_manager, hot_runtime, chain_store, hot_store)
    }

    fn view(
        &self,
        cmd: &ViewCmd,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let (.., hot_store) =
            Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadOnly);
        println!("DB version: {:?}", hot_store.get_db_version()?);
        for item in hot_store.iter(DBCol::FlatStorageStatus) {
            let (bytes_shard_uid, status) = item?;
            let shard_uid = ShardUId::try_from(bytes_shard_uid.as_ref()).unwrap();
            let status = FlatStorageStatus::try_from_slice(&status)?;
            if let Some(shard_id) = cmd.shard_id {
                if shard_id != shard_uid.shard_id() {
                    continue;
                }
            }

            match status {
                FlatStorageStatus::Ready(ready_status) => {
                    println!(
                        "Shard: {shard_uid:?} - flat storage @{:?} ({})",
                        ready_status.flat_head.height, ready_status.flat_head.hash,
                    );
                    print_deltas(&hot_store.flat_store(), shard_uid);
                }
                status => {
                    println!("Shard: {shard_uid:?} - no flat storage: {status:?}");
                }
            }
        }
        Ok(())
    }

    fn set_store_version(
        &self,
        cmd: &SetStoreVersionCmd,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let rw_storage = opener.open_in_mode(near_store::Mode::ReadWriteExisting)?;
        let rw_store = rw_storage.get_hot_store();
        println!("Setting storage DB version to: {:?}", cmd.version);
        rw_store.set_db_version(cmd.version)?;
        Ok(())
    }

    fn reset(
        &self,
        cmd: &ResetCmd,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let (_, epoch_manager, rw_hot_runtime, rw_chain_store, store) =
            Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadWriteExisting);
        let tip = rw_chain_store.final_head()?;

        // TODO: there should be a method that 'loads' the current flat storage state based on Storage.
        let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), cmd.shard_id, &tip.epoch_id)?;
        let flat_storage_manager = rw_hot_runtime.get_flat_storage_manager();
        flat_storage_manager.create_flat_storage_for_shard(shard_uid)?;
        let mut store_update = store.flat_store().store_update();
        flat_storage_manager.remove_flat_storage_for_shard(shard_uid, &mut store_update)?;
        store_update.commit()?;
        Ok(())
    }

    fn verify(
        &self,
        cmd: &VerifyCmd,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let (_, epoch_manager, hot_runtime, chain_store, hot_store) =
            Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadOnly);
        let tip = chain_store.final_head()?;
        let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), cmd.shard_id, &tip.epoch_id)?;
        let hot_store = hot_store.flat_store();

        let head_hash = match hot_store
            .get_flat_storage_status(shard_uid)
            .expect("failed to read flat storage status")
        {
            FlatStorageStatus::Ready(ready_status) => ready_status.flat_head.hash,
            status => {
                panic!("Flat storage is not ready for shard {:?}: {status:?}", cmd.shard_id);
            }
        };
        let block_header = chain_store.get_block_header(&head_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;

        println!(
            "Verifying flat storage for shard {:?} - flat head @{:?} ({:?})",
            cmd.shard_id,
            block_header.height(),
            block_header.hash()
        );
        let chunk_extra = chain_store.get_chunk_extra(
            &head_hash,
            &ShardUId::from_shard_id_and_layout(cmd.shard_id, &shard_layout),
        )?;

        // The state root must be from AFTER applying the final block (that's why we're taking it from the chunk extra).
        let state_root = chunk_extra.state_root();

        println!("Verifying using the {:?} as state_root", state_root);
        let tip = chain_store.final_head()?;

        let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), cmd.shard_id, &tip.epoch_id)?;
        hot_runtime.get_flat_storage_manager().create_flat_storage_for_shard(shard_uid)?;

        let trie = hot_runtime.get_view_trie_for_shard(cmd.shard_id, &head_hash, *state_root)?;

        let flat_state_entries_iter = hot_store.iter(shard_uid);

        let trie_iter = trie.disk_iter()?;
        let mut verified = 0;
        let mut success = true;
        for (item_trie, item_flat) in tqdm(std::iter::zip(trie_iter, flat_state_entries_iter)) {
            let item_flat = item_flat?;
            let value_ref = item_flat.1.to_value_ref();
            verified += 1;

            let item_trie = item_trie?;
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
                    "Different ValueRef::hash for key: {:?} in trie: {:?} vs flat storage: {:?}",
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
        Ok(())
    }

    fn construct_trie_from_flat(
        &self,
        cmd: &ConstructTriedFromFlatCmd,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let (_, epoch_manager, _, chain_store, store) =
            Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadWriteExisting);

        let write_opener =
            NodeStorage::opener(&cmd.write_store_path, &near_config.config.store, None);
        let write_node_storage = write_opener.open_in_mode(Mode::Create)?;
        let write_store = write_node_storage.get_hot_store();

        let tip = chain_store.final_head()?;
        let shard_uid = shard_id_to_uid(epoch_manager.as_ref(), cmd.shard_id, &tip.epoch_id)?;

        near_store::trie::construct_trie_from_flat(store, write_store, shard_uid);
        Ok(())
    }

    fn move_flat_head_back(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        runtime: &dyn RuntimeAdapter,
        chain_store: ChainStore,
        mut shard_uid: ShardUId,
        blocks: usize,
    ) -> anyhow::Result<()> {
        let store = chain_store.store();
        let flat_store = store.flat_store();
        let flat_head = match flat_store.get_flat_storage_status(shard_uid) {
            Ok(FlatStorageStatus::Ready(ready_status)) => ready_status.flat_head,
            status => {
                panic!("invalid flat storage status for shard {shard_uid:?}: {status:?}")
            }
        };
        let mut height = flat_head.height;
        let shard_id = shard_uid.shard_id();

        for _ in 0..blocks {
            let block_hash = chain_store.get_block_hash_by_height(height)?;
            let block = chain_store.get_block(&block_hash)?;
            let header = block.header();

            let epoch_id = header.epoch_id();
            let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
            shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id)?;
            let shard_index =
                shard_layout.get_shard_index(shard_id).map_err(Into::<EpochError>::into)?;

            let state_root = block.chunks().get(shard_index).unwrap().prev_state_root();
            let prev_hash = header.prev_hash();
            let prev_header = chain_store.get_block_header(&prev_hash)?;
            let prev_prev_hash = *prev_header.prev_hash();
            let prev_height = prev_header.height();

            let trie =
                runtime.get_trie_for_shard(shard_uid.shard_id(), &block_hash, state_root, false)?;

            // Find all items which were changed after applying block `block_hash`
            // and add them to delta.
            // This is done by iterating over `DBCol::StateChanges` corresponding
            // to given shard.
            let mut prev_delta = FlatStateChanges::default();
            for item in store.iter_prefix(DBCol::StateChanges, &block_hash.0) {
                let (key, _) = item.unwrap();
                let maybe_trie_key = &key[32..];
                let maybe_account_id =
                    near_primitives::trie_key::trie_key_parsers::parse_account_id_from_raw_key(
                        maybe_trie_key,
                    )?;
                let maybe_trie_key = match maybe_account_id {
                    Some(account_id) => {
                        let account_shard_id = shard_layout.account_id_to_shard_id(&account_id);
                        if shard_id == account_shard_id { Some(maybe_trie_key) } else { None }
                    }
                    None => {
                        assert!(maybe_trie_key.len() >= 8);
                        let (trie_key, shard_uid_raw) =
                            maybe_trie_key.split_at(maybe_trie_key.len() - 8);
                        if shard_uid.to_bytes() == shard_uid_raw { Some(trie_key) } else { None }
                    }
                };

                if let Some(trie_key) = maybe_trie_key {
                    // Take *previous* value for the key from trie corresponding
                    // to pre-state-root for this block.
                    let prev_value_ref = trie
                        .get_optimized_ref(
                            trie_key,
                            near_store::KeyLookupMode::MemOrTrie,
                            AccessOptions::DEFAULT,
                        )?
                        .map(|value_ref| value_ref.into_value_ref());
                    let value_ref =
                        flat_store.get(shard_uid, trie_key)?.map(|val| val.to_value_ref());
                    if prev_value_ref == value_ref {
                        // Value didn't change, skip it.
                        continue;
                    }

                    // Inline value if needed and add it to delta.
                    let prev_value = match prev_value_ref {
                        None => None,
                        Some(value_ref) => {
                            if value_ref.len() <= FlatStateValue::INLINE_DISK_VALUE_THRESHOLD {
                                let value =
                                    trie.retrieve_value(&value_ref.hash, AccessOptions::DEFAULT)?;
                                Some(FlatStateValue::Inlined(value))
                            } else {
                                Some(FlatStateValue::Ref(value_ref))
                            }
                        }
                    };
                    prev_delta.insert(trie_key.to_vec(), prev_value);
                }
            }

            // Change all keys values of which are different for previous
            // and current block.
            // Note that we don't write delta to DB, because this command is
            // used to simulate applying chunks from past blocks, and in that
            // simulations future deltas should not exist.
            let mut store_update = flat_store.store_update();
            prev_delta.apply_to_flat_state(&mut store_update, shard_uid);
            store_update.set_flat_storage_status(
                shard_uid,
                FlatStorageStatus::Ready(near_store::flat::FlatStorageReadyStatus {
                    flat_head: near_store::flat::BlockInfo {
                        hash: *prev_hash,
                        height: prev_height,
                        prev_hash: prev_prev_hash,
                    },
                }),
            );
            store_update.commit()?;

            height = prev_height;
            println!("moved to {height}");
        }
        Ok(())
    }

    fn move_flat_head(
        &self,
        cmd: &MoveFlatHeadCmd,
        home_dir: &PathBuf,
        near_config: &NearConfig,
        opener: StoreOpener,
    ) -> anyhow::Result<()> {
        let (_, epoch_manager, runtime, chain_store, _) =
            Self::get_db(&opener, home_dir, &near_config, near_store::Mode::ReadWriteExisting);

        let shard_uid = ShardUId::new(cmd.version, cmd.shard_id);
        let flat_storage_manager = runtime.get_flat_storage_manager();
        flat_storage_manager.create_flat_storage_for_shard(shard_uid)?;
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();

        match cmd.mode {
            MoveFlatHeadMode::Forward { new_flat_head_height } => {
                let header = chain_store.get_block_header_by_height(new_flat_head_height)?;
                println!("Moving flat head for shard {shard_uid} forward to header: {header:?}");
                flat_storage.update_flat_head(header.hash())?;
            }
            MoveFlatHeadMode::Back { blocks } => {
                println!("Moving flat head for shard {shard_uid} back by {blocks} blocks");
                self.move_flat_head_back(
                    epoch_manager.as_ref(),
                    runtime.as_ref(),
                    chain_store,
                    shard_uid,
                    blocks,
                )?;
            }
        }

        Ok(())
    }

    pub fn run(
        &self,
        home_dir: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.archival_config(),
        );

        match &self.subcmd {
            SubCommand::View(cmd) => self.view(cmd, home_dir, &near_config, opener),
            SubCommand::SetStoreVersion(cmd) => self.set_store_version(cmd, opener),
            SubCommand::Reset(cmd) => self.reset(cmd, home_dir, &near_config, opener),
            SubCommand::Verify(cmd) => self.verify(cmd, home_dir, &near_config, opener),
            SubCommand::ConstructTrieFromFlat(cmd) => {
                self.construct_trie_from_flat(cmd, home_dir, &near_config, opener)
            }
            SubCommand::MoveFlatHead(cmd) => {
                self.move_flat_head(cmd, home_dir, &near_config, opener)
            }
            SubCommand::ResumeResharding(cmd) => {
                resume_resharding(cmd, home_dir, &near_config, opener)
            }
        }
    }
}

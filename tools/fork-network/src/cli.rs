use std::path::Path;

use near_chain::types::Tip;
use near_chain_configs::{Genesis, GenesisRecords, GenesisValidationMode};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::{borsh::BorshSerialize, hash::CryptoHash, types::AccountInfo};
use near_store::{flat::FlatStorageStatus, DBCol, Mode, NodeStorage, HEAD_KEY};
use nearcore::load_config;

#[derive(clap::Parser)]
pub struct ForkNetworkCommand {}

impl ForkNetworkCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        if !near_config.config.store.flat_storage_creation_enabled {
            panic!("Flat storage is required");
        }

        if near_config.validator_signer.is_none() {
            panic!("Validator ID is required; please set up validator_keys.json");
        }

        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            None,
        );

        println!("Creating snapshot");
        store_opener.create_snapshots(Mode::ReadWrite)?;

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
        let head = store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)?.unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id)?;
        drop(epoch_manager);
        let all_shard_uids = shard_layout.get_shard_uids();

        let mut flat_head: Option<CryptoHash> = None;
        for shard_uid in all_shard_uids {
            let flat_storage_status = store
                .get_ser::<FlatStorageStatus>(DBCol::FlatStorageStatus, &shard_uid.to_bytes())?
                .unwrap();
            if let FlatStorageStatus::Ready(ready) = &flat_storage_status {
                let flat_head_for_this_shard = ready.flat_head.hash;
                if let Some(hash) = flat_head {
                    if hash != flat_head_for_this_shard {
                        panic!("Not all shards have the same flat head");
                    }
                }
                flat_head = Some(flat_head_for_this_shard);
            } else {
                panic!(
                    "Flat storage is not ready for shard {}: {:?}",
                    shard_uid, flat_storage_status
                );
            }
        }

        let flat_head_block = store
            .get_ser::<near_primitives::block::Block>(
                DBCol::Block,
                &flat_head.unwrap().try_to_vec().unwrap(),
            )?
            .unwrap();

        let state_roots = flat_head_block
            .chunks()
            .iter()
            .map(|chunk| chunk.prev_state_root())
            .collect::<Vec<_>>();

        println!("Creating a new genesis");
        let mut genesis_config = near_config.genesis.config.clone();
        genesis_config.chain_id += "-fork";
        genesis_config.genesis_height = flat_head_block.header().height();
        genesis_config.genesis_time = flat_head_block.header().timestamp();
        genesis_config.epoch_length = 1000;
        let self_validator = near_config.validator_signer.as_ref().unwrap();
        let self_account = AccountInfo {
            account_id: self_validator.validator_id().clone(),
            amount: 50000000000000000000000000000_u128,
            public_key: self_validator.public_key().clone(),
        };
        genesis_config.validators = vec![self_account];

        let mut update = store.store_update();
        // update.delete_all(DBCol::DbVersion);
        update.delete_all(DBCol::BlockMisc);
        update.delete_all(DBCol::Block);
        update.delete_all(DBCol::BlockHeader);
        update.delete_all(DBCol::BlockHeight);
        // update.delete_all(DBCol::State);
        update.delete_all(DBCol::ChunkExtra);
        update.delete_all(DBCol::_TransactionResult);
        update.delete_all(DBCol::OutgoingReceipts);
        update.delete_all(DBCol::IncomingReceipts);
        update.delete_all(DBCol::_Peers);
        update.delete_all(DBCol::RecentOutboundConnections);
        update.delete_all(DBCol::EpochInfo);
        update.delete_all(DBCol::BlockInfo);
        update.delete_all(DBCol::Chunks);
        update.delete_all(DBCol::PartialChunks);
        update.delete_all(DBCol::BlocksToCatchup);
        update.delete_all(DBCol::StateDlInfos);
        update.delete_all(DBCol::ChallengedBlocks);
        update.delete_all(DBCol::StateHeaders);
        update.delete_all(DBCol::InvalidChunks);
        update.delete_all(DBCol::BlockExtra);
        update.delete_all(DBCol::BlockPerHeight);
        update.delete_all(DBCol::StateParts);
        update.delete_all(DBCol::EpochStart);
        update.delete_all(DBCol::AccountAnnouncements);
        update.delete_all(DBCol::NextBlockHashes);
        update.delete_all(DBCol::EpochLightClientBlocks);
        update.delete_all(DBCol::ReceiptIdToShardId);
        update.delete_all(DBCol::_NextBlockWithNewChunk);
        update.delete_all(DBCol::_LastBlockWithNewChunk);
        update.delete_all(DBCol::PeerComponent);
        update.delete_all(DBCol::ComponentEdges);
        update.delete_all(DBCol::LastComponentNonce);
        update.delete_all(DBCol::Transactions);
        update.delete_all(DBCol::_ChunkPerHeightShard);
        update.delete_all(DBCol::StateChanges);
        update.delete_all(DBCol::BlockRefCount);
        update.delete_all(DBCol::TrieChanges);
        update.delete_all(DBCol::BlockMerkleTree);
        update.delete_all(DBCol::ChunkHashesByHeight);
        update.delete_all(DBCol::BlockOrdinal);
        update.delete_all(DBCol::_GCCount);
        update.delete_all(DBCol::OutcomeIds);
        update.delete_all(DBCol::_TransactionRefCount);
        update.delete_all(DBCol::ProcessedBlockHeights);
        update.delete_all(DBCol::Receipts);
        update.delete_all(DBCol::CachedContractCode);
        update.delete_all(DBCol::EpochValidatorInfo);
        update.delete_all(DBCol::HeaderHashesByHeight);
        update.delete_all(DBCol::StateChangesForSplitStates);
        update.delete_all(DBCol::TransactionResultForBlock);
        // update.delete_all(DBCol::FlatState);
        update.delete_all(DBCol::FlatStateChanges);
        update.delete_all(DBCol::FlatStateDeltaMetadata);
        update.delete_all(DBCol::FlatStorageStatus);

        let mut genesis = Genesis::new_validated(
            genesis_config,
            GenesisRecords::default(),
            GenesisValidationMode::UnsafeFast,
        )?;
        genesis.state_roots = Some(state_roots);
        // let epoch_manager = EpochManager::new_arc_handle(store, &genesis_config);
        // let runtime = NightshadeRuntime::from_config(home_dir, store, new_config, epoch_manager)?;

        let genesis_file = near_config.config.genesis_file;
        let original_genesis_file = home_dir.join(&genesis_file);
        let backup_genesis_file = home_dir.join(format!("{}.backup", &genesis_file));
        std::fs::rename(&original_genesis_file, &backup_genesis_file)?;
        genesis.to_file(&original_genesis_file);

        Ok(())
    }
}

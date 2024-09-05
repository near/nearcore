use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use itertools::Itertools;
use near_chain::BlockHeader;
use near_chain::ChainStore;
use near_chain::ChainStoreAccess;
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManager;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::EpochManagerHandle;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::types::AccountId;
use near_primitives::types::Balance;
use near_primitives::types::ValidatorKickoutReason;
use near_primitives::types::{BlockHeight, ValidatorInfoIdentifier};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::EpochValidatorInfo;
use near_store::db::{MixedDB, ReadOrder, TestDB};
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::NearConfig;

/// Replays the headers for the blocks between `start_height` and `end-height`.
/// If `start_height` is not set, uses the genesis height. If `end_height` is not set, uses the chain head.
/// The headers are replayed by updating the [`EpochManager`] with the headers (by calling `add_validator_proposals`)
/// and then comparing the resulting validator information ([`EpochValidatorInfo`]) in the original operation of
/// the chain (from the read-only store) and from the replay of the headers.
pub(crate) fn replay_headers(
    start_height: Option<BlockHeight>,
    end_height: Option<BlockHeight>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );
    let start_height: BlockHeight =
        start_height.unwrap_or_else(|| chain_store.get_genesis_height());
    let end_height: BlockHeight = end_height.unwrap_or_else(|| chain_store.head().unwrap().height);

    let epoch_manager = EpochManager::new_arc_handle(store, &near_config.genesis.config);
    let epoch_manager_replay = EpochManager::new_arc_handle(
        create_replay_store(home_dir, &near_config),
        &near_config.genesis.config,
    );

    for height in start_height..=end_height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let header = chain_store.get_block_header(&block_hash).unwrap().clone();
            tracing::trace!("Height: {}, header: {:#?}", height, header);

            let block_info = get_block_info(&header, &chain_store, epoch_manager.as_ref())
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to add chunk endorsements for block height {}: {:#}",
                        header.height(),
                        e
                    )
                });
            epoch_manager_replay
                .add_validator_proposals(block_info, *header.random_value())
                .unwrap()
                .commit()
                .unwrap();

            if epoch_manager
                .is_last_block_in_finished_epoch(&block_hash)
                .expect("Could not determine if block is last block in epoch")
            {
                let identifier = ValidatorInfoIdentifier::BlockHash(block_hash);
                let original_validators =
                    epoch_manager.get_validator_info(identifier.clone()).unwrap();
                let replayed_validators =
                    epoch_manager_replay.get_validator_info(identifier).unwrap();

                assert_eq!(original_validators.epoch_height, replayed_validators.epoch_height);
                println!(
                    "Comparing validator infos for epoch height {} block height {}",
                    original_validators.epoch_height, height
                );
                // Compare production and kickout statistics between original and replay runs.
                compare_validator_production_stats(&original_validators, &replayed_validators);
                compare_validator_kickout_stats(&original_validators, &replayed_validators);
            } else if header.height() % 1000 == 0 {
                tracing::debug!("@ height {}", header.height());
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ValidatorSummary {
    stake: i128,
    block_production_rate: i64,
    chunk_production_rate: i64,
    chunk_endorsement_rate: i64,
}

fn compare_validator_production_stats(left: &EpochValidatorInfo, right: &EpochValidatorInfo) {
    let left_summaries = get_validator_summary(left);
    let right_summaries = get_validator_summary(right);
    let left_accounts: HashSet<AccountId> = left_summaries.keys().cloned().collect();
    let right_accounts: HashSet<AccountId> = right_summaries.keys().cloned().collect();
    for account_id in left_accounts.union(&right_accounts) {
        let left_summary = left_summaries.get(account_id);
        let right_summary = right_summaries.get(account_id);
        if left_summary.is_some() ^ right_summary.is_some() {
            println!("{}: {:?} --> {:?}", account_id.as_str(), left_summary, right_summary);
            continue;
        }
        let left_summary = left_summary.unwrap();
        let right_summary = right_summary.unwrap();
        if left_summary.stake != right_summary.stake {
            println!(
                "{}: Stake [diff={}] {} --> {}",
                account_id.as_str(),
                right_summary.stake - left_summary.stake,
                left_summary.stake,
                right_summary.stake
            );
        }
        if left_summary.block_production_rate != right_summary.block_production_rate {
            println!(
                "{}: Blocks [diff=%{}] %{} --> %{}",
                account_id.as_str(),
                right_summary.block_production_rate - left_summary.block_production_rate,
                left_summary.block_production_rate,
                right_summary.block_production_rate
            );
        }
        if left_summary.chunk_production_rate != right_summary.chunk_production_rate {
            println!(
                "{}: Chunks [diff=%{}] %{} --> %{}",
                account_id.as_str(),
                right_summary.chunk_production_rate - left_summary.chunk_production_rate,
                left_summary.chunk_production_rate,
                right_summary.chunk_production_rate
            );
        }
        if left_summary.chunk_endorsement_rate != right_summary.chunk_endorsement_rate {
            println!(
                "{}: Endorsements [diff=%{}] %{} --> %{}",
                account_id.as_str(),
                right_summary.chunk_endorsement_rate - left_summary.chunk_endorsement_rate,
                left_summary.chunk_endorsement_rate,
                right_summary.chunk_endorsement_rate
            );
        }
    }
}

fn compare_validator_kickout_stats(left: &EpochValidatorInfo, right: &EpochValidatorInfo) {
    let left_kickouts = get_validator_kickouts(left);
    let right_kickouts = get_validator_kickouts(right);
    let left_accounts: HashSet<AccountId> = left_kickouts.keys().cloned().collect();
    let right_accounts: HashSet<AccountId> = right_kickouts.keys().cloned().collect();
    for account_id in left_accounts.union(&right_accounts) {
        let left_kickout = left_kickouts.get(account_id);
        let right_kickout = right_kickouts.get(account_id);
        if left_kickout.is_some() ^ right_kickout.is_some() {
            println!("{}: {:?} --> {:?}", account_id.as_str(), left_kickout, right_kickout);
            continue;
        }
        let left_kickout = left_kickout.unwrap();
        let right_kickout = right_kickout.unwrap();
        if left_kickout != right_kickout {
            println!("{}: {:?} --> {:?}", account_id.as_str(), left_kickout, right_kickout);
        }
    }
}

/// Returns a mapping from validator account id to the summary of production statistics.
fn get_validator_summary(
    validator_info: &EpochValidatorInfo,
) -> HashMap<AccountId, ValidatorSummary> {
    let mut summary = HashMap::new();
    for validator in validator_info.current_validators.iter() {
        summary.insert(
            validator.account_id.clone(),
            ValidatorSummary {
                stake: validator.stake.try_into().unwrap(),
                block_production_rate: (100.0 * (validator.num_produced_blocks as f64)
                    / (validator.num_expected_blocks as f64))
                    .floor() as i64,
                chunk_production_rate: (100.0 * (validator.num_produced_chunks as f64)
                    / (validator.num_expected_chunks as f64))
                    .floor() as i64,
                chunk_endorsement_rate: (100.0 * (validator.num_produced_endorsements as f64)
                    / (validator.num_expected_endorsements as f64))
                    .floor() as i64,
            },
        );
    }
    summary
}

/// Returns a mapping from validator account id to the kickout reasong (from previous epoch).
fn get_validator_kickouts(
    validator_info: &EpochValidatorInfo,
) -> HashMap<AccountId, ValidatorKickoutReason> {
    let mut kickouts = HashMap::new();
    for kickout in validator_info.prev_epoch_kickout.iter() {
        kickouts.insert(kickout.account_id.clone(), kickout.reason.clone());
    }
    kickouts
}

/// Returns the [`BlockInfo`] corresponding to the header.
/// This function may override the resulting [`BlockInfo`] based on certain protocol versions.
fn get_block_info(
    header: &BlockHeader,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<BlockInfo, Error> {
    // Note(#11900): Until the chunk endorsements in block header are enabled, we generate the chunk endorsement bitmap
    // in the following from the chunk endorsement signatures in the block body.
    // TODO(#11900): Remove this code after ChunkEndorsementsInBlockHeader is stabilized.
    let protocol_version = epoch_manager.get_epoch_protocol_version(header.epoch_id())?;
    let chunk_endorsements_bitmap: Option<ChunkEndorsementsBitmap> =
        if ProtocolFeature::StatelessValidation.enabled(protocol_version)
            && header.chunk_endorsements().is_none()
        {
            let block = chain_store.get_block(header.hash())?;
            let chunks = block.chunks();

            let endorsement_signatures = block.chunk_endorsements().to_vec();
            assert_eq!(endorsement_signatures.len(), chunks.len());

            let mut bitmap = ChunkEndorsementsBitmap::new(chunks.len());

            let height = header.height();
            let prev_block_epoch_id =
                epoch_manager.get_epoch_id_from_prev_block(header.prev_hash())?;
            for chunk_header in chunks.iter() {
                let shard_id = chunk_header.shard_id();
                let endorsements = &endorsement_signatures[shard_id as usize];
                if !chunk_header.is_new_chunk(height) {
                    assert_eq!(endorsements.len(), 0);
                    bitmap.add_endorsements(shard_id, vec![]);
                } else {
                    let assignments = epoch_manager
                        .get_chunk_validator_assignments(
                            &prev_block_epoch_id,
                            shard_id,
                            chunk_header.height_created(),
                        )?
                        .ordered_chunk_validators();
                    assert_eq!(endorsements.len(), assignments.len());
                    bitmap.add_endorsements(
                        shard_id,
                        endorsements.iter().map(|signature| signature.is_some()).collect_vec(),
                    );
                }
            }
            Some(bitmap)
        } else {
            None
        };
    Ok(BlockInfo::from_header_and_endorsements(
        &header,
        chain_store.get_block_height(header.last_final_block()).unwrap(),
        chunk_endorsements_bitmap,
    ))
}

/// Returns a stored that reads from the original chain store, but also allows writes to a temporary DB.
/// This allows to execute a new algorithm for EpochManager without changing the original chain data.
/// If the node has cold storage, the read DB is the split store (Hot+Cold). Write store is a TestDB (in memory).
fn create_replay_store(home_dir: &Path, near_config: &NearConfig) -> Store {
    let store_opener = NodeStorage::opener(
        home_dir,
        near_config.config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    );
    let storage = store_opener.open_in_mode(Mode::ReadOnly).unwrap();

    let read_db = if storage.has_cold() {
        storage.get_split_db().unwrap()
    } else {
        storage.into_inner(Temperature::Hot)
    };
    let write_db = TestDB::new();
    Store::new(MixedDB::new(read_db, write_db, ReadOrder::WriteDBFirst))
}

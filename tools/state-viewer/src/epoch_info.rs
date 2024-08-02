use borsh::BorshDeserialize;
use core::ops::Range;
use itertools::Itertools;
use near_chain::{ChainStore, ChainStoreAccess, Error};
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::account::id::AccountId;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochHeight, EpochId, ProtocolVersion, ShardId};
use near_store::{DBCol, Store};
use std::str::FromStr;
use std::sync::Arc;

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum EpochSelection {
    /// Current epoch.
    Current,
    /// All epochs.
    All,
    /// Fetch the given epoch.
    EpochId { epoch_id: String },
    /// Fetch epochs at the given height.
    EpochHeight { epoch_height: EpochHeight },
    /// Fetch an epoch containing the given block hash.
    BlockHash { block_hash: String },
    /// Fetch an epoch containing the given block height.
    BlockHeight { block_height: BlockHeight },
    /// Fetch all epochs with the given protocol version.
    ProtocolVersion { protocol_version: ProtocolVersion },
}

pub(crate) fn print_epoch_info(
    epoch_selection: EpochSelection,
    validator_account_id: Option<AccountId>,
    kickouts_summary: bool,
    store: Store,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) {
    let epoch_ids = get_epoch_ids(epoch_selection, store, chain_store, epoch_manager);

    let head_block_info =
        epoch_manager.get_block_info(&chain_store.head().unwrap().last_block_hash).unwrap();
    let head_epoch_height =
        epoch_manager.get_epoch_info(head_block_info.epoch_id()).unwrap().epoch_height();
    let mut epoch_infos: Vec<(EpochId, Arc<EpochInfo>)> = epoch_ids
        .iter()
        .map(|epoch_id| (*epoch_id, epoch_manager.get_epoch_info(epoch_id).unwrap()))
        .collect();
    // Sorted output is much easier to follow.
    epoch_infos.sort_by_key(|(_, epoch_info)| epoch_info.epoch_height());

    for (epoch_id, epoch_info) in &epoch_infos {
        println!("-------------------------");
        println!("EpochId: {}", epoch_id.0);
        if kickouts_summary {
            display_kickouts(epoch_info);
        } else {
            if let Err(err) = display_epoch_info(
                epoch_id,
                epoch_info,
                &validator_account_id,
                &head_epoch_height,
                chain_store,
                epoch_manager,
            ) {
                println!("Can't display Epoch Info: {err:?}");
                continue;
            }
            println!("---");
            if let Err(err) =
                display_block_and_chunk_producers(epoch_id, epoch_info, chain_store, epoch_manager)
            {
                println!("Can't display Epoch Info: {err:?}");
                continue;
            }
        }
    }
    println!("=========================");
    println!("Found {} epochs", epoch_ids.len());
}

fn display_block_and_chunk_producers(
    epoch_id: &EpochId,
    epoch_info: &EpochInfo,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> anyhow::Result<()> {
    let block_height_range: Range<BlockHeight> =
        get_block_height_range(epoch_id, chain_store, epoch_manager)?;
    let shard_ids = epoch_manager.shard_ids(epoch_id).unwrap();
    for block_height in block_height_range {
        let bp = epoch_info.sample_block_producer(block_height);
        let bp = epoch_info.get_validator(bp).account_id().clone();
        let cps: Vec<String> = shard_ids
            .iter()
            .map(|&shard_id| {
                let cp = epoch_info.sample_chunk_producer(block_height, shard_id).unwrap();
                let cp = epoch_info.get_validator(cp).account_id().clone();
                cp.as_str().to_string()
            })
            .collect();
        println!("{block_height}: BP=\"{bp}\" CP={cps:?}");
    }
    Ok(())
}

// Iterate over each epoch starting from the head. Find the requested epoch and its previous epoch
// and use that to determine the block range corresponding to the epoch.
fn get_block_height_range(
    epoch_id: &EpochId,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> anyhow::Result<Range<BlockHeight>> {
    let head = chain_store.head()?;
    let mut cur_block_info = epoch_manager.get_block_info(&head.last_block_hash)?;
    loop {
        if cur_block_info.epoch_id() == epoch_id {
            // Found the requested epoch.
            let epoch_start_height = epoch_manager.get_epoch_start_height(cur_block_info.hash())?;
            if &head.epoch_id == epoch_id {
                // This is the current epoch and we can't know when exactly it will end.
                // Estimate that the epoch should end at this height.
                let next_epoch_start_height =
                    epoch_start_height + epoch_manager.get_epoch_config(epoch_id)?.epoch_length;
                return Ok(
                    epoch_start_height..std::cmp::max(head.height + 1, next_epoch_start_height)
                );
            }
            // Head is in a different epoch, therefore the requested epoch must be complete.
            // Iterate over block heights to find the first block in a different epoch.
            for height in epoch_start_height..=head.height {
                match chain_store.get_block_hash_by_height(height) {
                    Ok(hash) => {
                        let header = chain_store.get_block_header(&hash)?;
                        if header.epoch_id() != epoch_id {
                            return Ok(epoch_start_height..height);
                        }
                    }
                    Err(Error::DBNotFoundErr(_)) => {
                        // Block is missing, skip.
                        continue;
                    }
                    Err(err) => {
                        panic!("Failed to get block hash at height {height}: {err:?}");
                    }
                };
            }
            return Ok(epoch_start_height..head.height + 1);
        } else {
            // Go to the previous epoch.
            let first_block_info =
                epoch_manager.get_block_info(cur_block_info.epoch_first_block())?;
            let prev_block_info = epoch_manager.get_block_info(first_block_info.prev_hash())?;
            cur_block_info = prev_block_info;
        }
    }
}

// Converts a bunch of optional filtering options into a vector of EpochIds.
fn get_epoch_ids(
    epoch_selection: EpochSelection,
    store: Store,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Vec<EpochId> {
    match epoch_selection {
        EpochSelection::All => iterate_and_filter(store, |_| true),
        EpochSelection::Current => {
            let epoch_id =
                epoch_manager.get_epoch_id(&chain_store.head().unwrap().last_block_hash).unwrap();
            vec![epoch_id]
        }
        EpochSelection::EpochId { epoch_id } => {
            let epoch_id = EpochId(CryptoHash::from_str(&epoch_id).unwrap());
            vec![epoch_id]
        }
        EpochSelection::EpochHeight { epoch_height } => {
            // Fetch epochs at the given height.
            // There should only be one epoch at a given height. But this is a debug tool, let's check
            // if there are multiple epochs at a given height.
            iterate_and_filter(store, |epoch_info| epoch_info.epoch_height() == epoch_height)
        }
        EpochSelection::BlockHash { block_hash } => {
            let block_hash = CryptoHash::from_str(&block_hash).unwrap();
            vec![epoch_manager.get_epoch_id(&block_hash).unwrap()]
        }
        EpochSelection::BlockHeight { block_height } => {
            // Fetch an epoch containing the given block height.
            let block_hash = chain_store.get_block_hash_by_height(block_height).unwrap();
            vec![epoch_manager.get_epoch_id(&block_hash).unwrap()]
        }
        EpochSelection::ProtocolVersion { protocol_version } => {
            // Fetch the first epoch of the given protocol version.
            iterate_and_filter(store, |epoch_info| {
                epoch_info.protocol_version() == protocol_version
            })
        }
    }
}

// Iterates over the DBCol::EpochInfo column, ignores AGGREGATOR_KEY and returns deserialized EpochId
// for EpochInfos that satisfy the given predicate.
pub(crate) fn iterate_and_filter(
    store: Store,
    predicate: impl Fn(EpochInfo) -> bool,
) -> Vec<EpochId> {
    store
        .iter(DBCol::EpochInfo)
        .map(Result::unwrap)
        .filter_map(|(key, value)| {
            if key.as_ref() == AGGREGATOR_KEY {
                None
            } else {
                let epoch_info = EpochInfo::try_from_slice(value.as_ref()).unwrap();
                if predicate(epoch_info) {
                    Some(EpochId::try_from_slice(key.as_ref()).unwrap())
                } else {
                    None
                }
            }
        })
        .collect()
}

fn display_kickouts(epoch_info: &EpochInfo) {
    for (account_id, kickout_reason) in
        epoch_info.validator_kickout().iter().sorted_by_key(|&(account_id, _)| account_id)
    {
        println!("{account_id:?}: {kickout_reason:?}");
    }
}

fn display_epoch_info(
    epoch_id: &EpochId,
    epoch_info: &EpochInfo,
    validator_account_id: &Option<AccountId>,
    head_epoch_height: &EpochHeight,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> anyhow::Result<()> {
    if epoch_info.epoch_height() >= *head_epoch_height {
        println!("Epoch information for this epoch is not yet available, skipping.");
        return Ok(());
    }
    println!("Epoch Height: {}", epoch_info.epoch_height());
    println!("Protocol Version: {}", epoch_info.protocol_version());
    let block_height_range = get_block_height_range(epoch_id, chain_store, epoch_manager)?;
    println!("Epoch Height Range: [{}..{})", block_height_range.start, block_height_range.end);
    if let Some(account_id) = validator_account_id.clone() {
        display_validator_info(epoch_id, epoch_info, account_id, chain_store, epoch_manager)?;
    }
    Ok(())
}

fn display_validator_info(
    epoch_id: &EpochId,
    epoch_info: &EpochInfo,
    account_id: AccountId,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> anyhow::Result<()> {
    if let Some(kickout) = epoch_info.validator_kickout().get(&account_id) {
        println!("Validator {account_id} kickout: {kickout:#?}");
    }
    if let Some(validator_id) = epoch_info.get_validator_id(&account_id) {
        let block_height_range: Range<BlockHeight> =
            get_block_height_range(epoch_id, chain_store, epoch_manager)?;
        let bp_for_blocks: Vec<BlockHeight> = block_height_range
            .clone()
            .filter(|&block_height| epoch_info.sample_block_producer(block_height) == *validator_id)
            .collect();
        println!("Block producer for {} blocks: {bp_for_blocks:?}", bp_for_blocks.len());

        let shard_ids = epoch_manager.shard_ids(epoch_id).unwrap();
        let cp_for_chunks: Vec<(BlockHeight, ShardId)> = block_height_range
            .flat_map(|block_height| {
                shard_ids
                    .iter()
                    .map(|&shard_id| (block_height, shard_id))
                    .filter(|&(block_height, shard_id)| {
                        epoch_info.sample_chunk_producer(block_height, shard_id)
                            == Some(*validator_id)
                    })
                    .collect::<Vec<(BlockHeight, ShardId)>>()
            })
            .collect();
        println!("Chunk producer for {} chunks: {cp_for_chunks:?}", cp_for_chunks.len());
        let mut missing_chunks = vec![];
        for (block_height, shard_id) in cp_for_chunks {
            if let Ok(block_hash) = chain_store.get_block_hash_by_height(block_height) {
                let block = chain_store.get_block(&block_hash).unwrap();
                if block.chunks()[shard_id as usize].height_included() != block_height {
                    missing_chunks.push((block_height, shard_id));
                }
            } else {
                missing_chunks.push((block_height, shard_id));
            }
        }
        println!("Missing {} chunks: {missing_chunks:?}", missing_chunks.len());
    } else {
        println!("Validator {account_id} didn't validate in epoch #{}", epoch_info.epoch_height());
    }
    Ok(())
}

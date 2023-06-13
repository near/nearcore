use borsh::BorshDeserialize;
use core::ops::Range;
use itertools::Itertools;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::{EpochManagerAdapter, EpochManagerHandle};
use near_primitives::account::id::AccountId;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
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
    chain_store: &mut ChainStore,
    epoch_manager: &EpochManagerHandle,
) {
    let epoch_ids = get_epoch_ids(epoch_selection, store, chain_store, epoch_manager);

    let head_block_info =
        epoch_manager.get_block_info(&chain_store.head().unwrap().last_block_hash).unwrap();
    let head_epoch_height =
        epoch_manager.get_epoch_info(head_block_info.epoch_id()).unwrap().epoch_height();
    let mut epoch_infos: Vec<(EpochId, Arc<EpochInfo>)> = epoch_ids
        .iter()
        .map(|epoch_id| (epoch_id.clone(), epoch_manager.get_epoch_info(epoch_id).unwrap()))
        .collect();
    // Sorted output is much easier to follow.
    epoch_infos.sort_by_key(|(_, epoch_info)| epoch_info.epoch_height());

    for (epoch_id, epoch_info) in &epoch_infos {
        println!("-------------------------");
        println!("EpochId: {:?}, EpochHeight: {}", epoch_id, epoch_info.epoch_height());
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
                println!("Can't display Epoch Info: {:?}", err);
                continue;
            }
            println!("---");
            if let Err(err) =
                display_block_and_chunk_producers(epoch_id, epoch_info, chain_store, epoch_manager)
            {
                println!("Can't display Epoch Info: {:?}", err);
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
    chain_store: &mut ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<(), anyhow::Error> {
    let block_height_range: Range<BlockHeight> =
        get_block_height_range(epoch_info, chain_store, epoch_manager)?;
    let num_shards = epoch_manager.num_shards(epoch_id).unwrap();
    for block_height in block_height_range {
        let bp = epoch_info.sample_block_producer(block_height);
        let bp = epoch_info.get_validator(bp).account_id().clone();
        let cps: Vec<AccountId> = (0..num_shards)
            .map(|shard_id| {
                let cp = epoch_info.sample_chunk_producer(block_height, shard_id);
                let cp = epoch_info.get_validator(cp).account_id().clone();
                cp
            })
            .collect();
        println!(
            "Block height: {}. Block Producer: {}. Chunk Producers: {:?}",
            block_height, bp, cps
        );
    }
    Ok(())
}

// Iterate over each epoch starting from the head. Find the requested epoch and its previous epoch
// and use that to determine the block range corresponding to the epoch.
fn get_block_height_range(
    epoch_info: &EpochInfo,
    chain_store: &ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<Range<BlockHeight>, anyhow::Error> {
    let head = chain_store.head()?;
    let mut cur_block_info = epoch_manager.get_block_info(&head.last_block_hash)?;
    loop {
        let cur_epoch_info = epoch_manager.get_epoch_info(cur_block_info.epoch_id())?;
        let cur_epoch_height = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            epoch_manager.get_block_info(cur_block_info.epoch_first_block())?;
        let prev_epoch_last_block_info =
            epoch_manager.get_block_info(epoch_first_block_info.prev_hash())?;
        let cur_epoch_start_height = epoch_manager.get_epoch_start_height(cur_block_info.hash())?;
        let cur_epoch_id = cur_block_info.epoch_id();
        let next_epoch_start_height =
            cur_epoch_start_height + epoch_manager.get_epoch_config(cur_epoch_id)?.epoch_length;
        if cur_epoch_height == epoch_info.epoch_height() {
            return Ok(cur_epoch_start_height..next_epoch_start_height);
        }
        cur_block_info = prev_epoch_last_block_info;
    }
}

// Converts a bunch of optional filtering options into a vector of EpochIds.
fn get_epoch_ids(
    epoch_selection: EpochSelection,
    store: Store,
    chain_store: &mut ChainStore,
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
        println!("{:?}: {:?}", account_id, kickout_reason);
    }
}

fn display_epoch_info(
    epoch_id: &EpochId,
    epoch_info: &EpochInfo,
    validator_account_id: &Option<AccountId>,
    head_epoch_height: &EpochHeight,
    chain_store: &mut ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<(), anyhow::Error> {
    if epoch_info.epoch_height() >= *head_epoch_height {
        println!("Epoch information for this epoch is not yet available, skipping.");
        return Ok(());
    }
    if let Some(account_id) = validator_account_id.clone() {
        display_validator_info(epoch_id, epoch_info, account_id, chain_store, epoch_manager)?;
    }
    Ok(())
}

fn display_validator_info(
    epoch_id: &EpochId,
    epoch_info: &EpochInfo,
    account_id: AccountId,
    chain_store: &mut ChainStore,
    epoch_manager: &EpochManagerHandle,
) -> Result<(), anyhow::Error> {
    if let Some(kickout) = epoch_info.validator_kickout().get(&account_id) {
        println!("Validator {} kickout: {:#?}", account_id, kickout);
    }
    if let Some(validator_id) = epoch_info.get_validator_id(&account_id) {
        let block_height_range: Range<BlockHeight> =
            get_block_height_range(epoch_info, chain_store, epoch_manager)?;
        let bp_for_blocks: Vec<BlockHeight> = block_height_range
            .clone()
            .filter(|&block_height| epoch_info.sample_block_producer(block_height) == *validator_id)
            .collect();
        println!("Block producer for {} blocks: {:?}", bp_for_blocks.len(), bp_for_blocks);

        let shard_ids = 0..epoch_manager.num_shards(epoch_id).unwrap();
        let cp_for_chunks: Vec<(BlockHeight, ShardId)> = block_height_range
            .flat_map(|block_height| {
                shard_ids
                    .clone()
                    .map(|shard_id| (block_height, shard_id))
                    .filter(|&(block_height, shard_id)| {
                        epoch_info.sample_chunk_producer(block_height, shard_id) == *validator_id
                    })
                    .collect::<Vec<(BlockHeight, ShardId)>>()
            })
            .collect();
        println!("Chunk producer for {} chunks: {:?}", cp_for_chunks.len(), cp_for_chunks);
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
        println!("Missing {} chunks: {:?}", missing_chunks.len(), missing_chunks);
    } else {
        println!(
            "Validator {} didn't validate in epoch #{}",
            account_id,
            epoch_info.epoch_height()
        );
    }
    Ok(())
}

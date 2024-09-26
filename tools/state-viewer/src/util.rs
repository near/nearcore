use std::{path::Path, sync::Arc};

use near_chain::{
    types::{ApplyChunkResult, Tip},
    Block, BlockHeader, ChainStore, ChainStoreAccess,
};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::types::{
    chunk_extra::ChunkExtra, BlockHeight, Gas, ProtocolVersion, ShardId, StateRoot,
};
use near_store::{ShardUId, Store};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};

pub enum LoadTrieMode {
    /// Load latest state
    Latest,
    /// Load prev state at some height
    Height(BlockHeight),
    /// Load the prev state of the last final block from some height
    LastFinalFromHeight(BlockHeight),
}

pub fn load_trie(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Vec<StateRoot>, BlockHeader) {
    load_trie_stop_at_height(store, home_dir, near_config, LoadTrieMode::Latest)
}

pub fn load_trie_stop_at_height(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
    mode: LoadTrieMode,
) -> (Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Vec<StateRoot>, BlockHeader) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime =
        NightshadeRuntime::from_config(home_dir, store, near_config, epoch_manager.clone());
    let runtime = runtime.expect("could not create the transaction runtime");

    let head = chain_store.head().unwrap();
    let block = match mode {
        LoadTrieMode::LastFinalFromHeight(height) => {
            get_last_final_from_height(height, &head, &chain_store)
        }
        LoadTrieMode::Height(height) => {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            chain_store.get_block(&block_hash).unwrap()
        }
        LoadTrieMode::Latest => chain_store.get_block(&head.last_block_hash).unwrap(),
    };
    let shard_layout = epoch_manager.get_shard_layout(&block.header().epoch_id()).unwrap();
    let mut state_roots = vec![];
    for chunk in block.chunks().iter() {
        let shard_uid = ShardUId::from_shard_id_and_layout(chunk.shard_id(), &shard_layout);
        let chunk_extra = chain_store.get_chunk_extra(&head.last_block_hash, &shard_uid).unwrap();
        let state_root = *chunk_extra.state_root();
        state_roots.push(state_root);
    }

    (epoch_manager, runtime, state_roots, block.header().clone())
}

/// find the first final block whose height is at least `height`.
fn get_last_final_from_height(height: u64, head: &Tip, chain_store: &ChainStore) -> Block {
    let mut cur_height = height + 1;
    loop {
        if cur_height >= head.height {
            panic!("No final block with height >= {} exists", height);
        }
        let cur_block_hash = match chain_store.get_block_hash_by_height(cur_height) {
            Ok(hash) => hash,
            Err(_) => {
                cur_height += 1;
                continue;
            }
        };
        let last_final_block_hash =
            *chain_store.get_block_header(&cur_block_hash).unwrap().last_final_block();
        let last_final_block = chain_store.get_block(&last_final_block_hash).unwrap();
        if last_final_block.header().height() >= height {
            break last_final_block;
        } else {
            cur_height += 1;
            continue;
        }
    }
}

fn chunk_extras_equal(l: &ChunkExtra, r: &ChunkExtra) -> bool {
    // explicitly enumerate the versions in a match here first so that if a new version is
    // added, we'll get a compile error here and be reminded to update it correctly.
    //
    // edit with v3: To avoid too many explicit combinations, use wildcards for
    // versions >= 3. The compiler will still notice the missing `(v1, new_v)`
    // combinations.
    match (&l, &r) {
        (ChunkExtra::V1(l), ChunkExtra::V1(r)) => return l == r,
        (ChunkExtra::V2(l), ChunkExtra::V2(r)) => return l == r,
        (ChunkExtra::V3(l), ChunkExtra::V3(r)) => return l == r,
        (ChunkExtra::V4(l), ChunkExtra::V4(r)) => return l == r,
        (ChunkExtra::V1(_), ChunkExtra::V2(_))
        | (ChunkExtra::V2(_), ChunkExtra::V1(_))
        | (_, ChunkExtra::V3(_))
        | (ChunkExtra::V3(_), _) 
        | (ChunkExtra::V4(_), _)
        | (_, ChunkExtra::V4(_)) => {}
    };
    if l.state_root() != r.state_root() {
        return false;
    }
    if l.outcome_root() != r.outcome_root() {
        return false;
    }
    if l.gas_used() != r.gas_used() {
        return false;
    }
    if l.gas_limit() != r.gas_limit() {
        return false;
    }
    if l.balance_burnt() != r.balance_burnt() {
        return false;
    }
    if l.congestion_info() != r.congestion_info() {
        return false;
    }
    l.validator_proposals().collect::<Vec<_>>() == r.validator_proposals().collect::<Vec<_>>()
}

pub fn resulting_chunk_extra(
    result: &ApplyChunkResult,
    gas_limit: Gas,
    protocol_version: ProtocolVersion,
) -> ChunkExtra {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&result.outcomes);
    ChunkExtra::new(
        protocol_version,
        &result.new_root,
        outcome_root,
        result.validator_proposals.clone(),
        result.total_gas_burnt,
        gas_limit,
        result.total_balance_burnt,
        result.congestion_info,
        vec![]
    )
}

pub fn check_apply_block_result(
    block: &Block,
    apply_result: &ApplyChunkResult,
    epoch_manager: &EpochManagerHandle,
    chain_store: &ChainStore,
    shard_id: ShardId,
) -> anyhow::Result<()> {
    let height = block.header().height();
    let block_hash = block.header().hash();
    let protocol_version = block.header().latest_protocol_version();
    let new_chunk_extra = resulting_chunk_extra(
        apply_result,
        block.chunks()[shard_id as usize].gas_limit(),
        protocol_version,
    );
    println!(
        "apply chunk for shard {} at height {}, resulting chunk extra {:?}",
        shard_id, height, &new_chunk_extra,
    );
    let shard_uid = epoch_manager.shard_id_to_uid(shard_id, block.header().epoch_id()).unwrap();
    if block.chunks()[shard_id as usize].height_included() == height {
        if let Ok(old_chunk_extra) = chain_store.get_chunk_extra(block_hash, &shard_uid) {
            if chunk_extras_equal(&new_chunk_extra, old_chunk_extra.as_ref()) {
                tracing::debug!("new chunk extra matches old chunk extra");
                Ok(())
            } else {
                Err(anyhow::anyhow!(
                    "mismatch in resulting chunk extra.\nold: {:?}\nnew: {:?}",
                    &old_chunk_extra,
                    &new_chunk_extra
                ))
            }
        } else {
            Err(anyhow::anyhow!("No existing chunk extra available"))
        }
    } else {
        tracing::warn!("No existing chunk extra available");
        Ok(())
    }
}

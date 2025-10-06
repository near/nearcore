use std::collections::BTreeMap;

use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::block::BlockHeader;
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use near_primitives::types::BlockExecutionResults;

use crate::types::ApplyChunkBlockContext;

pub fn build_spice_apply_chunk_block_context(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ApplyChunkBlockContext, Error> {
    // TODO(spice): gas price should be based on execution results and not part of the
    // block since it's calculated based on gas usage during execution.
    let gas_price = block_header.next_gas_price();
    let congestion_info =
        build_block_congestion_info(block_header, prev_block_execution_results, epoch_manager)?;
    let bandwidth_requests =
        build_block_bandwidth_requests(block_header, prev_block_execution_results, epoch_manager)?;
    Ok(ApplyChunkBlockContext::from_header(
        block_header,
        gas_price,
        congestion_info,
        bandwidth_requests,
    ))
}

fn build_block_bandwidth_requests(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<BlockBandwidthRequests, Error> {
    let shard_layout = epoch_manager.get_shard_layout(&block_header.epoch_id())?;
    let prev_block_hash = block_header.prev_hash();
    let mut result = BTreeMap::new();
    // TODO(spice-resharding): double-check if shards for block or prev_block should be
    // used as keys.
    for shard_id in shard_layout.shard_ids() {
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        let prev_shard_execution_result = prev_block_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("block execution result should contain execution results for all shards");
        if let Some(bandwidth_requests) =
            prev_shard_execution_result.chunk_extra.bandwidth_requests()
        {
            result.insert(shard_id, bandwidth_requests.clone());
        }
    }
    Ok(BlockBandwidthRequests { shards_bandwidth_requests: result })
}

fn build_block_congestion_info(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<BlockCongestionInfo, Error> {
    let shard_layout = epoch_manager.get_shard_layout(&block_header.epoch_id())?;
    let prev_block_hash = block_header.prev_hash();
    let mut result = BTreeMap::new();
    // TODO(spice-resharding): double-check if shards for block or prev_block should be
    // used as keys.
    for shard_id in shard_layout.shard_ids() {
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        let prev_shard_execution_result = prev_block_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("block execution result should contain execution results for all shards");
        let congestion_info = prev_shard_execution_result.chunk_extra.congestion_info();
        let missed_chunks_count = 0;
        result.insert(shard_id, ExtendedCongestionInfo::new(congestion_info, missed_chunks_count));
    }
    Ok(BlockCongestionInfo::new(result))
}

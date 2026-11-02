//! Block headers rewritten for tests of what the chain rejects.

use near_primitives::block_header::{BlockHeader, BlockHeaderInnerRestV7, BlockHeaderV7};
use near_primitives::types::EpochId;

/// A `BlockHeaderV7` copy of a V6 header, with the fields V7 adds at their defaults and
/// the signature and hash of the V6 header, which a caller re-signs.
pub fn convert_v6_header_to_v7(header: &BlockHeader) -> BlockHeader {
    let BlockHeader::BlockHeaderV6(header_v6) = header else {
        panic!("expected BlockHeaderV6, got {header:?}");
    };
    let rest = header_v6.inner_rest.clone();
    let inner_rest = BlockHeaderInnerRestV7 {
        block_body_hash: rest.block_body_hash,
        prev_chunk_outgoing_receipts_root: rest.prev_chunk_outgoing_receipts_root,
        chunk_headers_root: rest.chunk_headers_root,
        chunk_tx_root: rest.chunk_tx_root,
        random_value: rest.random_value,
        prev_validator_proposals: rest.prev_validator_proposals,
        chunk_mask: rest.chunk_mask,
        next_gas_price: rest.next_gas_price,
        total_supply: rest.total_supply,
        last_final_block: rest.last_final_block,
        last_ds_final_block: rest.last_ds_final_block,
        block_ordinal: rest.block_ordinal,
        prev_height: rest.prev_height,
        epoch_sync_data_hash: rest.epoch_sync_data_hash,
        approvals: rest.approvals,
        latest_protocol_version: rest.latest_protocol_version,
        chunk_endorsements: rest.chunk_endorsements,
        shard_split: rest.shard_split,
        prev_last_certified_block_epoch_id: EpochId::default(),
        spice_chunk_endorsement_stats: vec![],
    };
    BlockHeader::BlockHeaderV7(BlockHeaderV7 {
        prev_hash: header_v6.prev_hash,
        inner_lite: header_v6.inner_lite.clone(),
        inner_rest,
        signature: header_v6.signature.clone(),
        hash: header_v6.hash,
    })
}

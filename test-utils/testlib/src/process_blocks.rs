use near_chain::{Block, BlockHeader};
use near_primitives::block::Chunks;
use near_primitives::{
    stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap,
    test_utils::create_test_signer,
};

pub fn set_no_chunk_in_block(block: &mut Block, prev_block: &Block) {
    let chunk_headers = vec![prev_block.chunks()[0].clone()];
    let mut balance_burnt = 0;
    for chunk in block.chunks().iter_new() {
        balance_burnt += chunk.prev_balance_burnt();
    }
    block.set_chunks(chunk_headers.clone());
    block.set_chunk_endorsements(vec![vec![]; chunk_headers.len()]);
    let block_body_hash = block.compute_block_body_hash();
    let chunks = Chunks::from_chunk_headers(&chunk_headers, block.header().height());
    match block.mut_header() {
        BlockHeader::BlockHeaderV1(header) => {
            header.inner_rest.chunk_headers_root = chunks.compute_chunk_headers_root().0;
            header.inner_rest.chunk_tx_root = chunks.compute_chunk_tx_root();
            header.inner_rest.prev_chunk_outgoing_receipts_root =
                chunks.compute_chunk_prev_outgoing_receipts_root();
            header.inner_lite.prev_state_root = chunks.compute_state_root();
            header.inner_lite.prev_outcome_root = chunks.compute_outcome_root();
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.next_gas_price = prev_block.header().next_gas_price();
            header.inner_rest.total_supply += balance_burnt;
        }
        BlockHeader::BlockHeaderV2(header) => {
            header.inner_rest.chunk_headers_root = chunks.compute_chunk_headers_root().0;
            header.inner_rest.chunk_tx_root = chunks.compute_chunk_tx_root();
            header.inner_rest.prev_chunk_outgoing_receipts_root =
                chunks.compute_chunk_prev_outgoing_receipts_root();
            header.inner_lite.prev_state_root = chunks.compute_state_root();
            header.inner_lite.prev_outcome_root = chunks.compute_outcome_root();
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.next_gas_price = prev_block.header().next_gas_price();
            header.inner_rest.total_supply += balance_burnt;
        }
        BlockHeader::BlockHeaderV3(header) => {
            header.inner_rest.chunk_headers_root = chunks.compute_chunk_headers_root().0;
            header.inner_rest.chunk_tx_root = chunks.compute_chunk_tx_root();
            header.inner_rest.prev_chunk_outgoing_receipts_root =
                chunks.compute_chunk_prev_outgoing_receipts_root();
            header.inner_lite.prev_state_root = chunks.compute_state_root();
            header.inner_lite.prev_outcome_root = chunks.compute_outcome_root();
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.next_gas_price = prev_block.header().next_gas_price();
            header.inner_rest.total_supply += balance_burnt;
        }
        BlockHeader::BlockHeaderV4(header) => {
            header.inner_rest.chunk_headers_root = chunks.compute_chunk_headers_root().0;
            header.inner_rest.chunk_tx_root = chunks.compute_chunk_tx_root();
            header.inner_rest.prev_chunk_outgoing_receipts_root =
                chunks.compute_chunk_prev_outgoing_receipts_root();
            header.inner_lite.prev_state_root = chunks.compute_state_root();
            header.inner_lite.prev_outcome_root = chunks.compute_outcome_root();
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.next_gas_price = prev_block.header().next_gas_price();
            header.inner_rest.total_supply += balance_burnt;
            header.inner_rest.block_body_hash = block_body_hash.unwrap();
        }
        // Same as BlockHeader::BlockHeaderV4 branch but with inner_rest.chunk_endorsements field set.
        BlockHeader::BlockHeaderV5(header) => {
            header.inner_rest.chunk_headers_root = chunks.compute_chunk_headers_root().0;
            header.inner_rest.chunk_tx_root = chunks.compute_chunk_tx_root();
            header.inner_rest.prev_chunk_outgoing_receipts_root =
                chunks.compute_chunk_prev_outgoing_receipts_root();
            header.inner_lite.prev_state_root = chunks.compute_state_root();
            header.inner_lite.prev_outcome_root = chunks.compute_outcome_root();
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.next_gas_price = prev_block.header().next_gas_price();
            header.inner_rest.total_supply += balance_burnt;
            header.inner_rest.block_body_hash = block_body_hash.unwrap();
            header.inner_rest.chunk_endorsements =
                ChunkEndorsementsBitmap::new(chunk_headers.len());
        }
    }
    let validator_signer = create_test_signer("test0");
    block.mut_header().resign(&validator_signer);
}

use near_chain::{Block, BlockHeader};
use near_crypto::KeyType;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use std::sync::Arc;

pub fn set_no_chunk_in_block(block: &mut Block, prev_block: &Block) {
    let chunk_headers = vec![prev_block.chunks()[0].clone()];
    block.set_chunks(chunk_headers.clone());
    match block.mut_header() {
        BlockHeader::BlockHeaderV1(header) => {
            let header = Arc::make_mut(header);
            header.inner_rest.chunk_headers_root =
                Block::compute_chunk_headers_root(&chunk_headers).0;
            header.inner_rest.chunk_tx_root = Block::compute_chunk_tx_root(&chunk_headers);
            header.inner_rest.chunk_receipts_root =
                Block::compute_chunk_receipts_root(&chunk_headers);
            header.inner_lite.prev_state_root = Block::compute_state_root(&chunk_headers);
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.gas_price = prev_block.header().gas_price();
        }
        BlockHeader::BlockHeaderV2(header) => {
            let header = Arc::make_mut(header);
            header.inner_rest.chunk_headers_root =
                Block::compute_chunk_headers_root(&chunk_headers).0;
            header.inner_rest.chunk_tx_root = Block::compute_chunk_tx_root(&chunk_headers);
            header.inner_rest.chunk_receipts_root =
                Block::compute_chunk_receipts_root(&chunk_headers);
            header.inner_lite.prev_state_root = Block::compute_state_root(&chunk_headers);
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.gas_price = prev_block.header().gas_price();
        }
        BlockHeader::BlockHeaderV3(header) => {
            let header = Arc::make_mut(header);
            header.inner_rest.chunk_headers_root =
                Block::compute_chunk_headers_root(&chunk_headers).0;
            header.inner_rest.chunk_tx_root = Block::compute_chunk_tx_root(&chunk_headers);
            header.inner_rest.chunk_receipts_root =
                Block::compute_chunk_receipts_root(&chunk_headers);
            header.inner_lite.prev_state_root = Block::compute_state_root(&chunk_headers);
            header.inner_rest.chunk_mask = vec![false];
            header.inner_rest.gas_price = prev_block.header().gas_price();
        }
    }
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    block.mut_header().resign(&validator_signer);
}

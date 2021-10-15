use near_chain::Block;
use near_crypto::KeyType;
use near_primitives::validator_signer::InMemoryValidatorSigner;

pub fn set_no_chunk_in_block(block: &mut Block, prev_block: &Block) {
    let chunk_headers = vec![prev_block.chunks()[0].clone()];
    block.set_chunks(chunk_headers.clone());
    block.mut_header().get_mut().inner_rest.chunk_headers_root =
        Block::compute_chunk_headers_root(&chunk_headers).0;
    block.mut_header().get_mut().inner_rest.chunk_tx_root =
        Block::compute_chunk_tx_root(&chunk_headers);
    block.mut_header().get_mut().inner_rest.chunk_receipts_root =
        Block::compute_chunk_receipts_root(&chunk_headers);
    block.mut_header().get_mut().inner_lite.prev_state_root =
        Block::compute_state_root(&chunk_headers);
    block.mut_header().get_mut().inner_rest.chunk_mask = vec![false];
    block.mut_header().get_mut().inner_rest.gas_price = prev_block.header().gas_price();
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    block.mut_header().resign(&validator_signer);
}

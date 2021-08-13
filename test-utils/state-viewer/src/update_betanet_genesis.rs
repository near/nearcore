use near_chain_configs::Genesis;
use near_primitives::block::BlockHeader;

pub fn update_betanet_genesis(last_block_header: BlockHeader, genesis: &Genesis) -> Genesis {
    let mut genesis = genesis.clone();
    println!(
        "Updating betanet genesis from state data of #{} / {}",
        last_block_header.height(),
        last_block_header.hash()
    );
    // Ensure that the height of the new genesis is strictly greater than any block in the current
    // epoch. To be extra safe we'd need to add two epochs, but one epoch is good enough.
    let genesis_height = last_block_header.height() + genesis.config.epoch_length;
    genesis.config.genesis_height = genesis_height;
    // Record the protocol version of the latest block. Otherwise, the state
    // dump ignores the fact that the nodes can be running a newer protocol
    // version than the protocol version of the genesis.
    genesis.config.protocol_version = last_block_header.latest_protocol_version();
    genesis
}

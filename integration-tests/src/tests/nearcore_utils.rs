use actix::Addr;
use near_async::time::Clock;
use near_chain::Block;
use near_chain_configs::test_utils::TESTING_INIT_STAKE;
use near_chain_configs::Genesis;
use near_client::{BlockResponse, ClientActor};
use near_network::tcp;
use near_network::test_utils::convert_boot_nodes;
use near_network::types::PeerInfo;
use near_o11y::WithSpanContextExt;
use near_primitives::block::Approval;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::num_rational::{Ratio, Rational32};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeightDelta, EpochId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use nearcore::{load_test_config, NearConfig};

use super::genesis_helpers::genesis_block;

// This assumes that there is no height skipped. Otherwise epoch hash calculation will be wrong.
pub fn add_blocks(
    clock: Clock,
    mut blocks: Vec<Block>,
    client: Addr<ClientActor>,
    num: usize,
    epoch_length: BlockHeightDelta,
    signer: &ValidatorSigner,
) -> Vec<Block> {
    let mut prev = &blocks[blocks.len() - 1];
    let mut block_merkle_tree = PartialMerkleTree::default();
    for block in blocks.iter() {
        block_merkle_tree.insert(*block.hash());
    }
    for _ in 0..num {
        let prev_height = prev.header().height();
        let prev_epoch_height = prev_height / epoch_length;
        let prev_epoch_last_block_height = prev_epoch_height * epoch_length;

        let height = prev_height + 1;
        let epoch_id = if height <= epoch_length {
            EpochId::default()
        } else {
            let prev_prev_epoch_height = prev_epoch_height - 1;
            let prev_prev_epoch_last_block_height = prev_prev_epoch_height * epoch_length;
            EpochId(*blocks[prev_prev_epoch_last_block_height as usize].hash())
        };

        let next_epoch_id = EpochId(*blocks[prev_epoch_last_block_height as usize].hash());

        let next_bp_hash = CryptoHash::hash_borsh_iter([ValidatorStake::new(
            "other".parse().unwrap(),
            signer.public_key(),
            TESTING_INIT_STAKE,
        )]);
        let block = Block::produce(
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            prev.header(),
            prev.header().height() + 1,
            prev.header().block_ordinal() + 1,
            blocks[0].chunks().iter().cloned().collect(),
            vec![],
            epoch_id,
            next_epoch_id,
            None,
            vec![Some(Box::new(
                Approval::new(
                    *prev.hash(),
                    prev.header().height(),
                    prev.header().height() + 1,
                    signer,
                )
                .signature,
            ))],
            Ratio::from_integer(0),
            0,
            1000,
            Some(0),
            vec![],
            vec![],
            signer,
            next_bp_hash,
            block_merkle_tree.root(),
            clock.clone(),
            None,
        );
        block_merkle_tree.insert(*block.hash());
        let _ = client.do_send(
            BlockResponse {
                block: block.clone(),
                peer_id: PeerInfo::random().id,
                was_requested: false,
            }
            .with_span_context(),
        );
        blocks.push(block);
        prev = &blocks[blocks.len() - 1];
    }
    blocks
}

pub fn setup_configs_with_epoch_length(
    epoch_length: u64,
) -> (Genesis, Block, NearConfig, NearConfig) {
    let mut genesis = Genesis::test(vec!["other".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    // Avoid InvalidGasPrice error. Blocks must contain accurate `total_supply` value.
    // Accounting for the inflation in tests is hard.
    // Disabling inflation in tests is much simpler.
    genesis.config.max_inflation_rate = Rational32::from_integer(0);
    let genesis_block = genesis_block(&genesis);

    let (port1, port2) =
        (tcp::ListenerAddr::reserve_for_test(), tcp::ListenerAddr::reserve_for_test());
    let mut near1 = load_test_config("test1", port1, genesis.clone());
    near1.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![("test2", *port2)]);
    near1.client_config.min_num_peers = 1;
    near1.client_config.epoch_sync_enabled = false;
    near1.client_config.state_sync_enabled = true;

    let mut near2 = load_test_config("test2", port2, genesis.clone());
    near2.network_config.peer_store.boot_nodes = convert_boot_nodes(vec![("test1", *port1)]);
    near2.client_config.min_num_peers = 1;
    near2.client_config.epoch_sync_enabled = false;
    near2.client_config.state_sync_enabled = true;

    (genesis, genesis_block, near1, near2)
}

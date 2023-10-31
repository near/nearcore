use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::{Block, ChainGenesis, Error, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderInner};
use near_primitives::transaction::SignedTransaction;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives_core::types::BlockHeight;
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

fn create_tx_load(height: BlockHeight, last_block: &Block) -> Vec<SignedTransaction> {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        height + 10000,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        10,
        last_block.hash().clone(),
    );
    vec![tx]
}

/// Producing block with all `shard_id`s equal to 100.
/// Expecting it too fail processing.
#[test]
fn change_shard_id_to_invalid() {
    init_test_logger();
    let epoch_length = 5000000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let mut last_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    // Produce normal block

    let txs = create_tx_load(1, &last_block);
    for tx in txs {
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, block.clone(), Provenance::PRODUCED);
    last_block = block;

    // Produce block for coruption

    let txs = create_tx_load(2, &last_block);
    for tx in txs {
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let mut block = env.clients[0].produce_block(2).unwrap().unwrap();

    // 1. Corrupt chunks
    let mut new_chunks = vec![];
    for chunk in block.chunks().iter() {
        let mut new_chunk = chunk.clone();
        match &mut new_chunk {
            ShardChunkHeader::V1(new_chunk) => new_chunk.inner.shard_id = 100,
            ShardChunkHeader::V2(new_chunk) => new_chunk.inner.shard_id = 100,
            ShardChunkHeader::V3(new_chunk) => match &mut new_chunk.inner {
                ShardChunkHeaderInner::V1(inner) => inner.shard_id = 100,
                ShardChunkHeaderInner::V2(inner) => inner.shard_id = 100,
                ShardChunkHeaderInner::V3(inner) => inner.shard_id = 100,
            },
        };
        new_chunks.push(new_chunk);
    }
    block.set_chunks(new_chunks);

    // 2. Rehash and resign
    block.mut_header().get_mut().inner_rest.block_body_hash =
        block.compute_block_body_hash().unwrap();
    block.mut_header().resign(&InMemoryValidatorSigner::from_seed(
        "test0".parse().unwrap(),
        KeyType::ED25519,
        "test0",
    ));

    // Try to process corrupt block and expect code to notice invalid shard_id
    let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    match res {
        Err(Error::InvalidShardId(100)) => {
            tracing::debug!("process failed successfully");
        }
        Err(e) => {
            assert!(false, "Failed with error {:?} instead of Error::InvalidShardId(100).", e);
        }
        Ok(_) => {
            assert!(false, "Block processing of a corrupt block succeeded.");
        }
    }
}

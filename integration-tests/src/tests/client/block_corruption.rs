use anyhow::Context;
use borsh::BorshDeserialize;
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
        *last_block.hash(),
    );
    vec![tx]
}

/// Producing block with all `shard_id`s equal to 100.
/// Expecting it to fail processing.
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

    // Produce block for corruption

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

/// We ensure there are SOME change between these blocks.
/// We check that they are NOT in fields that can be mutated and still pass validation by design.
fn is_breaking_block_change(original: &Block, corrupt: &Block) -> bool {
    original.header().latest_protocol_version() == corrupt.header().latest_protocol_version()
        && original.header().block_ordinal() == corrupt.header().block_ordinal()
        && original.header().timestamp() == corrupt.header().timestamp()
        && original != corrupt
}

fn check_process_flipped_block_fails_on_bit(
    corrupted_bit_idx: usize,
) -> Result<anyhow::Error, anyhow::Error> {
    let epoch_length = 5000000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::new(&genesis))
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let mut last_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let mid_height = 3;
    for h in 1..=mid_height {
        let txs = create_tx_load(h, &last_block);
        for tx in txs {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_block = block;
    }

    let block_len = borsh::to_vec(&last_block).unwrap().len();
    if corrupted_bit_idx >= block_len * 8 {
        return Ok(anyhow::anyhow!("End"));
    }

    let h = mid_height + 1;

    let txs = create_tx_load(h, &last_block);
    for tx in txs {
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let correct_block = env.clients[0]
        .produce_block(h)
        .unwrap()
        .with_context(|| format!("Failed to produce block at height {:?}", h))
        .unwrap();

    let mut block_vec = borsh::to_vec(&correct_block).unwrap();

    // Technically we don't have to do first step, because of Ok("End") check.
    // But we may change that part in the future for some reason, and I want this part to be reliable.

    // get BIT index modulo BIT length of block
    let mod_idx = corrupted_bit_idx % (block_vec.len() * 8);
    // get index of BYTE that we are changing
    let byte_idx = mod_idx / 8;
    // get index of BIT in that BYTE that we are flipping
    let bit_idx = mod_idx % 8;
    // flip that BIT
    block_vec[byte_idx] ^= 1 << bit_idx;

    if let Ok(mut corrupt_block) = Block::try_from_slice(block_vec.as_slice()) {
        corrupt_block.mut_header().get_mut().inner_rest.block_body_hash =
            corrupt_block.compute_block_body_hash().unwrap();
        corrupt_block.mut_header().resign(&InMemoryValidatorSigner::from_seed(
            "test0".parse().unwrap(),
            KeyType::ED25519,
            "test0",
        ));

        if is_breaking_block_change(&correct_block, &corrupt_block) {
            match env.clients[0].process_block_test(corrupt_block.clone().into(), Provenance::NONE)
            {
                Ok(_) => {
                    return Err(anyhow::anyhow!(
                        "Was able to process default block with {} bit switched.",
                        corrupted_bit_idx
                    ));
                }
                Err(e) => {
                    if let Err(e) = env.clients[0]
                        .process_block_test(correct_block.into(), Provenance::NONE)
                    {
                        return Err(anyhow::anyhow!("Was unable to process default block after attempting to process default block with {} bit switched. {}",
                        corrupted_bit_idx, e)
                        );
                    }
                    return Ok(e.into());
                }
            }
        } else {
            return Ok(anyhow::anyhow!("Not a breaking change"));
        }
    }
    return Ok(anyhow::anyhow!("Corrupt block didn't parse"));
}

#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn check_process_flipped_block_fails() {
    init_test_logger();
    let mut corrupted_bit_idx = 0;
    let mut errs = vec![];
    let mut oks = vec![];
    loop {
        let res = check_process_flipped_block_fails_on_bit(corrupted_bit_idx);
        if let Ok(res) = &res {
            if res.to_string() == "End" {
                break;
            }
        }
        match res {
            Err(e) => errs.push(e),
            Ok(o) => oks.push(o),
        }
        corrupted_bit_idx += 1;
    }
    tracing::info!("All of the Errors:");
    for err in &errs {
        tracing::info!("{:?}", err);
    }
    tracing::info!("{}", ["-"; 100].concat());
    tracing::info!("All of the Oks:");
    for ok in oks {
        tracing::info!("{:?}", ok);
    }
    assert!(errs.is_empty());
}

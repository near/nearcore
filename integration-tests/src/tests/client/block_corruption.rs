use anyhow::Context;
use borsh::BorshDeserialize;
use near_chain::{Block, Error, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderInner};
use near_primitives::transaction::SignedTransaction;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives_core::types::BlockHeight;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

const NOT_BREAKING_CHANGE_MSG: &str = "Not a breaking change";
const BLOCK_NOT_PARSED_MSG: &str = "Corrupt block didn't parse";

fn create_tx_load(height: BlockHeight, last_block: &Block) -> Vec<SignedTransaction> {
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        height + 10000,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer.into(),
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
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

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
    let body_hash = block.compute_block_body_hash().unwrap();
    block.mut_header().set_block_body_hash(body_hash);
    block.mut_header().resign(
        &InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0")
            .into(),
    );

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

///  If the corrupt block can be parsed, we are trying to process it and expecting it to fail.
///  If it successfully fails, we are processing the correct block and expecting it to be ok.
///
/// Returns `Ok(processing_error)` if corrupt block was parsed and its processing failed.
/// Returns `Ok(reason)` if corrupt block wasn't parsed or had changes that are not breaking by design.
/// Returns `Err(reason)` if corrupt block was processed or correct block wasn't processed afterwards.
fn check_corrupt_block(
    mut env: TestEnv,
    corrupt_block_vec: Vec<u8>,
    correct_block: Block,
    corrupted_bit_idx: usize,
) -> Result<anyhow::Error, anyhow::Error> {
    if let Ok(mut corrupt_block) = Block::try_from_slice(corrupt_block_vec.as_slice()) {
        let body_hash = corrupt_block.compute_block_body_hash().unwrap();
        corrupt_block.mut_header().set_block_body_hash(body_hash);
        corrupt_block.mut_header().resign(
            &InMemoryValidatorSigner::from_seed(
                "test0".parse().unwrap(),
                KeyType::ED25519,
                "test0",
            )
            .into(),
        );

        if !is_breaking_block_change(&correct_block, &corrupt_block) {
            return Ok(anyhow::anyhow!(NOT_BREAKING_CHANGE_MSG));
        }

        match env.clients[0].process_block_test(corrupt_block.into(), Provenance::NONE) {
            Ok(_) => Err(anyhow::anyhow!(
                "Was able to process default block with {} bit switched.",
                corrupted_bit_idx
            )),
            Err(e) => {
                if let Err(e) =
                    env.clients[0].process_block_test(correct_block.into(), Provenance::NONE)
                {
                    return Err(anyhow::anyhow!("Was unable to process default block after attempting to process default block with {} bit switched. {}",
                    corrupted_bit_idx, e)
                    );
                }
                Ok(e.into())
            }
        }
    } else {
        return Ok(anyhow::anyhow!(BLOCK_NOT_PARSED_MSG));
    }
}

/// Each block contains one 'send' transaction.
/// First three blocks are produced without changes.
/// For the fourth block we are calculating two versions – correct and corrupt.
/// Corrupt block is produced by
/// - serializing correct block
/// - flipping one bit
/// - deserializing resulting string
/// - resigning the resulting block
///  If the corrupt block can be parsed, we are trying to process it and expecting it to fail.
///  If it successfully fails, we are processing the correct block and expecting it to be ok.
///
/// Returns `Ok(processing_error)` if corrupt block was parsed and its processing failed.
/// Returns `Ok(reason)` if corrupt block wasn't parsed or had changes that are not breaking by design.
/// Returns `Err(reason)` if corrupt block was processed or correct block wasn't processed afterwards.
fn check_process_flipped_block_fails_on_bit(
    corrupted_bit_idx: usize,
) -> Result<anyhow::Error, anyhow::Error> {
    let epoch_length = 5000000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

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

    if corrupted_bit_idx >= block_vec.len() * 8 {
        return Ok(anyhow::anyhow!("End"));
    }

    // get index of BYTE that we are changing
    let byte_idx = corrupted_bit_idx / 8;
    // get index of BIT in that BYTE that we are flipping
    let bit_idx = corrupted_bit_idx % 8;
    // flip that BIT
    block_vec[byte_idx] ^= 1 << bit_idx;

    check_corrupt_block(env, block_vec, correct_block, corrupted_bit_idx)
}

/// Produce a block. Flip a bit in it.
/// Check that corrupt block cannot be processed. Check that correct block can be processed after.
/// Do it for every bit in the block independently.
///
/// Checks are performed for all bits even if some of them fail.
/// Results are accumulated in `errs` and `oks` vectors, that are printed at the end of the test.
/// Test fails if
/// - `errs` is not empty which means that for some bit either corrupt block was processed, or correct block was not.
/// - `oks` only contain trivial errors which means that nothing was checked actually.
///
/// `oks` are printed to check the sanity of the test.
/// This vector should include various validation errors that correspond to data changed with a bit flip.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn check_process_flipped_block_fails() {
    init_test_logger();
    let mut corrupted_bit_idx = 0;
    // List of reasons `check_process_flipped_block_fails_on_bit` returned `Err`.
    // Should be empty.
    let mut errs = vec![];
    // List of reasons `check_process_flipped_block_fails_on_bit` returned `Ok`.
    // Should contain various validation errors.
    let mut oks = vec![];
    loop {
        let res = check_process_flipped_block_fails_on_bit(corrupted_bit_idx);
        if let Ok(res) = &res {
            if res.to_string() == "End" {
                // `corrupted_bit_idx` is out of bounds for correct block length. Should stop iteration.
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
    for ok in &oks {
        tracing::info!("{:?}", ok);
    }
    assert!(errs.is_empty());
    assert!(
        oks.iter()
            .filter(|e| e.to_string() != NOT_BREAKING_CHANGE_MSG
                && e.to_string() != BLOCK_NOT_PARSED_MSG)
            .count()
            > 0
    );
}

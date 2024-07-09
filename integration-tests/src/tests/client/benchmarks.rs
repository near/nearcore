//! High-level benchmarks for client.
//!
//! We are not using criterion or cargo-bench infrastructure because the things
//! we want to test here are pretty heavy and its enough to run them once and
//! note the wall-clock time.

use near_chain_configs::Genesis;
use near_client::test_utils::{create_chunk_on_height, TestEnv};
use near_client::{ProcessTxResponse, ProduceChunkResult};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::checked_feature;
use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use nearcore::test_utils::TestEnvNightshadeSetupExt;

/// How long does it take to produce a large chunk?
///
/// Chunk production work does **not** include any transaction execution: it is
/// just about packing receipts and a bunch of txes from a pool together and
/// computing merkle proof and erasure-codes for this. This is still pretty
/// computationally intensive.
///
/// In ths benchmark, we construct a large with a bunch of deploy_code txes
#[test]
fn benchmark_large_chunk_production_time() {
    let mb = 1024usize.pow(2);

    let n_txes = 20;
    let tx_size = if checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        mb / 2
    } else {
        3 * mb
    };

    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let account_id = env.get_client_id(0);
    let signer =
        InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref()).into();
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    for i in 0..n_txes {
        let tx = SignedTransaction::from_actions(
            i + 1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![92; tx_size] })],
            last_block_hash,
            0,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let t = std::time::Instant::now();
    let ProduceChunkResult { chunk, .. } = create_chunk_on_height(&mut env.clients[0], 0);
    let time = t.elapsed();

    let decoded_chunk = chunk.decode_chunk(0).unwrap();

    let size = borsh::object_length(&chunk).unwrap();
    eprintln!("chunk size: {}kb", size / 1024);
    eprintln!("time to produce: {:0.2?}", time);

    // Check that we limit the size of the chunk and not include all `n_txes`
    // transactions in the chunk.
    if ProtocolFeature::StatelessValidationV0.enabled(PROTOCOL_VERSION) {
        assert!(6 * mb < size && size < 8 * mb, "{size}");
        assert_eq!(decoded_chunk.transactions().len(), 7); // 4MiB limit allows for 7 x 0.5MiB transactions
    } else {
        assert!(30 * mb < size && size < 40 * mb, "{size}");
    }
}

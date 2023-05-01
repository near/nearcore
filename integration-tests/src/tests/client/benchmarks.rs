//! High-level benchmarks for client.
//!
//! We are not using criterion or cargo-bench infrastructure because the things
//! we want to test here are pretty heavy and its enough to run them once and
//! note the wall-clock time.

use crate::tests::client::utils::TestEnvNightshadeSetupExt;
use borsh::BorshSerialize;
use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::{create_chunk_on_height, TestEnv};
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::transaction::{Action, DeployContractAction, SignedTransaction};
use nearcore::config::GenesisExt;

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
    let tx_size = 3 * mb;

    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let chain_genesis = ChainGenesis::new(&genesis);
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let account_id = env.get_client_id(0).clone();
    let signer =
        InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    for i in 0..n_txes {
        let tx = SignedTransaction::from_actions(
            i + 1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![92; tx_size] })],
            last_block_hash,
        );
        assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    let t = std::time::Instant::now();
    let (chunk, _, _) = create_chunk_on_height(&mut env.clients[0], 0);
    let time = t.elapsed();

    let size = chunk.try_to_vec().unwrap().len();
    eprintln!("chunk size: {}kb", size / 1024);
    eprintln!("time to produce: {:0.2?}", time);

    // Check that we limit the size of the chunk and not include all `n_txes`
    // transactions in the chunk.
    assert!(30 * mb < size && size < 40 * mb, "{size}");
}

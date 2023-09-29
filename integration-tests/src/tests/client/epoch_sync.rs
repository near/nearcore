use near_chain::{ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_test_logger;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

fn generate_transactions(last_hash: &CryptoHash, h: BlockHeight) -> Vec<SignedTransaction> {
    let mut txs = vec![];
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    if h == 1 {
        txs.push(SignedTransaction::from_actions(
            h,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            })],
            last_hash.clone(),
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::from_actions(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_random_value".to_string(),
                args: vec![],
                gas: 100_000_000_000_000,
                deposit: 0,
            }))],
            last_hash.clone(),
        ));
    }

    for i in 0..5 {
        txs.push(SignedTransaction::send_money(
            h * 10 + i,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            last_hash.clone(),
        ));
    }
    txs
}

/// Produce 4 epochs with some transactions.
/// At the end of each epoch check that `EpochSyncInfo` has been recorded.
#[test]
fn test_continuous_epoch_sync_info_population() {
    init_test_logger();

    let epoch_length = 5;
    let max_height = epoch_length * 4 + 1;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);

    genesis.config.epoch_length = epoch_length;
    let mut chain_genesis = ChainGenesis::test();
    chain_genesis.epoch_length = epoch_length;
    let mut env = TestEnv::builder(chain_genesis)
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build();

    let mut last_hash = *env.clients[0].chain.genesis().hash();

    for h in 1..max_height {
        for tx in generate_transactions(&last_hash, h) {
            assert_eq!(env.clients[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        }

        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        last_hash = *block.hash();

        if env.clients[0].epoch_manager.is_next_block_epoch_start(&last_hash).unwrap() {
            let epoch_id = block.header().epoch_id().clone();

            tracing::debug!("Checking epoch: {:?}", &epoch_id);
            assert!(env.clients[0].chain.store().get_epoch_sync_info(&epoch_id).is_ok());
            tracing::debug!("OK");
        }
    }
}

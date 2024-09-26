use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_client::{test_utils::TestEnv, ProcessTxResponse};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::{hash::CryptoHash, transaction::SignedTransaction};
use nearcore::test_utils::TestEnvNightshadeSetupExt;

/// Test that in case there is a fork, the global contract root is properly maintained
#[test]
fn test_global_contract_fork_simple() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    assert_eq!(genesis_block.header().global_contract_root(), CryptoHash::default());

    let signer =
        InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0").into();
    let code = near_test_contracts::rs_contract();
    let tx = SignedTransaction::deploy_permanent_contract(
        1,
        &"test0".parse().unwrap(),
        code.to_vec(),
        &signer,
        *genesis_block.hash(),
    );

    let res = env.clients[0].process_tx(tx, false, false);
    assert!(matches!(res, ProcessTxResponse::ValidTx));
    let mut next_height = 1;
    env.produce_block(0, next_height);
    next_height += 1;
    let block1 = env.clients[0].produce_block(next_height).unwrap().unwrap();
    let block2 = env.clients[0].produce_block(next_height + 1).unwrap().unwrap();
    env.process_block(0, block1, Provenance::PRODUCED);
    env.process_block(0, block2, Provenance::PRODUCED);
    for height in 4..7 {
        env.produce_block(0, height);
    }
    let head = env.clients[0].chain.head().unwrap();
    let block = env.clients[0].chain.get_block_by_height(head.height).unwrap();
    let root = block.header().global_contract_root();
    assert_ne!(root, CryptoHash::default());
    
    
    let signer = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1").into();

    let tx = SignedTransaction::deploy_permanent_contract(
        1,
        &"test1".parse().unwrap(),
        code.to_vec(),
        &signer,
        head.last_block_hash,
    );
    let res = env.clients[0].process_tx(tx, false, false);
    assert!(matches!(res, ProcessTxResponse::ValidTx));
    for height in head.height + 1 ..head.height + 3 {
        env.produce_block(0, height);
    }
    let new_block = env.clients[0].produce_block_on(head.height + 3, head.last_block_hash).unwrap().unwrap();
    env.process_block(0, new_block, Provenance::PRODUCED);
    for height in head.height + 4..head.height + 7 {
        env.produce_block(0, height);
    }
    let head = env.clients[0].chain.head().unwrap();
    let block = env.clients[0].chain.get_block_by_height(head.height).unwrap();
    assert_ne!(block.header().global_contract_root(), root);

}

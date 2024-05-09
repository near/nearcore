use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::id::AccountId;
use near_primitives::errors::{ActionErrorKind, FunctionCallError, TxExecutionError};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_primitives::views::FinalExecutionStatus;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

fn setup_runtime(sender_id: AccountId) -> TestEnv {
    let epoch_length = 10;
    let mut genesis = Genesis::test_sharded_new_version(vec![sender_id], 1, vec![1, 1, 1, 1]);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = ProtocolFeature::CongestionControl.protocol_version() - 1;
    // Chain must be sharded to test cross-shard congestion control.
    genesis.config.shard_layout = ShardLayout::v1_test();
    TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build()
}

/// Simplest possible upgrade to new protocol with congestion control enabled,
/// no traffic at all.
#[test]
fn test_protocol_upgrade() {
    // The following only makes sense to test if the feature is enabled in the current build.
    if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        return;
    }

    let mut env = setup_runtime("test0".parse().unwrap());

    // Produce a few blocks to get out of initial state.
    let tip = env.clients[0].chain.head().unwrap();
    for i in 1..4 {
        env.produce_block(0, tip.height + i);
    }

    // Ensure we are still in the old version and no congestion info is shared.
    check_old_protocol(&env);

    env.upgrade_protocol_to_latest_version();

    // check we are in the new version
    assert!(ProtocolFeature::CongestionControl.enabled(env.get_head_protocol_version()));

    let block = env.clients[0].chain.get_head_block().unwrap();
    // check congestion info is available and represents "no congestion"
    let chunks = block.chunks();
    assert!(chunks.len() > 0);
    for chunk_header in chunks.iter() {
        let congestion_info = chunk_header
            .congestion_info()
            .expect("chunk header must have congestion info after upgrade");
        assert_eq!(congestion_info.congestion_level(), 0.0);
        assert!(congestion_info.shard_accepts_transactions());
    }
}

#[test]
fn test_protocol_upgrade_under_congestion() {
    // The following only makes sense to test if the feature is enabled in the current build.
    if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        return;
    }

    let sender_id: AccountId = "test0".parse().unwrap();
    let contract_id: AccountId = "contract.test0".parse().unwrap();
    let mut env = setup_runtime(sender_id.clone());

    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed(sender_id.clone(), KeyType::ED25519, sender_id.as_str());
    let mut nonce = 1;

    // prepare a contract to call
    let contract = near_test_contracts::rs_contract();
    let create_contract_tx = SignedTransaction::create_contract(
        nonce,
        sender_id.clone(),
        contract_id.clone(),
        contract.to_vec(),
        10 * 10u128.pow(24),
        PublicKey::from_seed(KeyType::ED25519, contract_id.as_str()),
        &signer,
        *genesis_block.hash(),
    );
    // this adds the tx to the pool and then produces blocks until the tx result is available
    env.execute_tx(create_contract_tx).unwrap().assert_success();
    nonce += 1;

    // Test the function call works as expected, ending in a gas exceeded error.
    let block = env.clients[0].chain.get_head_block().unwrap();
    let fn_tx = new_fn_call_100tgas(&mut nonce, &signer, contract_id.clone(), *block.hash());
    let FinalExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) =
        env.execute_tx(fn_tx).unwrap().status
    else {
        panic!("test setup error: should result in action error")
    };
    assert_eq!(
        action_error.kind,
        ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
            "Exceeded the prepaid gas.".to_owned()
        )),
        "test setup error: should result in gas exceeded error"
    );

    // Now, congest the network with ~1000 Pgas, enough to have some left after the protocol upgrade.
    let block = env.clients[0].chain.get_head_block().unwrap();
    for _ in 0..10000 {
        let fn_tx = new_fn_call_100tgas(&mut nonce, &signer, contract_id.clone(), *block.hash());
        // this only adds the tx to the pool, no chain progress is made
        let response = env.clients[0].process_tx(fn_tx, false, false);
        assert_eq!(response, ProcessTxResponse::ValidTx);
    }

    // Allow transactions to enter the chain
    let tip = env.clients[0].chain.head().unwrap();
    for i in 1..6 {
        env.produce_block(0, tip.height + i);
    }

    // Ensure we are still in the old version and no congestion info is shared.
    check_old_protocol(&env);

    env.upgrade_protocol_to_latest_version();

    // check we are in the new version
    assert!(ProtocolFeature::CongestionControl.enabled(env.get_head_protocol_version()));
    // check congestion info is available
    let block = env.clients[0].chain.get_head_block().unwrap();
    let chunks = block.chunks();
    for chunk_header in chunks.iter() {
        chunk_header
            .congestion_info()
            .expect("chunk header must have congestion info after upgrade");
    }
    let tip = env.clients[0].chain.head().unwrap();

    // check sender shard is still sending transactions from the pool
    let sender_shard_id =
        env.clients[0].epoch_manager.account_id_to_shard_id(&sender_id, &tip.epoch_id).unwrap();
    let sender_shard_chunk_header =
        &chunks.get(sender_shard_id as usize).expect("chunk must be available");
    let sender_chunk =
        env.clients[0].chain.get_chunk(&sender_shard_chunk_header.chunk_hash()).unwrap();
    assert!(
        sender_chunk.transactions().len() > 0,
        "test setup error: sender is out of transactions"
    );
    assert!(
        sender_chunk.prev_outgoing_receipts().len() > 0,
        "test setup error: sender is not sending receipts"
    );

    // check congestion is observed on the contract's shard
    let contract_shard_id =
        env.clients[0].epoch_manager.account_id_to_shard_id(&contract_id, &tip.epoch_id).unwrap();
    assert!(
        contract_shard_id != sender_shard_id,
        "test setup error: sender and contract are on the same shard"
    );
    let contract_shard_chunk_header =
        &chunks.get(contract_shard_id as usize).expect("chunk must be available");
    let _congestion_info = contract_shard_chunk_header.congestion_info().unwrap();
    // TODO(congestion_control) - properly initialize
    // assert_eq!(
    //     congestion_info.congestion_level(),
    //     1.0,
    //     "contract's shard should be fully congested"
    // );
}

/// Check we are still in the old version and no congestion info is shared.
#[track_caller]
fn check_old_protocol(env: &TestEnv) {
    assert!(
        !ProtocolFeature::CongestionControl.enabled(env.get_head_protocol_version()),
        "test setup error: chain already updated to new protocol"
    );
    let block = env.clients[0].chain.get_head_block().unwrap();
    let chunks = block.chunks();
    assert!(chunks.len() > 0, "no chunks in block");
    for chunk_header in chunks.iter() {
        assert!(
            chunk_header.congestion_info().is_none(),
            "old protocol should not have congestion info but found {:?}",
            chunk_header.congestion_info()
        );
    }
}

/// Create a function call that has 100 Tgas attached and will burn it all.
fn new_fn_call_100tgas(
    nonce_source: &mut u64,
    signer: &InMemorySigner,
    contract_id: AccountId,
    block_hash: near_primitives::hash::CryptoHash,
) -> SignedTransaction {
    let hundred_tgas = 100 * 10u64.pow(12);
    let deposit = 0;
    let nonce = *nonce_source;
    *nonce_source += 1;
    SignedTransaction::call(
        nonce,
        signer.account_id.clone(),
        contract_id,
        signer,
        deposit,
        // easy way to burn all attached gas
        "loop_forever".to_owned(),
        vec![],
        hundred_tgas,
        block_hash,
    )
}

use crate::tests::client::process_blocks::{
    create_nightshade_runtimes, produce_blocks_from_height,
};
use assert_matches::assert_matches;
use near_chain::chain::NUM_ORPHAN_ANCESTORS_CHECK;
use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess, Error, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::{create_chunk_with_transactions, TestEnv};
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_network::types::{MsgRecipient, NetworkClientResponses};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::errors::InvalidTxError;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::FinalExecutionStatus;
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use nearcore::{TrackedConfig, NEAR_BASE};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::path::Path;
use std::sync::Arc;

/// Try to process tx in the next blocks, check that tx and all generated receipts succeed.
/// Return height of the next block.
fn check_tx_processing(
    env: &mut TestEnv,
    tx: SignedTransaction,
    height: BlockHeight,
    blocks_number: u64,
) -> BlockHeight {
    let tx_hash = tx.get_hash();
    env.clients[0].process_tx(tx, false, false);
    let next_height = produce_blocks_from_height(env, blocks_number, height);
    let final_outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(final_outcome.status, FinalExecutionStatus::SuccessValue(_));
    next_height
}

/// Test that duplicate transactions are properly rejected.
#[test]
fn test_transaction_hash_collision() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer0 = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let signer1 = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let send_money_tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer1,
        100,
        *genesis_block.hash(),
    );
    let delete_account_tx = SignedTransaction::delete_account(
        2,
        "test1".parse().unwrap(),
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer1,
        *genesis_block.hash(),
    );

    env.clients[0].process_tx(send_money_tx.clone(), false, false);
    env.clients[0].process_tx(delete_account_tx, false, false);

    for i in 1..4 {
        env.produce_block(0, i);
    }

    let create_account_tx = SignedTransaction::create_account(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        NEAR_BASE,
        signer1.public_key(),
        &signer0,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(create_account_tx, false, false);
    assert_matches!(res, NetworkClientResponses::ValidTx);
    for i in 4..8 {
        env.produce_block(0, i);
    }

    let res = env.clients[0].process_tx(send_money_tx, false, false);
    assert_matches!(res, NetworkClientResponses::InvalidTx(_));
}

/// Helper for checking that duplicate transactions from implicit accounts are properly rejected.
/// It creates implicit account, deletes it and creates again, so that nonce of the access
/// key is updated. Then it tries to send tx from implicit account with invalid nonce, which
/// should fail since the protocol upgrade.
fn get_status_of_tx_hash_collision_for_implicit_account(
    protocol_version: ProtocolVersion,
) -> NetworkClientResponses {
    let epoch_length = 100;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = protocol_version;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer1 = InMemorySigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");

    let public_key = signer1.public_key.clone();
    let raw_public_key = public_key.unwrap_as_ed25519().0.to_vec();
    let implicit_account_id = AccountId::try_from(hex::encode(&raw_public_key)).unwrap();
    let implicit_account_signer =
        InMemorySigner::from_secret_key(implicit_account_id.clone(), signer1.secret_key.clone());
    let deposit_for_account_creation = 10u128.pow(23);
    let mut height = 1;
    let blocks_number = 5;

    // Send money to implicit account, invoking its creation.
    let send_money_tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        implicit_account_id.clone(),
        &signer1,
        deposit_for_account_creation,
        *genesis_block.hash(),
    );
    height = check_tx_processing(&mut env, send_money_tx, height, blocks_number);
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

    // Delete implicit account.
    let delete_account_tx = SignedTransaction::delete_account(
        // Because AccessKeyNonceRange is enabled, correctness of this nonce is guaranteed.
        (height - 1) * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER,
        implicit_account_id.clone(),
        implicit_account_id.clone(),
        "test0".parse().unwrap(),
        &implicit_account_signer,
        *block.hash(),
    );
    height = check_tx_processing(&mut env, delete_account_tx, height, blocks_number);
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

    // Send money to implicit account again, invoking its second creation.
    let send_money_again_tx = SignedTransaction::send_money(
        2,
        "test1".parse().unwrap(),
        implicit_account_id.clone(),
        &signer1,
        deposit_for_account_creation,
        *block.hash(),
    );
    height = check_tx_processing(&mut env, send_money_again_tx, height, blocks_number);
    let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();

    // Send money from implicit account with incorrect nonce.
    let send_money_from_implicit_account_tx = SignedTransaction::send_money(
        1,
        implicit_account_id.clone(),
        "test0".parse().unwrap(),
        &implicit_account_signer,
        100,
        *block.hash(),
    );
    let status = env.clients[0].process_tx(send_money_from_implicit_account_tx, false, false);

    // Check that sending money from implicit account with correct nonce is still valid.
    let send_money_from_implicit_account_tx = SignedTransaction::send_money(
        (height - 1) * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER,
        implicit_account_id,
        "test0".parse().unwrap(),
        &implicit_account_signer,
        100,
        *block.hash(),
    );
    check_tx_processing(&mut env, send_money_from_implicit_account_tx, height, blocks_number);

    status
}

/// Test that duplicate transactions from implicit accounts are properly rejected.
#[test]
fn test_transaction_hash_collision_for_implicit_account_fail() {
    let protocol_version = ProtocolFeature::AccessKeyNonceForImplicitAccounts.protocol_version();
    assert_matches!(
        get_status_of_tx_hash_collision_for_implicit_account(protocol_version),
        NetworkClientResponses::InvalidTx(InvalidTxError::InvalidNonce { .. })
    );
}

/// Test that duplicate transactions from implicit accounts are not rejected until protocol upgrade.
#[test]
fn test_transaction_hash_collision_for_implicit_account_ok() {
    let protocol_version =
        ProtocolFeature::AccessKeyNonceForImplicitAccounts.protocol_version() - 1;
    assert_matches!(
        get_status_of_tx_hash_collision_for_implicit_account(protocol_version),
        NetworkClientResponses::ValidTx
    );
}

/// Test that chunks with transactions that have expired are considered invalid.
#[test]
fn test_chunk_transaction_validity() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    for i in 1..200 {
        env.produce_block(0, i);
    }
    let (encoded_shard_chunk, merkle_path, receipts, block) =
        create_chunk_with_transactions(&mut env.clients[0], vec![tx]);
    let mut chain_store = ChainStore::new(
        env.clients[0].chain.store().store().clone(),
        genesis_block.header().height(),
        true,
    );
    env.clients[0]
        .shards_mgr
        .distribute_encoded_chunk(encoded_shard_chunk, merkle_path, receipts, &mut chain_store, 0)
        .unwrap();
    let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidTransactions);
}

#[test]
fn test_transaction_nonce_too_large() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let large_nonce = AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1;
    let tx = SignedTransaction::send_money(
        large_nonce,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(tx, false, false);
    assert_matches!(
        res,
        NetworkClientResponses::InvalidTx(InvalidTxError::InvalidAccessKeyError(_))
    );
}

/// This test tests the logic regarding requesting chunks for orphan.
/// The test tests the following scenario, there is one validator(test0) and one non-validator node(test1)
/// test0 produces and processes 20 blocks and test1 processes these blocks with some delays. We
/// want to test that test1 requests missing chunks for orphans ahead of time.
/// Note: this test assumes NUM_ORPHAN_ANCESTORS_CHECK <= 5 and >= 2
///
/// - test1 processes blocks 1, 2 successfully
/// - test1 processes blocks 3, 4, ..., 20, but it doesn't have chunks for these blocks, so block 3
///         will be put to the missing chunks pool while block 4 - 20 will be orphaned
/// - check that test1 sends missing chunk requests for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
/// - test1 processes partial chunk responses for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
/// - test1 processes partial chunk responses for block 3
/// - check that block 3 - 2 + NUM_ORPHAN_ANCESTORS_CHECK are accepted, this confirms that the missing chunk requests are sent
///   and processed successfully for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
/// - process until block 8 and check that the node sends missing chunk requests for the new orphans
///   add unlocked
/// - check that test1 does not send missing chunk requests for block 10, because it breaks
///   the requirement that the block must be in the same epoch as the next block after its accepted ancestor
/// - test1 processes partial chunk responses for block 8 and 9
/// - check that test1 sends missing chunk requests for block 11 to 10+NUM_ORPHAN_ANCESTORS+CHECK,
///   since now they satisfy the the requirements for requesting chunks for orphans
/// - process the rest of blocks
#[test]
fn test_request_chunks_for_orphan() {
    init_test_logger();

    // Skip the test if NUM_ORPHAN_ANCESTORS_CHECK is 1, which effectively disables
    // fetching chunks for orphan
    if NUM_ORPHAN_ANCESTORS_CHECK == 1 {
        return;
    }

    let num_clients = 2;
    let num_validators = 1;
    let epoch_length = 10;

    let accounts: Vec<AccountId> =
        (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
    let mut genesis = Genesis::test(accounts, num_validators);
    genesis.config.epoch_length = epoch_length;
    // make the blockchain to 4 shards
    genesis.config.shard_layout = ShardLayout::v1_test();
    genesis.config.num_block_producer_seats_per_shard =
        vec![num_validators, num_validators, num_validators, num_validators];
    let chain_genesis = ChainGenesis::new(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..2)
        .map(|_| {
            Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("."),
                create_test_store(),
                &genesis,
                TrackedConfig::AllShards,
                RuntimeConfigStore::test(),
            )) as Arc<dyn RuntimeAdapter>
        })
        .collect();
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(num_clients)
        .validator_seats(num_validators as usize)
        .runtime_adapters(runtimes)
        .build();

    let mut blocks = vec![];
    // produce 20 blocks
    for i in 1..=20 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }

    let _ = env.clients[1].process_block_test(blocks[0].clone().into(), Provenance::NONE).unwrap();
    // process blocks 1, 2 successfully
    for i in 1..3 {
        let res = env.clients[1].process_block_test(blocks[i].clone().into(), Provenance::NONE);
        assert_matches!(
            res.unwrap_err(),
            near_chain::Error::ChunksMissing(_) | near_chain::Error::Orphan
        );
        let _ = env.clients[1].finish_blocks_in_processing();
        env.process_partial_encoded_chunks_requests(1);
    }
    env.clients[1].finish_blocks_in_processing();

    // process blocks 3 to 15 without processing missing chunks
    // block 3 will be put into the blocks_with_missing_chunks pool
    let res = env.clients[1].process_block_test(blocks[3].clone().into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), near_chain::Error::ChunksMissing(_));
    // remove the missing chunk request from the network queue because we want to process it later
    let missing_chunk_request = env.network_adapters[1].pop().unwrap();
    // block 4-20 will be put to the orphan pool
    for i in 4..20 {
        let res = env.clients[1].process_block_test(blocks[i].clone().into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), near_chain::Error::Orphan);
    }
    // check that block 4-2+NUM_ORPHAN_ANCESTORS_CHECK requested partial encoded chunks already
    for i in 4..3 + NUM_ORPHAN_ANCESTORS_CHECK {
        assert!(
            env.clients[1].chain.check_orphan_partial_chunks_requested(blocks[i as usize].hash()),
            "{}",
            i
        );
    }
    assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(
        blocks[3 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
    ));
    assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(
        blocks[4 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
    ));
    // process all the partial encoded chunk requests for block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
    env.process_partial_encoded_chunks_requests(1);
    env.clients[1].finish_blocks_in_processing();

    // process partial encoded chunk request for block 3, which will unlock block 4 - 2 + NUM_ORPHAN_ANCESTORS_CHECK
    env.process_partial_encoded_chunk_request(1, missing_chunk_request);
    env.clients[1].finish_blocks_in_processing();
    assert_eq!(
        &env.clients[1].chain.head().unwrap().last_block_hash,
        blocks[2 + NUM_ORPHAN_ANCESTORS_CHECK as usize].hash()
    );

    // check that `check_orphans` will request PartialChunks for new orphans as new blocks are processed
    // keep processing the partial encoded chunk requests in the queue, which will process
    // block 3+NUM_ORPHAN_ANCESTORS to 8.
    for i in 4 + NUM_ORPHAN_ANCESTORS_CHECK..10 {
        assert!(env.clients[1]
            .chain
            .check_orphan_partial_chunks_requested(blocks[i as usize].hash()));
        for _ in 0..4 {
            let request = env.network_adapters[1].pop().unwrap();
            env.process_partial_encoded_chunk_request(1, request);
            env.clients[1].finish_blocks_in_processing();
        }
    }
    assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[8].hash());
    // blocks[10] is at the new epoch, so we can't request partial chunks for it yet
    assert!(!env.clients[1].chain.check_orphan_partial_chunks_requested(blocks[10].hash()));

    // process missing chunks for block 9, which has 4 chunks, so there are 4 requests in total
    for _ in 0..4 {
        let request = env.network_adapters[1].pop().unwrap();
        env.process_partial_encoded_chunk_request(1, request);
        env.clients[1].finish_blocks_in_processing();
    }
    assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[9].hash());

    for i in 11..10 + NUM_ORPHAN_ANCESTORS_CHECK {
        assert!(env.clients[1]
            .chain
            .check_orphan_partial_chunks_requested(blocks[i as usize].hash()));
    }

    // process the rest of blocks
    for i in 10..20 {
        // process missing chunk requests for the 4 chunks in each block
        for _ in 0..4 {
            let request = env.network_adapters[1].pop().unwrap();
            env.process_partial_encoded_chunk_request(1, request);
        }
        env.clients[1].finish_blocks_in_processing();
        assert_eq!(&env.clients[1].chain.head().unwrap().last_block_hash, blocks[i].hash());
    }
}

/// This test tests that if a node's requests for chunks are eventually answered,
/// it can process blocks, which also means chunks and parts and processed correctly.
/// It can be seen as a sanity test for the logic in processing chunks,
/// while abstracting away the logic for requesting chunks by assuming chunks requests are
/// always answered (it does test for delayed response).
///
/// This test tests the following scenario: there is one validator(test0) and one non-validator node(test1)
/// test0 produces and processes 21 blocks and test1 processes these blocks.
/// test1 processes the blocks in some random order, to simulate in production, a node may not
/// receive blocks in order. All of test1's requests for chunks are eventually answered, but
/// with some delays. In the end, we check that test1 processes all 21 blocks, and it only
/// requests for each chunk once
#[test]
fn test_processing_chunks_sanity() {
    init_test_logger();

    let num_clients = 2;
    let num_validators = 1;
    let epoch_length = 10;

    let accounts: Vec<AccountId> =
        (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
    let mut genesis = Genesis::test(accounts, num_validators);
    genesis.config.epoch_length = epoch_length;
    // make the blockchain to 4 shards
    genesis.config.shard_layout = ShardLayout::v1_test();
    genesis.config.num_block_producer_seats_per_shard =
        vec![num_validators, num_validators, num_validators, num_validators];
    let chain_genesis = ChainGenesis::new(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..2)
        .map(|_| {
            Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("."),
                create_test_store(),
                &genesis,
                TrackedConfig::AllShards,
                RuntimeConfigStore::test(),
            )) as Arc<dyn RuntimeAdapter>
        })
        .collect();
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(num_clients)
        .validator_seats(num_validators as usize)
        .runtime_adapters(runtimes)
        .build();

    let mut blocks = vec![];
    // produce 21 blocks
    for i in 1..=21 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }

    // make test1 process these blocks, while grouping blocks to groups of three
    // and process blocks in each group in a random order.
    // Verify that it can process the blocks successfully if all its requests for missing
    // chunks are answered
    let mut rng = thread_rng();
    let mut num_requests = 0;
    for i in 0..=6 {
        let mut next_blocks: Vec<_> = (3 * i..3 * i + 3).collect();
        next_blocks.shuffle(&mut rng);
        for ind in next_blocks {
            let _ = env.clients[1].start_process_block(
                blocks[ind].clone().into(),
                Provenance::NONE,
                Arc::new(|_| {}),
            );
            if rng.gen_bool(0.5) {
                env.clients[1].finish_block_in_processing(blocks[ind].hash());
            }
            while let Some(request) = env.network_adapters[1].pop() {
                // process the chunk request some times, otherwise keep it in the queue
                // this is to simulate delays in the network
                if rng.gen_bool(0.7) {
                    env.process_partial_encoded_chunk_request(1, request);
                    num_requests += 1;
                } else {
                    env.network_adapters[1].do_send(request);
                }
            }
        }
        env.clients[1].finish_blocks_in_processing();
    }
    // process the remaining chunk requests
    while let Some(request) = env.network_adapters[1].pop() {
        env.process_partial_encoded_chunk_request(1, request);
        env.clients[1].finish_blocks_in_processing();
        num_requests += 1;
    }

    assert_eq!(env.clients[1].chain.head().unwrap().height, 21);
    // Check each chunk is only requested once.
    // There are 21 blocks in total, but the first block has no chunks,
    assert_eq!(num_requests, 4 * 20);
}

/// Test asynchronous block processing (start_process_block_async).
/// test0 produces 20 blocks. Shuffle the 20 blocks and make test1 process these blocks.
/// Verify that test1 can succesfully finish processing the 20 blocks
#[test]
fn test_processing_blocks_async() {
    init_test_logger();

    let num_clients = 2;
    let num_validators = 1;
    let epoch_length = 10;

    let accounts: Vec<AccountId> =
        (0..num_clients).map(|i| format!("test{}", i).parse().unwrap()).collect();
    let mut genesis = Genesis::test(accounts, num_validators);
    genesis.config.epoch_length = epoch_length;
    // make the blockchain to 4 shards
    genesis.config.shard_layout = ShardLayout::v1_test();
    genesis.config.num_block_producer_seats_per_shard =
        vec![num_validators, num_validators, num_validators, num_validators];
    let chain_genesis = ChainGenesis::new(&genesis);
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = (0..2)
        .map(|_| {
            Arc::new(nearcore::NightshadeRuntime::test_with_runtime_config_store(
                Path::new("."),
                create_test_store(),
                &genesis,
                TrackedConfig::AllShards,
                RuntimeConfigStore::test(),
            )) as Arc<dyn RuntimeAdapter>
        })
        .collect();
    let mut env = TestEnv::builder(chain_genesis)
        .clients_count(num_clients)
        .validator_seats(num_validators as usize)
        .runtime_adapters(runtimes)
        .build();

    let mut blocks = vec![];
    // produce 20 blocks
    for i in 1..=20 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block, Provenance::PRODUCED);
    }

    let mut rng = thread_rng();
    blocks.shuffle(&mut rng);
    for ind in 0..blocks.len() {
        let _ = env.clients[1].start_process_block(
            blocks[ind].clone().into(),
            Provenance::NONE,
            Arc::new(|_| {}),
        );
    }

    env.clients[1].finish_blocks_in_processing();

    while let Some(request) = env.network_adapters[1].pop() {
        env.process_partial_encoded_chunk_request(1, request);
        env.clients[1].finish_blocks_in_processing();
    }

    assert_eq!(env.clients[1].chain.head().unwrap().height, 20);
}

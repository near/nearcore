#![cfg(feature = "test_features")] // required for adversarial behaviors
//! Test behaviors of the network when the chunk producer is malicious or misbehaving.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::ProcessTxRequest;
use near_client::client_actor::{AdvProduceChunksMode, NetworkAdversarialMessage};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::PROTOCOL_VERSION;

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_producer_with_expired_transactions() {
    init_test_logger();

    let accounts =
        (0..3).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let chunk_producer = accounts[0].as_str();
    let validators: Vec<_> = accounts[1..].iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &validators);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        .shard_layout(ShardLayout::multi_shard_custom(vec![], 1))
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .transaction_validity_period(10)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts.clone())
        .build();
    let TestLoopEnv { test_loop, node_datas, .. } = &mut test_loop_env;

    // First we're gonna ask the chunk producer to keep producing empty chunks and send some
    // transactions as well. This will keep transactions in the transaction pool for more blocks
    // than the transactions are valid for.
    let chunk_producer = &node_datas[0];
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("set chunk production without transactions".into(), move |_| {
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTx,
        ));
    });
    for account in &accounts[1..] {
        let chunk_producer = chunk_producer.clone();
        let sender = account.clone();
        let receiver = accounts[0].clone();
        test_loop.send_adhoc_event("transaction".into(), move |data| {
            let signer = create_user_test_signer(&sender);
            let node = TestLoopNode { data, node_data: &chunk_producer };
            let access_key = node.view_access_key_query(&sender, &signer.public_key()).unwrap();
            let anchor_hash = node.head().last_block_hash;
            let tx = SignedTransaction::send_money(
                access_key.nonce + 1,
                sender,
                receiver,
                &signer,
                Balance::from_near(1),
                anchor_hash,
            );
            let process_tx_request =
                ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
            chunk_producer.rpc_handler_sender.send(process_tx_request);
        });
    }

    // Now that we've sent the transactions, wait for 25 chunk productions without transactions
    // being included.
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data
                .get(&chunk_producer.client_sender.actor_handle())
                .client
                .chain
                .head()
                .unwrap()
                .height
                > 10025
        },
        Duration::seconds(30),
    );

    // Produce chunks without validity checks! The chunks should contain the transactions, but the
    // validators should simply discard the transactions.
    // For a good measure insert some invalid transactions that may be invalid in other ways than
    // them having been expired.
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("produce chunks without validity checks".into(), move |_| {
        data_clone[0]
            .client_sender
            .send(NetworkAdversarialMessage::AdvInsertInvalidTransactions(true));
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithoutTxValidityCheck,
        ));
    });
    test_loop.run_until(
        |test_loop_data| {
            for node in &node_datas[..] {
                let c = &test_loop_data.get(&node.client_sender.actor_handle()).client;
                let h = c.chain.head().unwrap().height;
                if h <= 10050 {
                    return false;
                }
            }
            true
        },
        Duration::seconds(30),
    );

    // I'd have loved to check this holds true for chunk validators but they do not track shards
    // and thus cannot provide the info about balances.
    let node = TestLoopNode { data: &test_loop.data, node_data: chunk_producer };
    for account in &accounts {
        let actual = node.query_balance(account);
        assert_eq!(actual, Balance::from_near(1000000), "no transfers should have happened");
    }

    let Some(applied_tx_metric) = near_o11y::metrics::prometheus::gather()
        .into_iter()
        .find(|m| m.get_name() == "near_transaction_applied_total")
    else {
        panic!("no applied transactions metric found");
    };
    let [metric] = applied_tx_metric.get_metric() else { panic!("unexpected metric shape") };
    let applied_txs = metric.get_counter().get_value();
    assert_eq!(applied_txs, 76.0, "should have applied the submitted transactions");

    test_loop_env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_producer_sending_large_encoded_length_chunks() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).gc_num_epochs_to_keep(20).build();

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let peer_manager_actor_handle = env.node_datas[0].peer_manager_sender.actor_handle();
    let peer_manager_actor = env.test_loop.data.get_mut(&peer_manager_actor_handle);
    peer_manager_actor.register_override_handler(Box::new(
        move |request| -> Option<NetworkRequests> {
            match request {
                NetworkRequests::PartialEncodedChunkMessage {
                    account_id,
                    mut partial_encoded_chunk,
                } => {
                    let header = partial_encoded_chunk.header;

                    let epoch_id = epoch_manager
                        .get_epoch_id_from_prev_block(header.prev_block_hash())
                        .unwrap();
                    let chunk_producer_info = epoch_manager
                        .get_chunk_producer_info(&ChunkProductionKey {
                            shard_id: header.shard_id(),
                            epoch_id,
                            height_created: header.height_created(),
                        })
                        .unwrap();
                    let signer = create_test_signer(chunk_producer_info.account_id().as_str());
                    let new_encoded_length = u64::MAX;
                    let new_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                        *header.prev_block_hash(),
                        header.prev_state_root(),
                        *header.prev_outcome_root(),
                        *header.encoded_merkle_root(),
                        new_encoded_length,
                        header.height_created(),
                        header.shard_id(),
                        header.prev_gas_used(),
                        header.gas_limit(),
                        header.prev_balance_burnt(),
                        *header.prev_outgoing_receipts_root(),
                        *header.tx_root(),
                        header.prev_validator_proposals().collect(),
                        header.congestion_info(),
                        header.bandwidth_requests().unwrap().clone(),
                        header.proposed_split().cloned(),
                        &signer,
                        PROTOCOL_VERSION,
                    ));
                    partial_encoded_chunk.header = new_header;
                    Some(NetworkRequests::PartialEncodedChunkMessage {
                        account_id,
                        partial_encoded_chunk,
                    })
                }
                _ => Some(request),
            }
        },
    ));

    env.node_runner(0).run_for_number_of_blocks(10);

    env.test_loop.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests chain behavior when a Byzantine chunk producer withholds chunk parts
/// from nodes other than the block producer.
#[test]
fn test_chunk_parts_withholding_attack() {
    init_test_logger();

    // 7 validators ensures num_data_parts=2 (formula: (total_parts-1)/3).
    // The byzantine node sends only 1 part to the block producer, which must be
    // insufficient to reconstruct (need 2). With fewer validators (e.g. 4),
    // num_data_parts=1, so a single part suffices and the attack is ineffective.
    let mut env = TestLoopBuilder::new().validators(7, 0).num_shards(7).build();

    let (byzantine_node, honest_node) = (0, 1);
    let epoch_manager = env.node(byzantine_node).client().epoch_manager.clone();
    let peer_manager_handle = env.node_datas[byzantine_node].peer_manager_sender.actor_handle();
    let peer_manager = env.test_loop.data.get_mut(&peer_manager_handle);

    // The Byzantine chunk producer only sends chunk parts to the block producer
    // for that height, withholding from all other validators. Also blocks
    // responses to part requests so other nodes can't fetch the withheld parts.
    peer_manager.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
        match &request {
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                let header = &partial_encoded_chunk.header;
                let epoch_id =
                    epoch_manager.get_epoch_id_from_prev_block(header.prev_block_hash()).unwrap();
                let block_producer =
                    epoch_manager.get_block_producer(&epoch_id, header.height_created()).unwrap();
                if *account_id == block_producer { Some(request) } else { None }
            }
            // Block responses to part requests so other nodes can't recover
            // the withheld parts by requesting them from the byzantine node.
            NetworkRequests::PartialEncodedChunkResponse { .. } => None,
            _ => Some(request),
        }
    }));

    let head_before = env.node(honest_node).head().height;
    env.node_runner(honest_node).run_for_number_of_blocks_with_timeout(15, Duration::seconds(60));
    let head_after = env.node(honest_node).head().height;

    let chain_store = env.node(honest_node).client().chain.chain_store();
    let epoch_manager = &env.node(honest_node).client().epoch_manager;
    let head_epoch_id = env.node(honest_node).head().epoch_id.clone();
    let shard_layout = epoch_manager.get_shard_layout(&head_epoch_id).unwrap();
    let byzantine_account: AccountId = format!("validator{byzantine_node}").parse().unwrap();

    // Find the shard produced by the byzantine node.
    let byzantine_shard = epoch_manager
        .shard_ids(&head_epoch_id)
        .unwrap()
        .into_iter()
        .find(|&shard_id| {
            let key = ChunkProductionKey {
                epoch_id: head_epoch_id.clone(),
                height_created: head_before + 1,
                shard_id,
            };
            *epoch_manager.get_chunk_producer_info(&key).unwrap().account_id() == byzantine_account
        })
        .unwrap();

    // Skip the first block after warmup: its chunk parts were distributed
    // during warmup before the override took effect.
    let mut skipped_count = 0;
    let mut prev_produced = false;
    for height in (head_before + 2)..=head_after {
        let block_producer = epoch_manager.get_block_producer(&head_epoch_id, height).unwrap();
        if chain_store.get_block_hash_by_height(height).is_err() {
            skipped_count += 1;
            prev_produced = false;
            continue;
        }
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        let block = chain_store.get_block(&block_hash).unwrap();
        let has_byzantine_shard = block.chunks().iter().enumerate().any(|(idx, ch)| {
            shard_layout.get_shard_id(idx).unwrap() == byzantine_shard && ch.is_new_chunk()
        });
        // The withheld shard should never be included in blocks visible to the
        // honest node, since no one can reconstruct it.
        assert!(
            !has_byzantine_shard,
            "height {height}: byzantine shard {byzantine_shard} not expected to be included"
        );
        // Blocks produced by the byzantine node should not be visible to the
        // honest node (other validators can't verify the withheld chunk and
        // won't send approvals).
        assert_ne!(
            block_producer, byzantine_account,
            "height {height}: block produced by byzantine node should not be accepted"
        );
        // When a block includes a chunk header whose parts were withheld,
        // validators that never received their parts won't approve it,
        // causing a skip.
        // The next block omits the withheld shard and succeeds. This
        // alternation is an artifact of the particular setup of this test:
        // The byzantine node builds the next chunk on its own (divergent)
        // chain, so it has a prev_block_hash which will cause it to be
        // excluded from the canonical chain.
        // When the byzantine learns of a valid block, it will be able to
        // cause the cycle again. In practice, shards should have multiple
        // chunk producers.
        assert!(
            !prev_produced,
            "height {height}: two consecutive blocks produced despite withholding attack"
        );
        prev_produced = true;
    }
    assert!(skipped_count > 0, "expected at least one skipped block due to withholding attack");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

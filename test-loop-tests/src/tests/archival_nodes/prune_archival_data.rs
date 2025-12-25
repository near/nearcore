use std::collections::{HashMap, HashSet};

use near_async::messaging::Handler;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_client::{GetExecutionOutcomesForBlock, TxStatus};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochHeight};
use near_primitives::utils::get_outcome_id_block_hash_rev;
use near_primitives::views::{FinalExecutionOutcomeViewEnum, TxExecutionStatus, TxStatusView};
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;

const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

#[test]
// TODO(spice): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_prune_archival_data() {
    init_test_logger();
    let accounts = ["a", "z"].map(|c| format!("{}-account", c).parse().unwrap());
    let shard_layout = ShardLayout::multi_shard(3, 3);
    let validator_id: AccountId = "cp0".parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator_id.as_str()], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .shard_layout(shard_layout)
        .build();

    let archival_id: AccountId = "archival".parse().unwrap();
    let all_clients = vec![archival_id.clone(), validator_id];
    let archival_clients = [archival_id.clone()].into_iter().collect();
    let archival_index = all_clients.iter().position(|id| id == &archival_id).unwrap();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(all_clients)
        .cold_storage_archival_clients(archival_clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();
    let archival_node = TestLoopNode::from(&env.node_datas[archival_index]);

    let num_epochs_to_run = 3 * GC_NUM_EPOCHS_TO_KEEP;
    let mut tx_hash_to_signer = HashMap::<CryptoHash, AccountId>::new();
    for nonce in 1..=num_epochs_to_run {
        tx_hash_to_signer.extend(submit_transactions(&env, &archival_node, &accounts, nonce));
        archival_node.run_until_new_epoch(&mut env.test_loop);
    }
    assert_eq!(num_epochs_to_run, epoch_height(&env, &archival_node));

    let hot_store = archival_node.client(env.test_loop_data()).chain.chain_store.store();
    let cold_store_actor_handle =
        archival_node.data().cold_store_sender.as_ref().unwrap().actor_handle();
    let cold_store = env.test_loop.data.get(&cold_store_actor_handle).get_cold_db().as_store();

    let prune_height = GC_NUM_EPOCHS_TO_KEEP * EPOCH_LENGTH;
    let (pruned_outcomes, remaining_outcomes) = prune_data(&hot_store, &cold_store, prune_height);
    verify_prune_data(
        &mut env.test_loop.data,
        &archival_node,
        tx_hash_to_signer,
        pruned_outcomes,
        remaining_outcomes,
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn epoch_height(env: &TestLoopEnv, node: &TestLoopNode) -> EpochHeight {
    let prev_block = node.head(env.test_loop_data()).prev_block_hash;
    let epoch_manager = &node.client(env.test_loop_data()).epoch_manager;
    epoch_manager.get_epoch_height_from_prev_block(&prev_block).unwrap()
}

fn submit_transactions(
    env: &TestLoopEnv,
    node: &TestLoopNode,
    accounts: &[AccountId],
    nonce: u64,
) -> HashMap<CryptoHash, AccountId> {
    let block_hash = node.head(env.test_loop_data()).last_block_hash;
    let mut tx_hash_to_signer = HashMap::<CryptoHash, AccountId>::new();
    for i in 0..accounts.len() {
        let sender = accounts[i].clone();
        let receiver = accounts[(i + 1) % accounts.len()].clone();
        let tx = SignedTransaction::send_money(
            nonce,
            sender.clone(),
            receiver.clone(),
            &create_user_test_signer(&sender),
            Balance::from_millinear(1),
            block_hash,
        );
        tx_hash_to_signer.insert(tx.get_hash(), sender);
        node.submit_tx(tx);
    }
    tx_hash_to_signer
}

fn prune_data(
    hot_store: &Store,
    cold_store: &Store,
    prune_height: BlockHeight,
) -> (HashSet<Box<[u8]>>, HashSet<Box<[u8]>>) {
    let col = DBCol::TransactionResultForBlock;
    let mut cold_store_update = cold_store.store_update();
    let mut pruned_outcomes = HashSet::new();
    let mut remaining_outcomes = HashSet::new();
    for kv in cold_store.iter(col) {
        let (key, _value) = kv.unwrap();
        let (_outcome_id, block_hash) = get_outcome_id_block_hash_rev(&key).unwrap();
        let block_height = hot_store.chain_store().get_block_height(&block_hash).unwrap();
        if block_height < prune_height {
            cold_store_update.delete(col, &key);
            pruned_outcomes.insert(key);
        } else {
            remaining_outcomes.insert(key);
        }
    }
    cold_store_update.commit().unwrap();
    (pruned_outcomes, remaining_outcomes)
}

fn verify_prune_data(
    test_loop_data: &mut TestLoopData,
    node: &TestLoopNode,
    tx_hash_to_signer: HashMap<CryptoHash, AccountId>,
    expected_pruned_outcomes: HashSet<Box<[u8]>>,
    expected_remaining_outcomes: HashSet<Box<[u8]>>,
) {
    let view_client_handle = node.data().view_client_sender.actor_handle();
    let view_client = test_loop_data.get_mut(&view_client_handle);
    for key in expected_pruned_outcomes {
        let (outcome_id, block_hash) = get_outcome_id_block_hash_rev(&key).unwrap();
        let request = GetExecutionOutcomesForBlock { block_hash };
        let res = view_client.handle(request);
        assert!(res.unwrap().values().all(|v| v.is_empty()));

        let Some(signer_account_id) = tx_hash_to_signer.get(&outcome_id).cloned() else {
            continue;
        };
        let request = TxStatus { tx_hash: outcome_id, signer_account_id, fetch_receipt: true };
        let res = view_client.handle(request).unwrap();
        assert!(matches!(
            res,
            TxStatusView { execution_outcome: None, status: TxExecutionStatus::Included }
        ));
    }
    let mut found_receipts = HashSet::<CryptoHash>::new();
    let mut expected_remaining_receipts = HashSet::<CryptoHash>::new();
    for key in &expected_remaining_outcomes {
        let (outcome_id, block_hash) = get_outcome_id_block_hash_rev(key).unwrap();
        let request = GetExecutionOutcomesForBlock { block_hash };
        let res = view_client.handle(request);
        assert!(res.unwrap().values().any(|v| !v.is_empty()));

        let Some(signer_account_id) = tx_hash_to_signer.get(&outcome_id).cloned() else {
            expected_remaining_receipts.insert(outcome_id);
            continue;
        };
        let request = TxStatus { tx_hash: outcome_id, signer_account_id, fetch_receipt: true };
        let res = view_client.handle(request);
        let Some(FinalExecutionOutcomeViewEnum::FinalExecutionOutcomeWithReceipt(outcome)) =
            res.unwrap().execution_outcome.try_into().unwrap()
        else {
            panic!("expected FinalExecutionOutcomeWithReceipt");
        };
        found_receipts.extend(outcome.receipts.iter().map(|r| r.receipt_id));
    }
    assert_eq!(found_receipts, expected_remaining_receipts);
}

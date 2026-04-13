use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_id, create_account_ids, create_validators_spec, validators_spec_clients,
};
use crate::utils::node::TestLoopNode;
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use crate::utils::transactions::check_txs;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_manager::{
    DynamicReshardingConfig, EpochConfigStore, ShardLayoutConfig,
};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptEnum, ReceiptToTxInfo};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::DBCol;
use near_vm_runner::ContractCode;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

const EPOCH_LENGTH: BlockHeightDelta = 5;

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_stale_global_contract_distribution_after_double_resharding() {
    init_test_logger();

    // The fix only works with V3 shard layouts (dynamic resharding).
    // With static resharding, the shard layout doesn't maintain a full split history.
    if !ProtocolFeature::DynamicResharding.enabled(PROTOCOL_VERSION) {
        return;
    }

    let epoch_length: BlockHeightDelta = 10;
    let base_boundary_accounts = create_account_ids(["user2", "user3"]).to_vec();
    let base_shard_layout = ShardLayout::multi_shard_custom(base_boundary_accounts, 3);
    let deploy_user: AccountId = create_account_id("user0");
    let users = create_account_ids(["user0", "user1", "user2", "user3", "user4", "user5"]).to_vec();
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let chunk_producer = clients[0].clone();
    let gas_limit = Gas::from_teragas(300);
    let base_pv = PROTOCOL_VERSION - 1;

    // Configure dynamic resharding to force-split two shards sequentially.
    // The first split targets the shard containing deploy_user (user0), so the
    // GlobalContractDistribution receipt becomes stale after two layout transitions.
    let first_split_shard = base_shard_layout.account_id_to_shard_id(&deploy_user);
    let second_split_shard = base_shard_layout.account_id_to_shard_id(&create_account_id("user4"));
    assert_ne!(first_split_shard, second_split_shard);

    let dynamic_config = DynamicReshardingConfig {
        memory_usage_threshold: u64::MAX,
        min_child_memory_usage: u64::MAX,
        max_number_of_shards: 100,
        min_epochs_between_resharding: 0,
        force_split_shards: vec![first_split_shard, second_split_shard],
        block_split_shards: vec![],
    };

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(base_pv)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(epoch_length)
        .gas_limit(gas_limit)
        .add_user_accounts_simple(&users, Balance::from_near(1_000_000))
        .build();
    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();

    let mut dynamic_epoch_config = base_epoch_config.clone();
    dynamic_epoch_config.shard_layout_config =
        ShardLayoutConfig::Dynamic { dynamic_resharding_config: dynamic_config };

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (base_pv, Arc::new(base_epoch_config)),
        (base_pv + 1, Arc::new(dynamic_epoch_config)),
    ]));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .gc_num_epochs_to_keep(5)
        .build();

    // Step 1: Deploy the test contract on user0's account so we can call burn_gas_raw.
    {
        let node = env.node_for_account(&chunk_producer);
        let tx = node.tx_deploy_test_contract(&deploy_user);
        node.submit_tx(tx);
    }
    env.runner_for_account(&chunk_producer).run_for_number_of_blocks(2);

    // Step 2: Deploy a global contract from user0. This creates a
    // GlobalContractDistribution receipt with target_shard = user0's shard (S_A),
    // which is the shard that will be split in the first resharding.
    {
        let node = env.node_for_account(&chunk_producer);
        let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
        let tx = node.tx_deploy_global_contract(
            &deploy_user,
            code.code().to_vec(),
            GlobalContractDeployMode::CodeHash,
        );
        node.submit_tx(tx);
    }

    // Step 3: Saturate compute on user0's shard every block so that the
    // GlobalContractDistribution receipt (arriving as incoming) gets pushed to
    // the delayed queue and stays there through both resharding events.
    //
    // Each burn_gas_raw call burns slightly more than half the gas limit, so
    // two local receipts exhaust the chunk's compute budget. We submit 3 per
    // block to ensure at least 2 are processed as local receipts.
    let gas_to_burn = gas_limit.checked_div(2).unwrap().checked_add(Gas::from_gas(1)).unwrap();
    let initial_num_shards = base_shard_layout.num_shards();
    let target_num_shards = initial_num_shards + 2; // after two splits

    let start_height = {
        let node = env.node_for_account(&chunk_producer);
        node.client().chain.chain_store().head().unwrap().height
    };

    // Keep saturating until both resharding events complete. Dynamic resharding has a
    // 2-epoch proposal-to-activation pipeline, so we need enough epochs for both splits.
    let max_saturation_height = start_height + epoch_length * 12;
    let mut both_splits_done = false;
    for target_height in (start_height + 1)..=max_saturation_height {
        // Submit 3 heavy transactions to saturate this block's compute budget.
        {
            let node = env.node_for_account(&chunk_producer);
            for _ in 0..3 {
                let tx = node.tx_call(
                    &deploy_user,
                    &deploy_user,
                    "burn_gas_raw",
                    gas_to_burn.as_gas().to_le_bytes().to_vec(),
                    Balance::ZERO,
                    gas_limit,
                );
                node.submit_tx(tx);
            }
        }
        env.runner_for_account(&chunk_producer).run_until_head_height(target_height);

        // Check if both resharding events have completed.
        let node = env.node_for_account(&chunk_producer);
        let epoch_id = node.client().chain.chain_store().head().unwrap().epoch_id;
        let current_layout = node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
        if current_layout.num_shards() >= target_num_shards {
            both_splits_done = true;
            break;
        }
    }
    assert!(both_splits_done, "both shard splits did not complete within the allotted blocks");

    // Step 4: Stop saturating. Let the delayed queue drain.
    // If the vulnerability exists, processing the stale GlobalContractDistribution
    // receipt will panic in receipt_filter_fn() when receiver_shard_id() fails
    // to remap the old target_shard after two resharding generations.
    let current_height = {
        let node = env.node_for_account(&chunk_producer);
        node.client().chain.chain_store().head().unwrap().height
    };
    let drain_end = current_height + epoch_length * 2;
    env.runner_for_account(&chunk_producer).run_until_head_height(drain_end);

    let head_height = {
        let node = env.node_for_account(&chunk_producer);
        node.client().chain.chain_store().head().unwrap().height
    };
    assert!(
        head_height >= drain_end,
        "chain stalled at height {}; expected >= {} (likely panicked processing stale receipt)",
        head_height,
        drain_end
    );
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_global_receipt_distribution_at_resharding_boundary() {
    init_test_logger();
    let mut env = GlobalContractsReshardingTestEnv::setup();
    let expected_new_shard_layout_height = EPOCH_LENGTH * 2 + 2;
    // This height is picked so that the first global contract distribution receipt reaches
    // shard that is being split at the first height after the resharding
    let send_deploy_tx_height = expected_new_shard_layout_height - 3;

    env.run_until_head_height(send_deploy_tx_height);
    assert_eq!(env.current_shard_layout(), env.base_shard_layout);

    // Deploying global contract with the user from the split shard.
    // The first target shard for the global contract distribution receipt
    // is user's shard, this way we ensure that we hit the shard that is
    // split at resharding.
    let deploy_user = env.users[0].clone();
    assert!(
        !env.new_shard_layout
            .shard_ids()
            .contains(&env.base_shard_layout.account_id_to_shard_id(&deploy_user)),
        "expected deploy user to be in the split shard"
    );
    let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let node = env.chunk_producer_node();
    let tx = node.tx_deploy_global_contract(
        &deploy_user,
        code.code().to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    let deploy_tx = node.submit_tx(tx);

    env.run_until_head_height(expected_new_shard_layout_height);
    check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &[deploy_tx]);
    assert_eq!(env.current_shard_layout(), env.new_shard_layout);

    // Verify that global contract distribution receipt has target shard from the old shard layout,
    // while its block has the new layout.
    {
        let client = env.chunk_producer_node().client();
        let block = client.chain.get_block_by_height(expected_new_shard_layout_height).unwrap();
        let block_shard_layout =
            client.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
        assert_eq!(block_shard_layout, env.new_shard_layout);
        let chunks = block.chunks();
        // Expect new chunk
        assert!(chunks[0].is_new_chunk(block.header().height()));
        let chunk = client.chain.get_chunk(&chunks[0].compute_hash()).unwrap();
        let [distribution_receipt] = chunk
            .prev_outgoing_receipts()
            .iter()
            .filter_map(|r| match r.receipt() {
                ReceiptEnum::GlobalContractDistribution(r) => Some(r),
                _ => None,
            })
            .collect_vec()[..]
        else {
            panic!("Expected exactly one global contract distribution receipt");
        };
        let target_shard = distribution_receipt.target_shard();
        assert!(!block_shard_layout.shard_ids().contains(&target_shard));
    };

    // Wait for the distribution to reach all shards.
    env.env.test_loop.run_for(Duration::seconds(3));

    // Check that users on all shards in the new layout can use the contract.
    let mut use_txs = vec![];
    let node = env.chunk_producer_node();
    for user in &env.users {
        let tx =
            node.tx_use_global_contract(user, GlobalContractIdentifier::CodeHash(*code.hash()));
        use_txs.push(node.submit_tx(tx));
    }
    env.env.test_loop.run_for(Duration::seconds(2));
    check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &use_txs);
}

/// Test that nonce-based idempotency prevents stale overwrites during global contract updates.
///
/// Deploys a trivial contract first (AccountId mode), waits for distribution,
/// then deploys rs_contract (AccountId mode) with a higher auto-incremented nonce.
/// Verifies all shards have the newer version by calling a function that only
/// exists in the rs_contract.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_global_contract_nonce_prevents_stale_overwrite() {
    init_test_logger();
    let mut env = GlobalContractsReshardingTestEnv::setup();

    let deploy_user = env.users[0].clone();

    // Step 1: Deploy trivial contract as first version (AccountId mode).
    tracing::info!(target: "test", "Deploying first version of global contract (trivial contract)...");
    let tx = env.chunk_producer_node().tx_deploy_global_contract(
        &deploy_user,
        near_test_contracts::trivial_contract().to_vec(),
        GlobalContractDeployMode::AccountId,
    );
    env.env.runner_for_account(&env.chunk_producer).run_tx(tx, Duration::seconds(5));

    // Step 2: Deploy rs_contract as second version (AccountId mode).
    // This will have a higher auto-incremented nonce.
    tracing::info!(target: "test", "Deploying second version of global contract (rs_contract)...");
    let tx = env.chunk_producer_node().tx_deploy_global_contract(
        &deploy_user,
        near_test_contracts::rs_contract().to_vec(),
        GlobalContractDeployMode::AccountId,
    );
    env.env.runner_for_account(&env.chunk_producer).run_tx(tx, Duration::seconds(5));

    // Step 3: Have all users use the global contract and verify that the rs_contract
    // version (v2) is active by calling "log_something" which only exists in rs_contract.
    tracing::info!(target: "test", "Calling use global contract from all users to verify rs_contract is active...");
    for user in &env.users {
        let identifier = GlobalContractIdentifier::AccountId(deploy_user.clone());
        let tx = env.chunk_producer_node().tx_use_global_contract(user, identifier);
        env.env.runner_for_account(&env.chunk_producer).run_tx(tx, Duration::seconds(5));
    }

    // Step 4: Call "log_something" on each user's account. This method only exists in
    // the rs_contract, so if the trivial contract had overwritten it, this would fail.
    tracing::info!(target: "test", "Calling contract method from all users to verify rs_contract is active...");
    for user in &env.users {
        let tx = env.chunk_producer_node().tx_call(
            user,
            user,
            "log_something",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        env.env.runner_for_account(&env.chunk_producer).run_tx(tx, Duration::seconds(5));
    }
}

struct GlobalContractsReshardingTestEnv {
    env: TestLoopEnv,
    base_shard_layout: ShardLayout,
    new_shard_layout: ShardLayout,
    chunk_producer: AccountId,
    users: Vec<AccountId>,
}

impl GlobalContractsReshardingTestEnv {
    fn setup() -> Self {
        Self::setup_impl(None)
    }

    fn setup_with_gc(gc_num_epochs_to_keep: u64) -> Self {
        Self::setup_impl(Some(gc_num_epochs_to_keep))
    }

    fn setup_impl(gc_num_epochs_to_keep: Option<u64>) -> Self {
        let base_boundary_accounts = create_account_ids(["user2", "user3", "user4"]).to_vec();
        let split_boundary_account: AccountId = create_account_id("user1");
        let base_shard_layout = ShardLayout::multi_shard_custom(base_boundary_accounts, 3);
        let users = create_account_ids(["user0", "user1", "user2", "user3", "user4"]).to_vec();
        let validators_spec = create_validators_spec(1, 0);
        let clients = validators_spec_clients(&validators_spec);
        let chunk_producer = clients[0].clone();
        let genesis = TestLoopBuilder::new_genesis_builder()
            .protocol_version(PROTOCOL_VERSION - 1)
            .validators_spec(validators_spec)
            .shard_layout(base_shard_layout.clone())
            .epoch_length(EPOCH_LENGTH)
            .add_user_accounts_simple(&users, Balance::from_near(1000_000))
            .build();

        let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
        let (new_epoch_config, new_shard_layout) =
            derive_new_epoch_config_from_boundary(&base_epoch_config, &split_boundary_account);

        assert_eq!(
            users
                .iter()
                .map(|acc| new_shard_layout.account_id_to_shard_id(acc))
                .collect::<HashSet<_>>(),
            new_shard_layout.shard_ids().collect::<HashSet<_>>(),
            "expected to have users for all shards"
        );

        let epoch_configs = vec![
            (genesis.config.protocol_version, Arc::new(base_epoch_config)),
            (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
        ];
        let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

        let mut builder = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store);
        if let Some(gc) = gc_num_epochs_to_keep {
            builder = builder.gc_num_epochs_to_keep(gc);
        }
        let env = builder.build();

        Self { env, chunk_producer, base_shard_layout, new_shard_layout, users }
    }

    fn run_until_head_height(&mut self, height: BlockHeight) {
        self.env.runner_for_account(&self.chunk_producer).run_until_head_height(height);
    }

    fn current_shard_layout(&self) -> ShardLayout {
        let client = self.chunk_producer_node().client();
        let epoch_id = client.chain.chain_store().head().unwrap().epoch_id;
        client.epoch_manager.get_shard_layout(&epoch_id).unwrap()
    }

    fn chunk_producer_node(&self) -> TestLoopNode<'_> {
        self.env.node_for_account(&self.chunk_producer)
    }
}

/// Tests that GlobalContractDistribution receipts have ReceiptToTx entries, including
/// forwarded distribution receipts that hop across shards.
#[test]
// Spice + resharding triggers a refcount bug in GC (testdb "Inserting value
// with non-positive refcount"). Re-enable once that is resolved.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_global_distribution_receipt_has_receipt_to_tx() {
    init_test_logger();
    let mut env = GlobalContractsReshardingTestEnv::setup();
    let expected_new_shard_layout_height = EPOCH_LENGTH * 2 + 2;
    let send_deploy_tx_height = expected_new_shard_layout_height - 3;

    env.run_until_head_height(send_deploy_tx_height);

    // Deploy global contract.
    let deploy_user = env.users[0].clone();
    let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let node = env.chunk_producer_node();
    let tx = node.tx_deploy_global_contract(
        &deploy_user,
        code.code().to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    let deploy_tx = node.submit_tx(tx);

    env.run_until_head_height(expected_new_shard_layout_height);
    check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &[deploy_tx]);

    // Run extra blocks so forwarded distribution receipts (which hop shard-by-shard)
    // have time to appear in chunks.
    let extra_blocks = 10;
    env.run_until_head_height(expected_new_shard_layout_height + extra_blocks);

    // Find all GlobalContractDistribution receipts from chunks in all blocks.
    let client = env.chunk_producer_node().client();
    let head_height = client.chain.head().unwrap().height;
    let mut distribution_receipt_ids = vec![];
    for height in 1..=head_height {
        let block = match client.chain.get_block_by_height(height) {
            Ok(block) => block,
            Err(_) => continue,
        };
        for chunk_header in block.chunks().iter() {
            if !chunk_header.is_new_chunk() {
                continue;
            }
            let chunk = match client.chain.get_chunk(&chunk_header.compute_hash()) {
                Ok(chunk) => chunk,
                Err(_) => continue,
            };
            for receipt in chunk.prev_outgoing_receipts() {
                if let ReceiptEnum::GlobalContractDistribution(_) = receipt.receipt() {
                    distribution_receipt_ids.push(*receipt.receipt_id());
                }
            }
        }
    }
    assert!(
        distribution_receipt_ids.len() > 1,
        "expected multiple GlobalContractDistribution receipts (initial + forwarded), found {}",
        distribution_receipt_ids.len()
    );

    // Verify that each distribution receipt has a ReceiptToTx entry.
    let store = env.chunk_producer_node().store();
    for receipt_id in &distribution_receipt_ids {
        let info = store
            .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
            .unwrap_or_else(|| {
                panic!(
                    "receipt_to_tx entry should exist for GlobalContractDistribution receipt {:?}",
                    receipt_id
                )
            });
        match &info {
            ReceiptToTxInfo::V1(v1) => {
                assert!(
                    matches!(&v1.origin, near_primitives::receipt::ReceiptOrigin::FromReceipt(_)),
                    "GlobalContractDistribution receipt should have FromReceipt origin"
                );
            }
        }
    }
}

/// Tests that ReceiptToTx entries for GlobalContractDistribution receipts are garbage collected.
///
/// GCD receipts (both initial and forwarded) don't produce execution outcomes — they just
/// install contract code on shards. Their ReceiptToTx entries are tracked via
/// `ReceiptSource::ReceiptToTxGc` in ProcessedReceiptIds and cleaned up during GC.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_global_distribution_receipt_to_tx_gc() {
    init_test_logger();
    let gc_num_epochs_to_keep = 3u64;
    let mut env = GlobalContractsReshardingTestEnv::setup_with_gc(gc_num_epochs_to_keep);
    let expected_new_shard_layout_height = EPOCH_LENGTH * 2 + 2;
    let send_deploy_tx_height = expected_new_shard_layout_height - 3;

    env.run_until_head_height(send_deploy_tx_height);

    // Deploy global contract.
    let deploy_user = env.users[0].clone();
    let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let node = env.chunk_producer_node();
    let tx = node.tx_deploy_global_contract(
        &deploy_user,
        code.code().to_vec(),
        GlobalContractDeployMode::CodeHash,
    );
    let deploy_tx = node.submit_tx(tx);

    env.run_until_head_height(expected_new_shard_layout_height);
    check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &[deploy_tx]);

    // Run extra blocks for forwarded distribution receipts to reach all shards.
    let extra_blocks = 10;
    env.run_until_head_height(expected_new_shard_layout_height + extra_blocks);

    // Collect all GlobalContractDistribution receipt IDs from chunks.
    let distribution_receipt_ids = {
        let client = env.chunk_producer_node().client();
        let head_height = client.chain.head().unwrap().height;
        let mut ids = vec![];
        for height in 1..=head_height {
            let block = match client.chain.get_block_by_height(height) {
                Ok(block) => block,
                Err(_) => continue,
            };
            for chunk_header in block.chunks().iter() {
                if !chunk_header.is_new_chunk() {
                    continue;
                }
                let chunk = match client.chain.get_chunk(&chunk_header.compute_hash()) {
                    Ok(chunk) => chunk,
                    Err(_) => continue,
                };
                for receipt in chunk.prev_outgoing_receipts() {
                    if let ReceiptEnum::GlobalContractDistribution(_) = receipt.receipt() {
                        ids.push(*receipt.receipt_id());
                    }
                }
            }
        }
        ids
    };
    assert!(
        distribution_receipt_ids.len() > 1,
        "expected multiple GlobalContractDistribution receipts (initial + forwarded), found {}",
        distribution_receipt_ids.len()
    );

    // Verify ReceiptToTx entries exist for all distribution receipts.
    let store = env.chunk_producer_node().store();
    for receipt_id in &distribution_receipt_ids {
        assert!(
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref()).is_some(),
            "receipt_to_tx entry should exist for distribution receipt {receipt_id}"
        );
    }

    // Assert distribution receipt IDs are absent from OutcomeIds — this is why GC can't find them.
    let all_outcome_ids: HashSet<CryptoHash> =
        store.iter_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds).flat_map(|(_, ids)| ids).collect();
    for receipt_id in &distribution_receipt_ids {
        assert!(
            !all_outcome_ids.contains(receipt_id),
            "distribution receipt {receipt_id} should NOT appear in OutcomeIds"
        );
    }

    // Run enough epochs for GC.
    let current_height = env.chunk_producer_node().client().chain.head().unwrap().height;
    let gc_target = current_height + EPOCH_LENGTH * gc_num_epochs_to_keep + 1;
    env.run_until_head_height(gc_target);

    // Assert all distribution ReceiptToTx entries have been garbage collected.
    let store = env.chunk_producer_node().store();
    for receipt_id in &distribution_receipt_ids {
        assert!(
            store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
            "receipt_to_tx for distribution receipt {receipt_id} should be garbage collected"
        );
    }
}

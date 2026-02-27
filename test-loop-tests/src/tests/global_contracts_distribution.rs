use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::receipt::{ReceiptEnum, ReceiptToTxInfo};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::DBCol;
use near_vm_runner::ContractCode;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_id, create_account_ids, create_validators_spec, validators_spec_clients,
};
use crate::utils::node::TestLoopNode;
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use crate::utils::transactions::{
    call_contract, check_txs, deploy_global_contract, use_global_contract,
};

const EPOCH_LENGTH: BlockHeightDelta = 5;

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
    let deploy_tx = deploy_global_contract(
        &mut env.env.test_loop,
        &env.env.node_datas,
        &env.chunk_producer,
        deploy_user,
        code.code().to_vec(),
        1,
        GlobalContractDeployMode::CodeHash,
    );

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
    for user in &env.users {
        let use_tx = use_global_contract(
            &mut env.env.test_loop,
            &env.env.node_datas,
            &env.chunk_producer,
            user.clone(),
            2,
            GlobalContractIdentifier::CodeHash(*code.hash()),
        );
        use_txs.push(use_tx);
    }
    env.env.test_loop.run_for(Duration::seconds(2));
    check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &use_txs);

    env.shutdown();
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
    let deploy_tx_v1 = deploy_global_contract(
        &mut env.env.test_loop,
        &env.env.node_datas,
        &env.chunk_producer,
        deploy_user.clone(),
        near_test_contracts::trivial_contract().to_vec(),
        1,
        GlobalContractDeployMode::AccountId,
    );
    env.env.test_loop.run_for(Duration::seconds(5));
    check_txs(
        &mut env.env.test_loop.data,
        &env.env.node_datas,
        &env.chunk_producer,
        &[deploy_tx_v1],
    );

    // Step 2: Deploy rs_contract as second version (AccountId mode).
    // This will have a higher auto-incremented nonce.
    tracing::info!(target: "test", "Deploying second version of global contract (rs_contract)...");
    let deploy_tx_v2 = deploy_global_contract(
        &mut env.env.test_loop,
        &env.env.node_datas,
        &env.chunk_producer,
        deploy_user.clone(),
        near_test_contracts::rs_contract().to_vec(),
        2,
        GlobalContractDeployMode::AccountId,
    );
    env.env.test_loop.run_for(Duration::seconds(5));
    check_txs(
        &mut env.env.test_loop.data,
        &env.env.node_datas,
        &env.chunk_producer,
        &[deploy_tx_v2],
    );

    // Step 3: Have all users use the global contract and verify that the rs_contract
    // version (v2) is active by calling "log_something" which only exists in rs_contract.
    tracing::info!(target: "test", "Calling use global contract from all users to verify rs_contract is active...");
    let mut nonce = 3u64;
    for user in &env.users {
        let use_tx = use_global_contract(
            &mut env.env.test_loop,
            &env.env.node_datas,
            &env.chunk_producer,
            user.clone(),
            nonce,
            GlobalContractIdentifier::AccountId(deploy_user.clone()),
        );
        nonce += 1;
        env.env.test_loop.run_for(Duration::seconds(5));
        check_txs(&mut env.env.test_loop.data, &env.env.node_datas, &env.chunk_producer, &[use_tx]);
    }

    // Step 4: Call "log_something" on each user's account. This method only exists in
    // the rs_contract, so if the trivial contract had overwritten it, this would fail.
    tracing::info!(target: "test", "Calling contract method from all users to verify rs_contract is active...");
    for user in &env.users {
        let call_tx = call_contract(
            &mut env.env.test_loop,
            &env.env.node_datas,
            &env.chunk_producer,
            user,
            user,
            "log_something".to_string(),
            vec![],
            nonce,
        );
        nonce += 1;
        env.env.test_loop.run_for(Duration::seconds(5));
        check_txs(
            &mut env.env.test_loop.data,
            &env.env.node_datas,
            &env.chunk_producer,
            &[call_tx],
        );
    }

    env.shutdown();
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

        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .clients(clients)
            .epoch_config_store(epoch_config_store)
            .build()
            .warmup();

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

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}

/// Tests that GlobalContractDistribution receipts have ReceiptToTx entries, including
/// forwarded distribution receipts that hop across shards.
#[test]
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
    let deploy_tx = deploy_global_contract(
        &mut env.env.test_loop,
        &env.env.node_datas,
        &env.chunk_producer,
        deploy_user,
        code.code().to_vec(),
        1,
        GlobalContractDeployMode::CodeHash,
    );

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

    env.shutdown();
}

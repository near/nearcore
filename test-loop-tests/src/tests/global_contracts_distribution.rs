use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::receipt::ReceiptEnum;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::ContractCode;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::account::{
    create_account_id, create_account_ids, create_validators_spec, validators_spec_clients,
};
use crate::utils::node::TestLoopNode;
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use crate::utils::transactions::{check_txs, deploy_global_contract, use_global_contract};

const EPOCH_LENGTH: BlockHeightDelta = 5;

#[test]
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
        let block =
            env.client().chain.get_block_by_height(expected_new_shard_layout_height).unwrap();
        let block_shard_layout = env
            .client()
            .epoch_manager
            .get_epoch_config(block.header().epoch_id())
            .unwrap()
            .shard_layout;
        assert_eq!(block_shard_layout, env.new_shard_layout);
        let chunks = block.chunks();
        // Expect new chunk
        assert!(chunks[0].is_new_chunk(block.header().height()));
        let chunk = env.client().chain.get_chunk(&chunks[0].compute_hash()).unwrap();
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
            .add_user_accounts_simple(&users, 1000_000 * ONE_NEAR)
            .build();

        let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
        let new_epoch_config =
            derive_new_epoch_config_from_boundary(&base_epoch_config, &split_boundary_account);
        let new_shard_layout = new_epoch_config.shard_layout.clone();

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
        TestLoopNode::for_account(&self.env.node_datas, &self.chunk_producer)
            .run_until_head_height(&mut self.env.test_loop, height);
    }

    fn client(&self) -> &Client {
        TestLoopNode::for_account(&self.env.node_datas, &self.chunk_producer)
            .client(self.env.test_loop_data())
    }

    fn current_shard_layout(&self) -> ShardLayout {
        let epoch_id = self.client().chain.chain_store().head().unwrap().epoch_id;
        let epoch_manager = self.client().epoch_manager.clone();
        epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }
}

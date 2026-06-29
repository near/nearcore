use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain::spice::core::all_stake_fallback_assignment;
use near_chain_configs::Genesis;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::BlockHeader;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, AccountInfo, Balance, BlockHeightDelta};
use std::collections::HashSet;

/// Genesis, epoch config, and client accounts for the fallback tests: 14 equal-stake validators
/// over 6 shards with 2 mandates each, keeping each chunk's designated subset under 1/3 of stake.
#[derive(Default)]
struct FallbackSetup {
    epoch_length: Option<BlockHeightDelta>,
    user_accounts: Vec<(AccountId, Balance)>,
}

impl FallbackSetup {
    fn new() -> Self {
        Self::default()
    }

    fn epoch_length(mut self, epoch_length: BlockHeightDelta) -> Self {
        self.epoch_length = Some(epoch_length);
        self
    }

    fn user_account(mut self, account_id: AccountId, balance: Balance) -> Self {
        self.user_accounts.push((account_id, balance));
        self
    }

    fn build(self) -> (Vec<AccountId>, Genesis, EpochConfigStore) {
        let num_producers = 4;
        let num_validators = 14;
        let accounts: Vec<AccountId> =
            (0..num_validators).map(|i| format!("validator{i}").parse().unwrap()).collect_vec();
        let all_validators: Vec<AccountInfo> = accounts
            .iter()
            .map(|account_id| AccountInfo {
                public_key: create_test_signer(account_id.as_str()).public_key(),
                account_id: account_id.clone(),
                amount: Balance::from_near(100),
            })
            .collect();
        let validators_spec =
            ValidatorsSpec::raw(all_validators, num_producers, num_producers, num_validators);

        let mut genesis_builder = TestLoopBuilder::new_genesis_builder()
            .shard_layout(ShardLayout::multi_shard(6, 0))
            .validators_spec(validators_spec);
        if let Some(epoch_length) = self.epoch_length {
            genesis_builder = genesis_builder.epoch_length(epoch_length);
        }
        for (account_id, balance) in self.user_accounts {
            genesis_builder = genesis_builder.add_user_account_simple(account_id, balance);
        }
        let genesis = genesis_builder.build();
        let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
            .target_validator_mandates_per_shard(2)
            .build_store_for_genesis_protocol_version();
        (accounts, genesis, epoch_config_store)
    }
}

/// Asserts every (height, shard) of the genesis epoch has designated validators under 1/3 of total
/// stake, so the fallback's non-designated remainder can reach 2/3. Genesis epoch is representative.
fn assert_fallback_has_enough_stake(node: &TestLoopNode) {
    let epoch_manager = node.client().epoch_manager.clone();
    let epoch_id = node.head().epoch_id;
    let epoch_length = epoch_manager.get_epoch_config(&epoch_id).unwrap().epoch_length;
    let shard_ids: Vec<_> =
        epoch_manager.get_shard_layout(&epoch_id).unwrap().shard_ids().collect();
    let total_stake: u128 = all_stake_fallback_assignment(epoch_manager.as_ref(), &epoch_id)
        .unwrap()
        .assignments()
        .iter()
        .map(|(_, stake)| stake.as_yoctonear())
        .sum();
    for height in 1..=epoch_length {
        for &shard_id in &shard_ids {
            let designated: u128 = epoch_manager
                .get_chunk_validator_assignments(&epoch_id, shard_id, height)
                .unwrap()
                .assignments()
                .iter()
                .map(|(_, stake)| stake.as_yoctonear())
                .sum();
            assert!(
                designated * 3 < total_stake,
                "designated stake reaches 1/3 at height {height} shard {shard_id}: {designated} of {total_stake}",
            );
        }
    }
}

/// Asserts `header`'s block is certified and at least one of its chunks certified via the all-stake
/// fallback: the present designated endorsements alone don't meet the designated 2/3 threshold, so
/// the non-designated remainder must have carried it.
fn assert_certified_via_fallback(node: &TestLoopNode, header: &BlockHeader) {
    let client = node.client();
    let core_reader = &client.chain.spice_core_reader;
    let epoch_manager = client.epoch_manager.as_ref();
    assert!(core_reader.all_execution_results_exist(header).unwrap(), "block is not certified");

    let epoch_id = header.epoch_id();
    let mut fallback_certified_chunks = 0;
    for shard_id in epoch_manager.get_shard_layout(epoch_id).unwrap().shard_ids() {
        let designated = epoch_manager
            .get_chunk_validator_assignments(epoch_id, shard_id, header.height())
            .unwrap();
        let designated_present: HashSet<AccountId> = designated
            .assignments()
            .iter()
            .map(|(account_id, _)| account_id)
            .filter(|account_id| {
                core_reader.get_endorsement(header.hash(), shard_id, account_id).is_some()
            })
            .cloned()
            .collect();
        if !designated.is_endorsed(&designated_present) {
            fallback_certified_chunks += 1;
        }
    }
    assert!(
        fallback_certified_chunks > 0,
        "every chunk met the designated threshold; none certified via the fallback",
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_without_designated_endorsements() {
    init_test_logger();

    let (accounts, genesis, epoch_config_store) = FallbackSetup::new().build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.node(0));

    // The certified frontier must keep advancing via the all-stake fallback though every designated
    // endorsement is dropped (slowly: ~1 block per fallback window under a total outage).
    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(60),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_across_epoch_boundary() {
    init_test_logger();

    let epoch_length = 25;
    let (accounts, genesis, epoch_config_store) =
        FallbackSetup::new().epoch_length(epoch_length).build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .build();
    assert_fallback_has_enough_stake(&env.node(0));

    // Run normally up to just before the epoch boundary, then drop designated endorsements: the
    // all-stake fallback alone must carry certification across into the next epoch.
    env.node_runner(0)
        .run_until(|node| node.head().height >= epoch_length - 5, Duration::seconds(60));
    let initial_epoch = env.node(0).head().epoch_id;
    let mut env = env.drop(DropCondition::DesignatedSpiceEndorsements);

    // The certified frontier must reach the second epoch (height past the boundary, with a different
    // epoch id), proving the fallback certified the boundary chunks under the new assignments.
    env.node_runner(0).run_until(
        |node| {
            let header = node.last_certified_block_header();
            let epoch_id = node.client().epoch_manager.get_epoch_id(header.hash()).unwrap();
            header.height() > epoch_length && epoch_id != initial_epoch
        },
        Duration::seconds(120),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_when_validators_track_all_shards() {
    init_test_logger();

    let (accounts, genesis, epoch_config_store) = FallbackSetup::new().build();

    // Every validator tracks every shard, so non-designated validators self-execute instead of
    // pulling witnesses; their stake must still certify the chunks via the fallback.
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .track_all_shards()
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.node(0));

    let target = env.node(0).last_certified_block_header().height() + 4;
    env.node_runner(0).run_until(
        |node| node.last_certified_block_header().height() >= target,
        Duration::seconds(60),
    );
    let frontier = env.node(0).last_certified_block_header();
    assert_certified_via_fallback(&env.node(0), frontier.as_ref());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_all_stake_fallback_certifies_chunk_accessing_contract_code() {
    init_test_logger();

    let contract_user = create_account_id("contract-user");
    // epoch_length 100 keeps the call chunk (~height 9) within the genesis epoch the precondition checks.
    let (accounts, genesis, epoch_config_store) = FallbackSetup::new()
        .epoch_length(100)
        .user_account(contract_user.clone(), Balance::from_near(100))
        .build();

    // The no-op compiled contract cache forces validators to fetch contract code via a
    // SpiceContractCodeRequest; otherwise the fallback code-request gate is never exercised.
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts)
        .enable_rpc()
        .disable_compiled_contract_cache()
        .build()
        .drop(DropCondition::DesignatedSpiceEndorsements);
    assert_fallback_has_enough_stake(&env.rpc_node());

    // Wait for the deploy to be included before submitting the call, so the call runs against an
    // already-settled contract (`record_contract_call` skips newly-deployed code).
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&contract_user);
    let deploy_hash = deploy_tx.get_hash();
    env.rpc_node().submit_tx(deploy_tx);
    env.rpc_runner().run_until_included(&[deploy_hash]);

    // The call's chunk witness omits the code, so a cold non-designated fallback validator must
    // fetch it via a contract-code request.
    let call_tx = env.rpc_node().tx_call(
        &contract_user,
        &contract_user,
        "log_something",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let call_hash = call_tx.get_hash();
    env.rpc_node().submit_tx(call_tx);
    let call_height = env.rpc_runner().run_until_included(&[call_hash]);

    // Execution advances only as the fallback certifies; wait for the certified frontier to reach
    // the call chunk. Pre-fix its code requests are rejected, so the frontier stalls below it.
    env.rpc_runner().run_until(
        |node| node.last_certified_block_header().height() >= call_height,
        Duration::seconds(120),
    );
    let frontier = env.rpc_node().last_certified_block_header();
    assert_certified_via_fallback(&env.rpc_node(), frontier.as_ref());
}

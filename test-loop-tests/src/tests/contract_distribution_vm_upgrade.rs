use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::action::{Action, DeployContractAction, FunctionCallAction};
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::Balance;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION};

/// The first chunk of an epoch replays the previous epoch's last chunk, so its accessed
/// contracts run under the previous epoch's wasm config. A contract compiled only under the
/// new config, from the deploys sent with that first chunk, must still be requested.
/// Here the first chunk's producer sends the deploys but withholds the witness and the
/// accesses, so the next witnesses replay the same old-config chunk.
#[test]
fn slow_test_withheld_first_chunk_after_vm_config_upgrade_does_not_stall_shard() {
    init_test_logger();

    let runtime_config_store = RuntimeConfigStore::new();
    let new_protocol = runtime_config_store.vm_generation(PROTOCOL_VERSION).0;
    let old_protocol = new_protocol - 1;
    // The contract is deployed under `stale_protocol`, so no node holds it compiled under
    // `old_protocol`'s config.
    let stale_protocol = old_protocol - 1;
    assert!(
        stale_protocol >= MIN_SUPPORTED_PROTOCOL_VERSION,
        "needs three supported versions ending at a wasm-config change; update or remove this test when bumping MIN_SUPPORTED_PROTOCOL_VERSION",
    );
    assert_ne!(
        runtime_config_store.get_config(old_protocol).wasm_config.non_crypto_hash(),
        runtime_config_store.get_config(new_protocol).wasm_config.non_crypto_hash(),
    );

    let contract = create_account_id("contract");
    let shard_layout = ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1);
    let contract_shard = shard_layout.account_id_to_shard_id(&contract);
    let contract_shard_index = shard_layout.get_shard_index(contract_shard).unwrap();
    let vote_date = |date| ProtocolUpgradeVotingSchedule::parse_datetime(date).unwrap();
    let upgrade_schedule = ProtocolUpgradeVotingSchedule::new_from_env_or_schedule(
        stale_protocol,
        new_protocol,
        // Both dates precede the test loop's clock, so each epoch votes for the next version.
        vec![
            (vote_date("1970-01-01 00:00:01"), old_protocol),
            (vote_date("1970-01-01 00:00:02"), new_protocol),
        ],
    )
    .unwrap();
    let mut env = TestLoopBuilder::new()
        .validators(4, 4)
        .shard_layout(shard_layout)
        .epoch_length(10)
        .protocol_version(stale_protocol)
        .protocol_upgrade_schedule(upgrade_schedule)
        .runtime_config_store(runtime_config_store)
        .add_user_account(&contract, Balance::from_near(1_000_000))
        .build();

    let code = near_test_contracts::trivial_contract().to_vec();
    let deploy = env.node(0).tx_deploy_contract(&contract, code.clone());
    env.node_runner(0).run_tx(deploy, Duration::seconds(10));
    assert_eq!(env.node(0).protocol_version_at_head(), stale_protocol);

    let mut upgrade_height = None;
    env.node_runner(0).run_until(
        |node| {
            upgrade_height = node
                .client()
                .epoch_manager
                .get_estimated_protocol_upgrade_block_height(node.head().last_block_hash)
                .unwrap();
            node.protocol_version_at_head() == old_protocol && upgrade_height.is_some()
        },
        Duration::seconds(120),
    );
    let upgrade_height = upgrade_height.unwrap();
    // A transaction submitted at this head lands in the last block of `old_protocol`.
    env.node_runner(0).run_until_head_height(upgrade_height - 3);

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let withheld_chunk = ChunkProductionKey {
        shard_id: contract_shard,
        epoch_id: epoch_manager.get_next_epoch_id(&env.node(0).head().last_block_hash).unwrap(),
        height_created: upgrade_height,
    };
    assert_eq!(
        epoch_manager.get_epoch_protocol_version(&withheld_chunk.epoch_id).unwrap(),
        new_protocol
    );
    let producer =
        epoch_manager.get_chunk_producer_info(&withheld_chunk).unwrap().take_account_id();
    env.node_datas[env.account_data_idx(&producer)].register_override_handler(
        &mut env.test_loop.data,
        move |request| {
            let withheld = match &request {
                NetworkRequests::PartialEncodedStateWitness(parts) => {
                    parts.iter().all(|(_, part)| part.chunk_production_key() == withheld_chunk)
                }
                NetworkRequests::ChunkContractAccesses(_, accesses) => {
                    accesses.chunk_production_key() == &withheld_chunk
                }
                _ => false,
            };
            if withheld {
                HandlerResult::Handled(NetworkResponses::NoResponse)
            } else {
                HandlerResult::Unhandled(request)
            }
        },
    );

    // The call puts the contract in the chunk's accesses, the redeploy in its deploys.
    let call_then_redeploy = env.node(0).tx_from_actions(
        &contract,
        &contract,
        vec![
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "main".to_string(),
                args: vec![],
                gas: Gas::from_teragas(30),
                deposit: Balance::ZERO,
            })),
            Action::DeployContract(DeployContractAction { code }),
        ],
    );
    let tx_hash = env.node(0).submit_tx(call_then_redeploy);
    let last_height = upgrade_height + 5;
    env.node_runner(0).run_until_head_height(last_height);

    let node = env.node(0);
    let tx_block = node.block(node.execution_outcome_with_proof(tx_hash).block_hash);
    assert_eq!(tx_block.header().height(), upgrade_height - 1);
    let chain_store = node.client().chain.chain_store();
    let chunk_included = |height| {
        chain_store.get_block_header_by_height(height).unwrap().chunk_mask()[contract_shard_index]
    };
    assert!(!chunk_included(upgrade_height), "the first chunk of the new epoch is withheld");
    assert!(
        (upgrade_height + 1..=last_height).any(chunk_included),
        "the contract's shard must include a chunk after the withheld one",
    );
}

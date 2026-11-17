use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::account::rpc_account_id;
use crate::utils::contract_distribution::{
    clear_compiled_contract_caches, run_until_caches_contain_contract,
};
use crate::utils::transactions::make_accounts;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ContractCodeRequest, MainTransitionKey,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, Balance};
use near_test_contracts::sized_contract;
use near_vm_runner::ContractCode;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 1;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

/// Enough back-to-back contract calls to produce a stream of
/// `ChunkContractAccesses` messages with `main_transition`s spaced only ~1
/// chunk apart, well inside the finality window so the "previous"
/// `main_transition` is still in storage when the forged request lands on
/// the producer.
const NUM_CONTRACT_CALLS: usize = 5;

/// Regression test for the `derive_main_transition_key` check in
/// `handle_contract_code_request`: a `ContractCodeRequest` whose
/// `main_transition` does not match the one derived from chain state must
/// not produce a `ContractCodeResponse`, even when the forged key still
/// points at valid transition data.
///
/// Mechanism: register an override handler on every node's peer manager.
/// On every outgoing `ChunkContractAccesses` after the first, replace it
/// with a `ContractCodeRequest` carrying the *previous* chunk's
/// `main_transition` (signed by a real chunk validator so admission checks
/// pass). Since the original accesses message is dropped, no legitimate
/// request for that chunk's key is ever sent — any observed response
/// bearing a forged key must be the handler accepting a mismatched
/// `main_transition`.
///
/// Correctness note: the forged `main_transition` must still be in storage
/// at the moment the producer processes the request, otherwise the request
/// gets rejected by the storage lookup rather than by the derive check and
/// the test would pass even without the fix. Using the *immediately
/// preceding* chunk's `main_transition` keeps the forged key within the
/// finality window.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_contract_code_request_rejects_tampered_main_transition() {
    init_test_logger();

    let accounts = make_accounts(NUM_VALIDATORS + 1);
    let mut env = setup(&accounts);

    let contract_id = accounts[NUM_VALIDATORS].clone();
    let contract = ContractCode::new(sized_contract(100), None);

    let tx = env.rpc_node().tx_deploy_contract(&contract_id, contract.code().to_vec());
    env.rpc_runner().run_tx(tx, Duration::seconds(5));
    run_until_caches_contain_contract(&mut env, contract.hash());

    let last_main_transition: Arc<Mutex<Option<MainTransitionKey>>> = Arc::new(Mutex::new(None));
    // Chunk production keys the handler injected a forged request for. A
    // response bearing one of these keys means the fix failed.
    let forged_request_keys: Arc<Mutex<HashSet<ChunkProductionKey>>> =
        Arc::new(Mutex::new(HashSet::new()));
    let forged_request_responses = Arc::new(AtomicUsize::new(0));

    let chunk_producer_account = accounts[0].clone();
    let forging_validator = accounts[1].clone();

    for node_data in &env.node_datas {
        let last_transition = last_main_transition.clone();
        let forged_keys = forged_request_keys.clone();
        let forged_response_count = forged_request_responses.clone();
        let chunk_producer_account = chunk_producer_account.clone();
        let forging_validator = forging_validator.clone();
        node_data.register_override_handler(&mut env.test_loop.data, move |request| {
            let NetworkRequests::ChunkContractAccesses(_, accesses) = &request else {
                if let NetworkRequests::ContractCodeResponse(_, response) = &request {
                    if forged_keys.lock().contains(response.chunk_production_key()) {
                        forged_response_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
                return HandlerResult::Unhandled(request);
            };

            let contracts: HashSet<_> = accesses.contracts().iter().cloned().collect();
            if contracts.is_empty() {
                return HandlerResult::Unhandled(request);
            }

            let current_main_transition = accesses.main_transition().clone();
            let mut guard = last_transition.lock();
            let maybe_stale = guard.clone();
            *guard = Some(current_main_transition.clone());
            drop(guard);

            let Some(stale_main_transition) = maybe_stale else {
                return HandlerResult::Unhandled(request);
            };
            // Nothing to tamper if the key already matches the correct one.
            if stale_main_transition == current_main_transition {
                return HandlerResult::Unhandled(request);
            }

            // Replace the accesses message with a `ContractCodeRequest`
            // bearing the previous chunk's `main_transition`. The accesses
            // message is dropped, so the chunk validator never sends its
            // own legitimate request for this chunk.
            let key = accesses.chunk_production_key().clone();
            forged_keys.lock().insert(key.clone());
            let signer = create_test_signer(forging_validator.as_str());
            let forged_request =
                ContractCodeRequest::new(key, contracts, stale_main_transition, &signer);
            HandlerResult::Unhandled(NetworkRequests::ContractCodeRequest(
                chunk_producer_account.clone(),
                forged_request,
            ))
        });
    }

    clear_compiled_contract_caches(&env);

    // Sequential contract calls, each given a couple of blocks to land in
    // its own chunk. Spacing them this way yields a stream of
    // `ChunkContractAccesses` messages with distinct `main_transition`s only
    // ~1 chunk apart — inside the finality window, so the forged key is
    // still in storage when the producer processes the request.
    for _ in 0..NUM_CONTRACT_CALLS {
        let tx = env.rpc_node().tx_call(
            &contract_id,
            &contract_id,
            "main",
            vec![],
            Balance::ZERO,
            Gas::from_teragas(300),
        );
        env.rpc_node().submit_tx(tx);
        env.node_runner(0).run_for_number_of_blocks(2);
    }
    // Extra blocks so the last forged request has a chance to reach the
    // producer and (without the fix) produce a response before we assert.
    env.node_runner(0).run_for_number_of_blocks(3);

    let forged = forged_request_keys.lock().len();
    let forged_responses = forged_request_responses.load(Ordering::SeqCst);

    assert!(forged > 0, "expected at least one forged ContractCodeRequest to be injected");
    assert_eq!(
        forged_responses, 0,
        "expected no ContractCodeResponse for forged requests, but got {}",
        forged_responses
    );
}

fn setup(accounts: &Vec<AccountId>) -> TestLoopEnv {
    let builder = TestLoopBuilder::new();

    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();

    let mut clients = accounts.iter().take(NUM_VALIDATORS).cloned().collect_vec();
    clients.push(rpc_account_id());

    let shard_layout = ShardLayout::single_shard();
    let validators_spec =
        ValidatorsSpec::desired_roles(&block_and_chunk_producers, &chunk_validators_only);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .add_user_account_simple(rpc_account_id(), Balance::from_near(1_000_000))
        .genesis_height(GENESIS_HEIGHT)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build()
}

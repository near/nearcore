use super::*;
use crate::bandwidth_scheduler::run_bandwidth_scheduler;
use crate::congestion_control::{DelayedReceiptQueueWrapper, ReceiptSink};
use crate::contract_compilation_status::{
    try_admit_contract_compilation, update_contract_compilation_status,
};
use crate::{ApplyProcessingReceiptState, ApplyProcessingState, ProcessingResult};
use near_primitives::types::ContractCompilationStatus;
use near_store::trie::receipts_column_helper::DelayedReceiptQueue;
use near_store::{TrieUpdate, get};
use near_test_contracts::wat_contract;
use near_vm_runner::{ContractPrecompilatonResult, precompile_contract};
use std::mem::replace;

struct Fixture {
    runtime: Runtime,
    tries: ShardTries,
    root: CryptoHash,
    state: ApplyState,
    signers: Vec<Arc<Signer>>,
    epochs: MockEpochInfoProvider,
}

fn setup() -> Fixture {
    let (runtime, tries, root, mut state, signers, epochs) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
    state.current_protocol_version = ProtocolFeature::ContractCompilationStatus.protocol_version();
    let configs = RuntimeConfigStore::new();
    state.config = configs.get_config(state.current_protocol_version).clone();
    state.vm_generation = configs.vm_generation(state.current_protocol_version);
    Fixture { runtime, tries, root, state, signers, epochs }
}

impl Fixture {
    fn receipt(&self, account: usize, actions: Vec<Action>) -> Receipt {
        let receiver = if account == 0 { alice_account() } else { bob_account() };
        create_receipt_with_actions(receiver, self.signers[account].clone(), actions)
    }

    fn apply(&mut self, receipts: &[Receipt]) -> ApplyResult {
        let result = self
            .runtime
            .apply(
                self.tries
                    .get_trie_for_shard(ShardUId::single_shard(), self.root)
                    .recording_reads_new_recorder(),
                &None,
                &self.state,
                receipts,
                SignedValidPeriodTransactions::empty(),
                &self.epochs,
                Default::default(),
            )
            .unwrap();
        let mut update = self.tries.store_update();
        self.root =
            self.tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut update);
        update.commit();
        if let Some(info) = &result.congestion_info {
            self.state
                .congestion_info
                .insert(self.state.shard_id, ExtendedCongestionInfo::new(*info, 0));
        }
        result
    }

    /// Execute without persisting, so completed and deferred attempts can share a pre-state.
    fn process_receipt_for_compute(
        &self,
        receipt: &Receipt,
    ) -> (ProcessingResult<()>, ApplyProcessingReceiptState<'_>) {
        let trie = self.tries.get_trie_for_shard(ShardUId::single_shard(), self.root);
        let mut processing = ApplyProcessingState::new(&self.state, trie, &self.epochs);
        let delayed = DelayedReceiptQueueWrapper::new(
            DelayedReceiptQueue::load(&processing.state_update).unwrap(),
            &self.epochs,
            self.state.shard_id,
            self.state.epoch_id,
        );
        let bandwidth = run_bandwidth_scheduler(
            &self.state,
            &mut processing.state_update,
            &self.epochs,
            &mut processing.stats.bandwidth_scheduler,
        )
        .unwrap();
        let mut processing = processing.into_processing_receipt_state(&[], delayed);
        let congestion = self.state.own_congestion_info(&processing.state_update).unwrap();
        let mut sink = ReceiptSink::new(
            &processing.state_update.trie,
            &self.state,
            congestion,
            bandwidth,
            &self.epochs,
        )
        .unwrap();
        let result = self
            .runtime
            .process_receipt_with_metrics(receipt, &mut processing, &mut sink, &mut Vec::new())
            .unwrap();
        (result, processing)
    }

    fn compilation_status(&self, code: &[u8]) -> Option<ContractCompilationStatus> {
        let state = self.tries.new_trie_update(ShardUId::single_shard(), self.root);
        get(&state, &TrieKey::ContractCompilationStatus { code_hash: hash(code) }).unwrap()
    }

    fn assert_cached(&self, code: &[u8]) {
        assert_eq!(
            self.compilation_status(code),
            Some(ContractCompilationStatus::Cached { vm_generation: self.state.vm_generation })
        );
    }

    fn assert_precompiled(&self, code: &[u8]) {
        let code = ContractCode::new(code.to_vec(), None);
        assert_matches!(
            precompile_contract(
                &code,
                Arc::clone(&self.state.config.wasm_config),
                self.state.cache.as_deref(),
            ),
            Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache))
        );
    }

    // Simulate code deployed before activation: no implicit warmness for old code.
    fn install_cold(&mut self, receipts: &[Receipt]) {
        self.state.current_protocol_version -= 1;
        let result = self.apply(receipts);
        assert_eq!(result.cold_contract_bytes, 0);
        assert!(
            result
                .outcomes
                .iter()
                .all(|o| !matches!(o.outcome.status, ExecutionStatus::Failure(_)))
        );
        self.state.current_protocol_version += 1;
    }
}

fn deploy(code: &[u8]) -> Action {
    Action::DeployContract(DeployContractAction { code: code.to_vec() })
}

fn call(method: &str) -> Action {
    Action::function_call(method, vec![], Gas::from_teragas(30))
}

fn use_global(id: GlobalContractIdentifier) -> Action {
    Action::UseGlobalContract(Box::new(UseGlobalContractAction { contract_identifier: id }))
}

fn distribute(id: GlobalContractIdentifier, code: &[u8]) -> Receipt {
    Receipt::new_global_contract_distribution(
        alice_account(),
        GlobalContractDistributionReceipt::new(
            id,
            ShardId::new(0),
            vec![],
            code.to_vec().into(),
            0,
        ),
    )
}

#[test]
fn status_proof_is_recorded_before_finalization_and_survives_rollback() {
    let mut f = setup();
    let code = near_test_contracts::trivial_contract();
    let install = f.receipt(0, vec![deploy(code)]);
    f.apply(&[install]);

    // Exercise both an existing status and an absent status in a populated subtree.
    for (code_hash, expected_cold_bytes) in [(hash(code), 0), (hash(b"cold code"), 9)] {
        let trie = f
            .tries
            .get_trie_for_shard(ShardUId::single_shard(), f.root)
            .recording_reads_new_recorder();
        let mut update = TrieUpdate::new(trie);
        let before = update.trie.recorded_storage_size_upper_bound();
        try_admit_contract_compilation(&mut update, &f.state, code_hash, 9).unwrap();
        let after = update.trie.recorded_storage_size_upper_bound();
        assert!(after > before, "status proof must be recorded during execution");

        // Repeated reads do not grow the proof or double-count cold bytes.
        try_admit_contract_compilation(&mut update, &f.state, code_hash, 9).unwrap();
        assert_eq!(update.trie.recorded_storage_size_upper_bound(), after);
        update.rollback();
        assert_eq!(
            update_contract_compilation_status(&mut update, &f.state).unwrap(),
            expected_cold_bytes
        );
        assert_eq!(update.trie.recorded_storage_size_upper_bound(), after);
        let key = TrieKey::ContractCompilationStatus { code_hash };
        assert_eq!(
            get(&update, &key).unwrap(),
            Some(ContractCompilationStatus::Cached { vm_generation: f.state.vm_generation })
        );
    }
}

#[test]
fn deduplicates_across_accounts_and_survives_cache_eviction() {
    let mut f = setup();
    let code = near_test_contracts::trivial_contract();
    let installs = [f.receipt(0, vec![deploy(code)]), f.receipt(1, vec![deploy(code)])];
    f.install_cold(&installs);
    assert_eq!(f.compilation_status(code), None);
    let calls = [f.receipt(0, vec![call("main"), call("main")]), f.receipt(1, vec![call("main")])];
    let pre_root = f.root;
    let pre_congestion = f.state.congestion_info.clone();
    let result = f.apply(&calls);
    assert_eq!(result.cold_contract_bytes, code.len() as u64);
    f.assert_cached(code);

    // Validation records the same protocol warmness even though it does not
    // populate the producer's contract-distribution tracker.
    f.state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;
    let next_congestion = replace(&mut f.state.congestion_info, pre_congestion);
    let replay = f
        .runtime
        .apply(
            Trie::from_recorded_storage(result.proof.unwrap(), pre_root, false),
            &None,
            &f.state,
            &calls,
            SignedValidPeriodTransactions::empty(),
            &f.epochs,
            Default::default(),
        )
        .unwrap();
    assert_eq!(replay.state_root, result.state_root);
    assert_eq!(replay.cold_contract_bytes, result.cold_contract_bytes);
    f.state.apply_reason = ApplyChunkReason::UpdateTrackedShard;
    f.state.congestion_info = next_congestion;

    f.state.cache = Some(Box::new(NoContractRuntimeCache));
    assert_eq!(f.apply(&calls).cold_contract_bytes, 0);
    // A new VM generation replaces the stale value at the same code-hash key.
    let old_generation = f.state.vm_generation;
    f.state.vm_generation.0 += 1;
    assert_eq!(
        f.compilation_status(code),
        Some(ContractCompilationStatus::Cached { vm_generation: old_generation })
    );
    assert_eq!(f.apply(&calls).cold_contract_bytes, code.len() as u64);
    f.assert_cached(code);
    assert_eq!(f.apply(&calls).cold_contract_bytes, 0);
    // The old generation is no longer retained, so switching back is cold, too.
    f.state.vm_generation = old_generation;
    assert_eq!(f.apply(&calls).cold_contract_bytes, code.len() as u64);
    f.assert_cached(code);
}

#[test]
fn counts_all_global_switches_and_local_code_in_one_receipt() {
    let mut f = setup();
    let local = near_test_contracts::trivial_contract();
    let global = near_test_contracts::rs_contract();
    let by_hash = GlobalContractIdentifier::CodeHash(hash(global));
    let by_account = GlobalContractIdentifier::AccountId(bob_account());
    let installs = [
        f.receipt(0, vec![deploy(local)]),
        distribute(by_hash.clone(), global),
        distribute(by_account.clone(), local),
    ];
    f.install_cold(&installs);
    let receipt = f.receipt(
        0,
        vec![
            call("main"),
            use_global(by_hash.clone()),
            call("log_something"),
            use_global(by_account),
            call("main"),
            use_global(by_hash),
            call("log_something"),
        ],
    );
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
    assert_eq!(result.cold_contract_bytes, (local.len() + global.len()) as u64);
    f.assert_cached(local);
    f.assert_cached(global);
}

/// Three distinct, small contracts keep budget tests fast while exercising the
/// same admission boundary as the production 4 MiB limit.
fn budget_contracts() -> [Vec<u8>; 3] {
    ["", "nop", "nop nop"]
        .map(|body| wat_contract(&format!("(module (func (export \"main\") {body}))")))
}

#[test]
fn pipeline_submission_limit_falls_back_to_demand_preparation() {
    let mut f = setup();
    let [first, second, _] = budget_contracts();
    f.apply(&[f.receipt(0, vec![deploy(&first)]), f.receipt(1, vec![deploy(&second)])]);
    // Keep consensus admission out of the way: only speculative preparation is limited.
    f.assert_cached(&first);
    f.assert_cached(&second);
    // Either contract fits alone, but their combined size exceeds the pipeline cap.
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk =
        first.len().max(second.len()) as u64;
    let receipts = [f.receipt(0, vec![call("main")]), f.receipt(1, vec![call("main")])];

    let trie = f.tries.get_trie_for_shard(ShardUId::single_shard(), f.root);
    let mut processing = ApplyProcessingState::new(&f.state, trie, &f.epochs);
    let delayed = DelayedReceiptQueueWrapper::new(
        DelayedReceiptQueue::load(&processing.state_update).unwrap(),
        &f.epochs,
        f.state.shard_id,
        f.state.epoch_id,
    );
    let bandwidth = run_bandwidth_scheduler(
        &f.state,
        &mut processing.state_update,
        &f.epochs,
        &mut processing.stats.bandwidth_scheduler,
    )
    .unwrap();
    let mut processing = processing.into_processing_receipt_state(&receipts, delayed);
    let congestion = f.state.own_congestion_info(&processing.state_update).unwrap();
    let mut sink =
        ReceiptSink::new(&processing.state_update.trie, &f.state, congestion, bandwidth, &f.epochs)
            .unwrap();

    assert!(processing.pipeline_manager.submit(&receipts[0], &processing.state_update));
    assert!(!processing.pipeline_manager.submit(&receipts[1], &processing.state_update));
    for receipt in &receipts {
        assert_matches!(
            f.runtime.process_receipt_with_metrics(
                receipt,
                &mut processing,
                &mut sink,
                &mut Vec::new(),
            ),
            Ok(ProcessingResult::Processed(()))
        );
    }
    assert_eq!(processing.outcomes.len(), receipts.len());
    for (outcome, receipt) in processing.outcomes.iter().zip(&receipts) {
        assert_eq!(outcome.id, *receipt.receipt_id());
        assert_eq!(outcome.outcome.status, ExecutionStatus::SuccessValue(vec![]));
    }
    assert!(!processing.state_update.contract_compilation_limit_reached());
    assert_eq!(
        update_contract_compilation_status(&mut processing.state_update, &f.state).unwrap(),
        0
    );
}

#[test]
fn multi_contract_receipt_compiles_across_chunks_and_preserves_fifo() {
    let mut f = setup();
    assert_eq!(f.state.config.max_cold_contract_bytes_per_chunk, 4 * 1024 * 1024);
    // Every deployable contract fits individually, so a cold receipt can always
    // make progress in an otherwise empty chunk.
    assert!(
        f.state.config.wasm_config.limit_config.max_contract_size
            <= f.state.config.max_cold_contract_bytes_per_chunk
    );
    let codes = budget_contracts();
    let ids = codes.each_ref().map(|code| GlobalContractIdentifier::CodeHash(hash(code)));
    f.install_cold(&[
        f.receipt(0, vec![deploy(&codes[0])]),
        distribute(ids[1].clone(), &codes[1]),
        distribute(ids[2].clone(), &codes[2]),
    ]);
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = codes[2].len() as u64;
    let receipt = f.receipt(
        0,
        vec![
            call("main"),
            call("main"), // Same-chunk duplicates do not consume budget.
            use_global(ids[1].clone()),
            call("main"),
            use_global(ids[2].clone()),
            call("main"),
        ],
    );
    let tail = f.receipt(0, vec![call("main")]);
    for chunk in 0..3 {
        let incoming = if chunk == 0 { vec![receipt.clone(), tail.clone()] } else { vec![] };
        let pre_root = f.root;
        let pre_congestion = f.state.congestion_info.clone();
        let result = f.apply(&incoming);
        assert_eq!(result.cold_contract_bytes, codes[chunk].len() as u64);
        for (index, code) in codes.iter().enumerate() {
            if index <= chunk {
                f.assert_cached(code);
            } else {
                assert_eq!(f.compilation_status(code), None);
            }
        }
        if chunk < 2 {
            assert!(result.outcomes.is_empty());
            assert!(result.outgoing_receipts.is_empty());
            assert_eq!(result.delayed_receipts_count, 2);
            let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
            assert_eq!(
                get_account(&state, &alice_account()).unwrap().unwrap().local_contract_hash(),
                Some(hash(&codes[0])),
                "actions must not execute while compilation progress persists"
            );
        } else {
            assert_eq!(result.delayed_receipts_count, 0);
            assert_eq!(result.outcomes.len(), 2);
            assert_eq!(result.outcomes[0].id, *receipt.receipt_id());
            assert_eq!(result.outcomes[1].id, *tail.receipt_id());
            assert!(
                result.outcomes.iter().all(|outcome| matches!(
                    outcome.outcome.status,
                    ExecutionStatus::SuccessValue(_)
                ))
            );
        }

        // A validator with only the recorded witness reaches the same partial
        // compilation statuses, queue contents and eventual receipt outcomes.
        f.state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;
        let next_congestion = replace(&mut f.state.congestion_info, pre_congestion);
        let replay = f
            .runtime
            .apply(
                Trie::from_recorded_storage(result.proof.unwrap(), pre_root, false),
                &None,
                &f.state,
                &incoming,
                SignedValidPeriodTransactions::empty(),
                &f.epochs,
                Default::default(),
            )
            .unwrap();
        assert_eq!(replay.state_root, result.state_root);
        assert_eq!(replay.cold_contract_bytes, result.cold_contract_bytes);
        f.state.apply_reason = ApplyChunkReason::UpdateTrackedShard;
        f.state.congestion_info = next_congestion;
    }
}

#[test]
fn preflight_defers_without_executing_actions_or_charging_loading_compute() {
    let mut f = setup();
    let codes = budget_contracts();
    let id = GlobalContractIdentifier::CodeHash(hash(&codes[1]));
    f.install_cold(&[f.receipt(0, vec![deploy(&codes[0])]), distribute(id.clone(), &codes[1])]);
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = codes[1].len() as u64;

    // The prefix alone executes, but must not execute speculatively when a
    // later contract prevents admission of the complete receipt.
    let prefix = f.receipt(0, vec![call("main")]);
    let (result, completed) = f.process_receipt_for_compute(&prefix);
    assert_matches!(result, ProcessingResult::Processed(()));
    let prefix_compute = completed.total.compute;
    assert!(prefix_compute > 0);
    assert!(completed.total.gas > 0);
    drop(completed);

    let switch = use_global(id);
    let deferred_call = call("main");
    let receipt = f.receipt(0, vec![call("main"), switch, deferred_call]);
    let (result, deferred) = f.process_receipt_for_compute(&receipt);
    assert_matches!(result, ProcessingResult::Deferred);
    assert_eq!(deferred.total.compute, 0);
    assert_eq!(deferred.total.gas, 0);
    assert!(deferred.outcomes.is_empty());
    assert_eq!(
        get_account(&deferred.state_update, &alice_account())
            .unwrap()
            .unwrap()
            .local_contract_hash(),
        Some(hash(&codes[0])),
    );
    drop(deferred);

    // A later deployment also gates execution of the entire receipt.
    let deferred_deploy = deploy(&codes[1]);
    let receipt = f.receipt(0, vec![call("main"), deferred_deploy]);
    let (result, deferred) = f.process_receipt_for_compute(&receipt);
    assert_matches!(result, ProcessingResult::Deferred);
    assert_eq!(deferred.total.compute, 0);
    assert_eq!(deferred.total.gas, 0);
    assert!(deferred.outcomes.is_empty());
}

#[test]
fn deployments_defer_without_committing_actions() {
    let mut f = setup();
    let codes = budget_contracts();
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = codes[2].len() as u64;
    let receipt = f.receipt(0, vec![deploy(&codes[0]), deploy(&codes[1]), call("main")]);
    let first = f.apply(&[receipt]);
    assert!(first.outcomes.is_empty());
    assert_eq!(first.cold_contract_bytes, codes[0].len() as u64);
    f.assert_cached(&codes[0]);
    f.assert_precompiled(&codes[0]);
    assert_eq!(f.compilation_status(&codes[1]), None);
    let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
    assert!(get_account(&state, &alice_account()).unwrap().unwrap().contract().is_none());
    let second = f.apply(&[]);
    assert_eq!(second.cold_contract_bytes, codes[1].len() as u64);
    assert_eq!(second.outcomes.len(), 1);
    assert_matches!(second.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
    assert_eq!(second.delayed_receipts_count, 0);
}

#[test]
fn calls_before_and_after_deployment_use_different_contracts() {
    let mut f = setup();
    let codes = budget_contracts();
    f.install_cold(&[f.receipt(0, vec![deploy(&codes[0])])]);
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = codes[1].len() as u64;
    let receipt = f.receipt(0, vec![call("main"), deploy(&codes[1]), call("main")]);
    let first = f.apply(&[receipt]);
    assert!(first.outcomes.is_empty());
    assert_eq!(first.cold_contract_bytes, codes[0].len() as u64);
    let second = f.apply(&[]);
    assert_eq!(second.cold_contract_bytes, codes[1].len() as u64);
    assert_matches!(second.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
    let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
    assert_eq!(
        get_account(&state, &alice_account()).unwrap().unwrap().local_contract_hash(),
        Some(hash(&codes[1]))
    );
}

#[test]
fn state_initialization_contracts_are_admitted_before_creating_accounts() {
    for universal in [false, true] {
        let mut f = setup();
        let codes = budget_contracts();
        let id = GlobalContractIdentifier::CodeHash(hash(&codes[0]));
        f.install_cold(&[distribute(id.clone(), &codes[0])]);
        Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk =
            codes[1].len() as u64;
        let (receiver, init) = if universal {
            let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
                code: Some(id),
                data: BTreeMap::new(),
                access_keys: BTreeSet::new(),
            });
            (
                state_init.derive_account_id(),
                Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                    state_init: state_init.to_raw(),
                    deposit: Balance::from_near(1),
                })),
            )
        } else {
            let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
                code: id,
                data: BTreeMap::new(),
            });
            (
                derive_near_deterministic_account_id(&state_init),
                Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
                    state_init,
                    deposit: Balance::from_near(1),
                })),
            )
        };
        let receipt = create_receipt_with_actions(
            receiver.clone(),
            f.signers[0].clone(),
            vec![init.clone(), init, call("main")],
        );
        let first = f.apply(&[f.receipt(0, vec![deploy(&codes[1])]), receipt]);
        assert_eq!(first.delayed_receipts_count, 1);
        assert_eq!(f.compilation_status(&codes[0]), None);
        let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
        assert!(get_account(&state, &receiver).unwrap().is_none());
        let second = f.apply(&[]);
        assert_eq!(second.delayed_receipts_count, 0);
        assert_eq!(second.cold_contract_bytes, codes[0].len() as u64);
        assert_matches!(second.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
        let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
        assert_eq!(
            get_account(&state, &receiver).unwrap().unwrap().contract().as_ref(),
            &AccountContract::Global(hash(&codes[0]))
        );
    }
}

#[test]
fn missing_global_remains_an_action_error() {
    let mut f = setup();
    let receipt = f.receipt(
        0,
        vec![use_global(GlobalContractIdentifier::AccountId(bob_account())), call("main")],
    );
    let result = f.apply(&[receipt]);
    assert_eq!(result.cold_contract_bytes, 0);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
}

#[test]
fn global_distribution_obeys_compilation_budget() {
    let mut f = setup();
    let codes = budget_contracts();
    Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = codes[1].len() as u64;
    let ids = codes.each_ref().map(|code| GlobalContractIdentifier::CodeHash(hash(code)));
    let first =
        f.apply(&[distribute(ids[0].clone(), &codes[0]), distribute(ids[1].clone(), &codes[1])]);
    assert_eq!(first.cold_contract_bytes, codes[0].len() as u64);
    assert_eq!(first.delayed_receipts_count, 1);
    f.assert_cached(&codes[0]);
    assert_eq!(f.compilation_status(&codes[1]), None);
    let second = f.apply(&[]);
    assert_eq!(second.cold_contract_bytes, codes[1].len() as u64);
    assert_eq!(second.delayed_receipts_count, 0);
    f.assert_cached(&codes[1]);
}

#[test]
fn failed_receipt_conservatively_warms_unreachable_code() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let later = near_test_contracts::rs_contract();
    let id = GlobalContractIdentifier::CodeHash(hash(later));
    let installs = [f.receipt(0, vec![deploy(first)]), distribute(id.clone(), later)];
    f.install_cold(&installs);
    let receipt = f.receipt(0, vec![call("missing_method"), use_global(id), call("log_something")]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, first.len().checked_add(later.len()).unwrap() as u64);
    f.assert_cached(first);
    f.assert_cached(later);
}

#[test]
fn deployments_survive_rollback_and_count_every_code() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let second = near_test_contracts::rs_contract();
    let receipt = f.receipt(0, vec![deploy(first), deploy(second), call("missing_method")]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, first.len().checked_add(second.len()).unwrap() as u64);
    f.assert_cached(first);
    f.assert_cached(second);
    let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
    assert!(get_account(&state, &alice_account()).unwrap().unwrap().contract().is_none());
}

#[test]
fn global_distribution_and_compilation_errors_are_warm() {
    let mut f = setup();
    let invalid = b"not wasm";
    let distribution = distribute(GlobalContractIdentifier::CodeHash(hash(invalid)), invalid);
    assert_eq!(f.apply(&[distribution]).cold_contract_bytes, invalid.len() as u64);
    f.assert_cached(invalid);
    let receipt = f.receipt(
        0,
        vec![use_global(GlobalContractIdentifier::CodeHash(hash(invalid))), call("main")],
    );
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, 0);
}

#[test]
fn preflight_admits_calls_and_deploys_even_after_a_loading_abort() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let second = near_test_contracts::rs_contract();
    f.install_cold(&[f.receipt(0, vec![deploy(first)])]);
    let receipt =
        f.receipt(0, vec![Action::function_call("main", vec![], Gas::from_gas(1)), deploy(second)]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, first.len().checked_add(second.len()).unwrap() as u64);
    f.assert_cached(first);
    f.assert_cached(second);
}

#[test]
fn postponed_receipt_counts_only_when_data_arrives() {
    check_data_dependency(false, false);
}

#[test]
fn yielded_receipt_counts_only_when_resumed() {
    check_data_dependency(true, false);
}

#[test]
fn postponed_receipt_keeps_inputs_when_compilation_defers() {
    check_data_dependency(false, true);
}

#[test]
fn yielded_receipt_keeps_inputs_when_compilation_defers() {
    check_data_dependency(true, true);
}

fn check_data_dependency(yielded: bool, defer: bool) {
    let mut f = setup();
    let code = near_test_contracts::trivial_contract();
    let later = budget_contracts()[1].clone();
    let id = GlobalContractIdentifier::CodeHash(hash(&later));
    f.install_cold(&[f.receipt(0, vec![deploy(code)]), distribute(id.clone(), &later)]);
    if defer {
        Arc::make_mut(&mut f.state.config).max_cold_contract_bytes_per_chunk = later.len() as u64;
    }
    let data_id = hash(b"dependency");
    let action = ActionReceipt {
        signer_id: alice_account(),
        signer_public_key: f.signers[0].public_key(),
        gas_price: GAS_PRICE,
        output_data_receivers: vec![],
        input_data_ids: vec![data_id],
        actions: if defer {
            vec![call("main"), use_global(id), call("main")]
        } else {
            vec![call("main")]
        },
    };
    let receipt = Receipt::V0(ReceiptV0 {
        predecessor_id: alice_account(),
        receiver_id: alice_account(),
        receipt_id: hash(b"postponed"),
        receipt: if yielded {
            ReceiptEnum::PromiseYield(action)
        } else {
            ReceiptEnum::Action(action)
        },
    });
    assert_eq!(f.apply(&[receipt]).cold_contract_bytes, 0);
    assert_eq!(f.compilation_status(code), None);
    let data = DataReceipt { data_id, data: Some(vec![]) };
    let data = Receipt::V0(ReceiptV0 {
        predecessor_id: alice_account(),
        receiver_id: alice_account(),
        receipt_id: hash(b"data"),
        receipt: if yielded { ReceiptEnum::PromiseResume(data) } else { ReceiptEnum::Data(data) },
    });
    let result = f.apply(&[data]);
    assert_eq!(result.cold_contract_bytes, code.len() as u64);
    f.assert_cached(code);
    if defer {
        assert!(result.outcomes.is_empty());
        assert_eq!(result.delayed_receipts_count, 1);
        assert_eq!(f.compilation_status(&later), None);
        let retry = f.apply(&[]);
        assert_eq!(retry.cold_contract_bytes, later.len() as u64);
        assert_eq!(retry.delayed_receipts_count, 0);
        assert_eq!(retry.outcomes.len(), 1);
        assert_eq!(retry.outcomes[0].id, hash(b"postponed"));
        assert_matches!(retry.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
        f.assert_cached(&later);
        let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
        assert!(get_received_data(&state, &alice_account(), data_id).unwrap().is_none());
    }
}

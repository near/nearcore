use super::*;
use crate::contract_compilation_status::{
    record_contract_compilation, update_contract_compilation_status,
};
use near_primitives::types::{ContractCompilationStatus, VmGeneration};
use near_store::{TrieUpdate, get};

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
        result
    }

    fn compilation_status_at_generation(
        &self,
        code: &[u8],
        vm_generation: VmGeneration,
    ) -> Option<ContractCompilationStatus> {
        let state = self.tries.new_trie_update(ShardUId::single_shard(), self.root);
        get(&state, &TrieKey::ContractCompilationStatus { vm_generation, code_hash: hash(code) })
            .unwrap()
    }

    fn compilation_status(&self, code: &[u8]) -> Option<ContractCompilationStatus> {
        self.compilation_status_at_generation(code, self.state.vm_generation)
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
        record_contract_compilation(&mut update, &f.state, code_hash, 9).unwrap();
        let after = update.trie.recorded_storage_size_upper_bound();
        assert!(after > before, "status proof must be recorded during execution");

        // Repeated reads do not grow the proof or double-count cold bytes.
        record_contract_compilation(&mut update, &f.state, code_hash, 9).unwrap();
        assert_eq!(update.trie.recorded_storage_size_upper_bound(), after);
        update.rollback();
        assert_eq!(
            update_contract_compilation_status(&mut update, &f.state).unwrap(),
            expected_cold_bytes
        );
        assert_eq!(update.trie.recorded_storage_size_upper_bound(), after);
        let key =
            TrieKey::ContractCompilationStatus { vm_generation: f.state.vm_generation, code_hash };
        assert_eq!(get(&update, &key).unwrap(), Some(ContractCompilationStatus::Cached));
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
    let result = f.apply(&calls);
    assert_eq!(result.cold_contract_bytes, code.len() as u64);
    assert_eq!(f.compilation_status(code), Some(ContractCompilationStatus::Cached));

    // Validation records the same protocol warmness even though it does not
    // populate the producer's contract-distribution tracker.
    f.state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;
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

    f.state.cache = Some(Box::new(NoContractRuntimeCache));
    assert_eq!(f.apply(&calls).cold_contract_bytes, 0);
    // Each VM generation has an independent status entry; local cache eviction is unrelated.
    let old_generation = f.state.vm_generation;
    f.state.vm_generation.0 += 1;
    assert_eq!(f.apply(&calls).cold_contract_bytes, code.len() as u64);
    assert_eq!(f.compilation_status(code), Some(ContractCompilationStatus::Cached));
    assert_eq!(
        f.compilation_status_at_generation(code, old_generation),
        Some(ContractCompilationStatus::Cached)
    );
    f.state.vm_generation = old_generation;
    assert_eq!(f.apply(&calls).cold_contract_bytes, 0);
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
    assert_eq!(f.compilation_status(local), Some(ContractCompilationStatus::Cached));
    assert_eq!(f.compilation_status(global), Some(ContractCompilationStatus::Cached));
}

#[test]
fn failed_receipt_warms_only_reached_code() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let later = near_test_contracts::rs_contract();
    let id = GlobalContractIdentifier::CodeHash(hash(later));
    let installs = [f.receipt(0, vec![deploy(first)]), distribute(id.clone(), later)];
    f.install_cold(&installs);
    let receipt = f.receipt(0, vec![call("missing_method"), use_global(id), call("log_something")]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, first.len() as u64);
    assert_eq!(f.compilation_status(first), Some(ContractCompilationStatus::Cached));
    assert_eq!(f.compilation_status(later), None);
}

#[test]
fn deployments_survive_rollback_and_count_every_code() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let second = near_test_contracts::rs_contract();
    let receipt = f.receipt(0, vec![deploy(first), deploy(second), call("missing_method")]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, (first.len() + second.len()) as u64);
    assert_eq!(f.compilation_status(first), Some(ContractCompilationStatus::Cached));
    assert_eq!(f.compilation_status(second), Some(ContractCompilationStatus::Cached));
    let state = f.tries.new_trie_update(ShardUId::single_shard(), f.root);
    assert!(get_account(&state, &alice_account()).unwrap().unwrap().contract().is_none());
}

#[test]
fn global_distribution_and_compilation_errors_are_warm() {
    let mut f = setup();
    let invalid = b"not wasm";
    let distribution = distribute(GlobalContractIdentifier::CodeHash(hash(invalid)), invalid);
    assert_eq!(f.apply(&[distribution]).cold_contract_bytes, invalid.len() as u64);
    assert_eq!(f.compilation_status(invalid), Some(ContractCompilationStatus::Cached));
    let receipt = f.receipt(
        0,
        vec![use_global(GlobalContractIdentifier::CodeHash(hash(invalid))), call("main")],
    );
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, 0);
}

#[test]
fn loading_abort_and_unexecuted_deploy_do_not_warm() {
    let mut f = setup();
    let first = near_test_contracts::trivial_contract();
    let second = near_test_contracts::rs_contract();
    f.install_cold(&[f.receipt(0, vec![deploy(first)])]);
    let receipt =
        f.receipt(0, vec![Action::function_call("main", vec![], Gas::from_gas(1)), deploy(second)]);
    let result = f.apply(&[receipt]);
    assert_matches!(result.outcomes[0].outcome.status, ExecutionStatus::Failure(_));
    assert_eq!(result.cold_contract_bytes, 0);
    assert_eq!(f.compilation_status(first), None);
    assert_eq!(f.compilation_status(second), None);
}

#[test]
fn postponed_receipt_counts_only_when_data_arrives() {
    check_data_dependency(false);
}

#[test]
fn yielded_receipt_counts_only_when_resumed() {
    check_data_dependency(true);
}

fn check_data_dependency(yielded: bool) {
    let mut f = setup();
    let code = near_test_contracts::trivial_contract();
    f.install_cold(&[f.receipt(0, vec![deploy(code)])]);
    let data_id = hash(b"dependency");
    let action = ActionReceipt {
        signer_id: alice_account(),
        signer_public_key: f.signers[0].public_key(),
        gas_price: GAS_PRICE,
        output_data_receivers: vec![],
        input_data_ids: vec![data_id],
        actions: vec![call("main")],
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
    assert_eq!(f.apply(&[data]).cold_contract_bytes, code.len() as u64);
    assert_eq!(f.compilation_status(code), Some(ContractCompilationStatus::Cached));
}

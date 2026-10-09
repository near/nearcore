use super::apply::setup_runtime;
use super::create_receipt_with_actions;
use crate::pending_compile::{COLD_BYTES_PER_CHUNK, VmGenerations};
use crate::{ApplyResult, ApplyState, Runtime, SignedValidPeriodTransactions};
use near_crypto::Signer;
use near_parameters::RuntimeConfig;
use near_primitives::account::AccountContract;
use near_primitives::action::{
    Action, DeployContractAction, DeterministicStateInitAction, FunctionCallAction,
    GlobalContractIdentifier, TransferAction,
};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::{
    AccountId, Balance, EpochInfoProvider, Gas, ProtocolVersion, StateChangeCause,
};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::ProtocolFeature;
use near_store::trie::receipts_column_helper::{PendingCompileReceiptQueue, TrieQueue};
use near_store::{ShardTries, ShardUId, TrieUpdate, get, get_account, set, set_account};
use near_vm_runner::ContractCode;
use std::collections::BTreeMap;
use std::sync::Arc;
use testlib::runtime_utils::{alice_account, bob_account};

/// A contract larger than half of the chunk budget, so two distinct ones do
/// not fit in one chunk.
fn large_contract(salt: usize) -> ContractCode {
    let size = usize::try_from(COLD_BYTES_PER_CHUNK / 2).unwrap() + 1 + salt;
    ContractCode::new(near_test_contracts::sized_contract(size), None)
}

fn generation() -> ProtocolVersion {
    ProtocolFeature::ColdContractAdmission.protocol_version() + 1
}

struct Chain<E> {
    runtime: Runtime,
    tries: ShardTries,
    root: CryptoHash,
    apply_state: ApplyState,
    signers: Vec<Arc<Signer>>,
    epoch_info_provider: E,
}

impl<E: EpochInfoProvider> Chain<E> {
    fn update_state(&mut self, update: impl FnOnce(&mut TrieUpdate)) {
        let mut state_update = self.tries.new_trie_update(ShardUId::single_shard(), self.root);
        update(&mut state_update);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = self.tries.store_update();
        self.root =
            self.tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();
    }

    /// Deploys `code` on `account_id` without writing its warmth.
    fn deploy_cold(&mut self, account_id: AccountId, code: &ContractCode) {
        self.update_state(|state_update| {
            let mut account = get_account(state_update, &account_id).unwrap().unwrap();
            account.set_contract(AccountContract::Local(*code.hash())).unwrap();
            set_account(state_update, account_id.clone(), &account);
            state_update.set(TrieKey::ContractCode { account_id }, code.code().to_vec());
        });
    }

    fn apply(&mut self, receipts: &[Receipt]) -> ApplyResult {
        let result = self
            .runtime
            .apply(
                self.tries.get_trie_for_shard(ShardUId::single_shard(), self.root),
                &None,
                &self.apply_state,
                receipts,
                SignedValidPeriodTransactions::empty(),
                &self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = self.tries.store_update();
        self.root =
            self.tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();
        result
    }

    fn state(&self) -> TrieUpdate {
        self.tries.new_trie_update(ShardUId::single_shard(), self.root)
    }

    fn warmth(&self, account_id: AccountId) -> Option<ProtocolVersion> {
        get(&self.state(), &TrieKey::ContractWarmth { account_id }).unwrap()
    }

    fn queued_receipts(&self, receiver_id: &AccountId) -> u64 {
        PendingCompileReceiptQueue::load(&self.state(), receiver_id).unwrap().len()
    }

    fn call(&self, account_id: AccountId, salt: &str) -> Receipt {
        let signer =
            if account_id == alice_account() { &self.signers[0] } else { &self.signers[1] };
        create_receipt_with_actions(
            account_id,
            Arc::clone(signer),
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "main".to_string(),
                args: salt.as_bytes().to_vec(),
                gas: Gas::from_teragas(1),
                deposit: Balance::ZERO,
            }))],
        )
    }
}

fn setup(generations: VmGenerations) -> Chain<impl EpochInfoProvider> {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
    apply_state.current_protocol_version =
        ProtocolFeature::ColdContractAdmission.protocol_version();
    apply_state.config = Arc::new(RuntimeConfig::free());
    apply_state.vm_generations = generations;
    Chain { runtime, tries, root, apply_state, signers, epoch_info_provider }
}

fn executed(result: &ApplyResult, receipt: &Receipt) -> bool {
    result.outcomes.iter().any(|outcome| outcome.id == *receipt.receipt_id())
}

#[test]
fn cold_code_over_the_chunk_budget_waits_in_the_queue_until_the_next_chunk() {
    let mut chain = setup(VmGenerations { current: generation(), next: generation() });
    chain.deploy_cold(alice_account(), &large_contract(0));
    chain.deploy_cold(bob_account(), &large_contract(1));
    let alice_call = chain.call(alice_account(), "");
    let bob_call = chain.call(bob_account(), "");

    let result = chain.apply(&[alice_call.clone(), bob_call.clone()]);
    assert!(executed(&result, &alice_call));
    assert!(!executed(&result, &bob_call));
    assert_eq!(chain.warmth(alice_account()), Some(generation()));
    assert_eq!(chain.warmth(bob_account()), None);
    assert_eq!(chain.queued_receipts(&bob_account()), 1);

    let result = chain.apply(&[]);
    assert!(executed(&result, &bob_call));
    assert_eq!(chain.warmth(bob_account()), Some(generation()));
    assert_eq!(chain.queued_receipts(&bob_account()), 0);
}

#[test]
fn receipt_for_a_receiver_with_queued_receipts_waits_behind_them_in_order() {
    let mut chain = setup(VmGenerations { current: generation(), next: generation() });
    chain.deploy_cold(alice_account(), &large_contract(0));
    chain.deploy_cold(bob_account(), &large_contract(1));
    let alice_call = chain.call(alice_account(), "");
    let bob_call = chain.call(bob_account(), "");
    // Runs no code, so only the queued call ahead of it holds it back.
    let bob_transfer = create_receipt_with_actions(
        bob_account(),
        Arc::clone(&chain.signers[1]),
        vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
    );

    let result = chain.apply(&[alice_call, bob_call.clone(), bob_transfer.clone()]);
    assert!(!executed(&result, &bob_call));
    assert!(!executed(&result, &bob_transfer));
    assert_eq!(chain.queued_receipts(&bob_account()), 2);

    let result = chain.apply(&[]);
    let bob_order: Vec<_> = result
        .outcomes
        .iter()
        .map(|outcome| outcome.id)
        .filter(|id| id == bob_call.receipt_id() || id == bob_transfer.receipt_id())
        .collect();
    assert_eq!(bob_order, vec![*bob_call.receipt_id(), *bob_transfer.receipt_id()]);
    assert_eq!(chain.queued_receipts(&bob_account()), 0);
}

#[test]
fn warm_code_runs_after_the_chunk_budget_is_spent() {
    let mut chain = setup(VmGenerations { current: generation(), next: generation() });
    chain.deploy_cold(alice_account(), &large_contract(0));
    chain.deploy_cold(bob_account(), &large_contract(1));
    chain.update_state(|state_update| {
        set(state_update, TrieKey::ContractWarmth { account_id: bob_account() }, &generation());
    });
    let alice_call = chain.call(alice_account(), "");
    let bob_call = chain.call(bob_account(), "");

    let result = chain.apply(&[alice_call.clone(), bob_call.clone()]);
    assert!(executed(&result, &alice_call));
    assert!(executed(&result, &bob_call));
}

#[test]
fn deploy_writes_the_warmth_of_the_current_generation() {
    let mut chain = setup(VmGenerations { current: generation(), next: generation() });
    let code = near_test_contracts::trivial_contract().to_vec();
    let deploy = create_receipt_with_actions(
        alice_account(),
        Arc::clone(&chain.signers[0]),
        vec![Action::DeployContract(DeployContractAction { code })],
    );
    chain.apply(&[deploy]);
    assert_eq!(chain.warmth(alice_account()), Some(generation()));
}

#[test]
fn call_before_a_vm_change_marks_the_code_for_the_next_generation() {
    let next = generation() + 1;
    let mut chain = setup(VmGenerations { current: generation(), next });
    chain.deploy_cold(alice_account(), &large_contract(0));
    chain.update_state(|state_update| {
        set(state_update, TrieKey::ContractWarmth { account_id: alice_account() }, &generation());
    });
    let alice_call = chain.call(alice_account(), "");

    let result = chain.apply(&[alice_call.clone()]);
    assert!(executed(&result, &alice_call));
    assert_eq!(chain.warmth(alice_account()), Some(next));
}

#[test]
fn call_after_a_state_init_is_gated_on_the_global_code_it_binds() {
    let mut chain = setup(VmGenerations { current: generation(), next: generation() });
    chain.deploy_cold(alice_account(), &large_contract(0));
    let global_code = large_contract(1);
    let identifier = GlobalContractCodeIdentifier::CodeHash(*global_code.hash());
    chain.update_state(|state_update| {
        state_update.set(
            TrieKey::GlobalContractCode { identifier: identifier.clone() },
            global_code.code().to_vec(),
        );
    });
    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: GlobalContractIdentifier::CodeHash(*global_code.hash()),
        data: BTreeMap::new(),
    });
    let receiver_id = derive_near_deterministic_account_id(&state_init);
    let init_and_call = Receipt::V0(ReceiptV0 {
        predecessor_id: alice_account(),
        receiver_id: receiver_id.clone(),
        receipt_id: CryptoHash::hash_bytes(b"init_and_call"),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: alice_account(),
            signer_public_key: chain.signers[0].public_key(),
            gas_price: Balance::ZERO,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![
                Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
                    state_init,
                    deposit: Balance::from_near(1),
                })),
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "main".to_string(),
                    args: vec![],
                    gas: Gas::from_teragas(1),
                    deposit: Balance::ZERO,
                })),
            ],
        }),
    });
    let alice_call = chain.call(alice_account(), "");

    let result = chain.apply(&[alice_call, init_and_call.clone()]);
    assert!(!executed(&result, &init_and_call));
    assert_eq!(chain.queued_receipts(&receiver_id), 1);

    let result = chain.apply(&[]);
    assert!(executed(&result, &init_and_call));
    let warmth: Option<ProtocolVersion> =
        get(&chain.state(), &TrieKey::GlobalContractWarmth { identifier }).unwrap();
    assert_eq!(warmth, Some(generation()));
}

use super::GAS_PRICE;
use crate::tests::apply::{commit_apply_result, setup_runtime};
use crate::tests::create_receipt_with_actions;
use crate::{ApplyResult, SignedValidPeriodTransactions};
use assert_matches::assert_matches;
use near_primitives::account::AccountContract;
use near_primitives::action::{
    Action, FunctionCallAction, GlobalContractIdentifier, UseGlobalContractAction,
};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::errors::StorageError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, GlobalContractDistributionReceipt, Receipt, ReceiptEnum, ReceiptV0,
};
use near_primitives::shard_layout::ShardUId;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::transaction::{ExecutionStatus, TransferAction};
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::{AccountId, Balance, Gas, StateChangeCause};
use near_store::trie::trie_storage::{TrieMemoryPartialStorage, TrieStorage};
use near_store::{Trie, get_account, set_account};
use near_vm_runner::{ContractCode, NoContractRuntimeCache};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::slice::from_ref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;
use testlib::runtime_utils::alice_account;

/// Witness storage for a chunk validator's apply that stops the main thread
/// until the pipeline has read one contract. In production the pipeline reads
/// on a pool thread while the main thread applies receipts, in no fixed order.
///
/// The preparation pipeline reads `pipeline_hash` on a pool thread. The first
/// read of `waiting_hash` waits for that read. So the receipts after the one
/// that reads `waiting_hash` run strictly after the pipeline task read its
/// code.
struct PipelineReadFirstStorage {
    inner: TrieMemoryPartialStorage<false>,
    pipeline_hash: CryptoHash,
    waiting_hash: CryptoHash,
    /// The pool thread sends on it after reading `pipeline_hash`.
    pipeline_read: (Sender<()>, Mutex<Receiver<()>>),
    /// Set once a read of `waiting_hash` has waited for the pipeline read.
    held: Arc<AtomicBool>,
}

impl PipelineReadFirstStorage {
    /// Returns the storage and the flag it sets once the hold fired.
    fn new(
        recorded: HashMap<CryptoHash, Arc<[u8]>>,
        pipeline_hash: CryptoHash,
        waiting_hash: CryptoHash,
    ) -> (Self, Arc<AtomicBool>) {
        let (sender, receiver) = channel();
        let held = Arc::new(AtomicBool::new(false));
        let storage = Self {
            inner: TrieMemoryPartialStorage::new(recorded),
            pipeline_hash,
            waiting_hash,
            pipeline_read: (sender, Mutex::new(receiver)),
            held: held.clone(),
        };
        (storage, held)
    }

    /// Returns once the pool thread has read `pipeline_hash`. Only the first
    /// call waits.
    fn wait_for_pipeline_read(&self) {
        let receiver = self.pipeline_read.1.lock();
        if self.held.load(Ordering::SeqCst) {
            return;
        }
        receiver
            .recv_timeout(Duration::from_secs(60))
            .expect("the pipeline did not read the contract in time");
        self.held.store(true, Ordering::SeqCst);
    }
}

impl TrieStorage for PipelineReadFirstStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, StorageError> {
        if *hash == self.waiting_hash {
            self.wait_for_pipeline_read();
        }
        let result = self.inner.retrieve_raw_bytes(hash);
        if *hash == self.pipeline_hash {
            self.pipeline_read.0.send(()).unwrap();
        }
        result
    }
}

#[test]
fn test_postponed_call_across_same_chunk_global_contract_switch_validates() {
    let user = alice_account();
    let aux: AccountId = "aux.near".parse().unwrap();
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![user.clone(), aux.clone()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    let shard_uid = ShardUId::single_shard();
    let user_signer = signers[0].clone();
    let aux_signer = signers[1].clone();
    // Setup: `alice` runs `code` by hash. `AccountId(alice)` exists with other
    // code, so the switch below succeeds. `aux` is a plain account. The
    // validator run below holds the read of its account to order the
    // receipts against the pipeline.
    let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let code_hash = *code.hash();
    // Never run. Any code with a hash other than `code` works here.
    let old_target_code =
        ContractCode::new(near_test_contracts::smallest_rs_contract().to_vec(), None);

    let mut state_update = tries.new_trie_update(shard_uid, root);
    state_update.set(
        TrieKey::GlobalContractCode {
            identifier: GlobalContractCodeIdentifier::CodeHash(code_hash),
        },
        code.code().to_vec(),
    );
    state_update.set(
        TrieKey::GlobalContractCode {
            identifier: GlobalContractCodeIdentifier::AccountId(user.clone()),
        },
        old_target_code.code().to_vec(),
    );
    let mut user_account = get_account(&state_update, &user).unwrap().unwrap();
    user_account.set_contract(AccountContract::Global(code_hash)).unwrap();
    set_account(&mut state_update, user.clone(), &user_account);
    let aux_account_hash =
        CryptoHash::hash_borsh(get_account(&state_update, &aux).unwrap().unwrap());
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    // Receipts, in chunk order:
    //   1. the callback to `alice`, waiting for data. The pipeline submits it
    //      on sight, and its task resolves to `CodeHash(code)`.
    //   2. a transfer to `aux` whose account read holds the main thread.
    //   3. `UseGlobalContract(AccountId(alice))`, the switch.
    //   4. the distribution of the same `code` under `AccountId(alice)`.
    //   5. the data receipt that runs the callback.
    let data_id = CryptoHash::hash_bytes(b"switch-callback-data");
    let callback = Receipt::V0(ReceiptV0 {
        predecessor_id: user.clone(),
        receiver_id: user.clone(),
        receipt_id: CryptoHash::hash_bytes(b"switch-callback"),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: user.clone(),
            signer_public_key: user_signer.public_key(),
            gas_price: GAS_PRICE,
            output_data_receivers: vec![],
            input_data_ids: vec![data_id],
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            }))],
        }),
    });
    let callback_id = *callback.receipt_id();
    let gate_transfer = create_receipt_with_actions(
        aux,
        aux_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1) })],
    );
    let switch = create_receipt_with_actions(
        user.clone(),
        user_signer,
        vec![Action::UseGlobalContract(Box::new(UseGlobalContractAction {
            contract_identifier: GlobalContractIdentifier::AccountId(user.clone()),
        }))],
    );
    let distribution = Receipt::new_global_contract_distribution(
        user.clone(),
        GlobalContractDistributionReceipt::new(
            GlobalContractIdentifier::AccountId(user.clone()),
            apply_state.shard_id,
            vec![],
            code.code().to_vec().into(),
            1,
        ),
    );
    let data = Receipt::V0(ReceiptV0 {
        predecessor_id: user.clone(),
        receiver_id: user,
        receipt_id: CryptoHash::hash_bytes(b"switch-callback-data-receipt"),
        receipt: ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![]) }),
    });
    let receipts = [callback, gate_transfer, switch, distribution, data];

    // Apply as the producer, over full storage, recording the witness.
    let producer_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let callback_status = |result: &ApplyResult| {
        result
            .outcomes
            .iter()
            .find(|outcome| outcome.id == callback_id)
            .expect("callback outcome missing")
            .outcome
            .status
            .clone()
    };
    // Check: the call runs.
    assert_matches!(callback_status(&producer_result), ExecutionStatus::SuccessValue(_));
    assert!(
        !producer_result.contract_updates.contract_accesses.contains(&CodeHash(code_hash)),
        "since code is distributed in this chunk, it should not be in contract accesses"
    );

    // Apply as a chunk validator, over the witness, with an empty compiled
    // contract cache.
    let PartialState::TrieValues(values) = producer_result.proof.unwrap().nodes;
    let recorded = values.into_iter().map(|value| (hash(&value), value)).collect();
    // The pipeline task reads `code` through `ContractStorage::get`, which
    // checks the deploys of this chunk before storage, and the distribution
    // receipt records `code` as such a deploy. This test covers the order in
    // which the task reads before the distribution is applied. Hold the read
    // of `aux`'s account, which the transfer makes before the switch and the
    // distribution, until the task has read.
    let (storage, held) = PipelineReadFirstStorage::new(recorded, code_hash, aux_account_hash);
    apply_state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;
    // Apply should work regardless of compilation cache state.
    apply_state.cache = Some(Box::new(NoContractRuntimeCache));
    // Check: the validator runs the call and reaches the producer's state
    // root.
    let validator_result = runtime
        .apply(
            Trie::new(Arc::new(storage), root, None),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .expect("validator apply failed");
    assert_matches!(callback_status(&validator_result), ExecutionStatus::SuccessValue(_));
    assert_eq!(validator_result.state_root, producer_result.state_root);
    // Check: the hold fired, so the receipts after it ran after the pipeline
    // read.
    assert!(held.load(Ordering::SeqCst), "the hold never fired");
}

#[test]
fn test_call_after_postponed_same_chunk_global_contract_switch_validates() {
    let user = alice_account();
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![user.clone()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    let shard_uid = ShardUId::single_shard();
    let user_signer = signers[0].clone();
    // Setup: `alice` runs `code` by hash. `AccountId(alice)` exists with other
    // code, so the switch below succeeds.
    let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let code_hash = *code.hash();
    // Never run. Any code with a hash other than `code` works here.
    let old_target_code =
        ContractCode::new(near_test_contracts::smallest_rs_contract().to_vec(), None);

    let mut state_update = tries.new_trie_update(shard_uid, root);
    state_update.set(
        TrieKey::GlobalContractCode {
            identifier: GlobalContractCodeIdentifier::CodeHash(code_hash),
        },
        code.code().to_vec(),
    );
    state_update.set(
        TrieKey::GlobalContractCode {
            identifier: GlobalContractCodeIdentifier::AccountId(user.clone()),
        },
        old_target_code.code().to_vec(),
    );
    let mut user_account = get_account(&state_update, &user).unwrap().unwrap();
    user_account.set_contract(AccountContract::Global(code_hash)).unwrap();
    set_account(&mut state_update, user.clone(), &user_account);
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    // The switch waits for two data receipts. It is postponed in the chunk
    // before the one under test, and the pipeline never sees it: the
    // scheduler follows a data receipt into its postponed receipt only when
    // that receipt waits for one more data receipt, as read at scan time.
    let first_data_id = CryptoHash::hash_bytes(b"postponed-switch-data-1");
    let second_data_id = CryptoHash::hash_bytes(b"postponed-switch-data-2");
    let switch = Receipt::V0(ReceiptV0 {
        predecessor_id: user.clone(),
        receiver_id: user.clone(),
        receipt_id: CryptoHash::hash_bytes(b"postponed-switch"),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: user.clone(),
            signer_public_key: user_signer.public_key(),
            gas_price: GAS_PRICE,
            output_data_receivers: vec![],
            input_data_ids: vec![first_data_id, second_data_id],
            actions: vec![Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                contract_identifier: GlobalContractIdentifier::AccountId(user.clone()),
            }))],
        }),
    });
    let postpone_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            from_ref(&switch),
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let root = commit_apply_result(&postpone_result, &mut apply_state, &tries, shard_uid);
    apply_state.block_height += 1;

    // Receipts, in chunk order:
    //   1. the first data receipt for the switch.
    //   2. the second data receipt. It runs the switch, whose stored receipt
    //      is read here for the first time.
    //   3. the distribution of the same `code` under `AccountId(alice)`.
    //   4. a call to `alice`. The pipeline submits it on sight, before any
    //      of the above runs, and its task resolves to `CodeHash(code)`.
    let data_receipt = |tag: &[u8], data_id| {
        Receipt::V0(ReceiptV0 {
            predecessor_id: user.clone(),
            receiver_id: user.clone(),
            receipt_id: CryptoHash::hash_bytes(tag),
            receipt: ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![]) }),
        })
    };
    let first_data = data_receipt(b"postponed-switch-data-receipt-1", first_data_id);
    let second_data = data_receipt(b"postponed-switch-data-receipt-2", second_data_id);
    let distribution = Receipt::new_global_contract_distribution(
        user.clone(),
        GlobalContractDistributionReceipt::new(
            GlobalContractIdentifier::AccountId(user.clone()),
            apply_state.shard_id,
            vec![],
            code.code().to_vec().into(),
            1,
        ),
    );
    let call = create_receipt_with_actions(
        user.clone(),
        user_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: vec![],
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
    );
    let call_id = *call.receipt_id();
    let receipts = [first_data, second_data, distribution, call];

    // Apply as the producer, over full storage, recording the witness.
    let producer_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let status = |result: &ApplyResult, receipt_id| {
        result
            .outcomes
            .iter()
            .find(|outcome| outcome.id == receipt_id)
            .expect("outcome missing")
            .outcome
            .status
            .clone()
    };
    // Check: the switch and the call run.
    assert_matches!(
        status(&producer_result, *switch.receipt_id()),
        ExecutionStatus::SuccessValue(_)
    );
    assert_matches!(status(&producer_result, call_id), ExecutionStatus::SuccessValue(_));
    assert!(
        !producer_result.contract_updates.contract_accesses.contains(&CodeHash(code_hash)),
        "since code is distributed in this chunk, it should not be in contract accesses"
    );

    // Apply as a chunk validator, over the witness, with an empty compiled
    // contract cache.
    let PartialState::TrieValues(values) = producer_result.proof.unwrap().nodes;
    let recorded = values.into_iter().map(|value| (hash(&value), value)).collect();
    // Hold the read of the stored switch receipt, which the second data
    // receipt makes before the switch and the distribution, until the
    // pipeline task has read `code`.
    let (storage, held) =
        PipelineReadFirstStorage::new(recorded, code_hash, CryptoHash::hash_borsh(&switch));
    apply_state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;
    // Apply should work regardless of compilation cache state.
    apply_state.cache = Some(Box::new(NoContractRuntimeCache));
    // Check: the validator runs the call and reaches the producer's state
    // root.
    let validator_result = runtime
        .apply(
            Trie::new(Arc::new(storage), root, None),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .expect("validator apply failed");
    assert_matches!(status(&validator_result, call_id), ExecutionStatus::SuccessValue(_));
    assert_eq!(validator_result.state_root, producer_result.state_root);
    // Check: the hold fired, so the receipts after it ran after the pipeline
    // read.
    assert!(held.load(Ordering::SeqCst), "the hold never fired");
}

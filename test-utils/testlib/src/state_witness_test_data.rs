use std::sync::Arc;

use crate::runtime_utils::{alice_account, bob_account};
use borsh;
use near_crypto::InMemorySigner;
use near_parameters::RuntimeConfig;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateTransition, ChunkStateWitness,
};
use near_primitives::test_utils::{MockEpochInfoProvider, account_new};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;
use near_primitives::types::{Balance, EpochInfoProvider, Gas, StateChangeCause};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::TestTriesBuilder;
use near_store::{set_access_key, set_account};
use near_vm_runner::FilesystemContractRuntimeCache;
use node_runtime::{ApplyState, Runtime, SignedValidPeriodTransactions};
use std::collections::HashMap;

/// Generates a realistic ChunkStateWitness from native token transfers.
#[allow(unused)]
pub fn generate_realistic_state_witness(target_size_bytes: usize) -> ChunkStateWitness {
    const GAS_PRICE: Balance = Balance::from_yoctonear(5000);
    const INITIAL_BALANCE: Balance = Balance::from_near(10_000); // 10k NEAR per account
    const TRANSFER_AMOUNT: Balance = Balance::from_near(1); // 1 NEAR

    // Create many test accounts for realistic transaction load
    let num_accounts = 10000;
    let mut initial_accounts = vec![alice_account(), bob_account()];
    for i in 0..num_accounts {
        initial_accounts.push(format!("test{}.near", i).parse().unwrap());
    }

    let epoch_info_provider = MockEpochInfoProvider::default();
    let shard_layout = epoch_info_provider.shard_layout(&Default::default()).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();

    let tries = TestTriesBuilder::new().build();
    let mut initial_state = tries.new_trie_update(shard_uid, CryptoHash::default());

    // Create all accounts with signers in the trie
    let mut signers = Vec::new();
    for account_id in &initial_accounts {
        let signer = InMemorySigner::test_signer(account_id);
        let mut account = account_new(INITIAL_BALANCE, CryptoHash::default());
        account.set_storage_usage(182);

        set_account(&mut initial_state, account_id.clone(), &account);
        set_access_key(
            &mut initial_state,
            account_id.clone(),
            signer.public_key(),
            &near_primitives::account::AccessKey::full_access(),
        );
        signers.push(signer);
    }

    // Commit initial state
    initial_state.commit(StateChangeCause::InitialState);
    let trie_changes = initial_state.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let mut current_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();

    // Setup runtime and apply state
    let runtime = Runtime::new();
    let shard_ids = shard_layout.shard_ids();
    let shards_congestion_info =
        shard_ids.map(|shard_id| (shard_id, ExtendedCongestionInfo::default())).collect();
    let congestion_info = BlockCongestionInfo::new(shards_congestion_info);

    let apply_state = ApplyState {
        apply_reason: ApplyChunkReason::UpdateTrackedShard,
        block_height: 1,
        prev_block_hash: Default::default(),
        shard_id: shard_uid.shard_id(),
        epoch_id: Default::default(),
        epoch_height: 0,
        gas_price: GAS_PRICE,
        block_timestamp: 100,
        gas_limit: Some(Gas::from_teragas(1000)),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config: Arc::new(RuntimeConfig::test()),
        cache: Some(Box::new(FilesystemContractRuntimeCache::test().unwrap())),
        is_new_chunk: true,
        congestion_info,
        bandwidth_requests: BlockBandwidthRequests::empty(),
        trie_access_tracker_state: Default::default(),
        on_post_state_ready: None,
    };

    // Collect data for building the witness
    let mut all_partial_states = Vec::new();
    let mut all_transactions = Vec::new();
    let mut all_receipts = Vec::new();
    let mut current_witness_size = 0;

    let batch_size = 1000;
    let total_batches = 100;

    for batch_num in 0..total_batches {
        if current_witness_size >= target_size_bytes {
            break;
        }
        // Create a batch of native transfer transactions
        let mut transactions = Vec::new();
        for i in 0..batch_size {
            let from_idx = ((batch_num * batch_size + i) % (initial_accounts.len() - 1)) + 1;
            let to_idx = (from_idx % (initial_accounts.len() - 1)) + 1;

            let amount =
                TRANSFER_AMOUNT.checked_add(Balance::from_millinear(i as u128 * 100)).unwrap(); // Slightly different amounts

            let tx = SignedTransaction::send_money(
                1 + batch_num as u64,
                initial_accounts[from_idx].clone(),
                initial_accounts[to_idx].clone(),
                &signers[from_idx],
                amount,
                CryptoHash::hash_bytes(&format!("batch_{}_tx_{}", batch_num, i).as_bytes()),
            );
            transactions.push(tx);
        }

        // Apply transactions with recording enabled to capture state witness
        let trie_with_recording =
            tries.get_trie_for_shard(shard_uid, current_root).recording_reads_new_recorder();

        match runtime.apply(
            trie_with_recording,
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::new(transactions.clone(), vec![true; batch_size]),
            &epoch_info_provider,
            Default::default(),
        ) {
            Ok(apply_result) => {
                // Collect structured data instead of raw bytes
                if let Some(proof) = apply_result.proof {
                    // Estimate size by serializing
                    if let Ok(serialized) = borsh::to_vec(&proof.nodes) {
                        current_witness_size += serialized.len();
                    }
                    all_partial_states.push(proof.nodes);
                }

                // Collect transactions and receipts
                all_transactions.extend(transactions.clone());
                all_receipts.extend(apply_result.outgoing_receipts.clone());

                // Estimate additional size
                for receipt in &apply_result.outgoing_receipts {
                    if let Ok(receipt_bytes) = borsh::to_vec(receipt) {
                        current_witness_size += receipt_bytes.len();
                    }
                }

                // Update the state for next batch
                let mut store_update = tries.store_update();
                current_root =
                    tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
                store_update.commit().unwrap();
            }
            Err(e) => {
                println!("Error applying batch {}: {:?}", batch_num, e);
                // Continue with next batch
            }
        }

        if current_witness_size >= target_size_bytes {
            break;
        }
    }

    // Combine all partial states into one large state
    let mut all_trie_values = Vec::new();
    for partial_state in all_partial_states {
        let PartialState::TrieValues(trie_values) = partial_state;
        all_trie_values.extend(trie_values);
    }
    let combined_partial_state = PartialState::TrieValues(all_trie_values);

    // Create a chunk header
    let chunk_header =
        ShardChunkHeader::new_dummy(100, shard_uid.shard_id(), CryptoHash::default());

    // Create main state transition
    let main_state_transition = ChunkStateTransition {
        block_hash: CryptoHash::default(),
        base_state: combined_partial_state,
        post_state_root: current_root,
    };

    // Create applied receipts hash
    let applied_receipts_hash = hash(&borsh::to_vec(&all_receipts).unwrap());

    println!("Generated ChunkStateWitness with estimated size: {} bytes", current_witness_size);

    ChunkStateWitness::new(
        "producer.near".parse().unwrap(),
        EpochId::default(),
        chunk_header,
        main_state_transition,
        HashMap::new(),
        applied_receipts_hash,
        all_transactions.clone(),
        vec![],
        all_transactions,
        PROTOCOL_VERSION,
    )
}

/// Returns a serialized state witness.
#[allow(unused)]
pub fn generate_realistic_test_data(target_size: usize) -> Vec<u8> {
    let witness = generate_realistic_state_witness(target_size);
    borsh::to_vec(&witness).unwrap()
}

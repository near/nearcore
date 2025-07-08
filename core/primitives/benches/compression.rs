//! Benchmark for encoding/decoding of State Witness.
//!
//! Run with `cargo bench --bench compression`

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use std::io::Write;
use std::time::Duration;

use near_crypto::InMemorySigner;

use near_primitives::hash::CryptoHash;

use near_primitives::transaction::SignedTransaction;

use near_parameters::RuntimeConfig;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};

use near_primitives::state::PartialState;
use near_primitives::test_utils::{MockEpochInfoProvider, account_new};
use near_primitives::types::{Balance, EpochInfoProvider, StateChangeCause};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::TestTriesBuilder;
use near_store::{set_access_key, set_account};
use near_vm_runner::FilesystemContractRuntimeCache;
use node_runtime::{ApplyState, Runtime, SignedValidPeriodTransactions};
use std::sync::Arc;
use testlib::runtime_utils::{alice_account, bob_account};

/// Compression levels to test
const COMPRESSION_LEVELS: &[i32] = &[1, 3];

/// Number of threads
const NUM_THREADS: u32 = 4;

/// Test data sizes (in bytes)
const DATA_SIZES: &[(&str, usize)] = &[("5MB", 5_000_000), ("15MB", 15_000_000)];

/// Generates test data aiming for 1.9x-2.0x compression ratio (what we observe in forknet for state witness)
#[allow(unused)]
fn generate_test_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut rng = 12345u64;

    for _ in 0..size {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);

        match rng % 100 {
            0..=60 => data.push(b'A' + (rng % 12) as u8), // 61% letters
            61..=75 => data.push(b'0' + (rng % 5) as u8), // 15% digits
            76..=92 => data.push(b' '),                   // 17% spaces
            93..=97 => data.push(b'\n'),                  // 5% newlines
            _ => data.push(b'A'),                         // 2% consistent 'A'
        }
    }

    data
}

/// Generates test data from a realistic looking state witness.
///
/// For now it uses native token transfers (same as all realistic sharded benchmarks).
fn generate_realistic_test_data(target_size: usize) -> Vec<u8> {
    const GAS_PRICE: Balance = 5000;
    const INITIAL_BALANCE: Balance = 10_000_000_000_000_000_000_000_000_000; // 10k NEAR per account
    const TRANSFER_AMOUNT: Balance = 1_000_000_000_000_000_000_000_000; // 1 NEAR

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
        gas_limit: Some(10u64.pow(15)),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config: Arc::new(RuntimeConfig::test()),
        cache: Some(Box::new(FilesystemContractRuntimeCache::test().unwrap())),
        is_new_chunk: true,
        congestion_info,
        bandwidth_requests: BlockBandwidthRequests::empty(),
        trie_access_tracker_state: Default::default(),
    };

    let mut collected_witness_data = Vec::new();
    let batch_size = 10000;
    let total_batches = 100;

    for batch_num in 0..total_batches {
        if collected_witness_data.len() >= target_size {
            break;
        }
        // Create a batch of native transfer transactions
        let mut transactions = Vec::new();
        for i in 0..batch_size {
            let from_idx = ((batch_num * batch_size + i) % (initial_accounts.len() - 1)) + 1;
            let to_idx = (from_idx % (initial_accounts.len() - 1)) + 1;

            let amount = TRANSFER_AMOUNT + (i as u128 * 100_000_000_000_000_000_000_000); // Slightly different amounts

            let tx = SignedTransaction::send_money(
                1 + batch_num as u64, // nonce
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
            SignedValidPeriodTransactions::new(transactions, vec![true; batch_size]),
            &epoch_info_provider,
            Default::default(),
        ) {
            Ok(apply_result) => {
                // Extract storage proof
                if let Some(proof) = apply_result.proof {
                    let PartialState::TrieValues(storage_proof) = proof.nodes;
                    // Add the actual trie nodes from the state witness
                    for node in storage_proof {
                        collected_witness_data.extend_from_slice(&node);
                        if collected_witness_data.len() >= target_size {
                            break;
                        }
                    }
                }

                // Also capture outgoing receipts
                for receipt in &apply_result.outgoing_receipts {
                    if let Ok(receipt_bytes) = borsh::to_vec(receipt) {
                        collected_witness_data.extend_from_slice(&receipt_bytes);
                        if collected_witness_data.len() >= target_size {
                            break;
                        }
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

        if collected_witness_data.len() >= target_size {
            break;
        }
    }

    println!("Generated {} bytes of state witness data", collected_witness_data.len());
    collected_witness_data
}

fn compression_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_analysis");
    group
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_millis(1500))
        .confidence_level(0.90)
        .significance_level(0.10)
        .noise_threshold(0.05);

    // Benchmark all combinations of levels and sizes
    for &(size_name, size_bytes) in DATA_SIZES {
        let test_data = generate_realistic_test_data(size_bytes);
        group.throughput(Throughput::Bytes(size_bytes as u64));

        for &level in COMPRESSION_LEVELS {
            // Pre-compress to show compression info
            let mut encoder = zstd::stream::Encoder::new(Vec::new(), level).unwrap();
            encoder.write_all(&test_data).unwrap();
            let compressed_once = encoder.finish().unwrap();
            let ratio = size_bytes as f64 / compressed_once.len() as f64;

            println!(
                "Level {}, {}: {} -> {} bytes (ratio: {:.2}x)",
                level,
                size_name,
                size_bytes,
                compressed_once.len(),
                ratio
            );

            // Single-threaded compression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("compress_L{}_1T", level), size_name),
                &(&test_data, level),
                |b, (data, level)| {
                    b.iter(|| {
                        let mut encoder = zstd::stream::Encoder::new(Vec::new(), *level).unwrap();
                        encoder.write_all(black_box(data)).unwrap();
                        let compressed = encoder.finish().unwrap();
                        black_box(compressed)
                    });
                },
            );

            // Multi-threaded compression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("compress_L{}_{}T", level, NUM_THREADS), size_name),
                &(&test_data, level),
                |b, (data, level)| {
                    b.iter(|| {
                        let mut encoder = zstd::stream::Encoder::new(Vec::new(), *level).unwrap();
                        encoder.multithread(NUM_THREADS).unwrap();
                        encoder.write_all(black_box(data)).unwrap();
                        let compressed = encoder.finish().unwrap();
                        black_box(compressed)
                    });
                },
            );

            // Use the already compressed data for decompression benchmark
            let compressed_data = compressed_once;

            // Decompression benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("decompress_L{}", level), size_name),
                &compressed_data,
                |b, compressed| {
                    b.iter(|| {
                        let mut decoder =
                            zstd::stream::Decoder::new(compressed.as_slice()).unwrap();
                        let mut decompressed = Vec::new();
                        std::io::copy(&mut decoder, &mut decompressed).unwrap();
                        black_box(decompressed)
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, compression_benchmark);
criterion_main!(benches);

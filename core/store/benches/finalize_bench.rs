//! Benchmarks for the in-memory preparation of writes before the commit, aka
//! `finalize`.
//!
//! All writes are batched to a DB transaction before being committed. This
//! involves copying around the data and deserializing it.
//!
//! Since this is done on the hot path, whereas the actual commit can often be
//! done in a different thread, we have this benchmark to test the speed.
//!
//! Right now, the module only contains test for `ShardChunk` and
//! `PartialEncodedChunk`. These have been observed to become quite large and
//! have ab impact on the overall client performance.

#[macro_use]
extern crate bencher;

use bencher::{black_box, Bencher};
use borsh::BorshSerialize;
use near_chain::Chain;
use near_chunks::ShardsManager;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{merklize, MerklePathItem};
use near_primitives::receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum};
use near_primitives::reed_solomon::ReedSolomonWrapper;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkPart,
    PartialEncodedChunkV2, ReceiptProof, ShardChunk, ShardChunkHeader, ShardChunkHeaderV3,
    ShardChunkV2, ShardProof,
};
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::types::AccountId;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::DBCol;
use rand::prelude::SliceRandom;

/// `ShardChunk` -> `StoreUpdate::insert_ser`.
///
/// ~6ms or faster for ~24MB receipts
fn benchmark_write_shard_chunk(bench: &mut Bencher) {
    let transactions = vec![];
    let receipts = create_benchmark_receipts();
    let chunk_hash: ChunkHash = CryptoHash::default().into();
    let chunk = create_shard_chunk(&chunk_hash, transactions, receipts);

    let chunks = spread_in_memory(chunk);

    let store = near_store::test_utils::create_test_store();
    bench.iter(|| {
        let mut update = store.store_update();
        update
            .insert_ser(DBCol::Chunks, chunk_hash.as_ref(), &chunks.choose(&mut rand::thread_rng()))
            .unwrap();
        black_box(update);
    });
}

/// `PartialEncodedChunk` -> `StoreUpdate::insert_ser`.
///
/// ~70ms for ~24MB receipts
fn benchmark_write_partial_encoded_chunk(bench: &mut Bencher) {
    let transactions = vec![];
    let receipts = create_benchmark_receipts();
    let chunk_hash: ChunkHash = CryptoHash::default().into();

    let (encoded_chunk, merkle_paths) = create_encoded_shard_chunk(transactions, &receipts);
    let partial_chunk =
        encoded_chunk_to_partial_encoded_chunk(encoded_chunk, receipts, merkle_paths);
    let chunks = spread_in_memory(partial_chunk);

    let store = near_store::test_utils::create_test_store();
    bench.iter(|| {
        let mut update = store.store_update();
        update
            .insert_ser(
                DBCol::PartialChunks,
                chunk_hash.as_ref(),
                &chunks.choose(&mut rand::thread_rng()),
            )
            .unwrap();
        black_box(update);
    });
}

fn validator_signer() -> InMemoryValidatorSigner {
    InMemoryValidatorSigner::from_random("test".parse().unwrap(), KeyType::ED25519)
}

/// All receipts together are ~24MB
///
/// 24 MB isn't the norm but certainly possible.
fn create_benchmark_receipts() -> Vec<Receipt> {
    let account_id: AccountId = "test".parse().unwrap();
    let signer = InMemorySigner::from_random(account_id.clone(), KeyType::ED25519);
    let action = Action::FunctionCall(Box::new(FunctionCallAction {
        args: vec![42u8; 2_000_000],
        method_name: "foo".to_owned(),
        gas: 10_000_000_000_000u64,
        deposit: 1,
    }));

    vec![
        create_action_receipt(&account_id, &signer, vec![action.clone()], vec![]),
        create_data_receipt(&account_id, CryptoHash([1; 32]), 3_000_000),
        create_data_receipt(&account_id, CryptoHash([2; 32]), 3_000_001),
        create_action_receipt(&account_id, &signer, vec![action.clone()], vec![]),
        create_data_receipt(&account_id, CryptoHash([3; 32]), 3_000_002),
        create_data_receipt(&account_id, CryptoHash([4; 32]), 3_000_003),
        create_action_receipt(&account_id, &signer, vec![action], vec![]),
        create_data_receipt(&account_id, CryptoHash([5; 32]), 3_000_004),
        create_data_receipt(&account_id, CryptoHash([6; 32]), 3_000_005),
    ]
}

fn create_chunk_header(height: u64, shard_id: u64) -> ShardChunkHeader {
    ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        PROTOCOL_VERSION,
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        CryptoHash::default(),
        1,
        height,
        shard_id,
        0,
        0,
        0,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        CongestionInfo::default(),
        &validator_signer(),
    ))
}

fn create_action_receipt(
    account_id: &AccountId,
    signer: &InMemorySigner,
    actions: Vec<Action>,
    input_data_ids: Vec<CryptoHash>,
) -> Receipt {
    Receipt {
        predecessor_id: account_id.clone(),
        receiver_id: account_id.clone(),
        receipt_id: CryptoHash::hash_borsh(actions.clone()),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: account_id.clone(),
            signer_public_key: signer.public_key(),
            gas_price: 100_000_000,
            output_data_receivers: vec![],
            input_data_ids,
            actions,
        }),
    }
}

fn create_data_receipt(account_id: &AccountId, data_id: CryptoHash, data_size: usize) -> Receipt {
    Receipt {
        predecessor_id: account_id.clone(),
        receiver_id: account_id.clone(),
        receipt_id: CryptoHash::hash_borsh(data_id),
        receipt: ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![77u8; data_size]) }),
    }
}

fn create_shard_chunk(
    chunk_hash: &ChunkHash,
    transactions: Vec<SignedTransaction>,
    receipts: Vec<Receipt>,
) -> ShardChunk {
    ShardChunk::V2(ShardChunkV2 {
        chunk_hash: chunk_hash.clone(),
        header: create_chunk_header(0, 0),
        transactions,
        prev_outgoing_receipts: receipts,
    })
}

fn create_encoded_shard_chunk(
    transactions: Vec<SignedTransaction>,
    receipts: &[Receipt],
) -> (EncodedShardChunk, Vec<Vec<MerklePathItem>>) {
    let mut rs = ReedSolomonWrapper::new(33, 67);
    ShardsManager::create_encoded_shard_chunk(
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        transactions,
        receipts,
        Default::default(),
        Default::default(),
        CongestionInfo::default(),
        &validator_signer(),
        &mut rs,
        100,
    )
    .unwrap()
}

fn encoded_chunk_to_partial_encoded_chunk(
    encoded_chunk: EncodedShardChunk,
    receipts: Vec<Receipt>,
    merkle_paths: Vec<Vec<MerklePathItem>>,
) -> PartialEncodedChunk {
    let header = encoded_chunk.cloned_header();
    let shard_id = header.shard_id();
    let shard_layout = ShardLayout::get_simple_nightshade_layout_v2();

    let hashes = Chain::build_receipts_hashes(&receipts, &shard_layout);
    let (_root, proofs) = merklize(&hashes);

    let mut receipts_by_shard = Chain::group_receipts_by_shard(receipts, &shard_layout);
    let receipt_proofs = proofs
        .into_iter()
        .enumerate()
        .map(move |(proof_shard_id, proof)| {
            let proof_shard_id = proof_shard_id as u64;
            let receipts = receipts_by_shard.remove(&proof_shard_id).unwrap_or_else(Vec::new);
            let shard_proof =
                ShardProof { from_shard_id: shard_id, to_shard_id: proof_shard_id, proof };
            ReceiptProof(receipts, shard_proof)
        })
        .collect();

    let partial_chunk = PartialEncodedChunk::V2(PartialEncodedChunkV2 {
        header,
        parts: encoded_chunk
            .content()
            .parts
            .clone()
            .into_iter()
            .zip(merkle_paths)
            .enumerate()
            .map(|(part_ord, (part, merkle_proof))| {
                let part_ord = part_ord as u64;
                let part = part.unwrap();
                PartialEncodedChunkPart { part_ord, part, merkle_proof }
            })
            .collect(),
        prev_outgoing_receipts: receipt_proofs,
    });
    partial_chunk
}

/// Copies the data to use more memory.
///
/// We want in-memory speed. Modern CPUs have large L3 caches and we want to spread it such that L3 hits are rare.
/// 13th gen intel-i7 has 30MB L3
/// Sapphire Rapids Xeon processors have up to 112.5MB smart cache.
/// AMD Ryzen 7000 series goes up to 128MB L3
/// AMD Epyc 9684X has 1152MB L3!
///
/// The problem is, spreading data far apart in memory makes the benchmark awfully slow.
/// For now, we pick 200MB and assume the benchmark is not run on a beast of a server CPU.
/// If you are running this benchmark on a machine with >128MB L3, increase the number below.
fn spread_in_memory<T>(chunk: T) -> Vec<T>
where
    T: Clone + BorshSerialize,
{
    let memory_range = 200_000_000;
    let num_chunks = memory_range / borsh::object_length(&chunk).unwrap();
    let chunks: Vec<_> = std::iter::repeat(chunk).take(num_chunks).collect();
    chunks
}

benchmark_group!(benches, benchmark_write_shard_chunk, benchmark_write_partial_encoded_chunk);

benchmark_main!(benches);

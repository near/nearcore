//! This benchmark measures the performance of breaking down state witnesses into
//! Reed Solomon encoded parts for distribution to chunk validators.
//!
//! Run with `cargo bench --bench generate_state_witness_parts`

use bencher::{Bencher, benchmark_group, benchmark_main, black_box};

use near_client::stateless_validation::partial_witness::partial_witness_actor::{
    WITNESS_RATIO_DATA_PARTS, compress_witness, generate_state_witness_parts,
};
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::state_witness::EncodedChunkStateWitness;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use testlib::state_witness_test_data;

const VALIDATOR_COUNT: usize = 20;

fn generate_test_witness_bytes(size: usize) -> EncodedChunkStateWitness {
    let witness = state_witness_test_data::generate_realistic_state_witness(size);
    let witness_bytes = compress_witness(&witness).unwrap();
    witness_bytes
}

fn generate_validators(count: usize) -> Vec<AccountId> {
    (0..count).map(|i| format!("validator{}.near", i).parse().unwrap()).collect()
}

fn generate_chunk_header() -> ShardChunkHeader {
    ShardChunkHeader::new_dummy(100, ShardId::new(0), CryptoHash::default())
}

fn generate_signer() -> ValidatorSigner {
    InMemoryValidatorSigner::from_seed(
        "producer.near".parse().unwrap(),
        near_crypto::KeyType::ED25519,
        "producer.near",
    )
}

/// Benchmark state witness part generation
fn bench_generate_state_witness_parts(bench: &mut Bencher) {
    let chunk_validators = generate_validators(VALIDATOR_COUNT);
    let encoder = ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS).entry(VALIDATOR_COUNT);
    let epoch_id = EpochId::default();
    let chunk_header = generate_chunk_header();
    let signer = generate_signer();
    let witness_bytes = generate_test_witness_bytes(15_000_000);

    bench.iter(|| {
        black_box(generate_state_witness_parts(
            encoder.clone(),
            epoch_id,
            &chunk_header,
            witness_bytes.clone(),
            &chunk_validators,
            &signer,
        ));
    });
}

/// Benchmark Reed Solomon encoding performance separately
fn bench_reed_solomon_encoding_only(bench: &mut Bencher) {
    let witness_bytes = generate_test_witness_bytes(15_000_000);
    let encoder = ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS).entry(VALIDATOR_COUNT);

    bench.iter(|| {
        black_box(encoder.encode(&witness_bytes));
    });
}

benchmark_group!(benches, bench_generate_state_witness_parts, bench_reed_solomon_encoding_only,);

benchmark_main!(benches);

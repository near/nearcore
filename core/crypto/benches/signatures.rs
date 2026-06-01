//! Criterion benchmarks comparing ed25519, secp256k1, and ML-DSA-65.
//!
//! Run with `cargo bench -p near-crypto --bench signatures`.
//!
//! Four groups are measured per scheme: keygen, sign, verify (one fixed
//! keypair + signature re-verified every iteration), and verify_random_keys
//! (round-robin over many random keypairs, to average over the
//! key-specific verify-cost variation a single-key benchmark can't see).

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use sha2::{Digest, Sha256};

const SCHEMES: [(KeyType, &str); 3] = [
    (KeyType::ED25519, "ed25519"),
    (KeyType::SECP256K1, "secp256k1"),
    (KeyType::MLDSA65, "ml-dsa-65"),
];

fn bench_keygen(c: &mut Criterion) {
    let mut group = c.benchmark_group("keygen");
    for (key_type, name) in SCHEMES {
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let _sk = SecretKey::from_random(key_type);
            });
        });
    }
    group.finish();
}

fn bench_sign(c: &mut Criterion) {
    let mut group = c.benchmark_group("sign");
    let msg: [u8; 32] = Sha256::digest(b"benchmark-sign").into();
    for (key_type, name) in SCHEMES {
        let sk = SecretKey::from_random(key_type);
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let _sig = sk.sign(&msg);
            });
        });
    }
    group.finish();
}

fn bench_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify");
    let msg: [u8; 32] = Sha256::digest(b"benchmark-verify").into();
    for (key_type, name) in SCHEMES {
        let sk = SecretKey::from_random(key_type);
        let pk: PublicKey = sk.public_key();
        let sig: Signature = sk.sign(&msg);
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                assert!(sig.verify(&msg, &pk));
            });
        });
    }
    group.finish();
}

fn bench_verify_random_keys(c: &mut Criterion) {
    // Pre-generate N (signature, pubkey) pairs and round-robin through them.
    // This averages over key-specific verify cost variation that a single-key
    // benchmark can't see.
    let mut group = c.benchmark_group("verify_random_keys");
    const N: usize = 256;
    let msg: [u8; 32] = Sha256::digest(b"random-keys").into();
    for (key_type, name) in SCHEMES {
        let mut pairs: Vec<(PublicKey, Signature)> = Vec::with_capacity(N);
        for _ in 0..N {
            let sk = SecretKey::from_random(key_type);
            let pk = sk.public_key();
            let sig = sk.sign(&msg);
            pairs.push((pk, sig));
        }
        let mut idx = 0usize;
        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| {
                let (pk, sig) = &pairs[idx % N];
                idx = idx.wrapping_add(1);
                assert!(sig.verify(&msg, pk));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_keygen, bench_sign, bench_verify, bench_verify_random_keys);
criterion_main!(benches);

//! Microbenchmark for native P-256 ECDSA verification.
//!
//! Used to cross-check gas calibration for the `p256_verify` host function
//! (NEP-635). Run with:
//!
//! ```text
//! cargo bench -p near-vm-runner --bench p256_verify
//! ```
//!
//! The wall-clock time measured here, multiplied by `GAS_IN_NS`
//! (1_000_000 gas/ns) and `SAFETY_MULTIPLIER` (3), gives an independent
//! ballpark for the values landed in `parameters.yaml`.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};

const SECRET_KEY: [u8; 32] = [
    0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6, 0x93,
    0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f, 0x67, 0x21,
];

fn setup(msg_len: usize) -> (Vec<u8>, Signature, VerifyingKey) {
    let signing_key = SigningKey::from_bytes(&SECRET_KEY.into()).unwrap();
    let verifying_key = VerifyingKey::from(&signing_key);
    let message = vec![0x7u8; msg_len];
    let signature: Signature = signing_key.sign(&message);
    (message, signature, verifying_key)
}

fn bench_verify_32b(c: &mut Criterion) {
    let (message, signature, verifying_key) = setup(32);
    c.bench_function("p256_verify/32B", |b| {
        b.iter(|| {
            let ok = verifying_key.verify(&message, &signature).is_ok();
            assert!(ok);
        });
    });
}

fn bench_verify_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("p256_verify/scaling");
    for &msg_len in &[32usize, 1024, 4096, 16 * 1024] {
        let (message, signature, verifying_key) = setup(msg_len);
        group.bench_with_input(BenchmarkId::from_parameter(msg_len), &msg_len, |b, _| {
            b.iter(|| {
                let ok = verifying_key.verify(&message, &signature).is_ok();
                assert!(ok);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_verify_32b, bench_verify_scaling);
criterion_main!(benches);

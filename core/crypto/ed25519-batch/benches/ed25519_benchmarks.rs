// cspell:ignore csprng
// This file is based on https://github.com/dalek-cryptography/curve25519-dalek/blob/curve25519-4.1.3/ed25519-dalek/benches/ed25519_benchmarks.rs
// Modifications:
// - Modified benchmark for `verify_batch` to use `safe_verify_batch`.
// - Modified batch sizes.
// - Removed other benchmarks.
//
// This file is part of ed25519-dalek.
// Copyright (c) 2018-2019 isis lovecruft
// See LICENSE for licensing information.
//
// Authors:
// - isis agora lovecruft <isis@patternsinthevoid.net>

use criterion::{Criterion, criterion_group};

mod ed25519_benches {
    use super::*;
    use ed25519_dalek::Signature;
    use ed25519_dalek::Signer;
    use ed25519_dalek::SigningKey;
    use near_crypto_ed25519_batch::safe_verify_batch;
    use near_crypto_ed25519_batch::test_utils::MESSAGE_TO_SIGN;
    use rand::prelude::ThreadRng;
    use rand::thread_rng;

    fn verify(c: &mut Criterion) {
        let mut csprng: ThreadRng = thread_rng();
        let keypair: SigningKey = SigningKey::generate(&mut csprng);
        let sig: Signature = keypair.sign(MESSAGE_TO_SIGN);

        c.bench_function("Ed25519 signature verification", move |b| {
            b.iter(|| keypair.verify(MESSAGE_TO_SIGN, &sig))
        });
    }

    fn safe_verify_batch_signatures(c: &mut Criterion) {
        // static BATCH_SIZES: [usize; 9] = [4, 8, 16, 32, 64, 96, 128, 256, 1024];
        static BATCH_SIZES: [usize; 3] = [128, 256, 1024];

        // Benchmark batch verification for all the above batch sizes
        let mut group = c.benchmark_group("Ed25519 safe batch signature verification");
        for size in BATCH_SIZES {
            let name = format!("size={size}");
            group.bench_function(name, |b| {
                let mut csprng: ThreadRng = thread_rng();
                let keypairs: Vec<SigningKey> =
                    (0..size).map(|_| SigningKey::generate(&mut csprng)).collect();
                let messages: Vec<&[u8]> = (0..size).map(|_| MESSAGE_TO_SIGN).collect();
                let signatures: Vec<Signature> =
                    keypairs.iter().map(|key| key.sign(MESSAGE_TO_SIGN)).collect();
                let verifying_keys: Vec<_> =
                    keypairs.iter().map(|key| key.verifying_key()).collect();

                b.iter(|| {
                    safe_verify_batch(&messages[..], &signatures[..], &verifying_keys[..])
                        .expect("Batch verification failed")
                });
            });
        }
    }

    criterion_group! {
        name = ed25519_benches;
        config = Criterion::default();
        targets =
            verify,
            safe_verify_batch_signatures,
    }
}

criterion::criterion_main!(ed25519_benches::ed25519_benches);

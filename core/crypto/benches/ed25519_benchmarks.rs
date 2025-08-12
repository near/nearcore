// -*- mode: rust; -*-
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
    use near_crypto::batch::safe_verify_batch;
    use rand::prelude::ThreadRng;
    use rand::thread_rng;

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
                let msg: &[u8] = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                let messages: Vec<&[u8]> = (0..size).map(|_| msg).collect();
                let signatures: Vec<Signature> = keypairs.iter().map(|key| key.sign(msg)).collect();
                let signatures_ref: Vec<&Signature> = signatures.iter().collect();
                let verifying_keys: Vec<_> =
                    keypairs.iter().map(|key| key.verifying_key()).collect();

                b.iter(|| {
                    safe_verify_batch(&messages[..], &signatures_ref[..], &verifying_keys[..])
                        .expect("Batch verification failed")
                });
            });
        }
    }

    criterion_group! {
        name = ed25519_benches;
        config = Criterion::default();
        targets =
            safe_verify_batch_signatures,
    }
}

criterion::criterion_main!(ed25519_benches::ed25519_benches);

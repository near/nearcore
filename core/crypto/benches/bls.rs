use criterion::{black_box, criterion_group, criterion_main, Benchmark, Criterion};
use near_crypto::BlsSecretKey;

fn signing(c: &mut Criterion) {
    let sk = milagro_bls::SecretKey::random(&mut rand::thread_rng());
    let pk = milagro_bls::PublicKey::from_secret_key(&sk);
    let message = [1; 256];
    let domain = 42;
    let sig = milagro_bls::Signature::new(&message, domain, &sk);
    c.bench(
        "signing",
        Benchmark::new("Create a signature", move |b| {
            b.iter(|| {
                black_box(milagro_bls::Signature::new(&message, domain, &sk));
            })
        })
        .sample_size(10),
    );
    c.bench(
        "signing",
        Benchmark::new("Verify a Signature", move |b| {
            b.iter(|| {
                black_box(sig.verify(&message, domain, &pk));
            })
        })
        .sample_size(10),
    );
}

fn verify_aggregate(c: &mut Criterion) {
    let sk = BlsSecretKey::from_random();
    let pk = sk.public_key();
    let message = [1; 32];
    let sig = sk.sign(&message);
    c.bench(
        "verify_aggregate",
        Benchmark::new("Verify signature", move |b| {
            b.iter(|| black_box(sig.verify_single(&message, &pk)))
        })
        .sample_size(10),
    );
}

fn aggregate(c: &mut Criterion) {
    let sk = milagro_bls::SecretKey::random(&mut rand::thread_rng());
    let pk = milagro_bls::PublicKey::from_secret_key(&sk);
    let n = 1000;
    let message = [1; 256];
    let domain = 42;
    let sig = milagro_bls::Signature::new(&message, domain, &sk);
    c.bench(
        "aggregation",
        Benchmark::new(format!("Aggregate {} pub keys", n), move |b| {
            b.iter(|| {
                let mut aggregate_key = milagro_bls::AggregatePublicKey::new();
                for _ in 0..n {
                    aggregate_key.add(&pk);
                }
            });
        })
        .sample_size(10),
    );
    c.bench(
        "aggregation",
        Benchmark::new(format!("Aggregate {} signatures", n), move |b| {
            b.iter(|| {
                let mut aggregate_signature = milagro_bls::AggregateSignature::new();
                for _ in 0..n {
                    aggregate_signature.add(&sig);
                }
            });
        })
        .sample_size(10),
    );
}

fn aggregate_verfication(c: &mut Criterion) {
    let n = 1000;

    let mut pubkeys = vec![];
    let mut agg_sig = milagro_bls::AggregateSignature::new();
    let msg = b"signed message";
    let domain = 0;

    for _ in 0..n {
        let keypair = milagro_bls::Keypair::random(&mut rand::thread_rng());
        let sig = milagro_bls::Signature::new(&msg[..], domain, &keypair.sk);
        agg_sig.add(&sig);
        pubkeys.push(keypair.pk);
    }

    assert_eq!(pubkeys.len(), n);

    c.bench(
        "aggregation",
        Benchmark::new(format!("Verifying aggregate of {} signatures", n), move |b| {
            b.iter(|| {
                let pubkeys_as_ref: Vec<&milagro_bls::PublicKey> = pubkeys.iter().collect();
                let agg_pub =
                    milagro_bls::AggregatePublicKey::from_public_keys(pubkeys_as_ref.as_slice());
                let verified = agg_sig.verify(&msg[..], domain, &agg_pub);
                assert!(verified);
            })
        })
        .sample_size(10),
    );
}
criterion_group!(benches, signing, aggregate, aggregate_verfication, verify_aggregate);
criterion_main!(benches);

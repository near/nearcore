#[macro_use]
extern crate bencher;

use bencher::Bencher;

extern crate primitives;

use primitives::aggregate_signature::{BlsAggregatePublicKey, BlsAggregateSignature, BlsSecretKey};

fn bls_sign(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let message = "Hello, world!";

    bench.iter(|| {
        key.sign(message.as_bytes());
    });
}

fn bls_verify(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let public = key.get_public_key();
    let message = "Hello, world!";
    let signature = key.sign(message.as_bytes());

    bench.iter(|| {
        public.verify(message.as_bytes(), &signature);
    });
}

fn bls_aggregate_signature(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let message = "Hello, world!";
    let signature = key.sign(message.as_bytes());
    let mut agg_sig = BlsAggregateSignature::new();

    bench.iter(|| {
        agg_sig.aggregate(&signature);
    });
}

fn bls_aggregate_pubkey(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let public = key.get_public_key();
    let mut agg_key = BlsAggregatePublicKey::new();

    bench.iter(|| {
        agg_key.aggregate(&public);
    });
}

/// Aggregate signatures, but keep them in affine coordinates at each step
fn bls_aggregate_signature_slow(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let message = "Hello, world!";
    let mut signature = key.sign(message.as_bytes());

    bench.iter(|| {
        let mut agg_sig = BlsAggregateSignature::new();
        agg_sig.aggregate(&signature);
        agg_sig.aggregate(&signature);
        signature = agg_sig.get_signature();
    });
}

/// Aggregate pubkeys, but keep them in affine coordinates at each step
fn bls_aggregate_pubkey_slow(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let mut public = key.get_public_key();

    bench.iter(|| {
        let mut agg_key = BlsAggregatePublicKey::new();
        agg_key.aggregate(&public);
        agg_key.aggregate(&public);
        public = agg_key.get_key();
    });
}

fn bls_decompress_pubkey(bench: &mut Bencher) {
    let public = BlsSecretKey::generate().get_public_key();
    let compressed = public.compress();

    bench.iter(|| {
        compressed.decompress().unwrap();
    });
}

fn bls_decompress_signature(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let message = "Hello, world!";
    let signature = key.sign(message.as_bytes());
    let compressed = signature.compress();

    bench.iter(|| {
        compressed.decode().unwrap();
    });
}

fn bls_decompress_pubkey_unchecked(bench: &mut Bencher) {
    let public = BlsSecretKey::generate().get_public_key();
    let compressed = public.compress();

    bench.iter(|| {
        compressed.decompress_unchecked();
    });
}

fn bls_decode_uncompressed_signature(bench: &mut Bencher) {
    let key = BlsSecretKey::generate();
    let message = "Hello, world!";
    let signature = key.sign(message.as_bytes());
    let encoded = signature.encode_uncompressed();

    bench.iter(|| {
        encoded.decode().ok();
    })
}

benchmark_group!(
    benches,
    bls_sign,
    bls_verify,
    bls_aggregate_signature,
    bls_aggregate_pubkey,
    bls_aggregate_signature_slow,
    bls_aggregate_pubkey_slow,
    bls_decompress_signature,
    bls_decompress_pubkey,
    bls_decompress_pubkey_unchecked,
    bls_decode_uncompressed_signature,
);
benchmark_main!(benches);

//! Custom benchmark harness that measures the *distribution* of verify
//! times — not just the mean — for ed25519, secp256k1, and ML-DSA-65.
//!
//! ML-DSA verify time is variable (rejection sampling), so a mean is
//! insufficient for gas-cost calibration. We need p99/p99.9/max to
//! price the worst case.
//!
//! Run with:
//!   cargo bench -p near-crypto --bench verify_distribution
//!
//! Output is one table written to stdout, with a row per (scheme, mode)
//! across three modes:
//!   1. valid           — same key signed/verified the message; verify -> Ok
//!   2. random garbage  — correct-length random bytes (from_parts-accepted),
//!                        real pubkey, real msg; verify -> false
//!   3. tampered msg    — valid sig, verified against a *different* message
//!
//! Together these show the spread between honest and adversarial paths,
//! which is what gas pricing has to cover.

use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use sha2::{Digest, Sha256};
use std::time::Instant;

const N: usize = 10_000;
const WARMUP: usize = 100;
const SCHEMES: [(KeyType, &str); 3] = [
    (KeyType::ED25519, "ed25519"),
    (KeyType::SECP256K1, "secp256k1"),
    (KeyType::MLDSA65, "ml-dsa-65"),
];

struct Stats {
    name: &'static str,
    mode: &'static str,
    mean_ns: u128,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
    p999_ns: u128,
    max_ns: u128,
    stddev_ns: f64,
    pubkey_bytes: usize,
    sig_bytes: usize,
    /// Fraction of iterations where `verify()` returned true.
    accept_rate: f64,
}

fn percentile(sorted_ns: &[u128], p: f64) -> u128 {
    let n = sorted_ns.len();
    if n == 0 {
        return 0;
    }
    let idx = ((p / 100.0) * n as f64).ceil() as usize;
    let idx = idx.clamp(1, n) - 1;
    sorted_ns[idx]
}

fn pubkey_size(pk: &PublicKey) -> usize {
    match pk {
        PublicKey::ED25519(_) => 32,
        PublicKey::SECP256K1(_) => 64,
        PublicKey::MLDSA65(k) => k.0.len(),
    }
}

fn sig_size(sig: &Signature) -> usize {
    match sig {
        Signature::ED25519(_) => 64,
        Signature::SECP256K1(_) => 65,
        Signature::MLDSA65(s) => s.0.len(),
    }
}

/// Build a `Signature` of the same scheme as `sample` whose bytes are
/// drawn from `rng_byte()`, retrying until `Signature::from_parts` accepts
/// them. Rejection is rare for fixed-length variants (e.g. ed25519 rejects
/// a non-canonical high bit in the last byte); retrying keeps every
/// iteration backed by a well-formed signature.
fn make_random_sig<F: FnMut() -> u8>(sample: &Signature, mut rng_byte: F) -> Signature {
    let len = sig_size(sample);
    let kt = sample.key_type();
    loop {
        let mut buf = vec![0u8; len];
        for b in &mut buf {
            *b = rng_byte();
        }
        if let Ok(sig) = Signature::from_parts(kt, &buf) {
            return sig;
        }
    }
}

/// Pre-generate (sk, pk, valid_sig) triples. Keygen is itself slow for
/// ML-DSA; doing it ahead of time keeps it out of the measured region.
fn pregen(key_type: KeyType, name: &str, n: usize, msg: &[u8]) -> Vec<(PublicKey, Signature)> {
    eprint!("  generating {n} {name} keypairs+signatures... ");
    let pairs: Vec<(PublicKey, Signature)> = (0..n)
        .map(|_| {
            let sk = SecretKey::from_random(key_type);
            let pk = sk.public_key();
            let sig = sk.sign(msg);
            (pk, sig)
        })
        .collect();
    eprintln!("done.");
    pairs
}

#[derive(Copy, Clone)]
enum Mode {
    Valid,
    RandomGarbage,
    TamperedMessage,
}

impl Mode {
    fn label(&self) -> &'static str {
        match self {
            Mode::Valid => "valid",
            Mode::RandomGarbage => "random",
            Mode::TamperedMessage => "tampered",
        }
    }
}

fn bench_one(mode: Mode, key_type: KeyType, name: &'static str) -> Stats {
    let msg: [u8; 32] = Sha256::digest(b"verify-distribution").into();
    let other_msg: [u8; 32] = Sha256::digest(b"a-different-message").into();
    let total = N + WARMUP;

    let valid_pairs = pregen(key_type, name, total, &msg);

    // Build the per-iteration (verify_pubkey, verify_signature, verify_msg)
    // ahead of the timing loop so the only thing measured is `verify()`.
    let mut iters: Vec<(PublicKey, Signature, &[u8])> = Vec::with_capacity(total);
    let mut counter: u64 = 0xCAFE_BABE_DEAD_BEEF;
    let mut next_byte = || -> u8 {
        // xorshift64 — cheap deterministic pseudo-random bytes.
        counter ^= counter << 13;
        counter ^= counter >> 7;
        counter ^= counter << 17;
        counter as u8
    };

    for (pk, sig) in valid_pairs {
        match mode {
            Mode::Valid => iters.push((pk, sig, &msg[..])),
            Mode::RandomGarbage => {
                let bad = make_random_sig(&sig, &mut next_byte);
                iters.push((pk, bad, &msg[..]));
            }
            Mode::TamperedMessage => {
                // Valid signature, verify against a different message —
                // structurally well-formed, fails on the final hash check
                // (drives the verifier through the full computation).
                iters.push((pk, sig, &other_msg[..]));
            }
        }
    }

    // Warmup: run verify to warm caches/branch predictors before timing.
    for (pk, sig, vmsg) in &iters[..WARMUP] {
        let _ = sig.verify(vmsg, pk);
    }

    // Measured region.
    let mut samples: Vec<u128> = Vec::with_capacity(N);
    let mut accepted = 0usize;
    for (pk, sig, vmsg) in &iters[WARMUP..WARMUP + N] {
        let t0 = Instant::now();
        let ok = sig.verify(vmsg, pk);
        let dt = t0.elapsed().as_nanos();
        if ok {
            accepted += 1;
        }
        samples.push(dt);
    }

    samples.sort_unstable();
    let sum: u128 = samples.iter().sum();
    let mean = sum / N as u128;
    let mean_f = mean as f64;
    let var: f64 = samples.iter().map(|&x| (x as f64 - mean_f).powi(2)).sum::<f64>() / N as f64;
    let stddev_ns = var.sqrt();
    let pubkey_bytes = pubkey_size(&iters[0].0);
    let sig_bytes = sig_size(&iters[0].1);

    Stats {
        name,
        mode: mode.label(),
        mean_ns: mean,
        p50_ns: percentile(&samples, 50.0),
        p95_ns: percentile(&samples, 95.0),
        p99_ns: percentile(&samples, 99.0),
        p999_ns: percentile(&samples, 99.9),
        max_ns: *samples.last().unwrap(),
        stddev_ns,
        pubkey_bytes,
        sig_bytes,
        accept_rate: accepted as f64 / N as f64,
    }
}

fn fmt_ns(ns: u128) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2}µs", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

fn print_table(rows: &[Stats]) {
    println!(
        "{:<10}  {:<10}  {:>5}  {:>5}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>7}",
        "scheme",
        "mode",
        "pk",
        "sig",
        "mean",
        "p50",
        "p95",
        "p99",
        "p99.9",
        "max",
        "stddev",
        "accept",
    );
    println!("{}", "-".repeat(132));
    for s in rows {
        println!(
            "{:<10}  {:<10}  {:>5}  {:>5}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6.2}%",
            s.name,
            s.mode,
            s.pubkey_bytes,
            s.sig_bytes,
            fmt_ns(s.mean_ns),
            fmt_ns(s.p50_ns),
            fmt_ns(s.p95_ns),
            fmt_ns(s.p99_ns),
            fmt_ns(s.p999_ns),
            fmt_ns(s.max_ns),
            fmt_ns(s.stddev_ns as u128),
            s.accept_rate * 100.0,
        );
    }
}

fn main() {
    eprintln!(
        "verify-distribution: N={N} per scheme/mode (warmup={WARMUP}). \
         Different random key+sig per iteration."
    );

    let mut rows = Vec::new();
    for mode in [Mode::Valid, Mode::RandomGarbage, Mode::TamperedMessage] {
        eprintln!("== mode: {} ==", mode.label());
        for (kt, name) in SCHEMES {
            eprintln!("-- {name} --");
            rows.push(bench_one(mode, kt, name));
        }
    }

    println!();
    print_table(&rows);
    println!();
    println!(
        "Notes:\n  \
         - 'valid':    keygen + sign + verify on matching pubkey.\n  \
         - 'random':   random bytes shaped as a signature, valid pubkey, real msg.\n  \
         - 'tampered': real signature, real pubkey, different message.\n  \
         The 'tampered' path is the most useful proxy for cost-of-rejection on\n  \
         honestly-shaped-but-wrong inputs: it forces the verifier through the\n  \
         full computation before failing the final hash check."
    );
}

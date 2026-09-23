//! ML-DSA-65 worst-case verify investigation.
//!
//! Walk-through of the AWS-LC `mld_sign_verify_internal` shows that the only
//! signature-data-dependent compute on the *valid-shape* path is
//! `polyveck_use_hint`, whose work is proportional to `weight(h)` and bounded
//! by `ω = 55`. Everything else is constant-time.
//!
//! This bench answers two questions:
//!
//!   H1: Does verify time grow with `weight(h)`? If `polyveck_use_hint` is the
//!       only data-dependent step, higher-weight signatures should verify
//!       slower. `experiment_h1_natural_distribution` buckets naturally-sampled
//!       signatures by weight and times each bucket.
//!   H2: How heavy is the tail? `experiment_h2_tampered_tail` samples 100k
//!       tampered signatures forced through the full verify computation and
//!       reports its percentiles, bounding the worst case an attacker can reach
//!       by trying many inputs.
//!
//! ML-DSA-65 signature layout (3309 bytes total):
//!   [0..48]      c_tilde       — challenge commitment (2λ/8 = 48 bytes for ML-DSA-65)
//!   [48..3248]   z             — response (5 polynomials × 640 bytes)
//!   [3248..3303] hint indices  — ω = 55 bytes
//!   [3303..3309] hint counts   — cumulative count per polynomial (k = 6)
//!
//! `weight(h)` = sig[3308] (last cumulative count).
//!
//! Run:
//!   cargo bench -p near-crypto --bench ml_dsa_worst_case

use near_crypto::{KeyType, PublicKey, SecretKey, Signature};
use sha2::{Digest, Sha256};
use std::time::Instant;

const HINT_TOTAL_OFFSET: usize = 3308; // last of the 6 cumulative count bytes

const PROBE_N: usize = 4096;
const VERIFY_REPS: usize = 200;
const TAIL_N: usize = 100_000;

fn time_verify(pk: &PublicKey, sig: &Signature, msg: &[u8]) -> u128 {
    let t0 = Instant::now();
    let _ = sig.verify(msg, pk);
    t0.elapsed().as_nanos()
}

/// One sampled signature: the keypair's public key, the signature, the
/// message it was signed over, and the extracted hint weight `weight(h)`.
struct Sample {
    pk: PublicKey,
    sig: Signature,
    msg: [u8; 32],
    weight: u8,
}

/// Sign `n` distinct messages with `n` distinct keys so callers can
/// investigate weight ↔ verify time. The message is returned alongside each
/// signature so the verify path can reuse it without re-deriving.
fn sample_signatures(n: usize) -> Vec<Sample> {
    eprint!("  sampling {n} ML-DSA-65 (key, sig) pairs and extracting h-weights... ");
    let out: Vec<_> = (0..n)
        .map(|i| {
            let sk = SecretKey::from_random(KeyType::MLDSA65);
            let pk = sk.public_key();
            let msg: [u8; 32] = Sha256::digest(format!("hint-weight-probe-{i}")).into();
            let sig = sk.sign(&msg);
            let Signature::MLDSA65(s) = &sig else { unreachable!() };
            let weight = s.0[HINT_TOTAL_OFFSET];
            Sample { pk, sig, msg, weight }
        })
        .collect();
    eprintln!("done.");
    out
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    let idx = idx.clamp(1, sorted.len()) - 1;
    sorted[idx]
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

fn experiment_h1_natural_distribution() {
    eprintln!("\n## H1: weight(h) vs verify time (natural-distribution signatures)\n");

    // Sample many signatures, then bucket their verify times by h-weight.
    let probe = sample_signatures(PROBE_N);

    // Histogram of h-weight.
    let mut hist = vec![0u32; 64];
    for s in &probe {
        hist[s.weight as usize] += 1;
    }
    println!("h-weight histogram (PROBE_N={PROBE_N}):");
    for w in 0..64 {
        if hist[w] > 0 {
            println!("  weight={w:>3}: {:>5}", hist[w]);
        }
    }

    // Time each signature against its own message, bucketed by h-weight.
    let mut by_weight: Vec<Vec<u128>> = (0..64).map(|_| Vec::new()).collect();
    for s in &probe {
        // Repeated verifies of the same (pk, sig, msg) tuple to denoise.
        let mut samples = Vec::with_capacity(VERIFY_REPS);
        for _ in 0..VERIFY_REPS {
            samples.push(time_verify(&s.pk, &s.sig, &s.msg));
        }
        samples.sort_unstable();
        // Median across the inner reps for this single signature.
        by_weight[s.weight as usize].push(samples[VERIFY_REPS / 2]);
    }

    println!();
    println!(
        "{:>7}  {:>5}  {:>10}  {:>10}  {:>10}  {:>10}",
        "weight", "n", "mean", "p50", "p99", "max"
    );
    println!("{}", "-".repeat(60));
    for w in 0..64 {
        let v = &by_weight[w];
        if v.len() < 5 {
            continue; // skip sparse buckets
        }
        let mut s = v.clone();
        s.sort_unstable();
        let mean: u128 = s.iter().sum::<u128>() / s.len() as u128;
        println!(
            "{:>7}  {:>5}  {:>10}  {:>10}  {:>10}  {:>10}",
            w,
            s.len(),
            fmt_ns(mean),
            fmt_ns(percentile(&s, 50.0)),
            fmt_ns(percentile(&s, 99.0)),
            fmt_ns(*s.last().unwrap()),
        );
    }
}

fn experiment_h2_tampered_tail() {
    eprintln!("\n## H2: tail of tampered-signature verify time across {TAIL_N} iterations\n");
    let msg: [u8; 32] = Sha256::digest(b"tampered-tail-base").into();
    let other: [u8; 32] = Sha256::digest(b"tampered-tail-not-this-one").into();

    eprintln!("  pre-generating {TAIL_N} (pk, sig) pairs...");
    let pairs: Vec<(PublicKey, Signature)> = (0..TAIL_N)
        .map(|_| {
            let sk = SecretKey::from_random(KeyType::MLDSA65);
            let pk = sk.public_key();
            let sig = sk.sign(&msg);
            (pk, sig)
        })
        .collect();
    eprintln!("  done.");

    // Warmup
    for (pk, sig) in &pairs[..200] {
        let _ = sig.verify(&other, pk);
    }

    let mut samples: Vec<u128> = Vec::with_capacity(TAIL_N);
    for (pk, sig) in &pairs {
        samples.push(time_verify(pk, sig, &other));
    }
    samples.sort_unstable();

    println!("tail (n={TAIL_N}):");
    for &p in &[50.0_f64, 90.0, 99.0, 99.9, 99.99] {
        println!("  p{:>5}: {}", p, fmt_ns(percentile(&samples, p)));
    }
    println!("  max:    {}", fmt_ns(*samples.last().unwrap()));
    let mean: u128 = samples.iter().sum::<u128>() / samples.len() as u128;
    println!("  mean:   {}", fmt_ns(mean));
}

fn main() {
    eprintln!(
        "ML-DSA-65 worst-case verify investigation\n\
         (release build; pin to a single core for stable numbers, e.g. \
         `taskset -c 0 cargo bench -p near-crypto --bench ml_dsa_worst_case`)"
    );

    experiment_h1_natural_distribution();
    experiment_h2_tampered_tail();

    println!(
        "\nInterpretation:\n  \
         - If H1 shows a flat line across weights, `polyveck_use_hint` is\n  \
           cheap and not exploitable.\n  \
         - If H2's p99.99 stays close to the verify_distribution tail\n  \
           (~600µs), the worst case is bounded and the gas constant only\n  \
           needs a small safety multiplier. The single-sample max is noisy\n  \
           (it swings ~0.7-1.3ms run to run) and is not a reliable bound.\n  \
         - If either shows a meaningful tail beyond that, that's the attack\n  \
           vector and a separate `polyveck_use_hint`-aware bound is needed."
    );
}

// cspell:ignore Vartime Multiscalar multiscalar impls initialising hram hrams zhrams csprng
// This file is based on https://github.com/dalek-cryptography/curve25519-dalek/blob/curve25519-4.1.3/ed25519-dalek/src/batch.rs
// Modifications:
// - verify_batch changed to safe_verify_batch, with additional checks added.
// - Code not used in safe_verify_batch has been removed.
//
// Many thanks to the original authors, whose copyright notice is reproduced below:
//
// This file is part of ed25519-dalek.
// Copyright (c) 2017-2019 isis lovecruft
// See LICENSE for licensing information.
//
// Authors:
// - isis agora lovecruft <isis@patternsinthevoid.net>

//! Batch signature verification.

use std::iter::once;

use curve25519_dalek::digest::Digest;
use curve25519_dalek::traits::VartimeMultiscalarMul;
use curve25519_dalek::{EdwardsPoint, Scalar, constants};
use ed25519_dalek::{VerifyingKey, ed25519};
use merlin::Transcript;
use rand_core::RngCore;
use sha2::Sha512;

use crate::errors::{InternalError, SignatureError};
use crate::extras::{IsCanonicalY, VerifyingKeyInternal};
use crate::signature::InternalSignature;

/// An implementation of `rand_core::RngCore` which does nothing. This is necessary because merlin
/// demands an `Rng` as input to `TranscriptRngBuilder::finalize()`. Using this with `finalize()`
/// yields a PRG whose input is the hashed transcript.
struct ZeroRng;

impl rand_core::RngCore for ZeroRng {
    fn next_u32(&mut self) -> u32 {
        rand_core::impls::next_u32_via_fill(self)
    }

    fn next_u64(&mut self) -> u64 {
        rand_core::impls::next_u64_via_fill(self)
    }

    /// A no-op function which leaves the destination bytes for randomness unchanged.
    ///
    /// In this case, the internal merlin code is initialising the destination
    /// by doing `[0u8; …]`, which means that when we call
    /// `merlin::TranscriptRngBuilder.finalize()`, rather than rekeying the
    /// STROBE state based on external randomness, we're doing an
    /// `ENC_{state}(00000000000000000000000000000000)` operation, which is
    /// identical to the STROBE `MAC` operation.
    fn fill_bytes(&mut self, _dest: &mut [u8]) {}

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}

// `TranscriptRngBuilder::finalize()` requires a `CryptoRng`
impl rand_core::CryptoRng for ZeroRng {}

// We write our own gen() function so we don't need to pull in the rand crate
fn gen_u128<R: RngCore>(rng: &mut R) -> u128 {
    let mut buf = [0u8; 16];
    rng.fill_bytes(&mut buf);
    u128::from_le_bytes(buf)
}

/// This is a batch implementation following [SSR:CGN20]
#[allow(non_snake_case)]
pub fn safe_verify_batch(
    messages: &[&[u8]],
    signatures: &[ed25519::Signature],
    verifying_keys: &[VerifyingKey],
) -> Result<(), SignatureError> {
    // Modification: Convert VerifyingKey to VerifyingKeyInternal
    // This is a cheap conversion that allows us to access `compressed` and `point`,
    // for similarity to the original implementation.
    let verifying_keys: Vec<VerifyingKeyInternal> =
        verifying_keys.iter().map(|vk| vk.try_into()).collect::<Result<Vec<_>, _>>()?;

    // Return an Error if any of the vectors were not the same size as the others.
    if signatures.len() != messages.len()
        || signatures.len() != verifying_keys.len()
        || verifying_keys.len() != messages.len()
    {
        return Err(InternalError::ArrayLength {
            name_a: "signatures",
            length_a: signatures.len(),
            name_b: "messages",
            length_b: messages.len(),
            name_c: "verifying_keys",
            length_c: verifying_keys.len(),
        }
        .into());
    }

    // Make a transcript which logs all inputs to this function
    let mut transcript: Transcript = Transcript::new(b"ed25519 batch verification");

    // We make one optimization in the transcript: since we will end up computing H(R || A || M)
    // for each (R, A, M) triplet, we will feed _that_ into our transcript rather than each R, A, M
    // individually. Since R and A are fixed-length, this modification is secure so long as SHA-512
    // is collision-resistant.
    // It suffices to take `verifying_keys[i].as_bytes()` even though a `VerifyingKey` has two
    // fields, and `as_bytes()` only returns the bytes of the first. This is because of an
    // invariant guaranteed by `VerifyingKey`: the second field is always the (unique)
    // decompression of the first. Thus, the serialized first field is a unique representation of
    // the entire `VerifyingKey`.
    let hrams: Vec<[u8; 64]> = (0..signatures.len())
        .map(|i| {
            // Compute H(R || A || M), where
            // R = sig.R
            // A = verifying key
            // M = msg
            let mut h: Sha512 = Sha512::default();
            h.update(signatures[i].r_bytes());
            h.update(verifying_keys[i].as_bytes());
            h.update(messages[i]);
            *h.finalize().as_ref()
        })
        .collect();

    // Update transcript with the hashes above. This covers verifying_keys, messages, and the R
    // half of signatures
    for hram in &hrams {
        transcript.append_message(b"hram", hram);
    }
    // Update transcript with the rest of the data. This covers the s half of the signatures
    for sig in signatures {
        transcript.append_message(b"sig.s", sig.s_bytes());
    }

    // All function inputs have now been hashed into the transcript. Finalize it and use it as
    // randomness for the batch verification.
    let mut rng = transcript.build_rng().finalize(&mut ZeroRng);

    // Convert all signatures to `InternalSignature`
    // Modification: Moved here for use in additional checks
    let signatures =
        signatures.iter().map(InternalSignature::try_from).collect::<Result<Vec<_>, _>>()?;

    // Modification: Moved here for use in additional checks
    let Rs: Vec<_> = signatures.iter().map(|sig| sig.R.decompress()).collect();

    // Modification: Perform extra checks as safe_verify
    // these checks are performed at this point (not earlier) for efficiency
    // performing the checks earlier do not impact the correctness or security
    for i in 0..messages.len() {
        let signature_R = Rs[i].ok_or_else(|| SignatureError::from(InternalError::Verify))?;
        // check public key is not of low order
        if signature_R.is_small_order() || verifying_keys[i].is_weak() {
            // TODO: If the chain guarantees public keys are well-formed, then
            // we can just check `if signature_R.is_small_order()` here.
            // Otherwise, we MUST check for `verifying_keys[i].is_weak()` here.
            return Err(InternalError::Verify.into());
        }
        // check canonicity of R and A
        if !signatures[i].R.is_canonical_y() || !verifying_keys[i].compressed.is_canonical_y() {
            // TODO: If the chain guarantees public keys are well-formed, then
            // we can just check `if signature_R.is_small_order()` here.
            // Otherwise, we MUST check for `verifying_keys[i].is_canonical_y()` here.
            return Err(InternalError::Verify.into());
        }
    }

    // Select a random 128-bit scalar for each signature.
    let zs: Vec<Scalar> = signatures.iter().map(|_| Scalar::from(gen_u128(&mut rng))).collect();

    // Compute the basepoint coefficient, ∑ s[i]z[i] (mod l)
    let B_coefficient: Scalar =
        signatures.iter().map(|sig| sig.s).zip(zs.iter()).map(|(s, z)| z * s).sum();

    // Convert the H(R || A || M) values into scalars
    let hrams: Vec<Scalar> = hrams.iter().map(Scalar::from_bytes_mod_order_wide).collect();

    let As = verifying_keys.iter().map(|pk| Some(pk.point));
    let B = once(Some(constants::ED25519_BASEPOINT_POINT));

    // Multiply each H(R || A || M) by the random value
    let zhrams = hrams.iter().zip(zs.iter()).map(|(hram, z)| hram * z);

    // Modification: Original code checks for the point being identity,
    // but we accept other small-order points as well.
    // Compute (-∑ z[i]s[i] (mod l)) B + ∑ z[i]R[i] + ∑ (z[i]H(R||A||M)[i] (mod l)) A[i] = 0
    let tmp_point = EdwardsPoint::optional_multiscalar_mul(
        once(-B_coefficient).chain(zs.iter().cloned()).chain(zhrams),
        B.chain(Rs).chain(As),
    )
    .ok_or(InternalError::Verify)?;

    // Accept small-order points (i.e., if point multiplied by 8 is identity)
    if tmp_point.is_small_order() { Ok(()) } else { Err(InternalError::Verify.into()) }
}

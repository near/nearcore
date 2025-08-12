use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::once;

use curve25519_dalek::digest::Digest;
use curve25519_dalek::edwards::CompressedEdwardsY;
use curve25519_dalek::traits::VartimeMultiscalarMul;
use curve25519_dalek::{EdwardsPoint, Scalar, constants};
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SIGNATURE_LENGTH, SignatureError, VerifyingKey, ed25519};
use merlin::Transcript;
use rand_core::RngCore;
use sha2::Sha512;

use crate::batch_errors::InternalError;

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

/// An ed25519 signature.
///
/// # Note
///
/// These signatures, unlike the ed25519 signature reference implementation, are
/// "detached"—that is, they do **not** include a copy of the message which has
/// been signed.
#[allow(non_snake_case)]
#[derive(Copy, Eq, PartialEq)]
pub(crate) struct InternalSignature {
    /// `R` is an `EdwardsPoint`, formed by using an hash function with
    /// 512-bits output to produce the digest of:
    ///
    /// - the nonce half of the `ExpandedSecretKey`, and
    /// - the message to be signed.
    ///
    /// This digest is then interpreted as a `Scalar` and reduced into an
    /// element in ℤ/lℤ.  The scalar is then multiplied by the distinguished
    /// basepoint to produce `R`, and `EdwardsPoint`.
    pub(crate) R: CompressedEdwardsY,

    /// `s` is a `Scalar`, formed by using an hash function with 512-bits output
    /// to produce the digest of:
    ///
    /// - the `r` portion of this `Signature`,
    /// - the `PublicKey` which should be used to verify this `Signature`, and
    /// - the message to be signed.
    ///
    /// This digest is then interpreted as a `Scalar` and reduced into an
    /// element in ℤ/lℤ.
    pub(crate) s: Scalar,
}

/// Ensures that the scalar `s` of a signature is within the bounds [0, ℓ)
#[inline(always)]
fn check_scalar(bytes: [u8; 32]) -> Result<Scalar, SignatureError> {
    match Scalar::from_canonical_bytes(bytes).into() {
        None => Err(InternalError::ScalarFormat.into()),
        Some(x) => Ok(x),
    }
}

impl InternalSignature {
    /// Construct a `Signature` from a slice of bytes.
    ///
    /// # Scalar Malleability Checking
    ///
    /// As originally specified in the ed25519 paper (cf. the "Malleability"
    /// section of the README in this repo), no checks whatsoever were performed
    /// for signature malleability.
    ///
    /// Later, a semi-functional, hacky check was added to most libraries to
    /// "ensure" that the scalar portion, `s`, of the signature was reduced `mod
    /// \ell`, the order of the basepoint:
    ///
    /// ```ignore
    /// if signature.s[31] & 224 != 0 {
    ///     return Err();
    /// }
    /// ```
    ///
    /// This bit-twiddling ensures that the most significant three bits of the
    /// scalar are not set:
    ///
    /// ```python,ignore
    /// >>> 0b00010000 & 224
    /// 0
    /// >>> 0b00100000 & 224
    /// 32
    /// >>> 0b01000000 & 224
    /// 64
    /// >>> 0b10000000 & 224
    /// 128
    /// ```
    ///
    /// However, this check is hacky and insufficient to check that the scalar is
    /// fully reduced `mod \ell = 2^252 + 27742317777372353535851937790883648493` as
    /// it leaves us with a guanteed bound of 253 bits.  This means that there are
    /// `2^253 - 2^252 + 2774231777737235353585193779088364849311` remaining scalars
    /// which could cause malleabilllity.
    ///
    /// RFC8032 [states](https://tools.ietf.org/html/rfc8032#section-5.1.7):
    ///
    /// > To verify a signature on a message M using public key A, [...]
    /// > first split the signature into two 32-octet halves.  Decode the first
    /// > half as a point R, and the second half as an integer S, in the range
    /// > 0 <= s < L.  Decode the public key A as point A'.  If any of the
    /// > decodings fail (including S being out of range), the signature is
    /// > invalid.
    ///
    /// However, by the time this was standardised, most libraries in use were
    /// only checking the most significant three bits.  (See also the
    /// documentation for [`crate::VerifyingKey::verify_strict`].)
    #[inline]
    #[allow(non_snake_case)]
    pub fn from_bytes(bytes: &[u8; SIGNATURE_LENGTH]) -> Result<InternalSignature, SignatureError> {
        // TODO: Use bytes.split_array_ref once it’s in MSRV.
        let mut R_bytes: [u8; 32] = [0u8; 32];
        let mut s_bytes: [u8; 32] = [0u8; 32];
        R_bytes.copy_from_slice(&bytes[00..32]);
        s_bytes.copy_from_slice(&bytes[32..64]);

        Ok(InternalSignature { R: CompressedEdwardsY(R_bytes), s: check_scalar(s_bytes)? })
    }
}

impl Clone for InternalSignature {
    fn clone(&self) -> Self {
        *self
    }
}

impl Debug for InternalSignature {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Signature( R: {:?}, s: {:?} )", &self.R, &self.s)
    }
}

impl TryFrom<&ed25519::Signature> for InternalSignature {
    type Error = SignatureError;

    fn try_from(sig: &ed25519::Signature) -> Result<InternalSignature, SignatureError> {
        InternalSignature::from_bytes(&sig.to_bytes())
    }
}

#[derive(Copy, Clone, Default)]
pub struct VerifyingKeyInternal {
    /// Serialized compressed Edwards-y point.
    compressed: CompressedEdwardsY,

    /// Decompressed Edwards point used for curve arithmetic operations.
    point: EdwardsPoint,
}

impl VerifyingKeyInternal {
    /// Returns the compressed bytes of the verifying key.
    pub fn as_bytes(&self) -> &[u8; PUBLIC_KEY_LENGTH] {
        &(self.compressed).0
    }

    /// Checks if the verifying key is weak (i.e., has a small order).
    pub fn is_weak(&self) -> bool {
        self.point.is_small_order()
    }
}

// TODO: TryFrom
impl From<&VerifyingKey> for VerifyingKeyInternal {
    fn from(verifying_key: &VerifyingKey) -> Self {
        let compressed_bytes = verifying_key.as_bytes();
        let compressed_point = CompressedEdwardsY::from_slice(compressed_bytes)
            .expect("VerifyingKey should always be a valid compressed point");
        let point = compressed_point
            .decompress()
            .expect("VerifyingKey should always be a valid compressed point");
        Self { compressed: compressed_point, point }
    }
}

trait IsCanonicalY {
    fn is_canonical_y(&self) -> bool;
}

impl IsCanonicalY for CompressedEdwardsY {
    /// Checks the canonicity of a point on Curve25519.
    /// a canonic point has its y-coordinate reduced mod q (2^255 - 19)
    fn is_canonical_y(&self) -> bool {
        if self.0[0] < 237 {
            return true;
        };
        for i in 1..=30 {
            if self.0[i] != 255 {
                return true;
            }
        }
        (self.0[31] | 128) != 255
    }
}

#[derive(Eq, PartialEq)]
struct OrderedScalar(Scalar);

// The comparision is not constant time
impl PartialOrd for OrderedScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedScalar {
    // scalar is implemented in little endian
    // testing the order is more efficient when starting from end
    fn cmp(&self, other: &Self) -> Ordering {
        let a_bytes = self.0.as_bytes();
        let b_bytes = other.0.as_bytes();
        for (a, b) in a_bytes.iter().rev().zip(b_bytes.iter().rev()) {
            match a.cmp(b) {
                Ordering::Equal => continue,
                non_eq => return non_eq,
            }
        }
        Ordering::Equal
    }
}

/// This is a batch implementation following [SSR:CGN20]
/// The safe_verify implementation should be used consistently with safe_verify_batch
#[allow(non_snake_case)]
pub fn safe_verify_batch(
    messages: &[&[u8]],
    signatures: &[&ed25519::Signature],
    verifying_keys: &[VerifyingKey],
) -> Result<(), SignatureError> {
    let verifying_keys: Vec<VerifyingKeyInternal> =
        verifying_keys.iter().map(|vk| vk.into()).collect();
    // Return an Error if any of the vectors were not the same size as the others.
    if signatures.len() != messages.len() || signatures.len() != verifying_keys.len() {
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
    for hram in hrams.iter() {
        transcript.append_message(b"hram", hram);
    }
    // Update transcript with the rest of the data. This covers the s half of the signatures
    for sig in signatures {
        transcript.append_message(b"sig.s", sig.s_bytes());
    }

    // All function inputs have now been hashed into the transcript. Finalize it and use it as
    // randomness for the batch verification.
    let mut rng = transcript.build_rng().finalize(&mut ZeroRng);

    let mut internal_signatures: Vec<InternalSignature> = Vec::new();
    // Convert all signatures to `InternalSignature`
    for &signature in signatures.iter() {
        internal_signatures.push(InternalSignature::try_from(signature)?);
    }

    let Rs: Vec<_> = internal_signatures.iter().map(|sig| sig.R.decompress()).collect();
    // perform extra checks as safe_verify
    // these checks are performed at this point (not earlier) for efficiency
    // performing the checks earlier do not impact the correctness or security
    for i in 0..messages.len() {
        // check s in {0,.., L-1}
        // non-optimized version implemented: lexicographic check
        // optimized version [SSR:CGN20, Sec. 4] rejects a specific authentic
        // signature (honestly generated with probability 1/2^128)
        if OrderedScalar(internal_signatures[i].s)
            >= OrderedScalar(curve25519_dalek::constants::BASEPOINT_ORDER)
        {
            return Err(InternalError::Verify.into());
        };

        let signature_R = Rs[i].ok_or_else(|| SignatureError::from(InternalError::Verify))?;
        // check public key is not of low order
        if signature_R.is_small_order() || verifying_keys[i].is_weak() {
            // for an extra optimization comment out the previous line and uncomment the next line
            // CAREFUL: do not use this optimization unless you have already verified the verification key well-formdness earlier
            // (when published on the chain)

            // if  signature_R.is_small_order() {
            return Err(InternalError::Verify.into());
        }
        // check canonicity of R and A
        if !internal_signatures[i].R.is_canonical_y()
            || !verifying_keys[i].compressed.is_canonical_y()
        {
            // for an extra optimization comment out the previous line and uncomment the next line
            // CAREFUL: do not use this optimization unless you have already verified the verification key well-formdness earlier
            // (when published on the chain)
            // if !signatures[i].R.is_canonical_y() {
            return Err(InternalError::Verify.into());
        }
    }

    // Select a random 128-bit scalar for each signature.
    let zs: Vec<Scalar> = signatures.iter().map(|_| Scalar::from(gen_u128(&mut rng))).collect();

    // Compute the basepoint coefficient, ∑ s[i]z[i] (mod l)
    let B_coefficient: Scalar =
        internal_signatures.iter().map(|sig| sig.s).zip(zs.iter()).map(|(s, z)| z * s).sum();

    // Convert the H(R || A || M) values into scalars
    let hrams: Vec<Scalar> = hrams.iter().map(Scalar::from_bytes_mod_order_wide).collect();

    let As = verifying_keys.iter().map(|pk| Some(pk.point));
    let B = once(Some(constants::ED25519_BASEPOINT_POINT));

    // Multiply each H(R || A || M) by the random value
    let zhrams = hrams.iter().zip(zs.iter()).map(|(hram, z)| hram * z);

    // Compute (-∑ z[i]s[i] (mod l)) B + ∑ z[i]R[i] + ∑ (z[i]H(R||A||M)[i] (mod l)) A[i] = 0
    let tmp_point = EdwardsPoint::optional_multiscalar_mul(
        once(-B_coefficient).chain(zs.iter().cloned()).chain(zhrams),
        B.chain(Rs).chain(As),
    )
    .ok_or(InternalError::Verify)?;

    // multiply by cofactor 8
    if tmp_point.is_small_order() { Ok(()) } else { Err(InternalError::Verify.into()) }
}

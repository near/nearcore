use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;

use crate::validator_signer::ValidatorSigner;

/// A borsh-serializable payload with domain-separated signing.
///
/// Implementors declare a unique `DIFFERENTIATOR` string. [`signing_hash`]
/// hashes it together with the payload, and [`SignedMessage`] uses that hash
/// symmetrically on sign and verify, so a signature produced over one type can
/// never pass verification under another — even when the raw borsh encodings
/// of the two payloads happen to collide.
///
/// `DIFFERENTIATOR` must be unique across all implementors in the codebase.
pub trait DomainSeparatedSignableMessage: BorshSerialize + BorshDeserialize {
    const DIFFERENTIATOR: &'static str;
}

/// The 32-byte digest a [`SignedMessage<T>`]'s signature covers. The
/// differentiator is placed first so the hasher state is fixed by the type
/// before any payload bytes mix in. `hash_borsh` streams borsh output straight
/// into Sha256, so this allocates nothing on the heap.
pub fn signing_hash<T: DomainSeparatedSignableMessage>(payload: &T) -> CryptoHash {
    CryptoHash::hash_borsh((T::DIFFERENTIATOR, payload))
}

/// A payload paired with the validator signature over its [`signing_hash`].
///
/// Fresh instances are produced via [`SignedMessage::sign`]. Wire-side
/// instances come from borsh deserialization; the caller must invoke
/// [`verify`](Self::verify) before trusting the inner payload.
///
/// Type-level domain separation: a `SignedMessage<Alpha>` cannot be substituted
/// for a `SignedMessage<Beta>` - the type system forbids it.
#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SignedMessage<T: DomainSeparatedSignableMessage> {
    inner: T,
    signature: Signature,
}

impl<T: DomainSeparatedSignableMessage> SignedMessage<T> {
    pub fn sign(inner: T, signer: &ValidatorSigner) -> Self {
        let signature = signer.sign_bytes(signing_hash(&inner).as_bytes());
        Self { inner, signature }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        self.signature.verify(signing_hash(&self.inner).as_bytes(), public_key)
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn signature(&self) -> &Signature {
        &self.signature
    }
}

#[cfg(test)]
mod tests {
    use super::{DomainSeparatedSignableMessage, SignedMessage, signing_hash};
    use crate::test_utils::create_test_signer;
    use borsh::{BorshDeserialize, BorshSerialize};

    /// Two payload types that borsh-serialize identically but carry different
    /// `DIFFERENTIATOR`s. If domain separation works, a signature produced
    /// under one must not verify under the other.
    #[derive(BorshSerialize, BorshDeserialize)]
    struct Alpha {
        payload: Vec<u8>,
    }
    impl DomainSeparatedSignableMessage for Alpha {
        const DIFFERENTIATOR: &'static str = "signing::tests::Alpha";
    }

    #[derive(BorshSerialize, BorshDeserialize)]
    struct Beta {
        payload: Vec<u8>,
    }
    impl DomainSeparatedSignableMessage for Beta {
        const DIFFERENTIATOR: &'static str = "signing::tests::Beta";
    }

    #[test]
    fn cross_type_replay_is_rejected_even_when_payload_bytes_collide() {
        let signer = create_test_signer("validator");
        let public_key = signer.public_key();
        let payload = vec![1u8, 2, 3, 4, 5];

        let alpha = Alpha { payload: payload.clone() };
        let beta = Beta { payload };

        // Precondition for the test to be meaningful: the two payloads
        // borsh-encode to the same bytes, so the only distinguishing input into
        // the signing hash is the differentiator.
        assert_eq!(borsh::to_vec(&alpha).unwrap(), borsh::to_vec(&beta).unwrap());

        // Signing hashes diverge because the differentiators do.
        assert_ne!(signing_hash(&alpha), signing_hash(&beta));

        let alpha_signed = SignedMessage::sign(alpha, &signer);
        let beta_signed = SignedMessage::sign(beta, &signer);

        // Honest paths verify.
        assert!(alpha_signed.verify(&public_key));
        assert!(beta_signed.verify(&public_key));

        // Cross-type replay at the raw-signature level: the signature bytes
        // from alpha, checked against beta's signing hash, must fail — and
        // vice versa. At the type level a `SignedMessage<Alpha>` cannot be
        // substituted for a `SignedMessage<Beta>`, so this exercises the
        // escape hatch of callers manipulating signatures directly.
        assert!(
            !alpha_signed
                .signature()
                .verify(signing_hash(beta_signed.inner()).as_bytes(), &public_key)
        );
        assert!(
            !beta_signed
                .signature()
                .verify(signing_hash(alpha_signed.inner()).as_bytes(), &public_key)
        );
    }
}

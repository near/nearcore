use crate::logic::HostError;
use crate::logic::VMLogicError;
use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::map;
use near_parameters::ExtCosts;
use p256::ecdsa::SigningKey;
use p256::ecdsa::signature::hazmat::PrehashSigner;
use std::collections::HashMap;

/// Deterministic key-pair plus arbitrary 32-byte prehash. RustCrypto's `p256`
/// uses deterministic RFC6979 nonces by default, so calling `sign_prehash` on
/// the same key/prehash produces the same signature every time; we rely on
/// that for reproducibility. The host function does not hash — per NEP-635 the
/// caller supplies a prehashed digest — so tests sign and verify the same raw
/// bytes.
fn p256_test_vectors() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let secret_key: [u8; 32] = [
        0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6,
        0x93, 0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f,
        0x67, 0x21,
    ];
    let message = vec![7u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_key.into()).unwrap();
    let signature: p256::ecdsa::Signature = signing_key.sign_prehash(&message).unwrap();
    let signature_bytes = signature.to_vec();
    let public_key = p256::ecdsa::VerifyingKey::from(&signing_key).to_encoded_point(true);
    let public_key_bytes = public_key.as_bytes().to_vec();
    (signature_bytes, message, public_key_bytes)
}

#[track_caller]
fn check_p256_verify(
    signature_len: u64,
    signature: &[u8],
    message_len: u64,
    message: &[u8],
    public_key_len: u64,
    public_key: &[u8],
    want: Result<u64, HostError>,
    want_costs: HashMap<ExtCosts, u64>,
) {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let signature_ptr = if signature_len == u64::MAX {
        logic.wrapped_internal_write_register(1, signature).unwrap();
        1
    } else {
        logic.internal_mem_write(signature).ptr
    };

    let message_ptr = if message_len == u64::MAX {
        logic.wrapped_internal_write_register(2, message).unwrap();
        2
    } else {
        logic.internal_mem_write(message).ptr
    };

    let public_key_ptr = if public_key_len == u64::MAX {
        logic.wrapped_internal_write_register(3, public_key).unwrap();
        3
    } else {
        logic.internal_mem_write(public_key).ptr
    };

    let result = logic.p256_verify(
        signature_len,
        signature_ptr,
        message_len,
        message_ptr,
        public_key_len,
        public_key_ptr,
    );

    let want = want.map_err(VMLogicError::HostError);
    assert_eq!(want, result);
    assert_costs(want_costs);
}

#[test]
fn test_p256_verify_valid_signature() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + message.len() + public_key.len()) as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_bad_signature_bytes() {
    // A 64-byte signature that is all-zeroes cannot be parsed by p256.
    let (_signature, message, public_key) = p256_test_vectors();
    let bad_signature = vec![0u8; 64];

    check_p256_verify(
        bad_signature.len() as u64,
        &bad_signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(0),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: bad_signature.len() as u64,
            ExtCosts::p256_verify_base: 1,
        },
    );
}

#[test]
fn test_p256_verify_tampered_message() {
    let (signature, message, public_key) = p256_test_vectors();
    let mut bad_message = message;
    bad_message[0] ^= 0x01;

    check_p256_verify(
        signature.len() as u64,
        &signature,
        bad_message.len() as u64,
        &bad_message,
        public_key.len() as u64,
        &public_key,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + bad_message.len() + public_key.len())
                as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: bad_message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_wrong_public_key() {
    // Generate a second, unrelated key pair to act as an attacker's public key.
    let other_secret: [u8; 32] = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x21,
    ];
    let other_signing_key = SigningKey::from_bytes(&other_secret.into()).unwrap();
    let other_public = p256::ecdsa::VerifyingKey::from(&other_signing_key).to_encoded_point(true);
    let other_public_bytes = other_public.as_bytes().to_vec();

    let (signature, message, _) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        other_public_bytes.len() as u64,
        &other_public_bytes,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + message.len() + other_public_bytes.len())
                as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_invalid_signature_length() {
    let (signature, message, public_key) = p256_test_vectors();
    let invalid_signature_len = 63u64;

    check_p256_verify(
        invalid_signature_len,
        &signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Err(HostError::P256VerifyInvalidInput { msg: "invalid signature length".to_string() }),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: invalid_signature_len,
            ExtCosts::p256_verify_base: 1,
        },
    );
}

#[test]
fn test_p256_verify_empty_signature_length() {
    let (_signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        0,
        &[],
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Err(HostError::P256VerifyInvalidInput { msg: "invalid signature length".to_string() }),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 0,
            ExtCosts::p256_verify_base: 1,
        },
    );
}

#[test]
fn test_p256_verify_invalid_public_key_length() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        public_key.len() as u64 - 1,
        &public_key,
        Err(HostError::P256VerifyInvalidInput { msg: "invalid public key length".to_string() }),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + message.len() + public_key.len() - 1)
                as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_public_key_not_on_curve() {
    // All-zero 33-byte public key fails SEC1 parsing.
    let (signature, message, _public_key) = p256_test_vectors();
    let bad_public_key = vec![0u8; 33];

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        bad_public_key.len() as u64,
        &bad_public_key,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + message.len() + bad_public_key.len())
                as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_empty_message() {
    // With a valid 64-byte signature, empty message should still verify the byte
    // cost correctly (0 message bytes). Expect verification failure since the
    // message doesn't match what was signed.
    let (signature, _message, public_key) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        0,
        &[],
        public_key.len() as u64,
        &public_key,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + public_key.len()) as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: 0,
        },
    );
}

#[test]
fn test_p256_verify_long_message() {
    // Feed the host a longer "prehash" (256 bytes) to ensure the per-byte gas
    // accounting scales with input length even though bits2field truncates the
    // prehash to the field size before verification.
    let secret_key: [u8; 32] = [
        0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6,
        0x93, 0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f,
        0x67, 0x21,
    ];
    let signing_key = SigningKey::from_bytes(&secret_key.into()).unwrap();
    let message: Vec<u8> = (0..256u16).map(|i| (i & 0xff) as u8).collect();
    let signature: p256::ecdsa::Signature = signing_key.sign_prehash(&message).unwrap();
    let signature = signature.to_vec();
    let public_key =
        p256::ecdsa::VerifyingKey::from(&signing_key).to_encoded_point(true).as_bytes().to_vec();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::read_memory_byte: (signature.len() + message.len() + public_key.len()) as u64,
            ExtCosts::read_memory_base: 3,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_signature_from_register() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        u64::MAX,
        &signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: signature.len() as u64,
            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: signature.len() as u64,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: (message.len() + public_key.len()) as u64,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_message_from_register() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        u64::MAX,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: message.len() as u64,
            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: message.len() as u64,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: (signature.len() + public_key.len()) as u64,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_public_key_from_register() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        u64::MAX,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: public_key.len() as u64,
            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: public_key.len() as u64,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: (signature.len() + message.len()) as u64,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_all_inputs_from_registers() {
    let (signature, message, public_key) = p256_test_vectors();

    check_p256_verify(
        u64::MAX,
        &signature,
        u64::MAX,
        &message,
        u64::MAX,
        &public_key,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 3,
            ExtCosts::write_register_byte: (signature.len() + message.len() + public_key.len())
                as u64,
            ExtCosts::read_register_base: 3,
            ExtCosts::read_register_byte: (signature.len() + message.len() + public_key.len())
                as u64,
            ExtCosts::p256_verify_base: 1,
            ExtCosts::p256_verify_byte: message.len() as u64,
        },
    );
}

#[test]
fn test_p256_verify_register_signature_invalid_length() {
    let (_signature, message, public_key) = p256_test_vectors();

    // Put a 1-byte value in the register, which should trigger the invalid
    // signature length error.
    check_p256_verify(
        u64::MAX,
        &[0u8],
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Err(HostError::P256VerifyInvalidInput { msg: "invalid signature length".to_string() }),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 1,
            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 1,
            ExtCosts::p256_verify_base: 1,
        },
    );
}

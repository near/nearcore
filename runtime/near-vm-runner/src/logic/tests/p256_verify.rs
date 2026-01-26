use crate::logic::HostError;
use crate::logic::VMLogicError;
use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::map;
use near_parameters::ExtCosts;
use p256::ecdsa::SigningKey;
use p256::ecdsa::signature::Signer;
use std::collections::HashMap;

fn p256_test_vectors() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let secret_key: [u8; 32] = [
        0xc9, 0xaf, 0xa9, 0xd8, 0x45, 0xba, 0x75, 0x16, 0x6b, 0x5c, 0x21, 0x57, 0x67, 0xb1, 0xd6,
        0x93, 0x4e, 0x50, 0xc3, 0xdb, 0x36, 0xe8, 0x9b, 0x12, 0x7b, 0x8a, 0x62, 0x2b, 0x12, 0x0f,
        0x67, 0x21,
    ];
    let message = vec![7u8; 32];
    let signing_key = SigningKey::from_bytes(&secret_key.into()).unwrap();
    let signature: p256::ecdsa::Signature = signing_key.sign(&message);
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
        logic.wrapped_internal_write_register(1, &signature).unwrap();
        1
    } else {
        logic.internal_mem_write(signature).ptr
    };

    let message_ptr = if message_len == u64::MAX {
        logic.wrapped_internal_write_register(2, &message).unwrap();
        2
    } else {
        logic.internal_mem_write(message).ptr
    };

    let public_key_ptr = if public_key_len == u64::MAX {
        logic.wrapped_internal_write_register(3, &public_key).unwrap();
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
fn test_p256_verify_behavior_and_errors() {
    let (signature, message, public_key) = p256_test_vectors();
    let bad_signature = vec![0u8; signature.len()];
    let invalid_signature_len = 63u64;
    let mut bad_message = message.clone();
    if let Some(first) = bad_message.first_mut() {
        *first ^= 0x01;
    }

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
    check_p256_verify(
        signature.len() as u64,
        &bad_signature,
        message.len() as u64,
        &message,
        public_key.len() as u64,
        &public_key,
        Ok(0),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: signature.len() as u64,
            ExtCosts::p256_verify_base: 1,
        },
    );
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
fn test_p256_verify_check_registers() {
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

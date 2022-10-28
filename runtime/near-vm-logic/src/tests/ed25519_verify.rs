use crate::tests::fixtures::get_context;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::VMLogic;
use crate::{map, ExtCosts};
use near_vm_errors::HostError;
use near_vm_errors::VMLogicError;

use std::collections::HashMap;

fn create_signature() -> [u8; 64] {
    [
        145, 193, 203, 18, 114, 227, 14, 117, 33, 213, 121, 66, 130, 14, 25, 4, 36, 120, 46, 142,
        226, 215, 7, 66, 122, 112, 97, 30, 249, 135, 61, 165, 221, 249, 252, 23, 105, 40, 56, 70,
        31, 152, 236, 141, 154, 122, 207, 20, 75, 118, 79, 90, 168, 6, 221, 122, 213, 29, 126, 196,
        216, 104, 191, 6,
    ]
}

fn create_public_key() -> [u8; 32] {
    [
        32, 122, 6, 120, 146, 130, 30, 37, 215, 112, 241, 251, 160, 196, 124, 17, 255, 75, 129, 62,
        84, 22, 46, 206, 158, 184, 57, 224, 118, 35, 26, 182,
    ]
}

#[track_caller]
fn check_ed25519_verify(
    logic: &mut VMLogic,
    signature_len: usize,
    signature: &[u8],
    message_len: usize,
    message: &[u8],
    public_key_len: usize,
    public_key: &[u8],
    want: Result<u64, VMLogicError>,
    want_costs: HashMap<ExtCosts, u64>,
) {
    let result = logic.ed25519_verify(
        signature_len as _,
        signature.as_ptr() as _,
        message_len as _,
        message.as_ptr() as _,
        public_key_len as _,
        public_key.as_ptr() as _,
    );

    assert_eq!(want, result);
    assert_costs(want_costs);
}

#[test]
fn test_ed25519_verify_behavior_and_errors() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let signature = create_signature();
    let bad_signature: [u8; 64] = [1; 64];

    let mut forged_signature = signature.clone();
    // create a forged signature with the `s` scalar not properly reduced
    // https://docs.rs/ed25519/latest/src/ed25519/lib.rs.html#302
    forged_signature[63] = 0b1110_0001;

    let public_key = create_public_key();

    let mut forged_public_key = public_key.clone();
    // create a forged public key to force a PointDecompressionError
    // https://docs.rs/ed25519-dalek/latest/src/ed25519_dalek/public.rs.html#142
    forged_public_key[31] = 0b1110_0001;

    // 32 bytes message
    let message: [u8; 32] = [
        107, 97, 106, 100, 108, 102, 107, 106, 97, 108, 107, 102, 106, 97, 107, 108, 102, 106, 100,
        107, 108, 97, 100, 106, 102, 107, 108, 106, 97, 100, 115, 107,
    ];

    let scenarios = [
        (
            signature.len(),
            signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len(),
            public_key.clone(),
            Ok(1),
            map! {
                ExtCosts::read_memory_byte: 128,
                ExtCosts::read_memory_base: 3,
                ExtCosts::ed25519_verify_base: 1,
                ExtCosts::ed25519_verify_byte: 32,
            },
        ),
        (
            signature.len(),
            signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len(),
            forged_public_key.clone(),
            Ok(0),
            map! {
                ExtCosts::read_memory_byte: 128,
                ExtCosts::read_memory_base: 3,
                ExtCosts::ed25519_verify_base: 1,
                ExtCosts::ed25519_verify_byte: 32,
            },
        ),
        (
            signature.len(),
            signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len() - 1,
            public_key.clone(),
            Err(VMLogicError::HostError(HostError::Ed25519VerifyInvalidInput {
                msg: "invalid public key length".to_string(),
            })),
            map! {
                ExtCosts::read_memory_byte: 127,
                ExtCosts::read_memory_base: 3,
                ExtCosts::ed25519_verify_base: 1,
                ExtCosts::ed25519_verify_byte: 32,
            },
        ),
        (
            bad_signature.len(),
            bad_signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len(),
            public_key.clone(),
            Ok(0),
            map! {
                ExtCosts::read_memory_byte: 128,
                ExtCosts::read_memory_base: 3,
                ExtCosts::ed25519_verify_base: 1,
                ExtCosts::ed25519_verify_byte: 32,
            },
        ),
        (
            signature.len() - 1,
            signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len(),
            public_key.clone(),
            Err(VMLogicError::HostError(HostError::Ed25519VerifyInvalidInput {
                msg: "invalid signature length".to_string(),
            })),
            map! {
                ExtCosts::read_memory_base: 1,
                ExtCosts::read_memory_byte: 63,
                ExtCosts::ed25519_verify_base: 1,
            },
        ),
        (
            forged_signature.len(),
            forged_signature.clone(),
            message.len(),
            message.as_slice(),
            public_key.len(),
            public_key.clone(),
            Ok(0),
            map! {
                ExtCosts::read_memory_base: 1,
                ExtCosts::read_memory_byte: 64,
                ExtCosts::ed25519_verify_base: 1,
            },
        ),
        (
            forged_signature.len(),
            forged_signature.clone(),
            0,
            message.as_slice(),
            public_key.len(),
            public_key.clone(),
            Ok(0),
            map! {
                ExtCosts::read_memory_base: 1,
                ExtCosts::read_memory_byte: 64,
                ExtCosts::ed25519_verify_base: 1,
            },
        ),
    ];

    for (
        signature_len,
        signature,
        message_len,
        message,
        public_key_len,
        public_key,
        expected_result,
        want_costs,
    ) in scenarios
    {
        check_ed25519_verify(
            &mut logic,
            signature_len as _,
            signature.as_ref(),
            message_len as _,
            message.as_ref(),
            public_key_len as _,
            public_key.as_ref(),
            expected_result,
            want_costs,
        );
    }
}

#[test]
fn test_ed25519_verify_check_registers() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let signature = create_signature();
    let public_key = create_public_key();

    let bad_signature: [u8; 64] = [1; 64];

    // 32 bytes message
    let message: [u8; 32] = [
        107, 97, 106, 100, 108, 102, 107, 106, 97, 108, 107, 102, 106, 97, 107, 108, 102, 106, 100,
        107, 108, 97, 100, 106, 102, 107, 108, 106, 97, 100, 115, 107,
    ];

    let mut forged_signature = signature.clone();
    // create a forged signature with the `s` scalar not properly reduced
    // https://docs.rs/ed25519/latest/src/ed25519/lib.rs.html#302
    forged_signature[63] = 0b1110_0001;

    // tests for data beingn read from registers
    logic.wrapped_internal_write_register(1, &signature).unwrap();
    let result = logic.ed25519_verify(
        u64::MAX,
        1 as _,
        message.len() as _,
        message.as_ptr() as _,
        public_key.len() as _,
        public_key.as_ptr() as _,
    );
    assert_eq!(Ok(1u64), result);

    logic.wrapped_internal_write_register(1, &bad_signature).unwrap();
    let result = logic.ed25519_verify(
        u64::MAX,
        1 as _,
        message.len() as _,
        message.as_ptr() as _,
        public_key.len() as _,
        public_key.as_ptr() as _,
    );
    assert_eq!(Ok(0), result);

    logic.wrapped_internal_write_register(1, &forged_signature).unwrap();
    let result = logic.ed25519_verify(
        u64::MAX,
        1 as _,
        message.len() as _,
        message.as_ptr() as _,
        public_key.len() as _,
        public_key.as_ptr() as _,
    );
    assert_eq!(Ok(0), result);

    logic.wrapped_internal_write_register(1, &message).unwrap();
    let result = logic.ed25519_verify(
        signature.len() as _,
        signature.as_ptr() as _,
        u64::MAX,
        1,
        public_key.len() as _,
        public_key.as_ptr() as _,
    );
    assert_eq!(Ok(1), result);

    logic.wrapped_internal_write_register(1, &public_key).unwrap();
    let result = logic.ed25519_verify(
        signature.len() as _,
        signature.as_ptr() as _,
        message.len() as _,
        message.as_ptr() as _,
        u64::MAX,
        1,
    );
    assert_eq!(Ok(1), result);
}

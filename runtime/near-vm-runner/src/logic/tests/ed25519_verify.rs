use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::HostError;
use crate::logic::{ExtCosts, VMLogicError};
use crate::map;
use std::collections::HashMap;

const SIGNATURE: [u8; 64] = [
    145, 193, 203, 18, 114, 227, 14, 117, 33, 213, 121, 66, 130, 14, 25, 4, 36, 120, 46, 142, 226,
    215, 7, 66, 122, 112, 97, 30, 249, 135, 61, 165, 221, 249, 252, 23, 105, 40, 56, 70, 31, 152,
    236, 141, 154, 122, 207, 20, 75, 118, 79, 90, 168, 6, 221, 122, 213, 29, 126, 196, 216, 104,
    191, 6,
];

const BAD_SIGNATURE: [u8; 64] = [1; 64];

// create a forged signature with the `s` scalar not properly reduced
// https://docs.rs/ed25519/latest/src/ed25519/lib.rs.html#302
const FORGED_SIGNATURE: [u8; 64] = {
    let mut sig = SIGNATURE;
    sig[63] = 0b1110_0001;
    sig
};

const PUBLIC_KEY: [u8; 32] = [
    32, 122, 6, 120, 146, 130, 30, 37, 215, 112, 241, 251, 160, 196, 124, 17, 255, 75, 129, 62, 84,
    22, 46, 206, 158, 184, 57, 224, 118, 35, 26, 182,
];

// create a forged public key to force a PointDecompressionError
// https://docs.rs/ed25519-dalek/latest/src/ed25519_dalek/public.rs.html#142
const FORGED_PUBLIC_KEY: [u8; 32] = {
    let mut key = PUBLIC_KEY;
    key[31] = 0b1110_0001;
    key
};

// 32 bytes message
const MESSAGE: [u8; 32] = [
    107, 97, 106, 100, 108, 102, 107, 106, 97, 108, 107, 102, 106, 97, 107, 108, 102, 106, 100,
    107, 108, 97, 100, 106, 102, 107, 108, 106, 97, 100, 115, 107,
];

#[track_caller]
fn check_ed25519_verify(
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

    let result = logic.ed25519_verify(
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
fn test_ed25519_verify_behavior_and_errors() {
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(1),
        map! {
            ExtCosts::read_memory_byte: 128,
            ExtCosts::read_memory_base: 3,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &FORGED_PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 128,
            ExtCosts::read_memory_base: 3,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64 - 1,
        &PUBLIC_KEY,
        Err(HostError::Ed25519VerifyInvalidInput { msg: "invalid public key length".to_string() }),
        map! {
            ExtCosts::read_memory_byte: 127,
            ExtCosts::read_memory_base: 3,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        BAD_SIGNATURE.len() as u64,
        &BAD_SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 128,
            ExtCosts::read_memory_base: 3,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64 - 1,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Err(HostError::Ed25519VerifyInvalidInput { msg: "invalid signature length".to_string() }),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 63,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        FORGED_SIGNATURE.len() as u64,
        &FORGED_SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 64,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        FORGED_SIGNATURE.len() as u64,
        &FORGED_SIGNATURE,
        0,
        &[],
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 64,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        0,
        &[],
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::read_memory_base: 3,
            ExtCosts::read_memory_byte: 96,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 0,
        },
    );
}

// tests for data being read from registers
#[test]
fn test_ed25519_verify_check_registers() {
    check_ed25519_verify(
        u64::MAX,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 64,

            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 64,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: 64,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        u64::MAX,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(1),
        map! {
           ExtCosts::write_register_base: 1,
           ExtCosts::write_register_byte: 32,

           ExtCosts::read_register_base: 1,
           ExtCosts::read_register_byte: 32,
           ExtCosts::read_memory_base: 2,
           ExtCosts::read_memory_byte: 96,
           ExtCosts::ed25519_verify_base: 1,
           ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        u64::MAX,
        &PUBLIC_KEY,
        Ok(1),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 32,

            ExtCosts::read_register_byte: 32,
            ExtCosts::read_register_base: 1,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: 96,
            ExtCosts::ed25519_verify_base: 1,
            ExtCosts::ed25519_verify_byte: 32,
        },
    );
    check_ed25519_verify(
        u64::MAX,
        &BAD_SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 64,

            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 64,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: 64,
            ExtCosts::ed25519_verify_byte: 32,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        u64::MAX,
        &FORGED_SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Ok(0),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 64,

            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 64,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        u64::MAX,
        &[0],
        MESSAGE.len() as u64,
        &MESSAGE,
        PUBLIC_KEY.len() as u64,
        &PUBLIC_KEY,
        Err(HostError::Ed25519VerifyInvalidInput { msg: "invalid signature length".to_string() }),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 1,

            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 1,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
    check_ed25519_verify(
        SIGNATURE.len() as u64,
        &SIGNATURE,
        MESSAGE.len() as u64,
        &MESSAGE,
        u64::MAX,
        &[0],
        Err(HostError::Ed25519VerifyInvalidInput { msg: "invalid public key length".to_string() }),
        map! {
            ExtCosts::write_register_base: 1,
            ExtCosts::write_register_byte: 1,

            ExtCosts::read_register_base: 1,
            ExtCosts::read_register_byte: 1,
            ExtCosts::read_memory_base: 2,
            ExtCosts::read_memory_byte: 96,
            ExtCosts::ed25519_verify_byte: 32,
            ExtCosts::ed25519_verify_base: 1,
        },
    );
}

use crate::tests::fixtures::get_context;
use std::collections::HashMap;
use hex::FromHex;
use near_primitives_core::config::ExtCosts;
use near_vm_errors::{HostError, VMLogicError};
use crate::map;
use crate::tests::helpers::assert_costs;
use crate::tests::vm_logic_builder::VMLogicBuilder;

#[track_caller]
fn check_bls12_381_verify(
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
    let mut logic = logic_builder.build(get_context(vec![], false));

    let signature_ptr = if signature_len == u64::MAX {
        logic.wrapped_internal_write_register(1, &signature).unwrap();
        1
    } else {
        signature.as_ptr() as u64
    };

    let message_ptr = if message_len == u64::MAX {
        logic.wrapped_internal_write_register(2, &message).unwrap();
        2
    } else {
        message.as_ptr() as u64
    };

    let public_key_ptr = if public_key_len == u64::MAX {
        logic.wrapped_internal_write_register(3, &public_key).unwrap();
        3
    } else {
        public_key.as_ptr() as u64
    };

    let result = logic.bls12_381_aggregate_verify(
        signature_ptr,
        signature_len,
        message_ptr,
        message_len,
        public_key_ptr,
        public_key_len,
    );

    let want = want.map_err(VMLogicError::HostError);
    assert_eq!(want, result);
    assert_costs(want_costs);
}


#[test]
fn test_bls12_381_verify_valid() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";
    let signature: Vec<u8> = <Vec<u8>>::from_hex("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f779746d830d1").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("5656565656565656565656565656565656565656565656565656565656565656").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg)
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 224,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 2,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("0000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 176,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 1,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("abababababababababababababababababababababababababababababababab").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );
}

#[test]
fn test_bls12_381_verify_tampered_signature() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";

    let signature: Vec<u8> = <Vec<u8>>::from_hex("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f7797ffffffff").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("5656565656565656565656565656565656565656565656565656565656565656").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380bffffffff").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("0000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfcffffffff").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("abababababababababababababababababababababababababababababababab").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),

    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );
}

#[test]
fn test_bls12_381_verify_na_pubkeys_and_na_signature() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";

    let signature: Vec<u8> = <Vec<u8>>::from_hex("000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("abababababababababababababababababababababababababababababababab").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );
}

#[test]
fn test_bls12_381_verify_na_pubkeys_and_infinity_signature() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";

    let signature: Vec<u8> = <Vec<u8>>::from_hex("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("abababababababababababababababababababababababababababababababab").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(4),
        map! {
            ExtCosts::read_memory_byte: 128,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 0,
        },
    );
}

#[test]
fn test_bls12_381_verify_infinity_pubkey() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";

    let signature: Vec<u8> = <Vec<u8>>::from_hex("afcb4d980f079265caa61aee3e26bf48bebc5dc3e7f2d7346834d76cbc812f636c937b6b44a9323d8bc4b1cdf71d6811035ddc2634017faab2845308f568f2b9a0356140727356eae9eded8b87fd8cb8024b440c57aee06076128bb32921f584").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("1212121212121212121212121212121212121212121212121212121212121212").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(0),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );
}

#[test]
fn test_bls12_381_verify_extra_pubkey() {
    let hex_convert_error_msg = "Error during converting hex string to bytes";

    let signature: Vec<u8> = <Vec<u8>>::from_hex("b6ed936746e01f8ecf281f020953fbf1f01debd5657c4a383940b020b26507f6076334f91e2366c96e9ab279fb5158090352ea1c5b0c9274504f4f0e7053af24802e51e4568d164fe986834f41e55c8e850ce1f98458c0cfc9ab380b55285a55").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("0000000000000000000000000000000000000000000000000000000000000000").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(5),
        map! {
            ExtCosts::read_memory_byte: 224,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 2,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("9712c3edd73a209c742b8250759db12549b3eaf43b5ca61376d9f30e2747dbcf842d8b2ac0901d2a093713e20284a7670fcf6954e9ab93de991bb9b313e664785a075fc285806fa5224c82bde146561b446ccfc706a64b8579513cfc4ff1d930").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("abababababababababababababababababababababababababababababababab").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(5),
        map! {
            ExtCosts::read_memory_byte: 320,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 4,
        },
    );

    let signature: Vec<u8> = <Vec<u8>>::from_hex("912c3615f69575407db9392eb21fee18fff797eeb2fbe1816366ca2a08ae574d8824dbfafb4c9eaa1cf61b63c6f9b69911f269b664c42947dd1b53ef1081926c1e82bb2a465f927124b08391a5249036146d6f3f1e17ff5f162f779746d830d1").expect(hex_convert_error_msg);
    let message: Vec<u8> = <Vec<u8>>::from_hex("5656565656565656565656565656565656565656565656565656565656565656").expect(hex_convert_error_msg);
    let pubkeys: Vec<Vec<u8>> = vec![
        <Vec<u8>>::from_hex("a491d1b0ecd9bb917989f0e74f0dea0422eac4a873e5e2644f368dffb9a6e20fd6e10c1b77654d067c0618f6e5a7f79a").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b301803f8b5ac4a1133581fc676dfedc60d891dd5fa99028805e5ea5b08d3491af75d0707adab3b70c6a6a580217bf81").expect(hex_convert_error_msg),
        <Vec<u8>>::from_hex("b53d21a4cfd562c469cc81514d4ce5a6b577d8403d32a394dc265dd190b47fa9f829fdd7963afdf972e5e77854051f6f").expect(hex_convert_error_msg),
    ];

    let pubkeys_raw: Vec<u8> = pubkeys.concat();

    check_bls12_381_verify(
        signature.len() as u64,
        &signature,
        message.len() as u64,
        &message,
        pubkeys_raw.len() as u64,
        &pubkeys_raw,
        Ok(5),
        map! {
            ExtCosts::read_memory_byte: 272,
            ExtCosts::bls12381_verify_base: 1,
            ExtCosts::read_memory_base: 3,
            ExtCosts::bls12381_verify_byte: 32,
            ExtCosts::bls12381_verify_elements: 3,
        },
    );
}
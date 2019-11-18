mod fixtures;
mod vm_logic_builder;

use fixtures::get_context;
use near_vm_errors::HostError;
use near_vm_logic::ExtCosts;
use vm_logic_builder::VMLogicBuilder;
mod helpers;
use helpers::*;

#[test]
fn test_valid_utf8() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    let len = string_bytes.len() as u64;
    logic.log_utf8(len, string_bytes.as_ptr() as _).expect("Valid utf-8 string_bytes");
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], String::from_utf8(string_bytes.clone()).unwrap());
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::log_base:  1,
        ExtCosts::log_byte: len,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: len,
    });
}

#[test]
fn test_invalid_utf8() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string_bytes = [128].to_vec();
    let len = string_bytes.len() as u64;
    assert_eq!(logic.log_utf8(len, string_bytes.as_ptr() as _), Err(HostError::BadUTF8.into()));
    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: len,
    });
}

#[test]
fn test_valid_null_terminated_utf8() {
    let mut logic_builder = VMLogicBuilder::default();

    let mut string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    string_bytes.push(0u8);
    let bytes_len = string_bytes.len();
    logic_builder.config.max_log_len = string_bytes.len() as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));
    logic
        .log_utf8(std::u64::MAX, string_bytes.as_ptr() as _)
        .expect("Valid null-terminated utf-8 string_bytes");
    string_bytes.pop();
    let outcome = logic.outcome();
    let len = bytes_len as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: len - 1,
        ExtCosts::read_memory_base: len,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: len - 1,
    });
    assert_eq!(outcome.logs[0], String::from_utf8(string_bytes.clone()).unwrap());
}

#[test]
fn test_log_max_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    logic_builder.config.max_log_len = (string_bytes.len() - 1) as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));

    assert_eq!(
        logic.log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _),
        Err(HostError::BadUTF8.into())
    );

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::utf8_decoding_base: 1,
    });

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
}

#[test]
fn test_log_utf8_max_limit_null_terminated() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    logic_builder.config.max_log_len = (string_bytes.len() - 1) as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));

    string_bytes.push(0u8);
    assert_eq!(
        logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _),
        Err(HostError::BadUTF8.into())
    );

    let len = string_bytes.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: len - 1 ,
        ExtCosts::read_memory_byte: len - 1,
        ExtCosts::utf8_decoding_base: 1,
    });

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
}

#[test]
fn test_valid_log_utf16() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    logic
        .log_utf16(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _)
        .expect("Valid utf-16 string_bytes");

    let len = utf16_bytes.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: len,
    });
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], string);
}

#[test]
fn test_valid_log_utf16_max_log_len_not_even() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.max_log_len = 5;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "ab";
    let mut utf16_bytes: Vec<u8> = Vec::new();
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.extend_from_slice(&[0, 0]);
    logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _).expect("Valid utf-16 string_bytes");

    let len = utf16_bytes.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: len / 2,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len - 2,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: len - 2 ,
    });

    let string = "abc";
    let mut utf16_bytes: Vec<u8> = Vec::new();
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.extend_from_slice(&[0, 0]);
    assert_eq!(
        logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _),
        Err(HostError::BadUTF16.into())
    );

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: logic_builder.config.max_log_len/2 + 1,
        ExtCosts::read_memory_byte: 2*(logic_builder.config.max_log_len/2 + 1),
        ExtCosts::utf16_decoding_base: 1,
    });
}

#[test]
fn test_log_utf8_max_limit_null_terminated_fail() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut string_bytes = "abcd".as_bytes().to_vec();
    string_bytes.push(0u8);
    logic_builder.config.max_log_len = 3;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let res = logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _);
    assert_eq!(res, Err(HostError::BadUTF8.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: logic_builder.config.max_log_len + 1,
        ExtCosts::read_memory_byte: logic_builder.config.max_log_len + 1,
        ExtCosts::utf8_decoding_base: 1,
    });
}

#[test]
fn test_valid_log_utf16_null_terminated() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.push(0);
    utf16_bytes.push(0);
    logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _).expect("Valid utf-16 string_bytes");

    let len = utf16_bytes.len() as u64;
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], string);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: len / 2 ,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len - 2,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: len - 2,
    });
}

#[test]
fn test_invalid_log_utf16() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(0);
        utf16_bytes.push(u16_ as u8);
    }
    let res = logic.log_utf8(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _);
    let len = utf16_bytes.len() as u64;
    assert_eq!(res, Err(HostError::BadUTF8.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: len,
    });
}

#[test]
fn test_valid_log_utf16_null_terminated_fail() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.push(0);
    utf16_bytes.push(123);
    utf16_bytes.push(0);
    utf16_bytes.push(0);
    logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _).expect("Valid utf-16 string_bytes");
    let len = utf16_bytes.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: len / 2 ,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len - 2,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: len - 2,
    });
    assert_ne!(logic.outcome().logs[0], string);
}

#[test]
fn test_hash256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = b"tesdsst";

    logic.sha256(data.len() as _, data.as_ptr() as _, 0).unwrap();
    let res = &vec![0u8; 32];
    logic.read_register(0, res.as_ptr() as _).expect("OK");
    assert_eq!(
        res,
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ]
    );
    let len = data.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::sha256_base: 1,
        ExtCosts::sha256_byte: len,
    });
}

#[test]
fn test_hash256_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = b"tesdsst";
    logic.wrapped_internal_write_register(1, data).unwrap();

    logic.sha256(std::u64::MAX, 1, 0).unwrap();
    let res = &vec![0u8; 32];
    logic.read_register(0, res.as_ptr() as _).unwrap();
    assert_eq!(
        res,
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ]
    );

    let len = data.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 2,
        ExtCosts::read_register_byte: 32 + len,
        ExtCosts::write_register_base: 2,
        ExtCosts::write_register_byte: 32 + len,
        ExtCosts::sha256_base: 1,
        ExtCosts::sha256_byte: len,
    });
}

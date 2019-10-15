mod fixtures;

use crate::fixtures::get_context;
use near_vm_errors::HostError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, VMLogic};

fn check_gas_for_data_len(len: u64, used_gas: u64, config: &Config) {
    let base = config.runtime_fees.ext_costs.log_base;
    let per_byte = config.runtime_fees.ext_costs.log_per_byte;
    assert_eq!(base + per_byte * len, used_gas, "Wrong amount of gas spent");
}

#[test]
fn test_valid_utf8() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    logic
        .log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _)
        .expect("Valid utf-8 string_bytes");
    let outcome = logic.outcome();
    assert_eq!(
        outcome.logs[0],
        format!("LOG: {}", String::from_utf8(string_bytes.clone()).unwrap())
    );
    check_gas_for_data_len(string_bytes.len() as _, outcome.used_gas, &config);
}

#[test]
fn test_invalid_utf8() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string_bytes = [128].to_vec();
    assert_eq!(
        logic.log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _),
        Err(HostError::BadUTF8.into())
    );
    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
    check_gas_for_data_len(string_bytes.len() as _, outcome.used_gas, &config);
}

#[test]
fn test_valid_null_terminated_utf8() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let mut config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    string_bytes.push(0u8);
    let bytes_len = string_bytes.len();
    config.max_log_len = string_bytes.len() as u64;
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    logic
        .log_utf8(std::u64::MAX, string_bytes.as_ptr() as _)
        .expect("Valid null-terminated utf-8 string_bytes");
    string_bytes.pop();
    let outcome = logic.outcome();
    assert_eq!(
        outcome.logs[0],
        format!("LOG: {}", String::from_utf8(string_bytes.clone()).unwrap())
    );
    check_gas_for_data_len(bytes_len as _, outcome.used_gas, &config);
}

#[test]
fn test_log_max_limit() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let mut config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    let bytes_len = string_bytes.len();
    config.max_log_len = (string_bytes.len() - 1) as u64;
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    assert_eq!(
        logic.log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _),
        Err(HostError::BadUTF8.into())
    );
    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
    assert_eq!(outcome.used_gas, config.runtime_fees.ext_costs.log_base);
}

#[test]
fn test_log_utf8_max_limit_null_terminated() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let mut config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    config.max_log_len = (string_bytes.len() - 1) as u64;
    string_bytes.push(0u8);
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    assert_eq!(
        logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _),
        Err(HostError::BadUTF8.into())
    );
    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);

    check_gas_for_data_len(35, outcome.used_gas, &config);
}

#[test]
fn test_valid_log_utf16() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    logic
        .log_utf16(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _)
        .expect("Valid utf-16 string_bytes");
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], format!("LOG: {}", string));
    assert_eq!(outcome.used_gas, 13);
}

#[test]
fn test_valid_log_utf16_max_log_len_not_even() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let mut config = Config::default();
    config.max_log_len = 5;
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context.clone(), &config, &promise_results, &mut memory);
    let string = "ab";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _).expect("Valid utf-16 string_bytes");
    assert_eq!(logic.outcome().logs[0], format!("LOG: {}", string));

    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string = "abc";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    assert_eq!(
        logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _),
        Err(HostError::BadUTF16.into())
    );
}

#[test]
fn test_log_utf8_max_limit_null_terminated_fail() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let mut config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut string_bytes = "abcd".as_bytes().to_vec();
    string_bytes.push(0u8);
    config.max_log_len = 3;
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let res = logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _);
    assert_eq!(res, Err(HostError::BadUTF8.into()));
    check_gas_for_data_len(4, logic.outcome().used_gas, &config);
}

#[test]
fn test_valid_log_utf16_null_terminated() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.push(0);
    utf16_bytes.push(0);
    logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _).expect("Valid utf-16 string_bytes");
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], format!("LOG: {}", string));
    assert_eq!(outcome.used_gas, 15);
}

#[test]
fn test_invalid_log_utf16() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(0);
        utf16_bytes.push(u16_ as u8);
    }
    let res = logic.log_utf8(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _);
    assert_eq!(res, Err(HostError::BadUTF8.into()));
    assert_eq!(logic.outcome().used_gas, 13);
}

#[test]
fn test_valid_log_utf16_null_terminated_fail() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
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
    assert_ne!(logic.outcome().logs[0], format!("LOG: {}", string));
}

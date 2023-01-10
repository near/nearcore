use crate::tests::fixtures::get_context;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::{map, ExtCosts};
use hex::FromHex;
use near_vm_errors::HostError;
use serde::{de::Error, Deserialize, Deserializer};
use serde_json::from_slice;
use std::{fmt::Display, fs};

#[test]
fn test_valid_utf8() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%";
    let bytes = logic.internal_mem_write(string.as_bytes());
    logic.log_utf8(bytes.len, bytes.ptr).expect("Valid UTF-8 in bytes");
    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs[0], string);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::log_base:  1,
        ExtCosts::log_byte: bytes.len,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: bytes.len,
    });
}

#[test]
fn test_invalid_utf8() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(b"\x80");
    assert_eq!(logic.log_utf8(bytes.len, bytes.ptr), Err(HostError::BadUTF8.into()));
    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len(), 0);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: bytes.len,
    });
}

#[test]
fn test_valid_null_terminated_utf8() {
    let mut logic_builder = VMLogicBuilder::default();

    let cstring = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%\x00";
    let string = &cstring[..cstring.len() - 1];
    logic_builder.config.limit_config.max_total_log_length = string.len() as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(cstring.as_bytes());
    logic.log_utf8(u64::MAX, bytes.ptr).expect("Valid null-terminated utf-8 string_bytes");
    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: string.len() as u64,
        ExtCosts::read_memory_base: bytes.len,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf8_decoding_base: 1,
        ExtCosts::utf8_decoding_byte: string.len() as u64,
    });
    assert_eq!(outcome.logs[0], string);
}

#[test]
fn test_log_max_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%";
    let limit = string.len() as u64 - 1;
    logic_builder.config.limit_config.max_total_log_length = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(string.as_bytes());

    assert_eq!(
        logic.log_utf8(bytes.len, bytes.ptr),
        Err(HostError::TotalLogLengthExceeded { length: bytes.len, limit }.into())
    );

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::utf8_decoding_base: 1,
    });

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len(), 0);
}

#[test]
fn test_log_total_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes();
    let num_logs = 10;
    let limit = string.len() as u64 * num_logs - 1;
    logic_builder.config.limit_config.max_total_log_length = limit;
    logic_builder.config.limit_config.max_number_logs = num_logs;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(string);

    for _ in 0..num_logs - 1 {
        logic.log_utf8(bytes.len, bytes.ptr).expect("total is still under the limit");
    }
    assert_eq!(
        logic.log_utf8(bytes.len, bytes.ptr),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
    );

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len() as u64, num_logs - 1);
}

#[test]
fn test_log_number_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string = "blabla";
    let max_number_logs = 3;
    logic_builder.config.limit_config.max_total_log_length =
        (string.len() + 1) as u64 * (max_number_logs + 1);
    logic_builder.config.limit_config.max_number_logs = max_number_logs;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(string.as_bytes());
    for _ in 0..max_number_logs {
        logic
            .log_utf8(bytes.len, bytes.ptr)
            .expect("Valid utf-8 string_bytes under the log number limit");
    }
    assert_eq!(
        logic.log_utf8(bytes.len, bytes.ptr),
        Err(HostError::NumberOfLogsExceeded { limit: max_number_logs }.into())
    );

    assert_costs(map! {
        ExtCosts::base: max_number_logs + 1,
        ExtCosts::log_base: max_number_logs,
        ExtCosts::log_byte: bytes.len * max_number_logs,
        ExtCosts::read_memory_base: max_number_logs,
        ExtCosts::read_memory_byte: bytes.len * max_number_logs,
        ExtCosts::utf8_decoding_base: max_number_logs,
        ExtCosts::utf8_decoding_byte: bytes.len * max_number_logs,
    });

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len() as u64, max_number_logs);
}

fn append_utf16(dst: &mut Vec<u8>, string: &str) {
    for code_unit in string.encode_utf16() {
        dst.extend_from_slice(&code_unit.to_le_bytes());
    }
}

#[test]
fn test_log_utf16_number_limit() {
    let string = "$ qò$`";
    let mut bytes = Vec::new();
    append_utf16(&mut bytes, string);

    let mut logic_builder = VMLogicBuilder::default();
    let max_number_logs = 3;
    logic_builder.config.limit_config.max_total_log_length =
        (bytes.len() + 1) as u64 * (max_number_logs + 1);
    logic_builder.config.limit_config.max_number_logs = max_number_logs;

    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(&bytes);
    for _ in 0..max_number_logs {
        logic
            .log_utf16(bytes.len, bytes.ptr)
            .expect("Valid utf-16 string_bytes under the log number limit");
    }
    assert_eq!(
        logic.log_utf16(bytes.len, bytes.ptr),
        Err(HostError::NumberOfLogsExceeded { limit: max_number_logs }.into())
    );

    assert_costs(map! {
        ExtCosts::base: max_number_logs + 1,
        ExtCosts::log_base: max_number_logs,
        ExtCosts::log_byte: string.len() as u64 * max_number_logs,
        ExtCosts::read_memory_base: max_number_logs,
        ExtCosts::read_memory_byte: bytes.len * max_number_logs,
        ExtCosts::utf16_decoding_base: max_number_logs,
        ExtCosts::utf16_decoding_byte: bytes.len * max_number_logs,
    });

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len() as u64, max_number_logs);
}

#[test]
fn test_log_total_length_limit_mixed() {
    let mut logic_builder = VMLogicBuilder::default();

    let string = "abc";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    append_utf16(&mut utf16_bytes, string);

    let num_logs_each = 10;
    let limit = string.len() as u64 * (num_logs_each * 2 + 1) - 1;
    logic_builder.config.limit_config.max_total_log_length = limit;
    logic_builder.config.limit_config.max_number_logs = num_logs_each * 2 + 1;
    let mut logic = logic_builder.build(get_context(vec![], false));

    let utf8_bytes = logic.internal_mem_write(string.as_bytes());
    let utf16_bytes = logic.internal_mem_write(&utf16_bytes);

    for _ in 0..num_logs_each {
        logic.log_utf16(utf16_bytes.len, utf16_bytes.ptr).expect("total is still under the limit");

        logic.log_utf8(utf8_bytes.len, utf8_bytes.ptr).expect("total is still under the limit");
    }
    assert_eq!(
        logic.log_utf8(utf8_bytes.len, utf8_bytes.ptr),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
    );

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len() as u64, num_logs_each * 2);
}

#[test]
fn test_log_utf8_max_limit_null_terminated() {
    let mut logic_builder = VMLogicBuilder::default();
    let bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%\x00".as_bytes();
    let limit = (bytes.len() - 2) as u64;
    logic_builder.config.limit_config.max_total_log_length = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(bytes);

    assert_eq!(
        logic.log_utf8(u64::MAX, bytes.ptr),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
    );

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: bytes.len - 1,
        ExtCosts::read_memory_byte: bytes.len - 1,
        ExtCosts::utf8_decoding_base: 1,
    });

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs.len(), 0);
}

#[test]
fn test_valid_log_utf16() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let string = "$ qò$`";
    let mut bytes = Vec::new();
    append_utf16(&mut bytes, string);
    let bytes = logic.internal_mem_write(&bytes);

    logic.log_utf16(bytes.len, bytes.ptr).expect("Valid utf-16 string_bytes");

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: bytes.len,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: string.len() as u64,
    });
    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs[0], string);
}

#[test]
fn test_valid_log_utf16_max_log_len_not_even() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_total_log_length = 5;
    let mut logic = logic_builder.build(get_context(vec![], false));

    let string = "ab";
    let mut bytes = Vec::new();
    append_utf16(&mut bytes, string);
    append_utf16(&mut bytes, "\0");
    let bytes = logic.internal_mem_write(&bytes);
    logic.log_utf16(u64::MAX, bytes.ptr).expect("Valid utf-16 bytes");

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: bytes.len / 2,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: bytes.len - 2,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: string.len() as u64,
    });

    let string = "abc";
    let mut bytes = Vec::new();
    append_utf16(&mut bytes, string);
    append_utf16(&mut bytes, "\0");
    let bytes = logic.internal_mem_write(&bytes);
    assert_eq!(
        logic.log_utf16(u64::MAX, bytes.ptr),
        Err(HostError::TotalLogLengthExceeded { length: 6, limit: 5 }.into())
    );

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 2,
        ExtCosts::read_memory_byte: 2 * 2,
        ExtCosts::utf16_decoding_base: 1,
    });
}

#[test]
fn test_log_utf8_max_limit_null_terminated_fail() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_total_log_length = 3;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let bytes = logic.internal_mem_write(b"abcdefgh\0");
    let res = logic.log_utf8(u64::MAX, bytes.ptr);
    assert_eq!(res, Err(HostError::TotalLogLengthExceeded { length: 4, limit: 3 }.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: logic_builder.config.limit_config.max_total_log_length + 1,
        ExtCosts::read_memory_byte: logic_builder.config.limit_config.max_total_log_length + 1,
        ExtCosts::utf8_decoding_base: 1,
    });
}

#[test]
fn test_valid_log_utf16_null_terminated() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let string = "$ qò$`";
    let mut bytes = Vec::new();
    append_utf16(&mut bytes, string);
    bytes.extend_from_slice(&[0, 0]);
    let bytes = logic.internal_mem_write(&bytes);

    logic.log_utf16(u64::MAX, bytes.ptr).expect("Valid utf-16 string_bytes");

    let outcome = logic.compute_outcome_and_distribute_gas();
    assert_eq!(outcome.logs[0], string);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: bytes.len / 2 ,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: bytes.len - 2,
        ExtCosts::log_base: 1,
        ExtCosts::log_byte: string.len() as u64,
    });
}

#[test]
fn test_invalid_log_utf16() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let mut bytes: Vec<u8> = Vec::new();
    for u16_ in [0xD834u16, 0xDD1E, 0x006d, 0x0075, 0xD800, 0x0069, 0x0063] {
        bytes.extend_from_slice(&u16_.to_le_bytes());
    }
    let bytes = logic.internal_mem_write(&bytes);
    let res = logic.log_utf16(bytes.len, bytes.ptr);
    assert_eq!(res, Err(HostError::BadUTF16.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: bytes.len,
    });
}

#[test]
fn test_valid_log_utf16_null_terminated_fail() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let mut bytes = Vec::new();
    append_utf16(&mut bytes, "$ qò$`");
    bytes.extend_from_slice(&[0x00, 0xD8]); // U+D800, unpaired surrogate
    append_utf16(&mut bytes, "foobarbaz\0");
    let bytes = logic.internal_mem_write(&bytes);

    let res = logic.log_utf16(u64::MAX, bytes.ptr);
    assert_eq!(res, Err(HostError::BadUTF16.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: bytes.len / 2,
        ExtCosts::read_memory_byte: bytes.len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: bytes.len - 2,
    });
}

#[test]
fn test_sha256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = logic.internal_mem_write(b"tesdsst");

    logic.sha256(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::sha256_base: 1,
        ExtCosts::sha256_byte: data.len,
    });
}

#[test]
fn test_keccak256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let data = logic.internal_mem_write(b"tesdsst");
    logic.keccak256(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            104, 110, 58, 122, 230, 181, 215, 145, 231, 229, 49, 162, 123, 167, 177, 58, 26, 142,
            129, 173, 7, 37, 9, 26, 233, 115, 64, 102, 61, 85, 10, 159,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::keccak256_base: 1,
        ExtCosts::keccak256_byte: data.len,
    });
}

#[test]
fn test_keccak512() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let data = logic.internal_mem_write(b"tesdsst");
    logic.keccak512(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            55, 134, 96, 137, 168, 122, 187, 95, 67, 76, 18, 122, 146, 11, 225, 106, 117, 194, 154,
            157, 48, 160, 90, 146, 104, 209, 118, 126, 222, 230, 200, 125, 48, 73, 197, 236, 123,
            173, 192, 197, 90, 153, 167, 121, 100, 88, 209, 240, 137, 86, 239, 41, 87, 128, 219,
            249, 136, 203, 220, 109, 46, 168, 234, 190,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 64,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 64,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 64,
        ExtCosts::keccak512_base: 1,
        ExtCosts::keccak512_byte: data.len,
    });
}

#[test]
fn test_ripemd160() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let data = logic.internal_mem_write(b"tesdsst");
    logic.ripemd160(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[21, 102, 156, 115, 232, 3, 58, 215, 35, 84, 129, 30, 143, 86, 212, 104, 70, 97, 14, 225],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 20,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 20,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 20,
        ExtCosts::ripemd160_base: 1,
        ExtCosts::ripemd160_block: 1,
    });
}

#[derive(Deserialize)]
struct EcrecoverTest {
    #[serde(with = "hex::serde")]
    m: [u8; 32],
    v: u8,
    #[serde(with = "hex::serde")]
    sig: [u8; 64],
    mc: bool,
    #[serde(deserialize_with = "deserialize_option_hex")]
    res: Option<[u8; 64]>,
}

fn deserialize_option_hex<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromHex,
    <T as FromHex>::Error: Display,
{
    Deserialize::deserialize(deserializer)
        .map(|v: Option<&str>| v.map(FromHex::from_hex).transpose().map_err(Error::custom))
        .and_then(|v| v)
}

#[test]
fn test_ecrecover() {
    for EcrecoverTest { m, v, sig, mc, res } in
        from_slice::<'_, Vec<_>>(fs::read("src/tests/ecrecover-tests.json").unwrap().as_slice())
            .unwrap()
    {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build(get_context(vec![], false));
        let m = logic.internal_mem_write(&m);
        let sig = logic.internal_mem_write(&sig);

        let b = logic.ecrecover(m.len, m.ptr, sig.len, sig.ptr, v as _, mc as _, 1).unwrap();
        assert_eq!(b, res.is_some() as u64);

        if let Some(res) = res {
            assert_costs(map! {
                ExtCosts::read_memory_base: 2,
                ExtCosts::read_memory_byte: 96,
                ExtCosts::write_register_base: 1,
                ExtCosts::write_register_byte: 64,
                ExtCosts::ecrecover_base: 1,
            });
            logic.assert_read_register(&res, 1);
        } else {
            assert_costs(map! {
                ExtCosts::read_memory_base: 2,
                ExtCosts::read_memory_byte: 96,
                ExtCosts::ecrecover_base: 1,
            });
        }

        reset_costs_counter();
    }
}

#[test]
fn test_hash256_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = b"tesdsst";
    logic.wrapped_internal_write_register(1, data).unwrap();

    logic.sha256(u64::MAX, 1, 0).unwrap();
    logic.assert_read_register(
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ],
        0,
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

#[test]
fn test_key_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_storage_key = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));

    // Under the limit. Valid calls.
    let key = crate::MemSlice { ptr: 0, len: limit };
    let val = crate::MemSlice { ptr: 0, len: 5 };
    logic
        .storage_has_key(key.len, key.ptr)
        .expect("storage_has_key: key length is under the limit");
    logic
        .storage_write(key.len, key.ptr, val.len, val.ptr, 0)
        .expect("storage_write: key length is under the limit");
    logic.storage_read(key.len, key.ptr, 0).expect("storage_read: key length is under the limit");
    logic
        .storage_remove(key.len, key.ptr, 0)
        .expect("storage_remove: key length is under the limit");

    // Over the limit. Invalid calls.
    let key = crate::MemSlice { ptr: 0, len: limit + 1 };
    assert_eq!(
        logic.storage_has_key(key.len, key.ptr),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_read(key.len, key.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_remove(key.len, key.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
}

#[test]
fn test_value_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_storage_value = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let key = logic.internal_mem_write(b"hello");

    logic
        .storage_write(key.len, key.ptr, limit / 2, 0, 0)
        .expect("Value length doesn’t exceed the limit");
    logic
        .storage_write(key.len, key.ptr, limit, 0, 0)
        .expect("Value length doesn’t exceed the limit");
    assert_eq!(
        logic.storage_write(key.len, key.ptr, limit + 1, 0, 0),
        Err(HostError::ValueLengthExceeded { length: limit + 1, limit }.into())
    );
}

#[test]
fn test_num_promises() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_promises = 10;
    logic_builder.config.limit_config.max_promises_per_function_call_action = num_promises;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let account_id = logic.internal_mem_write(b"alice");
    for _ in 0..num_promises {
        logic
            .promise_batch_create(account_id.len, account_id.ptr)
            .expect("Number of promises is under the limit");
    }
    assert_eq!(
        logic.promise_batch_create(account_id.len, account_id.ptr),
        Err(HostError::NumberPromisesExceeded {
            number_of_promises: num_promises + 1,
            limit: num_promises
        }
        .into())
    );
}

#[test]
fn test_num_joined_promises() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_deps = 10;
    logic_builder.config.limit_config.max_number_input_data_dependencies = num_deps;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let account_id = logic.internal_mem_write(b"alice");
    let promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    let promises =
        logic.internal_mem_write(&promise_id.to_le_bytes().repeat(num_deps as usize + 1));
    for num in 0..num_deps {
        logic.promise_and(promises.ptr, num).expect("Number of joined promises is under the limit");
    }
    assert_eq!(
        logic.promise_and(promises.ptr, num_deps + 1),
        Err(HostError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: num_deps + 1,
            limit: num_deps,
        }
        .into())
    );
}

#[test]
fn test_num_input_dependencies_recursive_join() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_steps = 10;
    logic_builder.config.limit_config.max_number_input_data_dependencies = 1 << num_steps;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let account_id = logic.internal_mem_write(b"alice");
    let original_promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    let mut promise_id = original_promise_id;
    for _ in 1..num_steps {
        let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
        logic.internal_mem_write(&promise_id.to_le_bytes());
        promise_id = logic
            .promise_and(promises_ptr, 2)
            .expect("Number of joined promises is under the limit");
    }
    // The length of joined promises is exactly the limit (1024).
    let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
    logic.internal_mem_write(&promise_id.to_le_bytes());
    logic.promise_and(promises_ptr, 2).expect("Number of joined promises is under the limit");

    // The length of joined promises exceeding the limit by 1 (total 1025).
    let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
    logic.internal_mem_write(&promise_id.to_le_bytes());
    logic.internal_mem_write(&original_promise_id.to_le_bytes());
    assert_eq!(
        logic.promise_and(promises_ptr, 3),
        Err(HostError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: logic_builder
                .config
                .limit_config
                .max_number_input_data_dependencies
                + 1,
            limit: logic_builder.config.limit_config.max_number_input_data_dependencies,
        }
        .into())
    );
}

#[test]
fn test_return_value_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_returned_data = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.value_return(limit, 0).expect("Returned value length is under the limit");
    assert_eq!(
        logic.value_return(limit + 1, 0),
        Err(HostError::ReturnedValueLengthExceeded { length: limit + 1, limit }.into())
    );
}

#[test]
fn test_contract_size_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_contract_size = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));

    let account_id = logic.internal_mem_write(b"alice");

    let promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    logic
        .promise_batch_action_deploy_contract(promise_id, limit, 0)
        .expect("The length of the contract code is under the limit");
    assert_eq!(
        logic.promise_batch_action_deploy_contract(promise_id, limit + 1, 0),
        Err(HostError::ContractSizeExceeded { size: limit + 1, limit }.into())
    );
}

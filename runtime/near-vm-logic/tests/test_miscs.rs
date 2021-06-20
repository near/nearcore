use fixtures::get_context;
use helpers::*;
use near_vm_errors::HostError;
use near_vm_logic::ExtCosts;
use vm_logic_builder::VMLogicBuilder;

mod fixtures;
mod vm_logic_builder;

mod helpers;

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
    logic_builder.config.limit_config.max_total_log_length = string_bytes.len() as u64;
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
    let limit = (string_bytes.len() - 1) as u64;
    logic_builder.config.limit_config.max_total_log_length = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));

    assert_eq!(
        logic.log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _),
        Err(HostError::TotalLogLengthExceeded { length: string_bytes.len() as _, limit }.into())
    );

    assert_costs(map! {
      ExtCosts::base: 1,
      ExtCosts::utf8_decoding_base: 1,
    });

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len(), 0);
}

#[test]
fn test_log_total_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    let num_logs = 10;
    let limit = string_bytes.len() as u64 * num_logs - 1;
    logic_builder.config.limit_config.max_total_log_length = limit;
    logic_builder.config.limit_config.max_number_logs = num_logs;
    let mut logic = logic_builder.build(get_context(vec![], false));

    for _ in 0..num_logs - 1 {
        logic
            .log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _)
            .expect("total is still under the limit");
    }
    assert_eq!(
        logic.log_utf8(string_bytes.len() as _, string_bytes.as_ptr() as _),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
    );

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len() as u64, num_logs - 1);
}

#[test]
fn test_log_number_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string_bytes = "blabla".as_bytes().to_vec();
    let max_number_logs = 3;
    logic_builder.config.limit_config.max_total_log_length =
        (string_bytes.len() + 1) as u64 * (max_number_logs + 1);
    logic_builder.config.limit_config.max_number_logs = max_number_logs;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let len = string_bytes.len() as u64;
    for _ in 0..max_number_logs {
        logic
            .log_utf8(len, string_bytes.as_ptr() as _)
            .expect("Valid utf-8 string_bytes under the log number limit");
    }
    assert_eq!(
        logic.log_utf8(len, string_bytes.as_ptr() as _),
        Err(HostError::NumberOfLogsExceeded { limit: max_number_logs }.into())
    );

    assert_costs(map! {
        ExtCosts::base: max_number_logs + 1,
        ExtCosts::log_base: max_number_logs,
        ExtCosts::log_byte: len * max_number_logs,
        ExtCosts::read_memory_base: max_number_logs,
        ExtCosts::read_memory_byte: len * max_number_logs,
        ExtCosts::utf8_decoding_base: max_number_logs,
        ExtCosts::utf8_decoding_byte: len * max_number_logs,
    });

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len() as u64, max_number_logs);
}

#[test]
fn test_log_utf16_number_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let string = "$ qò$`";
    let mut string_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        string_bytes.push(u16_ as u8);
        string_bytes.push((u16_ >> 8) as u8);
    }
    let max_number_logs = 3;
    logic_builder.config.limit_config.max_total_log_length =
        (string_bytes.len() + 1) as u64 * (max_number_logs + 1);
    logic_builder.config.limit_config.max_number_logs = max_number_logs;

    let mut logic = logic_builder.build(get_context(vec![], false));
    let len = string_bytes.len() as u64;
    for _ in 0..max_number_logs {
        logic
            .log_utf16(len, string_bytes.as_ptr() as _)
            .expect("Valid utf-16 string_bytes under the log number limit");
    }
    assert_eq!(
        logic.log_utf16(len, string_bytes.as_ptr() as _),
        Err(HostError::NumberOfLogsExceeded { limit: max_number_logs }.into())
    );

    assert_costs(map! {
        ExtCosts::base: max_number_logs + 1,
        ExtCosts::log_base: max_number_logs,
        ExtCosts::log_byte: string.len() as u64 * max_number_logs,
        ExtCosts::read_memory_base: max_number_logs,
        ExtCosts::read_memory_byte: len * max_number_logs,
        ExtCosts::utf16_decoding_base: max_number_logs,
        ExtCosts::utf16_decoding_byte: len * max_number_logs,
    });

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len() as u64, max_number_logs);
}

#[test]
fn test_log_total_length_limit_mixed() {
    let mut logic_builder = VMLogicBuilder::default();
    let utf8_bytes = "abc".as_bytes().to_vec();

    let string = "abc";
    let mut utf16_bytes: Vec<u8> = vec![0u8; 0];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }

    let final_bytes = "abc".as_bytes().to_vec();

    let num_logs_each = 10;
    let limit = utf8_bytes.len() as u64 * num_logs_each
        + string.as_bytes().len() as u64 * num_logs_each
        + final_bytes.len() as u64
        - 1;
    logic_builder.config.limit_config.max_total_log_length = limit;
    logic_builder.config.limit_config.max_number_logs = num_logs_each * 2 + 1;
    let mut logic = logic_builder.build(get_context(vec![], false));

    for _ in 0..num_logs_each {
        logic
            .log_utf16(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _)
            .expect("total is still under the limit");

        logic
            .log_utf8(utf8_bytes.len() as _, utf8_bytes.as_ptr() as _)
            .expect("total is still under the limit");
    }
    assert_eq!(
        logic.log_utf8(final_bytes.len() as _, final_bytes.as_ptr() as _),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
    );

    let outcome = logic.outcome();
    assert_eq!(outcome.logs.len() as u64, num_logs_each * 2);
}

#[test]
fn test_log_utf8_max_limit_null_terminated() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut string_bytes = "j ñ r'ø qò$`5 y'5 øò{%÷ `Võ%".as_bytes().to_vec();
    let limit = (string_bytes.len() - 1) as u64;
    logic_builder.config.limit_config.max_total_log_length = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));

    string_bytes.push(0u8);
    assert_eq!(
        logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _),
        Err(HostError::TotalLogLengthExceeded { length: limit + 1, limit }.into())
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
        ExtCosts::log_byte: string.len() as u64,
    });
    let outcome = logic.outcome();
    assert_eq!(outcome.logs[0], string);
}

#[test]
fn test_valid_log_utf16_max_log_len_not_even() {
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_total_log_length = 5;
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
        ExtCosts::log_byte: string.len() as u64 ,
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
        Err(HostError::TotalLogLengthExceeded {
            length: 6,
            limit: logic_builder.config.limit_config.max_total_log_length,
        }
        .into())
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
    let mut string_bytes = "abcd".as_bytes().to_vec();
    string_bytes.push(0u8);
    logic_builder.config.limit_config.max_total_log_length = 3;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let res = logic.log_utf8(std::u64::MAX, string_bytes.as_ptr() as _);
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
        ExtCosts::log_byte: string.len() as u64 ,
    });
}

#[test]
fn test_invalid_log_utf16() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let utf16: Vec<u16> = vec![0xD834, 0xDD1E, 0x006d, 0x0075, 0xD800, 0x0069, 0x0063];
    let mut utf16_bytes: Vec<u8> = vec![];
    for u16_ in utf16 {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    let res = logic.log_utf16(utf16_bytes.len() as _, utf16_bytes.as_ptr() as _);
    let len = utf16_bytes.len() as u64;
    assert_eq!(res, Err(HostError::BadUTF16.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len,
    });
}

#[test]
fn test_valid_log_utf16_null_terminated_fail() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let string = "$ qò$`";
    let mut utf16_bytes: Vec<u8> = vec![];
    for u16_ in string.encode_utf16() {
        utf16_bytes.push(u16_ as u8);
        utf16_bytes.push((u16_ >> 8) as u8);
    }
    utf16_bytes.push(0);
    utf16_bytes.push(0xD8u8); // Bad utf-16
    utf16_bytes.push(0);
    utf16_bytes.push(0);
    let res = logic.log_utf16(std::u64::MAX, utf16_bytes.as_ptr() as _);
    let len = utf16_bytes.len() as u64;
    assert_eq!(res, Err(HostError::BadUTF16.into()));
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: len / 2,
        ExtCosts::read_memory_byte: len,
        ExtCosts::utf16_decoding_base: 1,
        ExtCosts::utf16_decoding_byte: len - 2,
    });
}

#[test]
fn test_sha256() {
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
fn test_keccak256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = b"tesdsst";

    logic.keccak256(data.len() as _, data.as_ptr() as _, 0).unwrap();
    let res = &vec![0u8; 32];
    logic.read_register(0, res.as_ptr() as _).expect("OK");
    assert_eq!(
        res.as_slice(),
        &[
            104, 110, 58, 122, 230, 181, 215, 145, 231, 229, 49, 162, 123, 167, 177, 58, 26, 142,
            129, 173, 7, 37, 9, 26, 233, 115, 64, 102, 61, 85, 10, 159
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
        ExtCosts::keccak256_base: 1,
        ExtCosts::keccak256_byte: len,
    });
}

#[test]
fn test_keccak512() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = b"tesdsst";

    logic.keccak512(data.len() as _, data.as_ptr() as _, 0).unwrap();
    let res = &vec![0u8; 64];
    logic.read_register(0, res.as_ptr() as _).expect("OK");
    assert_eq!(
        res,
        &[
            55, 134, 96, 137, 168, 122, 187, 95, 67, 76, 18, 122, 146, 11, 225, 106, 117, 194, 154,
            157, 48, 160, 90, 146, 104, 209, 118, 126, 222, 230, 200, 125, 48, 73, 197, 236, 123,
            173, 192, 197, 90, 153, 167, 121, 100, 88, 209, 240, 137, 86, 239, 41, 87, 128, 219,
            249, 136, 203, 220, 109, 46, 168, 234, 190
        ]
        .to_vec()
    );
    let len = data.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 64,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 64,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 64,
        ExtCosts::keccak512_base: 1,
        ExtCosts::keccak512_byte: len,
    });
}

#[test]
#[cfg(feature = "protocol_feature_evm")]
fn test_ripemd160() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let data = b"tesdsst";
    logic.ripemd160(data.len() as _, data.as_ptr() as _, 0).unwrap();
    let res = &vec![0u8; 20];
    logic.read_register(0, res.as_ptr() as _).expect("OK");
    assert_eq!(
        res,
        &[21, 102, 156, 115, 232, 3, 58, 215, 35, 84, 129, 30, 143, 86, 212, 104, 70, 97, 14, 225,]
    );
    let len = data.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: len,
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

#[test]
#[cfg(feature = "protocol_feature_evm")]
fn test_blake2b() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let rounds = 12;
    let h: [u64; 8] = [
        0x6a09e667f2bdc948,
        0xbb67ae8584caa73b,
        0x3c6ef372fe94f82b,
        0xa54ff53a5f1d36f1,
        0x510e527fade682d1,
        0x9b05688c2b3e6c1f,
        0x1f83d9abfb41bd6b,
        0x5be0cd19137e2179,
    ];
    let m = b"abc";
    let t0 = 0;
    let t1 = 0;
    let f0 = !0;
    let f1 = 0;

    logic
        .blake2b(rounds, h.as_ptr() as _, m.len() as u64, m.as_ptr() as _, t0, t1, f0, f1, 0)
        .unwrap();

    let res = [0u8; 64];
    logic.read_register(0, res.as_ptr() as _).expect("OK");

    let expected: [u8; 64] = [
        0xba, 0x80, 0xa5, 0x3f, 0x98, 0x1c, 0x4d, 0x0d, 0x6a, 0x27, 0x97, 0xb6, 0x9f, 0x12, 0xf6,
        0xe9, 0x4c, 0x21, 0x2f, 0x14, 0x68, 0x5a, 0xc4, 0xb7, 0x4b, 0x12, 0xbb, 0x6f, 0xdb, 0xff,
        0xa2, 0xd1, 0x7d, 0x87, 0xc5, 0x39, 0x2a, 0xab, 0x79, 0x2d, 0xc2, 0x52, 0xd5, 0xde, 0x45,
        0x33, 0xcc, 0x95, 0x18, 0xd3, 0x8a, 0xa8, 0xdb, 0xf1, 0x92, 0x5a, 0xb9, 0x23, 0x86, 0xed,
        0xd4, 0x0, 0x99, 0x23,
    ];
    assert_eq!(res, expected);

    let len = m.len() as u64 + (h.len() as u64 * 8);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 2,
        ExtCosts::read_memory_byte: len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 64,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 64,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 64,
        ExtCosts::blake2b_base: 1,
        ExtCosts::blake2b_block: 1,
        ExtCosts::blake2b_round: 12,
    });
}

#[test]
#[cfg(feature = "protocol_feature_evm")]
fn test_blake2s() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let rounds = 10;
    // These must be u64, even though they are actually u32.
    let h: [u32; 8] = [
        0x6b08e647, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let m: &[u8; 3] = b"abc";
    let t: u64 = 0;
    // equivalent to !0 for u32.
    let f = u32::MAX as u64;

    logic.blake2s(rounds, h.as_ptr() as _, m.len() as u64, m.as_ptr() as _, t, f, 0).unwrap();

    let res = [0u8; 32];
    logic.read_register(0, res.as_ptr() as _).expect("OK");

    let expected: [u8; 32] = [
        0x50, 0x8c, 0x5e, 0x8c, 0x32, 0x7c, 0x14, 0xe2, 0xe1, 0xa7, 0x2b, 0xa3, 0x4e, 0xeb, 0x45,
        0x2f, 0x37, 0x45, 0x8b, 0x20, 0x9e, 0xd6, 0x3a, 0x29, 0x4d, 0x99, 0x9b, 0x4c, 0x86, 0x67,
        0x59, 0x82,
    ];
    assert_eq!(res, expected);

    let len = m.len() as u64 + (h.len() as u64 * 4);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 2,
        ExtCosts::read_memory_byte: len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::blake2b_base: 1,
        ExtCosts::blake2b_block: 1,
        ExtCosts::blake2b_round: 10,
    });
}

#[test]
#[cfg(feature = "protocol_feature_evm")]
fn test_ecrecover() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    // See: https://github.com/OpenZeppelin/openzeppelin-contracts/blob/master/test/cryptography/ECDSA.test.js
    use sha3::Digest;
    let hash = sha3::Keccak256::digest(b"OpenZeppelin");
    let signature: [u8; 65] = [
        0x5d, 0x99, 0xb6, 0xf7, 0xf6, 0xd1, 0xf7, 0x3d, 0x1a, 0x26, 0x49, 0x7f, 0x2b, 0x1c, 0x89,
        0xb2, 0x4c, 0x09, 0x93, 0x91, 0x3f, 0x86, 0xe9, 0xa2, 0xd0, 0x2c, 0xd6, 0x98, 0x87, 0xd9,
        0xc9, 0x4f, 0x3c, 0x88, 0x03, 0x58, 0x57, 0x9d, 0x81, 0x1b, 0x21, 0xdd, 0x1b, 0x7f, 0xd9,
        0xbb, 0x01, 0xc1, 0xd8, 0x1d, 0x10, 0xe6, 0x9f, 0x03, 0x84, 0xe6, 0x75, 0xc3, 0x2b, 0x39,
        0x64, 0x3b, 0xe8, 0x92, 0x1b,
    ];
    let signer: [u8; 65] = [
        0x04, 0xb3, 0x68, 0x70, 0xea, 0xab, 0x31, 0xcb, 0xeb, 0x1a, 0x5c, 0x07, 0x46, 0x7b, 0x42,
        0x97, 0x40, 0x7c, 0x62, 0x11, 0x7a, 0x31, 0x15, 0x47, 0xc4, 0x30, 0x5e, 0x14, 0x71, 0x52,
        0x1b, 0x53, 0x01, 0xc2, 0x59, 0x9d, 0x4e, 0xad, 0xdf, 0xd3, 0x84, 0x9d, 0xf9, 0xd5, 0x99,
        0x38, 0xfd, 0x8f, 0x16, 0x56, 0x47, 0x77, 0x32, 0x66, 0x80, 0x66, 0xff, 0xa1, 0x2e, 0xb3,
        0x47, 0xea, 0xb4, 0x7b, 0x9c,
    ];

    logic.ecrecover(hash.as_ptr() as _, signature.as_ptr() as _, 0).unwrap();

    let result = &vec![0u8; 65];
    logic.read_register(0, result.as_ptr() as _).expect("OK");

    assert_eq!(result.to_vec(), signer);
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 2,
        ExtCosts::read_memory_byte: 97,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 65,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 65,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 65,
        ExtCosts::ecrecover_base: 1,
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

#[test]
fn test_key_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut key = "a".repeat(1024).as_bytes().to_vec();
    let val = b"hello";
    let limit = key.len() as u64;
    logic_builder.config.limit_config.max_length_storage_key = limit;
    let mut logic = logic_builder.build(get_context(vec![], false));
    // Under the limit. Valid calls.
    logic
        .storage_has_key(key.len() as _, key.as_ptr() as _)
        .expect("storage_has_key: key length is under the limit");
    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage_read: key length is under the limit");
    logic
        .storage_read(key.len() as _, key.as_ptr() as _, 0)
        .expect("storage_read: key length is under the limit");
    logic
        .storage_remove(key.len() as _, key.as_ptr() as _, 0)
        .expect("storage_remove: key length is under the limit");
    // Over the limit. Invalid calls.
    key.push(b'a');
    assert_eq!(
        logic.storage_has_key(key.len() as _, key.as_ptr() as _),
        Err(HostError::KeyLengthExceeded { length: key.len() as _, limit }.into())
    );
    assert_eq!(
        logic.storage_write(
            key.len() as _,
            key.as_ptr() as _,
            val.len() as _,
            val.as_ptr() as _,
            0
        ),
        Err(HostError::KeyLengthExceeded { length: key.len() as _, limit }.into())
    );
    assert_eq!(
        logic.storage_read(key.len() as _, key.as_ptr() as _, 0),
        Err(HostError::KeyLengthExceeded { length: key.len() as _, limit }.into())
    );
    assert_eq!(
        logic.storage_remove(key.len() as _, key.as_ptr() as _, 0),
        Err(HostError::KeyLengthExceeded { length: key.len() as _, limit }.into())
    );
}

#[test]
fn test_value_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut val = "a".repeat(1024).as_bytes().to_vec();
    logic_builder.config.limit_config.max_length_storage_value = val.len() as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let key = b"hello";
    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("Value length is under the limit");
    val.push(b'a');
    assert_eq!(
        logic.storage_write(
            key.len() as _,
            key.as_ptr() as _,
            val.len() as _,
            val.as_ptr() as _,
            0
        ),
        Err(HostError::ValueLengthExceeded {
            length: val.len() as u64,
            limit: logic_builder.config.limit_config.max_length_storage_value
        }
        .into())
    );
}

#[test]
fn test_num_promises() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_promises = 10;
    logic_builder.config.limit_config.max_promises_per_function_call_action = num_promises;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let account_id = b"alice";
    for _ in 0..num_promises {
        logic
            .promise_batch_create(account_id.len() as _, account_id.as_ptr() as _)
            .expect("Number of promises is under the limit");
    }
    assert_eq!(
        logic.promise_batch_create(account_id.len() as _, account_id.as_ptr() as _),
        Err(HostError::NumberPromisesExceeded {
            number_of_promises: num_promises + 1,
            limit: logic_builder.config.limit_config.max_promises_per_function_call_action
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
    let account_id = b"alice";
    let promise_id = logic
        .promise_batch_create(account_id.len() as _, account_id.as_ptr() as _)
        .expect("Number of promises is under the limit");
    for num in 0..num_deps {
        let promises = vec![promise_id; num as usize];
        logic
            .promise_and(promises.as_ptr() as _, promises.len() as _)
            .expect("Number of joined promises is under the limit");
    }
    let promises = vec![promise_id; (num_deps + 1) as usize];
    assert_eq!(
        logic.promise_and(promises.as_ptr() as _, promises.len() as _),
        Err(HostError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: promises.len() as u64,
            limit: logic_builder.config.limit_config.max_number_input_data_dependencies,
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
    let account_id = b"alice";
    let original_promise_id = logic
        .promise_batch_create(account_id.len() as _, account_id.as_ptr() as _)
        .expect("Number of promises is under the limit");
    let mut promise_id = original_promise_id;
    for _ in 1..num_steps {
        let promises = vec![promise_id, promise_id];
        promise_id = logic
            .promise_and(promises.as_ptr() as _, promises.len() as _)
            .expect("Number of joined promises is under the limit");
    }
    // The length of joined promises is exactly the limit (1024).
    let promises = vec![promise_id, promise_id];
    logic
        .promise_and(promises.as_ptr() as _, promises.len() as _)
        .expect("Number of joined promises is under the limit");

    // The length of joined promises exceeding the limit by 1 (total 1025).
    let promises = vec![promise_id, promise_id, original_promise_id];
    assert_eq!(
        logic.promise_and(promises.as_ptr() as _, promises.len() as _),
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
    let mut val = "a".repeat(1024).as_bytes().to_vec();
    logic_builder.config.limit_config.max_length_returned_data = val.len() as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));
    logic
        .value_return(val.len() as _, val.as_ptr() as _)
        .expect("Returned value length is under the limit");
    val.push(b'a');
    assert_eq!(
        logic.value_return(val.len() as _, val.as_ptr() as _),
        Err(HostError::ReturnedValueLengthExceeded {
            length: val.len() as u64,
            limit: logic_builder.config.limit_config.max_length_returned_data
        }
        .into())
    );
}

#[test]
fn test_contract_size_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut code = "a".repeat(1024).as_bytes().to_vec();
    logic_builder.config.limit_config.max_contract_size = code.len() as u64;
    let mut logic = logic_builder.build(get_context(vec![], false));
    let account_id = b"alice";
    let promise_id = logic
        .promise_batch_create(account_id.len() as _, account_id.as_ptr() as _)
        .expect("Number of promises is under the limit");
    logic
        .promise_batch_action_deploy_contract(promise_id, code.len() as u64, code.as_ptr() as _)
        .expect("The length of the contract code is under the limit");
    code.push(b'a');
    assert_eq!(
        logic.promise_batch_action_deploy_contract(
            promise_id,
            code.len() as u64,
            code.as_ptr() as _
        ),
        Err(HostError::ContractSizeExceeded {
            size: code.len() as u64,
            limit: logic_builder.config.limit_config.max_contract_size
        }
        .into())
    );
}

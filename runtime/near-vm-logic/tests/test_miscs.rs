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
#[cfg(feature = "protocol_feature_btp")]
fn test_sha3_256() {
    use hex_literal::hex;

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    let data = hex!("0448250ebe88d77e0a12bcf530fe6a2cf1ac176945638d309b840d631940c93b78c2bd6d16f227a8877e3f1604cd75b9c5a8ab0cac95174a8a0a0f8ea9e4c10bca");
    let hash = hex!("c7647f7e251bf1bd70863c8693e93a4e77dd0c9a689073e987d51254317dc704");

    logic.sha3_256(data.len() as _, data.as_ptr() as _, 0).unwrap();
    let res = &vec![0u8; 32];
    logic.read_register(0, res.as_ptr() as _).expect("OK");

    assert_eq!(res, &hash);

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
        ExtCosts::sha3_256_base: 1,
        ExtCosts::sha3_256_byte: len,
    });
}

#[test]
#[cfg(feature = "protocol_feature_btp")]
fn test_ecrecover_public_key() {
    use hex_literal::hex;

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let hash = hex!("c5d6c454e4d7a8e8a654f5ef96e8efe41d21a65b171b298925414aa3dc061e37");
    let signature = hex!("4011de30c04302a2352400df3d1459d6d8799580dceb259f45db1d99243a8d0c64f548b7776cb93e37579b830fc3efce41e12e0958cda9f8c5fcad682c61079500");
    let signer = hex!("0448250ebe88d77e0a12bcf530fe6a2cf1ac176945638d309b840d631940c93b78c2bd6d16f227a8877e3f1604cd75b9c5a8ab0cac95174a8a0a0f8ea9e4c10bca");

    let (r, s, v) = (&signature[0..32], &signature[32..64], signature[64] as u32);
    logic.ecrecover_public_key(hash.as_ptr() as _, v, r.as_ptr() as _, s.as_ptr() as _, 0).unwrap();

    let result = &vec![0u8; 65];
    logic.read_register(0, result.as_ptr() as _).expect("OK");

    assert_eq!(result.to_vec(), signer);

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 3,
        ExtCosts::read_memory_byte: 96,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 65,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 65,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 65,
        ExtCosts::ecrecover_public_key_base: 1,
    });
}

#[test]
#[cfg(feature = "protocol_feature_btp")]
fn test_ecrecover_public_key_invalid_parity_byte() {
    use hex_literal::hex;

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let hash = hex!("c5d6c454e4d7a8e8a654f5ef96e8efe41d21a65b171b298925414aa3dc061e37");
    let signature = hex!("4011de30c04302a2352400df3d1459d6d8799580dceb259f45db1d99243a8d0c64f548b7776cb93e37579b830fc3efce41e12e0958cda9f8c5fcad682c61079528");
    let signer = vec![0u8; 65];

    let (r, s, v) = (&signature[0..32], &signature[32..64], signature[64] as u32);
    logic.ecrecover_public_key(hash.as_ptr() as _, v, r.as_ptr() as _, s.as_ptr() as _, 0).unwrap();

    let result = &vec![0u8; 65];
    logic.read_register(0, result.as_ptr() as _).expect("OK");

    assert_eq!(result.to_vec(), signer);

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 3,
        ExtCosts::read_memory_byte: 96,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 0,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 0,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 0,
        ExtCosts::ecrecover_public_key_base: 1,
    });
}

#[test]
#[cfg(feature = "protocol_feature_btp")]
fn test_ecrecover_public_key_invalid_signature() {
    use hex_literal::hex;

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let hash = hex!("c5d6c454e4d7a8e8a654f5ef96e8efe41d21a65b171b298925414aa3dc061e37");
    let signature = hex!("1000de30c04302a2352400df3d1459d6d8799580dceb259f45db1d99243a8d0c64f548b7776cb93e37579b830fc3efce41e12e0958cda9f8c5fcad682c61079500");
    let signer = vec![0u8; 65];

    let (r, s, v) = (&signature[0..32], &signature[32..64], signature[64] as u32);
    logic.ecrecover_public_key(hash.as_ptr() as _, v, r.as_ptr() as _, s.as_ptr() as _, 0).unwrap();

    let result = &vec![0u8; 65];
    logic.read_register(0, result.as_ptr() as _).expect("OK");

    assert_eq!(result.to_vec(), signer);

    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 3,
        ExtCosts::read_memory_byte: 96,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 0,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 0,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 0,
        ExtCosts::ecrecover_public_key_base: 1,
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

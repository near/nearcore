use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::HostError;
use crate::logic::{ExtCosts, MemSlice, VMLogic, VMLogicError};
use crate::map;

#[test]
fn test_valid_utf8() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();
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

    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();

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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();

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
    let mut logic = logic_builder.build();

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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();

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
    let mut logic = logic_builder.build();
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
    let mut logic = logic_builder.build();

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

mod utf8_mem_violation {
    use super::*;

    fn check(read_ok: bool, test: fn(&mut VMLogic<'_>, MemSlice) -> Result<(), VMLogicError>) {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let bytes = b"foo bar \xff baz qux";
        let bytes = logic.internal_mem_write_at(64 * 1024 - bytes.len() as u64, bytes);
        let err = if read_ok { HostError::BadUTF8 } else { HostError::MemoryAccessViolation };
        assert_eq!(Err(err.into()), test(&mut logic, bytes));
    }

    #[test]
    fn test_good_read() {
        // The data is read correctly but it has invalid UTF-8 thus it ends up
        // with BadUTF8 error and user being charged for decoding.
        check(true, |logic, slice| logic.log_utf8(slice.len, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 17,
            ExtCosts::utf8_decoding_base: 1,
            ExtCosts::utf8_decoding_byte: 17,
        });
    }

    #[test]
    fn test_read_past_end() {
        // The data goes past the end of the memory resulting in memory access
        // violation.  User is not charged for UTF-8 decoding (except for the
        // base cost which is always charged).
        check(false, |logic, slice| logic.log_utf8(slice.len + 1, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 18,
            ExtCosts::utf8_decoding_base: 1,
        });
    }

    #[test]
    fn test_nul_past_end() {
        // The call goes past the end of the memory trying to find NUL byte
        // resulting in memory access violation.  User is not charged for UTF-8
        // decoding (except for the base cost which is always charged).
        check(false, |logic, slice| logic.log_utf8(u64::MAX, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 18,
            ExtCosts::read_memory_byte: 18,
            ExtCosts::utf8_decoding_base: 1,
        });
    }
}

mod utf16_mem_violation {
    use super::*;

    fn check(read_ok: bool, test: fn(&mut VMLogic<'_>, MemSlice) -> Result<(), VMLogicError>) {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut bytes = Vec::new();
        append_utf16(&mut bytes, "$ qò$`");
        bytes.extend_from_slice(&[0x00, 0xD8]); // U+D800, unpaired surrogate
        append_utf16(&mut bytes, "foobarbaz");
        let bytes = logic.internal_mem_write_at(64 * 1024 - bytes.len() as u64, &bytes);
        let err = if read_ok { HostError::BadUTF16 } else { HostError::MemoryAccessViolation };
        assert_eq!(Err(err.into()), test(&mut logic, bytes));
    }

    #[test]
    fn test_good_read() {
        // The data is read correctly but it has invalid UTF-16 thus it ends up
        // with BadUTF16 error and user being charged for decoding.
        check(true, |logic, slice| logic.log_utf16(slice.len, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 32,
            ExtCosts::utf16_decoding_base: 1,
            ExtCosts::utf16_decoding_byte: 32,
        });
    }

    #[test]
    fn test_read_past_end() {
        // The data goes past the end of the memory resulting in memory access
        // violation.  User is not charged for UTF-16 decoding (except for the
        // base cost which is always charged).
        check(false, |logic, slice| logic.log_utf16(slice.len + 2, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 1,
            ExtCosts::read_memory_byte: 34,
            ExtCosts::utf16_decoding_base: 1,
        });
    }

    #[test]
    fn test_nul_past_end() {
        // The call goes past the end of the memory trying to find NUL word
        // resulting in memory access violation.  User is not charged for UTF-16
        // decoding (except for the base cost which is always charged).
        check(false, |logic, slice| logic.log_utf16(u64::MAX, slice.ptr));
        assert_costs(map! {
            ExtCosts::base: 1,
            ExtCosts::read_memory_base: 17,
            ExtCosts::read_memory_byte: 34,
            ExtCosts::utf16_decoding_base: 1,
        });
    }
}

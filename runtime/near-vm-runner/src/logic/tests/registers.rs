use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::VMConfig;
use crate::logic::{HostError, VMLogicError};

#[test]
fn test_one_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    logic.wrapped_internal_write_register(0, &[0, 1, 2]).unwrap();
    assert_eq!(logic.register_len(0).unwrap(), 3u64);
    logic.assert_read_register(&[0, 1, 2], 0);
}

#[test]
fn test_non_existent_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    assert_eq!(logic.register_len(0), Ok(u64::MAX) as Result<u64, VMLogicError>);
    let buffer = [0u8; 3];
    assert_eq!(
        logic.read_register(0, buffer.as_ptr() as u64),
        Err(HostError::InvalidRegisterId { register_id: 0 }.into())
    );
}

#[test]
fn test_many_registers() {
    let mut logic_builder = VMLogicBuilder::default();
    let max_registers = logic_builder.config.limit_config.max_number_registers;
    let mut logic = logic_builder.build();

    for i in 0..max_registers {
        let value = (i * 10).to_le_bytes();
        logic.wrapped_internal_write_register(i, &value).unwrap();
        logic.assert_read_register(&(i * 10).to_le_bytes(), i);
    }

    // One more register hits the boundary check.
    assert_eq!(
        logic.wrapped_internal_write_register(max_registers, &[]),
        Err(HostError::MemoryAccessViolation.into())
    )
}

#[test]
fn test_max_register_size() {
    let mut logic_builder = VMLogicBuilder::free();
    let max_register_size = logic_builder.config.limit_config.max_register_size;
    let mut logic = logic_builder.build();

    let value = vec![0u8; (max_register_size + 1) as usize];

    assert_eq!(
        logic.wrapped_internal_write_register(0, &value),
        Err(HostError::MemoryAccessViolation.into())
    );
}

#[test]
fn test_max_register_memory_limit() {
    let mut logic_builder = VMLogicBuilder::free();
    let config = VMConfig::free();
    logic_builder.config = config.clone();
    let mut logic = logic_builder.build();

    let max_registers =
        config.limit_config.registers_memory_limit / config.limit_config.max_register_size;

    for i in 0..max_registers {
        let value = vec![1u8; config.limit_config.max_register_size as usize];
        logic.wrapped_internal_write_register(i, &value).expect("should be written successfully");
    }
    let last = vec![1u8; config.limit_config.max_register_size as usize];
    assert_eq!(
        logic.wrapped_internal_write_register(max_registers, &last),
        Err(HostError::MemoryAccessViolation.into())
    );
}

#[test]
fn test_register_is_not_used() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    assert_eq!(logic.register_len(0), Ok(u64::MAX));
}

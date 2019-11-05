mod fixtures;
mod vm_logic_builder;

use fixtures::get_context;
use near_vm_logic::{HostError, HostErrorOrStorageError, VMConfig};
use vm_logic_builder::VMLogicBuilder;

#[test]
fn test_one_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.write_register(0, &vec![0, 1, 2]).unwrap();
    assert_eq!(logic.register_len(0).unwrap(), 3u64);
    let buffer = [0u8; 3];
    logic.read_register(0, buffer.as_ptr() as u64).unwrap();
    assert_eq!(buffer, [0u8, 1, 2]);
}

#[test]
fn test_non_existent_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    assert_eq!(logic.register_len(0), Ok(std::u64::MAX) as Result<u64, HostErrorOrStorageError>);
    let buffer = [0u8; 3];
    assert_eq!(
        logic.read_register(0, buffer.as_ptr() as u64),
        Err(HostError::InvalidRegisterId.into())
    );
}

#[test]
fn test_many_registers() {
    let mut logic_builder = VMLogicBuilder::default();
    let max_registers = logic_builder.config.max_number_registers;
    let mut logic = logic_builder.build(get_context(vec![], false));

    for i in 0..max_registers {
        let value = (i * 10).to_le_bytes();
        logic.write_register(i, &value).unwrap();

        let buffer = [0u8; std::mem::size_of::<u64>()];
        logic.read_register(i, buffer.as_ptr() as u64).unwrap();
        assert_eq!(i * 10, u64::from_le_bytes(buffer));
    }

    // One more register hits the boundary check.
    assert_eq!(
        logic.write_register(max_registers, &[]),
        Err(HostError::MemoryAccessViolation.into())
    )
}

#[test]
fn test_max_register_size() {
    let mut logic_builder = VMLogicBuilder::default();
    let max_register_size = logic_builder.config.max_register_size;
    let mut logic = logic_builder.build(get_context(vec![], false));

    let value = vec![0u8; (max_register_size + 1) as usize];

    assert_eq!(logic.write_register(0, &value), Err(HostError::MemoryAccessViolation.into()));
}

#[test]
fn test_max_register_memory_limit() {
    let mut logic_builder = VMLogicBuilder::free();
    let config = VMConfig::free();
    logic_builder.config = config.clone();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let max_registers = config.registers_memory_limit / config.max_register_size;

    for i in 0..max_registers {
        let value = vec![1u8; config.max_register_size as usize];
        logic.write_register(i, &value).expect("should be written successfully");
    }
    let last = vec![1u8; config.max_register_size as usize];
    assert_eq!(
        logic.write_register(max_registers, &last),
        Err(HostError::MemoryAccessViolation.into())
    );
}

#[test]
fn test_register_is_not_used() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));
    assert_eq!(logic.register_len(0), Ok(std::u64::MAX));
}

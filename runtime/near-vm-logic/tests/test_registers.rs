use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, HostError, VMLogic};
use std::mem::size_of;

mod fixtures;

#[test]
fn test_one_register() {
    let mut ext = MockedExternal::new();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::new();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    logic.write_register(0, &vec![0, 1, 2]).unwrap();
    assert_eq!(logic.register_len(0).unwrap(), 3u64);
    let buffer = [0u8; 3];
    logic.read_register(0, buffer.as_ptr() as u64).unwrap();
    assert_eq!(buffer, [0u8, 1, 2]);
}

#[test]
fn test_non_existent_register() {
    let mut ext = MockedExternal::new();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::new();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    assert_eq!(logic.register_len(0), Ok(std::u64::MAX) as Result<u64, HostError>);
    let buffer = [0u8; 3];
    assert_eq!(
        logic.read_register(0, buffer.as_ptr() as u64),
        Err(HostError::InvalidRegisterId) as Result<(), HostError>
    );
}

#[test]
fn test_many_registers() {
    let mut ext = MockedExternal::new();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::new();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let max_registers = config.max_number_registers;
    for i in 0..max_registers {
        let value = (i * 10).to_le_bytes();
        logic.write_register(i, &value).unwrap();

        let buffer = [0u8; size_of::<u64>()];
        logic.read_register(i, buffer.as_ptr() as u64).unwrap();
        assert_eq!(i * 10, u64::from_le_bytes(buffer));
    }

    // One more register hits the boundary check.
    assert_eq!(
        logic.write_register(max_registers, &[]),
        Err(HostError::MemoryAccessViolation) as Result<(), HostError>
    )
}

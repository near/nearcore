use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, VMLogic};

mod fixtures;

#[test]
fn test_storage_write_counter() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let data_record_cost = config.runtime_fees.storage_usage_config.data_record_cost;
    let key = b"foo";
    let val = b"bar";

    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    let cost_expected = (data_record_cost as usize + key.len() + val.len()) as u64;

    assert_eq!(logic.storage_usage().unwrap(), cost_expected);

    let key = b"foo";
    let val = b"bar";

    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    assert_eq!(logic.storage_usage().unwrap(), cost_expected);
}

#[test]
fn test_storage_remove() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let data_record_cost = config.runtime_fees.storage_usage_config.data_record_cost;
    let key = b"foo";
    let val = b"bar";

    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    logic.storage_remove(key.len() as _, key.as_ptr() as _, 0).expect("storage remove ok");

    assert_eq!(logic.storage_usage().unwrap(), 0u64);
}

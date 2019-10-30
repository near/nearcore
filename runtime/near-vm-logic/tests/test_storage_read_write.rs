mod fixtures;

use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, External, VMLogic};

#[test]
fn test_storage_write_with_register() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    logic.write_register(1, key).unwrap();
    logic.write_register(2, val).unwrap();

    logic.storage_write(std::u64::MAX, 1 as _, std::u64::MAX, 2 as _, 0).expect("storage write ok");

    assert_eq!(ext.storage_get(key), Ok(Some(val.to_vec())));
}

#[test]
fn test_storage_read_with_register() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    ext.storage_set(key, val).unwrap();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    logic.write_register(1, key).unwrap();

    logic.storage_read(std::u64::MAX, 1 as _, 0).expect("storage read ok");
    let res = [0u8; 3];
    logic.read_register(0, res.as_ptr() as _).unwrap();
    assert_eq!(&res, b"bar");
}

#[test]
fn test_storage_remove_with_register() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    logic.write_register(1, key).unwrap();

    logic.storage_remove(std::u64::MAX, 1 as _, 0).expect("storage remove ok");
    let res = [0u8; 3];
    logic.read_register(0, res.as_ptr() as _).unwrap();
    assert_eq!(&res, b"bar");
}

#[test]
fn test_storage_has_key_with_register() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";
    ext.storage_set(key, val).unwrap();

    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    logic.write_register(1, key).unwrap();

    assert_eq!(logic.storage_has_key(std::u64::MAX, 1 as _), Ok(1));
}

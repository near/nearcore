mod fixtures;
mod vm_logic_builder;

use fixtures::get_context;
use near_vm_logic::External;
use vm_logic_builder::VMLogicBuilder;

#[test]
fn test_storage_write_with_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    logic.wrapped_internal_write_register(1, key).unwrap();
    logic.wrapped_internal_write_register(2, val).unwrap();

    logic.storage_write(std::u64::MAX, 1 as _, std::u64::MAX, 2 as _, 0).expect("storage write ok");

    assert_eq!(logic_builder.ext.storage_get(key), Ok(Some(val.to_vec())));
}

#[test]
fn test_storage_read_with_register() {
    let mut logic_builder = VMLogicBuilder::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    logic_builder.ext.storage_set(key, val).unwrap();
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.wrapped_internal_write_register(1, key).unwrap();

    logic.storage_read(std::u64::MAX, 1 as _, 0).expect("storage read ok");
    let res = [0u8; 3];
    logic.read_register(0, res.as_ptr() as _).unwrap();
    assert_eq!(&res, b"bar");
}

#[test]
fn test_storage_remove_with_register() {
    let mut logic_builder = VMLogicBuilder::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    let mut logic = logic_builder.build(get_context(vec![], false));
    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    logic.wrapped_internal_write_register(1, key).unwrap();

    logic.storage_remove(std::u64::MAX, 1 as _, 0).expect("storage remove ok");
    let res = [0u8; 3];
    logic.read_register(0, res.as_ptr() as _).unwrap();
    assert_eq!(&res, b"bar");
}

#[test]
fn test_storage_has_key_with_register() {
    let mut logic_builder = VMLogicBuilder::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";
    logic_builder.ext.storage_set(key, val).unwrap();

    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.wrapped_internal_write_register(1, key).unwrap();

    assert_eq!(logic.storage_has_key(std::u64::MAX, 1 as _), Ok(1));
}

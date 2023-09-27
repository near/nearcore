use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::{External, StorageGetMode};

#[test]
fn test_storage_write_with_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    logic.wrapped_internal_write_register(1, key).unwrap();
    logic.wrapped_internal_write_register(2, val).unwrap();

    logic.storage_write(u64::MAX, 1 as _, u64::MAX, 2 as _, 0).expect("storage write ok");

    let value_ptr = logic_builder.ext.storage_get(key, StorageGetMode::Trie).unwrap().unwrap();
    assert_eq!(value_ptr.deref().unwrap(), val.to_vec());
}

#[test]
fn test_storage_read_with_register() {
    let mut logic_builder = VMLogicBuilder::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";

    logic_builder.ext.storage_set(key, val).unwrap();
    let mut logic = logic_builder.build();

    logic.wrapped_internal_write_register(1, key).unwrap();

    logic.storage_read(u64::MAX, 1 as _, 0).expect("storage read ok");
    logic.assert_read_register(val, 0);
}

#[test]
fn test_storage_remove_with_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let key = logic.internal_mem_write(b"foo");
    let val = logic.internal_mem_write(b"bar");

    logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0).expect("storage write ok");

    logic.wrapped_internal_write_register(1, b"foo").unwrap();

    logic.storage_remove(u64::MAX, 1 as _, 0).expect("storage remove ok");
    logic.assert_read_register(b"bar", 0);
}

#[test]
fn test_storage_has_key_with_register() {
    let mut logic_builder = VMLogicBuilder::default();

    let key: &[u8] = b"foo";
    let val: &[u8] = b"bar";
    logic_builder.ext.storage_set(key, val).unwrap();

    let mut logic = logic_builder.build();

    logic.wrapped_internal_write_register(1, key).unwrap();

    assert_eq!(logic.storage_has_key(u64::MAX, 1 as _), Ok(1));
}

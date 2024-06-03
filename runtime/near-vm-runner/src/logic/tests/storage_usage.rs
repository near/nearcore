use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_storage_write_counter() {
    let mut logic_builder = VMLogicBuilder::default();
    let data_record_cost = logic_builder.fees_config.storage_usage_config.num_extra_bytes_record;
    let mut logic = logic_builder.build();
    let key = logic.internal_mem_write(b"foo");
    let val = logic.internal_mem_write(b"bar");

    logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0).expect("storage write ok");

    let cost_expected = data_record_cost + key.len + val.len;

    assert_eq!(logic.storage_usage().unwrap(), cost_expected);

    logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0).expect("storage write ok");

    assert_eq!(logic.storage_usage().unwrap(), cost_expected);
}

#[test]
fn test_storage_remove() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let key = logic.internal_mem_write(b"foo");
    let val = logic.internal_mem_write(b"bar");

    logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0).expect("storage write ok");

    logic.storage_remove(key.len, key.ptr, 0).expect("storage remove ok");

    assert_eq!(logic.storage_usage().unwrap(), 0u64);
}

use crate::tests::fixtures::get_context;
use crate::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_storage_write_counter() {
    let mut logic_builder = VMLogicBuilder::default();
    let data_record_cost = logic_builder.fees_config.storage_usage_config.num_extra_bytes_record;
    let mut l = logic_builder.build(get_context(vec![], false));
    let key = b"foo";
    let val = b"bar";

    l.logic
        .storage_write(
            l.mem,
            key.len() as _,
            key.as_ptr() as _,
            val.len() as _,
            val.as_ptr() as _,
            0,
        )
        .expect("storage write ok");

    let cost_expected = (data_record_cost as usize + key.len() + val.len()) as u64;

    assert_eq!(l.logic.storage_usage(l.mem).unwrap(), cost_expected);

    let key = b"foo";
    let val = b"bar";

    l.logic
        .storage_write(
            l.mem,
            key.len() as _,
            key.as_ptr() as _,
            val.len() as _,
            val.as_ptr() as _,
            0,
        )
        .expect("storage write ok");

    assert_eq!(l.logic.storage_usage(l.mem).unwrap(), cost_expected);
}

#[test]
fn test_storage_remove() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut l = logic_builder.build(get_context(vec![], false));

    let key = b"foo";
    let val = b"bar";

    l.logic
        .storage_write(
            l.mem,
            key.len() as _,
            key.as_ptr() as _,
            val.len() as _,
            val.as_ptr() as _,
            0,
        )
        .expect("storage write ok");

    l.logic.storage_remove(l.mem, key.len() as _, key.as_ptr() as _, 0).expect("storage remove ok");

    assert_eq!(l.logic.storage_usage(l.mem).unwrap(), 0u64);
}

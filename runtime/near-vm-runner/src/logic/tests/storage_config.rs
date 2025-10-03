use near_primitives_core::types::Balance;

use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_storage_config_byte_cost() {
    let expected_cost = Balance::from_yoctonear(123);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.fees_config.storage_usage_config.storage_amount_per_byte = expected_cost;
    let mut logic = logic_builder.build();

    logic.storage_config_byte_cost(0).unwrap();

    let returned_cost = u128::from_le_bytes(logic.internal_mem_read(0, 16).try_into().unwrap());

    assert_eq!(expected_cost.as_yoctonear(), returned_cost);
}

#[test]
fn test_storage_config_num_bytes_account() {
    let expected_bytes = 1234;

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.fees_config.storage_usage_config.num_bytes_account = expected_bytes;
    let mut logic = logic_builder.build();

    let returned_bytes = logic.storage_config_num_bytes_account().unwrap();

    assert_eq!(expected_bytes, returned_bytes);
}
#[test]
fn test_storage_config_num_extra_bytes_record() {
    let expected_bytes = 12345;

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.fees_config.storage_usage_config.num_extra_bytes_record = expected_bytes;
    let mut logic = logic_builder.build();

    let returned_bytes = logic.storage_config_num_extra_bytes_record().unwrap();

    assert_eq!(expected_bytes, returned_bytes);
}

use crate::tests::fixtures::get_context;
use crate::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_current_gas_price() {
    let expected: u128 = 10u128.pow(8) + 123456;

    let mut context = get_context(vec![], false);
    context.current_gas_price = expected;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);

    let mut output: u128 = 0x0abc_abcd_bcde_cdef_0123_1234_3456_4567;
    logic.current_gas_price((&mut output) as *mut u128 as _).unwrap();

    assert_eq!(output, expected);
}

#[test]
fn test_receipt_gas_price() {
    let expected: u128 = 10u128.pow(8) + 123456;

    let mut context = get_context(vec![], false);
    context.receipt_gas_price = expected;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);

    let mut output: u128 = 0x0abc_abcd_bcde_cdef_0123_1234_3456_4567;
    logic.pessimistic_receipt_gas_price((&mut output) as *mut u128 as _).unwrap();
    assert_eq!(output, expected);
}

use crate::tests::fixtures::get_context;
use crate::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_burn_gas_price() {
    let expected: u128 = 10u128.pow(8) + 123456;

    let mut context = get_context(vec![], false);
    context.burn_gas_price = expected;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);

    let mut output: u128 = 0x0abc_abcd_bcde_cdef_0123_1234_3456_4567;
    logic.burn_gas_price((&mut output) as *mut u128 as _).unwrap();

    assert_eq!(output, expected);
}

#[test]
fn test_purchased_gas_price() {
    let expected: u128 = 10u128.pow(8) + 123456;

    let mut context = get_context(vec![], false);
    context.purchased_gas_price = expected;
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(context);

    let mut output: u128 = 0x0abc_abcd_bcde_cdef_0123_1234_3456_4567;
    logic.purchased_gas_price((&mut output) as *mut u128 as _).unwrap();
    assert_eq!(output, expected);
}

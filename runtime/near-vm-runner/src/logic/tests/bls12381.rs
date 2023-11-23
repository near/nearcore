use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

#[test]
fn test_bls12381_g1_sum_edge_cases() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    // 0 + 0
    let mut zero: [u8; 97] = [0; 97];
    zero[0] = 64;
    let buffer: [[u8; 97]; 2] = [zero; 2];

    let input = logic.internal_mem_write(buffer.concat().as_slice());

    let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
    assert_eq!(res, 0);
    let got = logic.registers().get_for_free(0).unwrap();
    assert_eq!(&zero[..96], got);

}

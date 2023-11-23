mod tests {
    use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
    use amcl::bls381::big::Big;
    use amcl::bls381::ecp::ECP;
    use amcl::rand::RAND;
    use amcl::bls381::bls381::utils::serialize_uncompressed_g1;

    fn get_random_g1_point(rnd: &mut RAND) -> ECP {
        let r: Big = Big::random(rnd);
        let g: ECP = ECP::generator();

        g.mul(&r)
    }

    #[test]
    fn test_bls12381_g1_sum_edge_cases() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // 0 + 0
        let mut zero: [u8; 97] = [0; 97];
        zero[1] = 64;
        let buffer: [[u8; 97]; 2] = [zero; 2];

        let input = logic.internal_mem_write(buffer.concat().as_slice());

        let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        let got = logic.registers().get_for_free(0).unwrap();
        assert_eq!(&zero[1..97], got);


        // 0 + P = P + 0 = P
        let mut rnd: RAND = RAND::new();
        for _ in 0..10 {
            let p = get_random_g1_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let mut buffer = vec![vec![0], p_ser.to_vec(), zero.to_vec()];

            let mut input = logic.internal_mem_write(buffer.concat().as_slice());

            let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            let got = logic.registers().get_for_free(0).unwrap();
            assert_eq!(&p_ser, got);

            buffer = vec![zero.to_vec(), vec![0], p_ser.to_vec()];
            input = logic.internal_mem_write(buffer.concat().as_slice());

            let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            let got = logic.registers().get_for_free(0).unwrap();
            assert_eq!(&p_ser, got);
        }
    }
}

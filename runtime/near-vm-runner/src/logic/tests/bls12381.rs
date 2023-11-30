mod tests {
    use crate::logic::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
    use amcl::bls381::big::Big;
    use amcl::bls381::bls381::core::deserialize_g1;
    use amcl::bls381::ecp::ECP;
    use amcl::rand::RAND;
    use amcl::bls381::bls381::utils::{subgroup_check_g1, serialize_uncompressed_g1};
    use rand::thread_rng;
    use rand::seq::SliceRandom;

    fn get_random_g1_point(rnd: &mut RAND) -> ECP {
        let r: Big = Big::random(rnd);
        let g: ECP = ECP::generator();

        g.mul(&r)
    }

    fn get_random_curve_point(rnd: &mut RAND) -> ECP {
        let mut r: Big = Big::random(rnd);
        r.mod2m(381);
        let mut p: ECP = ECP::new_big(&r);

        while p.is_infinity() {
            r = Big::random(rnd);
            r.mod2m(381);
            p = ECP::new_big(&r);
        }

        p
    }

    fn get_random_not_g1_curve_point(rnd: &mut RAND) -> ECP {
        let mut r: Big = Big::random(rnd);
        r.mod2m(381);
        let mut p: ECP = ECP::new_big(&r);

        while p.is_infinity() || subgroup_check_g1(&p) {
            r = Big::random(rnd);
            r.mod2m(381);
            p = ECP::new_big(&r);
        }

        p
    }

    fn get_rnd() -> RAND {
        let mut rnd: RAND = RAND::new();
        rnd.clean();
        let mut raw : [u8;100]=[0;100];
        for i in 0..100 {raw[i]=i as u8}

        rnd.seed(100,&raw);

        rnd
    }

    fn get_g1_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_inverse(p: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![1], p.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_sum_many_points(points: &Vec<(u8, ECP)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(vec![points[i].0]);
            buffer.push(serialize_uncompressed_g1(&points[i].1).to_vec());
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_g1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    #[test]
    fn test_bls12381_g1_sum_edge_cases() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // 0 + 0
        let mut zero: [u8; 96] = [0; 96];
        zero[0] = 64;
        let got = get_g1_sum(0, &zero, 0, &zero, &mut logic);
        assert_eq!(zero.to_vec(), got);

        // 0 + P = P + 0 = P
        let mut rnd = get_rnd();
        for _ in 0..10 {
            let p = get_random_g1_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let got = get_g1_sum(0, &zero, 0, &p_ser, &mut logic);
            assert_eq!(p_ser.to_vec(), got);

            let got = get_g1_sum(0,&p_ser, 0, &zero, &mut logic);
            assert_eq!(p_ser.to_vec(), got);
        }

        // P + (-P) = (-P) + P =  0
        for _ in 0..10 {
            let mut p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g1(&p);

            let got = get_g1_sum(0, &p_neg_ser, 0, &p_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);

            let got = get_g1_sum(0, &p_ser, 0, &p_neg_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);
        }


        // P + P
        for _ in 0..10 {
            let p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let pmul2 = p.mul(&Big::from_bytes(&[2]));
            let pmul2_ser = serialize_uncompressed_g1(&pmul2);

            let got = get_g1_sum(0, &p_ser, 0, &p_ser, &mut logic);
            assert_eq!(pmul2_ser.to_vec(), got);
        }

        // P + (-(P + P))
        for _ in 0..10 {
            let mut p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let mut pmul2 = p.mul(&Big::from_bytes(&[2]));
            pmul2.neg();
            let pmul2_neg_ser = serialize_uncompressed_g1(&pmul2);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g1(&p);
            let got = get_g1_sum(0, &p_ser, 0, &pmul2_neg_ser, &mut logic);
            assert_eq!(p_neg_ser.to_vec(), got);
        }
    }

    #[test]
    fn test_bls12381_g1_sum() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        for _ in 0..100 {
            let mut p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let q = get_random_curve_point(&mut rnd);
            let q_ser = serialize_uncompressed_g1(&q);

            // P + Q = Q + P
            let got1 = get_g1_sum(0, &p_ser, 0, &q_ser, &mut logic);
            let got2 = get_g1_sum(0, &q_ser,0,  &p_ser, &mut logic);
            assert_eq!(got1, got2);

            // compare with library results
            p.add(&q);
            let library_sum = serialize_uncompressed_g1(&p);

            assert_eq!(library_sum.to_vec(), got1);
        }

        // generate points from G1
        for _ in 0..100 {
            let p = get_random_g1_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let q = get_random_g1_point(&mut rnd);
            let q_ser = serialize_uncompressed_g1(&q);

            let got1 = get_g1_sum(0, &p_ser, 0, &q_ser, &mut logic);

            let result_point = deserialize_g1(&got1).unwrap();
            assert!(subgroup_check_g1(&result_point));
        }
    }

    #[test]
    fn test_bls12381_g1_sum_not_g1_points() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        //points not from G1
        for _ in 0..100 {
            let mut p = get_random_not_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let q = get_random_not_g1_curve_point(&mut rnd);
            let q_ser = serialize_uncompressed_g1(&q);

            // P + Q = Q + P
            let got1 = get_g1_sum(0, &p_ser, 0, &q_ser, &mut logic);
            let got2 = get_g1_sum(0, &q_ser, 0, &p_ser, &mut logic);
            assert_eq!(got1, got2);

            // compare with library results
            p.add(&q);
            let library_sum = serialize_uncompressed_g1(&p);

            assert_eq!(library_sum.to_vec(), got1);
        }
    }

    #[test]
    fn test_bls12381_g1_sum_inverse() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        let mut zero: [u8; 96] = [0; 96];
        zero[0] = 64;

        for _ in 0..10 {
            let p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            // P - P = - P + P = 0
            let got1 = get_g1_sum(1, &p_ser, 0,&p_ser, &mut logic);
            let got2 = get_g1_sum(0,&p_ser, 1, &p_ser, &mut logic);
            assert_eq!(got1, got2);
            assert_eq!(got1, zero.to_vec());

            // -(-P)
            let p_inv = get_g1_inverse(&p_ser, &mut logic);
            let p_inv_inv = get_g1_inverse(p_inv.as_slice(), &mut logic);

            assert_eq!(p_ser.to_vec(), p_inv_inv);
        }

        // P in G1 => -P in G1
        for _ in 0..10 {
            let p = get_random_g1_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let p_inv = get_g1_inverse(&p_ser, &mut logic);

            let result_point = deserialize_g1(&p_inv).unwrap();
            assert!(subgroup_check_g1(&result_point));
        }

        // Random point check with library
        for _ in 0..10 {
            let mut p = get_random_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let p_inv = get_g1_inverse(&p_ser, &mut logic);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g1(&p);

            assert_eq!(p_neg_ser.to_vec(), p_inv);
        }

        // Not from G1 points
        for _ in 0..10 {
            let mut p = get_random_not_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let p_inv = get_g1_inverse(&p_ser, &mut logic);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g1(&p);

            assert_eq!(p_neg_ser.to_vec(), p_inv);
        }

        // -0
        let zero_inv = get_g1_inverse(&zero, &mut logic);
        assert_eq!(zero.to_vec(), zero_inv);
    }

    #[test]
    fn test_bls12381_g1_sum_many_points() {
        let mut rnd = get_rnd();

        let mut zero: [u8; 96] = [0; 96];
        zero[0] = 64;

        //empty input
        let res = get_g1_sum_many_points(&vec![]);
        assert_eq!(zero.to_vec(), res);

        const MAX_N: usize = 676;

        for n in 0 .. MAX_N {
            let mut res3 = ECP::new();

            let mut points: Vec<(u8, ECP)> = vec![];
            for i in 0 .. n {
                points.push((rnd.getbyte() % 2, get_random_curve_point(&mut rnd)));

                let mut current_point = points[i].1.clone();
                if points[i].0 == 1 {
                    current_point.neg();
                }

                res3.add(&current_point);
            }

            let res1 = get_g1_sum_many_points(&points);

            points.shuffle(&mut thread_rng());
            let res2 = get_g1_sum_many_points(&points);
            assert_eq!(res1, res2);

            assert_eq!(res1, serialize_uncompressed_g1(&res3).to_vec());
        }
    }

}

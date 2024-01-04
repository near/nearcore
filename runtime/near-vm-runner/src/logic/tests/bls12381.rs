mod tests {
    use crate::logic::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
    use amcl::bls381::big::Big;
    use amcl::bls381::bls381::core::deserialize_g1;
    use amcl::bls381::bls381::core::deserialize_g2;
    use amcl::bls381::bls381::core::map_to_curve_g1;
    use amcl::bls381::bls381::core::map_to_curve_g2;
    use amcl::bls381::ecp::ECP;
    use amcl::bls381::ecp2::ECP2;
    use amcl::bls381::pair;
    use amcl::rand::RAND;
    use amcl::bls381::bls381::utils::{subgroup_check_g1,
                                      subgroup_check_g2,
                                      serialize_uncompressed_g1,
                                      serialize_uncompressed_g2,
                                      serialize_g1, serialize_g2};
    use amcl::bls381::fp2::FP2;
    use amcl::bls381::fp::FP;
    use amcl::bls381::rom::H_EFF_G1;
    use borsh::BorshSerialize;
    use rand::thread_rng;
    use rand::seq::SliceRandom;
    use rand::RngCore;

    fn get_random_g1_point(rnd: &mut RAND) -> ECP {
        let r: Big = Big::random(rnd);
        let g: ECP = ECP::generator();

        g.mul(&r)
    }

    fn get_random_g2_point(rnd: &mut RAND) -> ECP2 {
        let r: Big = Big::random(rnd);
        let g: ECP2 = ECP2::generator();

        g.mul(&r)
    }

    fn get_random_g1_curve_point(rnd: &mut RAND) -> ECP {
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

    fn get_random_g2_curve_point(rnd: &mut RAND) -> ECP2 {
        let mut c: Big = Big::random(rnd);
        c.mod2m(381);

        let mut d: Big = Big::random(rnd);
        d.mod2m(381);
        let mut p: ECP2 = ECP2::new_fp2(&FP2::new_bigs(c, d));

        while p.is_infinity() {
            c = Big::random(rnd);
            c.mod2m(381);

            d = Big::random(rnd);
            d.mod2m(381);

            p = ECP2::new_fp2(&FP2::new_bigs(c, d));
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

    fn get_random_not_g2_curve_point(rnd: &mut RAND) -> ECP2 {
        let mut c: Big = Big::random(rnd);
        c.mod2m(381);

        let mut d: Big = Big::random(rnd);
        d.mod2m(381);
        let mut p: ECP2 = ECP2::new_fp2(&FP2::new_bigs(c, d));

        while p.is_infinity() || subgroup_check_g2(&p) {
            c = Big::random(rnd);
            c.mod2m(381);

            d = Big::random(rnd);
            d.mod2m(381);

            p = ECP2::new_fp2(&FP2::new_bigs(c, d));
        }

        p
    }

    fn get_random_fp(rnd: &mut RAND) -> FP {
        let mut c: Big = Big::random(rnd);
        c.mod2m(381);

        FP::new_big(c)
    }

    fn get_random_fp2(rnd: &mut RAND) -> FP2 {
        let mut c: Big = Big::random(rnd);
        c.mod2m(381);

        let mut d: Big = Big::random(rnd);
        d.mod2m(381);

        FP2::new_bigs(c, d)
    }

    fn get_rnd() -> RAND {
        let mut rnd: RAND = RAND::new();
        rnd.clean();
        let mut raw: [u8; 100] = [0; 100];
        for i in 0..100 { raw[i] = i as u8 }

        rnd.seed(100, &raw);

        rnd
    }

    fn decompress_p1(p1: ECP) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(&serialize_g1(&p1));
        let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn decompress_p2(p2: ECP2) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(&serialize_g2(&p2));
        let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn map_fp_to_g1(fp: FP) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut fp_vec: [u8; 48] = [0u8; 48];
        fp.redc().to_byte_array(&mut fp_vec, 0);

        let fp_vec = fp_vec.try_to_vec().unwrap();

        let input = logic.internal_mem_write(fp_vec.as_slice());
        let res = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn map_fp2_to_g2(fp2: FP2) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut fp2_vec: [u8; 96] = [0u8; 96];
        fp2.getb().to_byte_array(&mut fp2_vec, 0);
        fp2.geta().to_byte_array(&mut fp2_vec, 48);

        let fp2_vec = fp2_vec.try_to_vec().unwrap();

        let input = logic.internal_mem_write(fp2_vec.as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn pairing_check(p1: ECP, p2: ECP2) -> u64 {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        buffer.push(serialize_uncompressed_g1(&p1).to_vec());
        buffer.push(serialize_uncompressed_g2(&p2).to_vec());

        let input = logic.internal_mem_write(&buffer.concat().as_slice());
        let res = logic.bls12381_pairing_check(input.len, input.ptr).unwrap();
        return res;
    }

    fn pairing_check_vec(p1: Vec<u8>, p2: Vec<u8>) -> u64 {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer: Vec<Vec<u8>> = vec![p1, p2];

        let input = logic.internal_mem_write(&buffer.concat().as_slice());
        let res = logic.bls12381_pairing_check(input.len, input.ptr).unwrap();
        return res;
    }

    fn get_g1_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_inverse(p: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![1], p.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_inverse(p: &[u8], logic: &mut TestVMLogic) -> Vec<u8> {
        let buffer = vec![vec![1], p.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
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
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_sum_many_points(points: &Vec<(u8, ECP2)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(vec![points[i].0]);
            buffer.push(serialize_uncompressed_g2(&points[i].1).to_vec());
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_multiexp_many_points(points: &Vec<(u8, ECP)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g1(&points[i].1).to_vec());
            if points[i].0 == 0 {
                buffer.push(vec![vec![1], vec![0; 31]].concat());
            } else {
                //r - 1
                buffer.push(hex::decode("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000000").unwrap().into_iter().rev().collect());
            }
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_multiexp_many_points(points: &Vec<(u8, ECP2)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g2(&points[i].1).to_vec());
            if points[i].0 == 0 {
                buffer.push(vec![vec![1], vec![0; 31]].concat());
            } else {
                // r - 1
                buffer.push(hex::decode("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000000").unwrap().into_iter().rev().collect());
            }
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_multiexp_small(points: &Vec<(u8, ECP)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g1(&points[i].1).to_vec());
            let mut n_vec: [u8; 32] = [0u8; 32];
            n_vec[0] = points[i].0;
            buffer.push(n_vec.try_to_vec().unwrap());
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_multiexp_small(points: &Vec<(u8, ECP2)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g2(&points[i].1).to_vec());
            let mut n_vec: [u8; 32] = [0u8; 32];
            n_vec[0] = points[i].0;
            buffer.push(n_vec.try_to_vec().unwrap());
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_multiexp(points: &Vec<(Big, ECP)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g1(&points[i].1).to_vec());
            let mut n_vec: [u8; 48] = [0u8; 48];
            points[i].0.to_byte_array(&mut n_vec, 0);

            let mut n_vec = n_vec.try_to_vec().unwrap();
            n_vec.reverse();
            n_vec.resize(32, 0);

            buffer.push(n_vec);
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_multiexp(points: &Vec<(Big, ECP2)>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..points.len() {
            buffer.push(serialize_uncompressed_g2(&points[i].1).to_vec());
            let mut n_vec: [u8; 48] = [0u8; 48];
            points[i].0.to_byte_array(&mut n_vec, 0);

            let mut n_vec = n_vec.try_to_vec().unwrap();
            n_vec.reverse();
            n_vec.resize(32, 0);

            buffer.push(n_vec);
        }
        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn check_multipoint_g1_sum(n: usize, rnd: &mut RAND) {
        let mut res3 = ECP::new();

        let mut points: Vec<(u8, ECP)> = vec![];
        for i in 0..n {
            points.push((rnd.getbyte() % 2, get_random_g1_curve_point(rnd)));

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

    fn check_multipoint_g2_sum(n: usize, rnd: &mut RAND) {
        let mut res3 = ECP2::new();

        let mut points: Vec<(u8, ECP2)> = vec![];
        for i in 0..n {
            points.push((rnd.getbyte() % 2, get_random_g2_curve_point(rnd)));

            let mut current_point = points[i].1.clone();
            if points[i].0 == 1 {
                current_point.neg();
            }

            res3.add(&current_point);
        }

        let res1 = get_g2_sum_many_points(&points);

        points.shuffle(&mut thread_rng());
        let res2 = get_g2_sum_many_points(&points);
        assert_eq!(res1, res2);

        assert_eq!(res1, serialize_uncompressed_g2(&res3).to_vec());
    }

    //==== TESTS FOR G1_SUM

    #[test]
    fn test_bls12381_p1_sum_edge_cases() {
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

            let got = get_g1_sum(0, &p_ser, 0, &zero, &mut logic);
            assert_eq!(p_ser.to_vec(), got);
        }

        // P + (-P) = (-P) + P =  0
        for _ in 0..10 {
            let mut p = get_random_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g1(&p);

            let got = get_g1_sum(0, &p_neg_ser, 0, &p_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);

            let got = get_g1_sum(0, &p_ser, 0, &p_neg_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);
        }


        // P + P&mut
        for _ in 0..10 {
            let p = get_random_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let pmul2 = p.mul(&Big::from_bytes(&[2]));
            let pmul2_ser = serialize_uncompressed_g1(&pmul2);

            let got = get_g1_sum(0, &p_ser, 0, &p_ser, &mut logic);
            assert_eq!(pmul2_ser.to_vec(), got);
        }

        // P + (-(P + P))
        for _ in 0..10 {
            let mut p = get_random_g1_curve_point(&mut rnd);
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
    fn test_bls12381_p1_sum() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        for _ in 0..100 {
            let mut p = get_random_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            let q = get_random_g1_curve_point(&mut rnd);
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
    fn test_bls12381_p1_sum_not_g1_points() {
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
    fn test_bls12381_p1_sum_inverse() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        let mut zero: [u8; 96] = [0; 96];
        zero[0] = 64;

        for _ in 0..10 {
            let p = get_random_g1_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g1(&p);

            // P - P = - P + P = 0
            let got1 = get_g1_sum(1, &p_ser, 0, &p_ser, &mut logic);
            let got2 = get_g1_sum(0, &p_ser, 1, &p_ser, &mut logic);
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
            let mut p = get_random_g1_curve_point(&mut rnd);
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
    fn test_bls12381_p1_sum_many_points() {
        let mut rnd = get_rnd();

        let mut zero: [u8; 96] = [0; 96];
        zero[0] = 64;

        //empty input
        let res = get_g1_sum_many_points(&vec![]);
        assert_eq!(zero.to_vec(), res);

        const MAX_N: usize = 676;

        for _ in 0..100 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;
            check_multipoint_g1_sum(n, &mut rnd);
        }

        check_multipoint_g1_sum(MAX_N - 1, &mut rnd);
        check_multipoint_g1_sum(1, &mut rnd);

        for _ in 0..10 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;
            let mut points: Vec<(u8, ECP)> = vec![];
            for _ in 0..n {
                points.push((rnd.getbyte() % 2, get_random_g1_point(&mut rnd)));
            }

            let res1 = get_g1_sum_many_points(&points);
            let sum = deserialize_g1(&res1).unwrap();

            assert!(subgroup_check_g1(&sum));
        }
    }

    #[test]
    fn test_bls12381_p1_crosscheck_sum_and_multiexp() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 500;

        for _ in 0..10 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;

            let mut points: Vec<(u8, ECP)> = vec![];
            for _ in 0..n {
                points.push((rnd.getbyte() % 2, get_random_g1_point(&mut rnd)));
            }

            let res1 = get_g1_sum_many_points(&points);
            let res2 = get_g1_multiexp_many_points(&points);
            assert_eq!(res1, res2);
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p1_sum_incorrect_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 96];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_p1_sum_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // Incorrect sign encoding
        let mut buffer = vec![0u8; 97];
        buffer[0] = 2;

        let input = logic.internal_mem_write(buffer.as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 96];
        zero[0] = 64;
        zero[95] = 1;

        let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 96];
        zero[0] = 192;

        let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] |= 0x80;

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;


        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_uncompressed_g1(&p);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    // Input is beyond memory bounds.
    #[test]
    #[should_panic]
    fn test_bls12381_p1_sum_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 97 * 676];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
    }


    //==== TESTS FOR G2_SUM
    #[test]
    fn test_bls12381_p2_sum_edge_cases() {
        const POINT_LEN: usize = 192;

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // 0 + 0
        let mut zero: [u8; POINT_LEN] = [0; POINT_LEN];
        zero[0] = 64;
        let got = get_g2_sum(0, &zero, 0, &zero, &mut logic);
        assert_eq!(zero.to_vec(), got);


        // 0 + P = P + 0 = P
        let mut rnd = get_rnd();
        for _ in 0..10 {
            let p = get_random_g2_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let got = get_g2_sum(0, &zero, 0, &p_ser, &mut logic);
            assert_eq!(p_ser.to_vec(), got);

            let got = get_g2_sum(0, &p_ser, 0, &zero, &mut logic);
            assert_eq!(p_ser.to_vec(), got);
        }

        // P + (-P) = (-P) + P =  0
        for _ in 0..10 {
            let mut p = get_random_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            p.neg();
            let p_neg_ser = serialize_uncompressed_g2(&p);

            let got = get_g2_sum(0, &p_neg_ser, 0, &p_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);

            let got = get_g2_sum(0, &p_ser, 0, &p_neg_ser, &mut logic);
            assert_eq!(zero.to_vec(), got);
        }


        // P + P&mutg1
        for _ in 0..10 {
            let p = get_random_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let pmul2 = p.mul(&Big::from_bytes(&[2]));
            let pmul2_ser = serialize_uncompressed_g2(&pmul2);

            let got = get_g2_sum(0, &p_ser, 0, &p_ser, &mut logic);
            assert_eq!(pmul2_ser.to_vec(), got);
        }

        // P + (-(P + P))
        for _ in 0..10 {
            let mut p = get_random_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let mut pmul2 = p.mul(&Big::from_bytes(&[2]));
            pmul2.neg();
            let pmul2_neg_ser = serialize_uncompressed_g2(&pmul2);

            p.neg();

            let p_neg_ser = serialize_uncompressed_g2(&p);
            let got = get_g2_sum(0, &p_ser, 0, &pmul2_neg_ser, &mut logic);
            assert_eq!(p_neg_ser.to_vec(), got);
        }
    }

    #[test]
    fn test_bls12381_p2_sum() {
        for _ in 0..100 {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let mut rnd = get_rnd();

            let mut p = get_random_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let q = get_random_g2_curve_point(&mut rnd);
            let q_ser = serialize_uncompressed_g2(&q);

            // P + Q = Q + P
            let got1 = get_g2_sum(0, &p_ser, 0, &q_ser, &mut logic);
            let got2 = get_g2_sum(0, &q_ser, 0, &p_ser, &mut logic);
            assert_eq!(got1, got2);

            // compare with library results
            p.add(&q);
            let library_sum = serialize_uncompressed_g2(&p);

            assert_eq!(library_sum.to_vec(), got1);
        }

        // generate points from G2
        for _ in 0..100 {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let mut rnd = get_rnd();

            let p = get_random_g2_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let q = get_random_g2_point(&mut rnd);
            let q_ser = serialize_uncompressed_g2(&q);

            let got1 = get_g2_sum(0, &p_ser, 0, &q_ser, &mut logic);

            let result_point = deserialize_g2(&got1).unwrap();
            assert!(subgroup_check_g2(&result_point));
        }
    }

    #[test]
    fn test_bls12381_p2_sum_not_g2_points() {
        //points not from G2
        for _ in 0..100 {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let mut rnd = get_rnd();

            let mut p = get_random_not_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let q = get_random_not_g2_curve_point(&mut rnd);
            let q_ser = serialize_uncompressed_g2(&q);

            // P + Q = Q + P
            let got1 = get_g2_sum(0, &p_ser, 0, &q_ser, &mut logic);
            let got2 = get_g2_sum(0, &q_ser, 0, &p_ser, &mut logic);
            assert_eq!(got1, got2);

            // compare with library results
            p.add(&q);
            let library_sum = serialize_uncompressed_g2(&p);

            assert_eq!(library_sum.to_vec(), got1);
        }
    }

    #[test]
    fn test_bls12381_p2_sum_inverse() {
        const POINT_LEN: usize = 192;

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut rnd = get_rnd();

        let mut zero: [u8; POINT_LEN] = [0; POINT_LEN];
        zero[0] |= 0x40;

        for _ in 0..10 {
            let p = get_random_g2_curve_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            // P - P = - P + P = 0
            let got1 = get_g2_sum(1, &p_ser, 0, &p_ser, &mut logic);
            let got2 = get_g2_sum(0, &p_ser, 1, &p_ser, &mut logic);
            assert_eq!(got1, got2);
            assert_eq!(got1, zero.to_vec());

            // -(-P)
            let p_inv = get_g2_inverse(&p_ser, &mut logic);
            let p_inv_inv = get_g2_inverse(p_inv.as_slice(), &mut logic);

            assert_eq!(p_ser.to_vec(), p_inv_inv);
        }

        // P in G2 => -P in G2
        for _ in 0..10 {
            let p = get_random_g2_point(&mut rnd);
            let p_ser = serialize_uncompressed_g2(&p);

            let p_inv = get_g2_inverse(&p_ser, &mut logic);

            let result_point = deserialize_g2(&p_inv).unwrap();
            assert!(subgroup_check_g2(&result_point));
        }
    }

    #[test]
    fn test_bls12381_p2_sum_many_points() {
        const POINT_LEN: usize = 192;

        let mut rnd = get_rnd();

        let mut zero: [u8; POINT_LEN] = [0; POINT_LEN];
        zero[0] = 64;

        //empty input
        let res = get_g2_sum_many_points(&vec![]);
        assert_eq!(zero.to_vec(), res);

        const MAX_N: usize = 338;

        for _ in 0..100 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;
            check_multipoint_g2_sum(n, &mut rnd);
        }

        check_multipoint_g2_sum(MAX_N - 1, &mut rnd);
        check_multipoint_g2_sum(1, &mut rnd);

        for _ in 0..10 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;
            let mut points: Vec<(u8, ECP2)> = vec![];
            for _ in 0..n {
                points.push((rnd.getbyte() % 2, get_random_g2_point(&mut rnd)));
            }

            let res1 = get_g2_sum_many_points(&points);
            let sum = deserialize_g2(&res1).unwrap();

            assert!(subgroup_check_g2(&sum));
        }
    }

    #[test]
    fn test_bls12381_p2_crosscheck_sum_and_multiexp() {
        let mut rnd = get_rnd();

        for n in 0..10 {
            let mut points: Vec<(u8, ECP2)> = vec![];
            for _ in 0..n {
                points.push((rnd.getbyte() % 2, get_random_g2_point(&mut rnd)));
            }

            let res1 = get_g2_sum_many_points(&points);
            let res2 = get_g2_multiexp_many_points(&points);
            assert_eq!(res1, res2);
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p2_sum_incorrect_length() {
        const POINT_LEN: usize = 192;

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; POINT_LEN];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_p2_sum_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // Incorrect sign encoding
        let mut buffer = vec![0u8; 193];
        buffer[0] = 2;

        let input = logic.internal_mem_write(buffer.as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 192];
        zero[0] = 64;
        zero[191] = 1;

        let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 192];
        zero[0] = 192;

        let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[0] ^= 0x80;

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[191] ^= 0x01;


        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[0] ^= 0x20;

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut yabig = p.gety().geta();
        yabig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_uncompressed_g2(&p);
        yabig.to_byte_array(&mut p_ser[0..192], 96 + 48);

        let input = logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    // Input is beyond memory bounds.
    #[test]
    #[should_panic]
    fn test_bls12381_p2_sum_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 193 * 339];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
    }

    // Tests for P1 multiplication
    #[test]
    fn test_bls12381_p1_multiexp_mul() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p = get_random_g1_curve_point(&mut rnd);
            let n = rnd.getbyte();

            let points: Vec<(u8, ECP)> = vec![(0, p.clone()); n as usize];
            let res1 = get_g1_sum_many_points(&points);
            let res2 = get_g1_multiexp_small(&vec![(n, p.clone())]);

            assert_eq!(res1, res2);

            let res3 = p.mul(&Big::new_int(n as isize));

            assert_eq!(res1, serialize_uncompressed_g1(&res3));
        }

        for _ in 0..100 {
            let p = get_random_g1_curve_point(&mut rnd);
            let mut n = Big::random(&mut rnd);
            n.mod2m(32*8);

            let res1 = get_g1_multiexp(&vec![(n.clone(), p.clone())]);
            let res2 = p.mul(&n);

            assert_eq!(res1, serialize_uncompressed_g1(&res2));
        }
    }

    #[test]
    fn test_bls12381_p1_multiexp_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 500;

        for _ in 0..10 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;

            let mut res2 = ECP::new();

            let mut points: Vec<(Big, ECP)> = vec![];
            for i in 0..n {
                let mut scalar = Big::random(&mut rnd);
                scalar.mod2m(32*8);
                points.push((scalar, get_random_g1_curve_point(&mut rnd)));
                res2.add(&points[i].1.mul(&points[i].0));
            }

            let res1 = get_g1_multiexp(&points);
            assert_eq!(res1, serialize_uncompressed_g1(&res2));
        }
    }

    #[test]
    fn test_bls12381_p1_multiexp_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let zero_scalar = vec![0u8; 32];

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 96];
        zero[0] = 64;
        zero[95] = 1;

        let input = logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 96];
        zero[0] = 192;

        let input = logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] |= 0x80;

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;


        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_uncompressed_g1(&p);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p1_multiexp_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 129].as_slice());
        logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
    }

    // Tests for P2 multiplication
    #[test]
    fn test_bls12381_p2_multiexp_mul() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p = get_random_g2_curve_point(&mut rnd);
            let n = rnd.getbyte();

            let points: Vec<(u8, ECP2)> = vec![(0, p.clone()); n as usize];
            let res1 = get_g2_sum_many_points(&points);
            let res2 = get_g2_multiexp_small(&vec![(n, p.clone())]);

            assert_eq!(res1, res2);

            let res3 = p.mul(&Big::new_int(n as isize));

            assert_eq!(res1, serialize_uncompressed_g2(&res3));
        }

        for _ in 0..100 {
            let p = get_random_g2_curve_point(&mut rnd);
            let mut n = Big::random(&mut rnd);
            n.mod2m(32*8);

            let res1 = get_g2_multiexp(&vec![(n.clone(), p.clone())]);
            let res2 = p.mul(&n);

            assert_eq!(res1, serialize_uncompressed_g2(&res2));
        }
    }

    #[test]
    fn test_bls12381_p2_multiexp_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 250;

        for _ in 0..10 {
            let n: usize = (thread_rng().next_u32() as usize) % MAX_N;

            let mut res2 = ECP2::new();

            let mut points: Vec<(Big, ECP2)> = vec![];
            for i in 0..n {
                let mut scalar = Big::random(&mut rnd);
                scalar.mod2m(32*8);
                points.push((scalar, get_random_g2_curve_point(&mut rnd)));
                res2.add(&points[i].1.mul(&points[i].0));
            }

            let res1 = get_g2_multiexp(&points);
            assert_eq!(res1, serialize_uncompressed_g2(&res2));
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p2_multiexp_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 225].as_slice());
        logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_p2_multiexp_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let zero_scalar = vec![0u8; 32];

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 192];
        zero[0] = 64;
        zero[191] = 1;

        let input = logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 192];
        zero[0] = 192;

        let input = logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[0] ^= 0x80;

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[191] ^= 0x01;


        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[0] ^= 0x20;

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut yabig = p.gety().geta();
        yabig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_uncompressed_g2(&p);
        yabig.to_byte_array(&mut p_ser[0..192], 96 + 48);

        let input = logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    fn test_bls12381_map_fp_to_g1() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let fp = get_random_fp(&mut rnd);
            let res1 = map_fp_to_g1(fp.clone());

            let mut res2 = map_to_curve_g1(fp);
            res2 = res2.mul(&Big::new_ints(&H_EFF_G1));

            assert_eq!(res1, serialize_uncompressed_g1(&res2));
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_map_fp_to_g1_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 49].as_slice());
        logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_map_fp_to_g1_incorrect_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let p = hex::decode("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab").unwrap();

        let input = logic.internal_mem_write(p.as_slice());
        let res = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);
    }

    #[test]
    fn test_bls12381_map_fp2_to_g2() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let fp2 = get_random_fp2(&mut rnd);
            let res1 = map_fp2_to_g2(fp2.clone());

            let mut res2 = map_to_curve_g2(fp2);
            res2.clear_cofactor();

            assert_eq!(res1, serialize_uncompressed_g2(&res2));
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_map_fp2_to_g2_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 97].as_slice());
        logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_map_fp2_to_g2_incorrect_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let p = hex::decode("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab").unwrap();

        let input = logic.internal_mem_write(vec![p.clone(), vec![0u8; 48]].concat().as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);

        let input = logic.internal_mem_write(vec![vec![0u8; 48], p.clone()].concat().as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);
    }


    #[test]
    fn test_bls12381_p1_decompress() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p1 = get_random_g1_curve_point(&mut rnd);
            let res1 = decompress_p1(p1.clone());

            assert_eq!(res1, serialize_uncompressed_g1(&p1));
        }
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p1_decompress_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 49].as_slice());
        logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_p1_decompress_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 48];
        zero[0] = 0x80 | 0x40;
        zero[47] = 1;

        let input = logic.internal_mem_write(zero.as_slice());
        let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 48];
        zero[0] = 0x40;

        let input = logic.internal_mem_write(zero.as_slice());
        let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_g1(&p);
        p_ser[0] ^= 0x80;

        let input = logic.internal_mem_write(p_ser.as_slice());
        let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Point with a coordinate larger than 'p'.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut xbig = p.getx();
        xbig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_g1(&p);
        xbig.to_byte_array(&mut p_ser[0..48], 0);
        p_ser[0] |= 0x80;

        let input = logic.internal_mem_write(p_ser.as_slice());
        let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    fn test_bls12381_p2_decompress() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p2 = get_random_g2_curve_point(&mut rnd);
            let res1 = decompress_p2(p2.clone());

            assert_eq!(res1, serialize_uncompressed_g2(&p2));
        }
    }

    #[test]
    fn test_bls12381_p2_decompress_incorrect_input() {
        let mut rnd = get_rnd();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 96];
        zero[0] = 0x80 | 0x40;
        zero[95] = 1;

        let input = logic.internal_mem_write(zero.as_slice());
        let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 96];
        zero[0] = 0x40;

        let input = logic.internal_mem_write(zero.as_slice());
        let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_g2(&p);
        p_ser[0] ^= 0x80;

        let input = logic.internal_mem_write(p_ser.as_slice());
        let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Point with a coordinate larger than 'p'.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut xabig = p.getx().geta();
        xabig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_g2(&p);
        xabig.to_byte_array(&mut p_ser[0..96], 48);

        let input = logic.internal_mem_write(p_ser.as_slice());
        let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p2_decompress_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 97].as_slice());
        logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_pairing_check_one_point() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p1 = get_random_g1_point(&mut rnd);
            let p2 = get_random_g2_point(&mut rnd);

            let zero1 = ECP::new();
            let zero2 = ECP2::new();

            let mut r = pair::initmp();
            pair::another(&mut r, &zero2, &p1);
            let mut v = pair::miller(&r);

            v = pair::fexp(&v);
            assert!(v.is_unity());

            assert_eq!(pairing_check(zero1.clone(), zero2.clone()), 0);
            assert_eq!(pairing_check(zero1.clone(), p2.clone()), 0);
            assert_eq!(pairing_check(p1.clone(), zero2.clone()), 0);
            assert_eq!(pairing_check(p1.clone(), p2.clone()), 2);
        }
    }

    #[test]
    fn test_bls12381_pairing_incorrect_input_point() {
        let mut rnd = get_rnd();

        let p1_not_from_g1 = get_random_not_g1_curve_point(&mut rnd);
        let p2 = get_random_g2_point(&mut rnd);

        let p1 = get_random_g1_point(&mut rnd);
        let p2_not_from_g2 = get_random_not_g2_curve_point(&mut rnd);

        assert_eq!(pairing_check(p1_not_from_g1.clone(), p2.clone()), 1);
        assert_eq!(pairing_check(p1.clone(), p2_not_from_g2.clone()), 1);

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 96];
        zero[0] = 64;
        zero[95] = 1;
        assert_eq!(pairing_check_vec(zero.clone(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 96];
        zero[0] = 192;
        assert_eq!(pairing_check_vec(zero.clone(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] |= 0x80;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // Point not on the curve
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab".to_string()));
        let mut p_ser = serialize_uncompressed_g1(&p);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);
    }

    #[test]
    #[should_panic]
    fn test_bls12381_pairing_check_incorrect_input_length() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let input = logic.internal_mem_write(vec![0u8; 289].as_slice());
        logic.bls12381_pairing_check(input.len, input.ptr).unwrap();
    }
}

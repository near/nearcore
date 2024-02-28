mod tests {
    use crate::logic::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
    use crate::logic::MemSlice;
    use amcl::bls381::big::Big;
    use amcl::bls381::bls381::core::deserialize_g1;
    use amcl::bls381::bls381::core::deserialize_g2;
    use amcl::bls381::bls381::core::map_to_curve_g1;
    use amcl::bls381::bls381::core::map_to_curve_g2;
    use amcl::bls381::bls381::utils::{
        serialize_g1, serialize_g2, serialize_uncompressed_g1, serialize_uncompressed_g2,
        subgroup_check_g1, subgroup_check_g2,
    };
    use amcl::bls381::ecp::ECP;
    use amcl::bls381::ecp2::ECP2;
    use amcl::bls381::fp::FP;
    use amcl::bls381::fp2::FP2;
    use amcl::bls381::pair;
    use amcl::bls381::rom::H_EFF_G1;
    use amcl::rand::RAND;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use rand::RngCore;
    use std::fs;

    const P: &str = "1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab";
    const R: &str = "73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001";

    struct G1Operations;
    struct G2Operations;

    impl G1Operations {
        fn get_random_g_point(rnd: &mut RAND) -> ECP {
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

        fn get_random_not_g_curve_point(rnd: &mut RAND) -> ECP {
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

        fn decompress_p(p1: Vec<ECP>) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let mut p1s_vec: Vec<Vec<u8>> = vec![vec![]];
            for i in 0..p1.len() {
                p1s_vec.push(serialize_g1(&p1[i]).to_vec());
            }

            let input = logic.internal_mem_write(p1s_vec.concat().as_slice());
            let res = logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }

        fn get_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8]) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

            let input = logic.internal_mem_write(buffer.concat().as_slice());
            let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }

        fn get_inverse(p: &[u8]) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let buffer = vec![vec![1], p.to_vec()];

            let input = logic.internal_mem_write(buffer.concat().as_slice());
            let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }
    }

    impl G2Operations {
        fn get_random_g_point(rnd: &mut RAND) -> ECP2 {
            let r: Big = Big::random(rnd);
            let g: ECP2 = ECP2::generator();

            g.mul(&r)
        }

        fn get_random_curve_point(rnd: &mut RAND) -> ECP2 {
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

        fn get_random_not_g_curve_point(rnd: &mut RAND) -> ECP2 {
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

        fn decompress_p(p2: Vec<ECP2>) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let mut p2s_vec: Vec<Vec<u8>> = vec![vec![]];
            for i in 0..p2.len() {
                p2s_vec.push(serialize_g2(&p2[i]).to_vec());
            }

            let input = logic.internal_mem_write(p2s_vec.concat().as_slice());
            let res = logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }

        fn get_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8]) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

            let input = logic.internal_mem_write(buffer.concat().as_slice());
            let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }

        fn get_inverse(p: &[u8]) -> Vec<u8> {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let buffer = vec![vec![1], p.to_vec()];

            let input = logic.internal_mem_write(buffer.concat().as_slice());
            let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }
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
        for i in 0..100 {
            raw[i] = i as u8
        }

        rnd.seed(100, &raw);

        rnd
    }

    fn get_zero(point_len: usize) -> Vec<u8> {
        let mut zero1 = vec![0; point_len];
        zero1[0] |= 0x40;
        zero1
    }

    fn map_fp_to_g1(fps: Vec<FP>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut fp_vec: Vec<Vec<u8>> = vec![vec![]];

        for i in 0..fps.len() {
            let mut fp_slice: [u8; 48] = [0u8; 48];
            fps[i].redc().to_byte_array(&mut fp_slice, 0);

            fp_vec.push(fp_slice.to_vec());
        }

        let input = logic.internal_mem_write(fp_vec.concat().as_slice());
        let res = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn map_fp2_to_g2(fp2: Vec<FP2>) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut fp2_vec: Vec<Vec<u8>> = vec![vec![]];

        for i in 0..fp2.len() {
            let mut fp2_res: [u8; 96] = [0u8; 96];
            fp2[i].getb().to_byte_array(&mut fp2_res, 0);
            fp2[i].geta().to_byte_array(&mut fp2_res, 48);

            fp2_vec.push(fp2_res.to_vec());
        }

        let input = logic.internal_mem_write(fp2_vec.concat().as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn pairing_check(p1s: Vec<ECP>, p2s: Vec<ECP2>) -> u64 {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..p1s.len() {
            buffer.push(serialize_uncompressed_g1(&p1s[i]).to_vec());
            buffer.push(serialize_uncompressed_g2(&p2s[i]).to_vec());
        }

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
                buffer.push(
                    hex::decode("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000000")
                        .unwrap()
                        .into_iter()
                        .rev()
                        .collect(),
                );
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
                buffer.push(
                    hex::decode("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000000")
                        .unwrap()
                        .into_iter()
                        .rev()
                        .collect(),
                );
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
            buffer.push(n_vec.to_vec());
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
            buffer.push(n_vec.to_vec());
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

            let mut n_vec = n_vec.to_vec();
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

            let mut n_vec = n_vec.to_vec();
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
            points.push((rnd.getbyte() % 2, G1Operations::get_random_curve_point(rnd)));

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
            points.push((rnd.getbyte() % 2, G2Operations::get_random_curve_point(rnd)));

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

    macro_rules! test_bls12381_sum {
        (
            $GOperations:ident,
            $POINT_LEN:expr,
            $serialize_uncompressed:ident,
            $deserialize:ident,
            $subgroup_check:ident,
            $MAX_N:expr,
            $get_g_sum_many_points:ident,
            $check_multipoint_g_sum:ident,
            $point_type:ident,
            $get_g_multiexp_many_points:ident,
            $MAX_N_MULTIEXP:expr,
            $bls12381_sum:ident,
            $test_bls12381_sum_edge_cases:ident,
            $test_bls12381_sum:ident,
            $test_bls12381_sum_not_g_points:ident,
            $test_bls12381_sum_inverse:ident,
            $test_bls12381_sum_many_points:ident,
            $test_bls12381_crosscheck_sum_and_multiexp:ident,
            $test_bls12381_sum_incorrect_input:ident
        ) => {
            #[test]
            fn $test_bls12381_sum_edge_cases() {
                // 0 + 0
                let mut zero: [u8; $POINT_LEN] = [0; $POINT_LEN];
                zero[0] = 64;
                let got = $GOperations::get_sum(0, &zero, 0, &zero);
                assert_eq!(zero.to_vec(), got);

                // 0 + P = P + 0 = P
                let mut rnd = get_rnd();
                for _ in 0..10 {
                    let p = $GOperations::get_random_g_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let got = $GOperations::get_sum(0, &zero, 0, &p_ser);
                    assert_eq!(p_ser.to_vec(), got);

                    let got = $GOperations::get_sum(0, &p_ser, 0, &zero);
                    assert_eq!(p_ser.to_vec(), got);
                }

                // P + (-P) = (-P) + P =  0
                for _ in 0..10 {
                    let mut p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed(&p);

                    let got = $GOperations::get_sum(0, &p_neg_ser, 0, &p_ser);
                    assert_eq!(zero.to_vec(), got);

                    let got = $GOperations::get_sum(0, &p_ser, 0, &p_neg_ser);
                    assert_eq!(zero.to_vec(), got);
                }

                // P + P
                for _ in 0..10 {
                    let p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let pmul2 = p.mul(&Big::from_bytes(&[2]));
                    let pmul2_ser = $serialize_uncompressed(&pmul2);

                    let got = $GOperations::get_sum(0, &p_ser, 0, &p_ser);
                    assert_eq!(pmul2_ser.to_vec(), got);
                }

                // P + (-(P + P))
                for _ in 0..10 {
                    let mut p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let mut pmul2 = p.mul(&Big::from_bytes(&[2]));
                    pmul2.neg();
                    let pmul2_neg_ser = $serialize_uncompressed(&pmul2);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed(&p);
                    let got = $GOperations::get_sum(0, &p_ser, 0, &pmul2_neg_ser);
                    assert_eq!(p_neg_ser.to_vec(), got);
                }
            }

            #[test]
            fn $test_bls12381_sum() {
                let mut rnd = get_rnd();

                for _ in 0..100 {
                    let mut p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let q = $GOperations::get_random_curve_point(&mut rnd);
                    let q_ser = $serialize_uncompressed(&q);

                    // P + Q = Q + P
                    let got1 = $GOperations::get_sum(0, &p_ser, 0, &q_ser);
                    let got2 = $GOperations::get_sum(0, &q_ser, 0, &p_ser);
                    assert_eq!(got1, got2);

                    // compare with library results
                    p.add(&q);
                    let library_sum = $serialize_uncompressed(&p);

                    assert_eq!(library_sum.to_vec(), got1);
                }

                for _ in 0..100 {
                    let p = $GOperations::get_random_g_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let q = $GOperations::get_random_g_point(&mut rnd);
                    let q_ser = $serialize_uncompressed(&q);

                    let got1 = $GOperations::get_sum(0, &p_ser, 0, &q_ser);

                    let result_point = $deserialize(&got1).unwrap();
                    assert!($subgroup_check(&result_point));
                }
            }

            #[test]
            fn $test_bls12381_sum_not_g_points() {
                let mut rnd = get_rnd();

                //points not from G
                for _ in 0..100 {
                    let mut p = $GOperations::get_random_not_g_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let q = $GOperations::get_random_not_g_curve_point(&mut rnd);
                    let q_ser = $serialize_uncompressed(&q);

                    // P + Q = Q + P
                    let got1 = $GOperations::get_sum(0, &p_ser, 0, &q_ser);
                    let got2 = $GOperations::get_sum(0, &q_ser, 0, &p_ser);
                    assert_eq!(got1, got2);

                    // compare with library results
                    p.add(&q);
                    let library_sum = $serialize_uncompressed(&p);

                    assert_eq!(library_sum.to_vec(), got1);
                }
            }

            #[test]
            fn $test_bls12381_sum_inverse() {
                let mut rnd = get_rnd();

                let mut zero: [u8; $POINT_LEN] = [0; $POINT_LEN];
                zero[0] = 64;

                for _ in 0..10 {
                    let p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    // P - P = - P + P = 0
                    let got1 = $GOperations::get_sum(1, &p_ser, 0, &p_ser);
                    let got2 = $GOperations::get_sum(0, &p_ser, 1, &p_ser);
                    assert_eq!(got1, got2);
                    assert_eq!(got1, zero.to_vec());

                    // -(-P)
                    let p_inv = $GOperations::get_inverse(&p_ser);
                    let p_inv_inv = $GOperations::get_inverse(p_inv.as_slice());

                    assert_eq!(p_ser.to_vec(), p_inv_inv);
                }

                // P in G => -P in G
                for _ in 0..10 {
                    let p = $GOperations::get_random_g_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $GOperations::get_inverse(&p_ser);

                    let result_point = $deserialize(&p_inv).unwrap();
                    assert!($subgroup_check(&result_point));
                }

                // Random point check with library
                for _ in 0..10 {
                    let mut p = $GOperations::get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $GOperations::get_inverse(&p_ser);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed(&p);

                    assert_eq!(p_neg_ser.to_vec(), p_inv);
                }

                // Not from G points
                for _ in 0..10 {
                    let mut p = $GOperations::get_random_not_g_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $GOperations::get_inverse(&p_ser);

                    p.neg();

                    let p_neg_ser = $serialize_uncompressed(&p);

                    assert_eq!(p_neg_ser.to_vec(), p_inv);
                }

                // -0
                let zero_inv = $GOperations::get_inverse(&zero);
                assert_eq!(zero.to_vec(), zero_inv);
            }

            #[test]
            fn $test_bls12381_sum_many_points() {
                let mut rnd = get_rnd();

                let mut zero: [u8; $POINT_LEN] = [0; $POINT_LEN];
                zero[0] = 64;

                //empty input
                let res = $get_g_sum_many_points(&vec![]);
                assert_eq!(zero.to_vec(), res);

                for _ in 0..100 {
                    let n: usize = (thread_rng().next_u32() as usize) % $MAX_N;
                    $check_multipoint_g_sum(n, &mut rnd);
                }

                $check_multipoint_g_sum($MAX_N - 1, &mut rnd);
                $check_multipoint_g_sum(1, &mut rnd);

                for _ in 0..10 {
                    let n: usize = (thread_rng().next_u32() as usize) % $MAX_N;
                    let mut points: Vec<(u8, $point_type)> = vec![];
                    for _ in 0..n {
                        points
                            .push((rnd.getbyte() % 2, $GOperations::get_random_g_point(&mut rnd)));
                    }

                    let res1 = $get_g_sum_many_points(&points);
                    let sum = $deserialize(&res1).unwrap();

                    assert!($subgroup_check(&sum));
                }
            }

            #[test]
            fn $test_bls12381_crosscheck_sum_and_multiexp() {
                let mut rnd = get_rnd();

                for _ in 0..10 {
                    let n: usize = (thread_rng().next_u32() as usize) % $MAX_N_MULTIEXP;

                    let mut points: Vec<(u8, $point_type)> = vec![];
                    for _ in 0..n {
                        points
                            .push((rnd.getbyte() % 2, $GOperations::get_random_g_point(&mut rnd)));
                    }

                    let res1 = $get_g_sum_many_points(&points);
                    let res2 = $get_g_multiexp_many_points(&points);
                    assert_eq!(res1, res2);
                }
            }

            #[test]
            fn $test_bls12381_sum_incorrect_input() {
                let mut rnd = get_rnd();

                let mut logic_builder = VMLogicBuilder::default();
                let mut logic = logic_builder.build();

                // Incorrect sign encoding
                let mut buffer = vec![0u8; $POINT_LEN + 1];
                buffer[0] = 2;

                let input = logic.internal_mem_write(buffer.as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Incorrect encoding of the point at infinity
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 64;
                zero[$POINT_LEN - 1] = 1;

                let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 192;

                let input = logic.internal_mem_write(vec![vec![0], zero].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[0] |= 0x80;

                let input =
                    logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Point not on the curve
                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[95] ^= 0x01;

                let input =
                    logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[0] ^= 0x20;

                let input =
                    logic.internal_mem_write(vec![vec![0], p_ser.to_vec()].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);
            }
        };
    }

    test_bls12381_sum!(
        G1Operations,
        96,
        serialize_uncompressed_g1,
        deserialize_g1,
        subgroup_check_g1,
        676,
        get_g1_sum_many_points,
        check_multipoint_g1_sum,
        ECP,
        get_g1_multiexp_many_points,
        500,
        bls12381_p1_sum,
        test_bls12381_p1_sum_edge_cases,
        test_bls12381_p1_sum,
        test_bls12381_p1_sum_not_g1_points,
        test_bls12381_p1_sum_inverse,
        test_bls12381_p1_sum_many_points,
        test_bls12381_p1_crosscheck_sum_and_multiexp,
        test_bls12381_p1_sum_incorrect_input
    );
    test_bls12381_sum!(
        G2Operations,
        192,
        serialize_uncompressed_g2,
        deserialize_g2,
        subgroup_check_g2,
        338,
        get_g2_sum_many_points,
        check_multipoint_g2_sum,
        ECP2,
        get_g2_multiexp_many_points,
        250,
        bls12381_p2_sum,
        test_bls12381_p2_sum_edge_cases,
        test_bls12381_p2_sum,
        test_bls12381_p2_sum_not_g2_points,
        test_bls12381_p2_sum_inverse,
        test_bls12381_p2_sum_many_points,
        test_bls12381_p2_crosscheck_sum_and_multiexp,
        test_bls12381_p2_sum_incorrect_input
    );

    macro_rules! test_bls12381_memory_limit {
        (
            $namespace_name:ident,
            $INPUT_SIZE:expr,
            $MAX_N:expr,
            $run_bls_fn:ident
        ) => {
            mod $namespace_name {
                use crate::logic::tests::bls12381::tests::$run_bls_fn;
                use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

                // Input is beyond memory bounds.
                #[test]
                #[should_panic]
                fn test_bls12381_too_big_input() {
                    let mut logic_builder = VMLogicBuilder::default();
                    let mut logic = logic_builder.build();

                    let buffer = vec![0u8; $INPUT_SIZE * $MAX_N];

                    let input = logic.internal_mem_write(buffer.as_slice());
                    $run_bls_fn(input, &mut logic);
                }

                #[test]
                #[should_panic]
                fn test_bls12381_incorrect_length() {
                    let mut logic_builder = VMLogicBuilder::default();
                    let mut logic = logic_builder.build();

                    let buffer = vec![0u8; $INPUT_SIZE - 1];

                    let input = logic.internal_mem_write(buffer.as_slice());
                    $run_bls_fn(input, &mut logic);
                }
            }
        };
    }

    test_bls12381_memory_limit!(memory_limit_p1_sum, 97, 676, sum_g1_return_value);
    test_bls12381_memory_limit!(memory_limit_p2_sum, 193, 340, sum_g2_return_value);
    test_bls12381_memory_limit!(memory_limit_p1_multiexp, 128, 600, multiexp_g1_return_value);
    test_bls12381_memory_limit!(memory_limit_p2_multiexp, 224, 300, multiexp_g2_return_value);
    test_bls12381_memory_limit!(memory_limit_map_fp_to_g1, 48, 1500, map_fp_to_g1_return_value);
    test_bls12381_memory_limit!(memory_limit_map_fp2_to_g2, 96, 700, map_fp2tog2_return_value);
    test_bls12381_memory_limit!(memory_limit_p1_decompress, 48, 1500, decompress_g1_return_value);
    test_bls12381_memory_limit!(memory_limit_p2_decompress, 96, 700, decompress_g2_return_value);
    test_bls12381_memory_limit!(memory_limit_pairing_check, 288, 500, run_pairing_check_raw);

    macro_rules! test_bls12381_multiexp {
        (
            $GOperations:ident,
            $POINT_LEN:expr,
            $serialize_uncompressed:ident,
            $MAX_N:expr,
            $point_type:ident,
            $add_p_y:ident,
            $get_sum_many_points:ident,
            $get_multiexp_small:ident,
            $get_multiexp:ident,
            $bls12381_multiexp:ident,
            $bls12381_sum:ident,
            $test_bls12381_multiexp_mul:ident,
            $test_bls12381_multiexp_many_points: ident,
            $test_bls12381_multiexp_incorrect_input: ident,
            $test_bls12381_multiexp_invariants_checks: ident,
            $test_bls12381_error_encoding: ident
        ) => {
            #[test]
            fn $test_bls12381_multiexp_mul() {
                let mut rnd = get_rnd();

                for _ in 0..100 {
                    let p = $GOperations::get_random_curve_point(&mut rnd);
                    let n = rnd.getbyte();

                    let points: Vec<(u8, $point_type)> = vec![(0, p.clone()); n as usize];
                    let res1 = $get_sum_many_points(&points);
                    let res2 = $get_multiexp_small(&vec![(n, p.clone())]);

                    assert_eq!(res1, res2);

                    let res3 = p.mul(&Big::new_int(n as isize));

                    assert_eq!(res1, $serialize_uncompressed(&res3));
                }

                for _ in 0..100 {
                    let p = $GOperations::get_random_curve_point(&mut rnd);
                    let mut n = Big::random(&mut rnd);
                    n.mod2m(32 * 8);

                    let res1 = $get_multiexp(&vec![(n.clone(), p.clone())]);
                    let res2 = p.mul(&n);

                    assert_eq!(res1, $serialize_uncompressed(&res2));
                }
            }

            #[test]
            fn $test_bls12381_multiexp_many_points() {
                let mut rnd = get_rnd();

                for i in 0..10 {
                    let n: usize =
                        if i == 0 { $MAX_N } else { (thread_rng().next_u32() as usize) % $MAX_N };

                    let mut res2 = $point_type::new();

                    let mut points: Vec<(Big, $point_type)> = vec![];
                    for i in 0..n {
                        let mut scalar = Big::random(&mut rnd);
                        scalar.mod2m(32 * 8);
                        points.push((scalar, $GOperations::get_random_curve_point(&mut rnd)));
                        res2.add(&points[i].1.mul(&points[i].0));
                    }

                    let res1 = $get_multiexp(&points);
                    assert_eq!(res1, $serialize_uncompressed(&res2));
                }
            }

            #[test]
            fn $test_bls12381_multiexp_incorrect_input() {
                let mut rnd = get_rnd();

                let mut logic_builder = VMLogicBuilder::default();
                let mut logic = logic_builder.build();

                let zero_scalar = vec![0u8; 32];

                // Incorrect encoding of the point at infinity
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 64;
                zero[$POINT_LEN - 1] = 1;

                let input =
                    logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 192;

                let input =
                    logic.internal_mem_write(vec![zero, zero_scalar.clone()].concat().as_slice());
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[0] |= 0x80;

                let input = logic.internal_mem_write(
                    vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice(),
                );
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Point not on the curve
                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[$POINT_LEN - 1] ^= 0x01;

                let input = logic.internal_mem_write(
                    vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice(),
                );
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_uncompressed(&p);
                p_ser[0] ^= 0x20;

                let input = logic.internal_mem_write(
                    vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice(),
                );
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);
            }

            #[test]
            fn $test_bls12381_multiexp_invariants_checks() {
                let mut zero1: [u8; $POINT_LEN] = [0; $POINT_LEN];
                zero1[0] |= 0x40;

                let mut rnd = get_rnd();
                let r = Big::from_string(R.to_string());

                for _ in 0..10 {
                    let p = $GOperations::get_random_g_point(&mut rnd);

                    // group_order * P = 0
                    let res = $get_multiexp(&vec![(r.clone(), p.clone())]);
                    assert_eq!(res.as_slice(), zero1);

                    let mut scalar = Big::random(&mut rnd);
                    scalar.mod2m(32 * 7);

                    // (scalar + group_order) * P = scalar * P
                    let res1 = $get_multiexp(&vec![(scalar.clone(), p.clone())]);
                    scalar.add(&r);
                    let res2 = $get_multiexp(&vec![(scalar.clone(), p.clone())]);
                    assert_eq!(res1, res2);

                    // P + P + ... + P = N * P
                    let n = rnd.getbyte();
                    let res1 = $get_multiexp(&vec![(Big::new_int(1), p.clone()); n as usize]);
                    let res2 = $get_multiexp(&vec![(Big::new_int(n.clone() as isize), p.clone())]);
                    assert_eq!(res1, res2);

                    // 0 * P = 0
                    let res1 = $get_multiexp(&vec![(Big::new_int(0), p.clone())]);
                    assert_eq!(res1, zero1);

                    // 1 * P = P
                    let res1 = $get_multiexp(&vec![(Big::new_int(1), p.clone())]);
                    assert_eq!(res1, $serialize_uncompressed(&p));
                }
            }

            #[test]
            fn $test_bls12381_error_encoding() {
                let mut rnd = get_rnd();

                let mut logic_builder = VMLogicBuilder::default();
                let mut logic = logic_builder.build();

                let zero_scalar = vec![0u8; 32];

                //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
                let p = $GOperations::get_random_curve_point(&mut rnd);
                let p_ser = $add_p_y(&p).to_vec();

                let input = logic
                    .internal_mem_write(vec![vec![0], p_ser.clone()].concat().as_slice());
                let res = logic.$bls12381_sum(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                let input = logic.internal_mem_write(
                    vec![p_ser, zero_scalar.clone()].concat().as_slice(),
                );
                let res = logic.$bls12381_multiexp(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);
            }
        };
    }

    test_bls12381_multiexp!(
        G1Operations,
        96,
        serialize_uncompressed_g1,
        500,
        ECP,
        add_p_y,
        get_g1_sum_many_points,
        get_g1_multiexp_small,
        get_g1_multiexp,
        bls12381_p1_multiexp,
        bls12381_p1_sum,
        test_bls12381_p1_multiexp_mul,
        test_bls12381_p1_multiexp_many_points,
        test_bls12381_p1_multiexp_incorrect_input,
        test_bls12381_p1_multiexp_invariants_checks,
        test_bls12381_error_g1_encoding
    );
    test_bls12381_multiexp!(
        G2Operations,
        192,
        serialize_uncompressed_g2,
        250,
        ECP2,
        add2_p_y,
        get_g2_sum_many_points,
        get_g2_multiexp_small,
        get_g2_multiexp,
        bls12381_p2_multiexp,
        bls12381_p2_sum,
        test_bls12381_p2_multiexp_mul,
        test_bls12381_p2_multiexp_many_points,
        test_bls12381_p2_multiexp_incorrect_input,
        test_bls12381_p2_multiexp_invariants_checks,
        test_bls12381_error_g2_encoding
    );

    fn add_p_y(point: &ECP) -> [u8; 96] {
        let mut ybig = point.gety();
        ybig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_uncompressed_g1(&point);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        p_ser
    }

    fn add2_p_y(point: &ECP2) -> [u8; 192] {
        let mut yabig = point.gety().geta();
        yabig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_uncompressed_g2(&point);
        yabig.to_byte_array(&mut p_ser[0..192], 96 + 48);

        p_ser
    }

    #[test]
    fn test_bls12381_map_fp_to_g1() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let fp = get_random_fp(&mut rnd);
            let res1 = map_fp_to_g1(vec![fp.clone()]);

            let mut res2 = map_to_curve_g1(fp);
            res2 = res2.mul(&Big::new_ints(&H_EFF_G1));

            assert_eq!(res1, serialize_uncompressed_g1(&res2));
        }
    }

    #[test]
    fn test_bls12381_map_fp_to_g1_edge_cases() {
        let fp = FP::new_big(Big::new_int(0));
        let res1 = map_fp_to_g1(vec![fp.clone()]);

        let mut res2 = map_to_curve_g1(fp);
        res2 = res2.mul(&Big::new_ints(&H_EFF_G1));

        assert_eq!(res1, serialize_uncompressed_g1(&res2));

        let fp = FP::new_big(Big::from_string("1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaaa".to_string()));
        let res1 = map_fp_to_g1(vec![fp.clone()]);

        let mut res2 = map_to_curve_g1(fp);
        res2 = res2.mul(&Big::new_ints(&H_EFF_G1));

        assert_eq!(res1, serialize_uncompressed_g1(&res2));
    }

    #[test]
    fn test_bls12381_map_fp_to_g1_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 500;

        for i in 0..10 {
            let n: usize = if i == 0 { MAX_N } else { (thread_rng().next_u32() as usize) % MAX_N };

            let mut fps: Vec<FP> = vec![];
            let mut res2_mul: Vec<u8> = vec![];
            for i in 0..n {
                fps.push(get_random_fp(&mut rnd));

                let mut res2 = map_to_curve_g1(fps[i].clone());
                res2 = res2.mul(&Big::new_ints(&H_EFF_G1));

                res2_mul.append(&mut serialize_uncompressed_g1(&res2).to_vec());
            }

            let res1 = map_fp_to_g1(fps);
            assert_eq!(res1, res2_mul);
        }
    }

    #[test]
    fn test_bls12381_map_fp_to_g1_incorrect_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let p = hex::decode(P.to_string()).unwrap();

        let input = logic.internal_mem_write(p.as_slice());
        let res = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);
    }

    #[test]
    fn test_bls12381_map_fp2_to_g2() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let fp2 = get_random_fp2(&mut rnd);
            let res1 = map_fp2_to_g2(vec![fp2.clone()]);

            let mut res2 = map_to_curve_g2(fp2);
            res2.clear_cofactor();

            assert_eq!(res1, serialize_uncompressed_g2(&res2));
        }
    }

    #[test]
    fn test_bls12381_map_fp_to_g2_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 250;

        for i in 0..10 {
            let n: usize = if i == 0 { MAX_N } else { (thread_rng().next_u32() as usize) % MAX_N };

            let mut fps: Vec<FP2> = vec![];
            let mut res2_mul: Vec<u8> = vec![];
            for i in 0..n {
                fps.push(get_random_fp2(&mut rnd));

                let mut res2 = map_to_curve_g2(fps[i].clone());
                res2.clear_cofactor();

                res2_mul.append(&mut serialize_uncompressed_g2(&res2).to_vec());
            }

            let res1 = map_fp2_to_g2(fps);
            assert_eq!(res1, res2_mul);
        }
    }

    #[test]
    fn test_bls12381_map_fp2_to_g2_incorrect_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let p = hex::decode(P.to_string()).unwrap();

        let input = logic.internal_mem_write(vec![p.clone(), vec![0u8; 48]].concat().as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);

        let input = logic.internal_mem_write(vec![vec![0u8; 48], p.clone()].concat().as_slice());
        let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();

        assert_eq!(res, 1);
    }

    macro_rules! test_bls12381_decompress {
        (
            $GOperations:ident,
            $serialize_uncompressed_g:ident,
            $serialize_g:ident,
            $POINT_LEN:expr,
            $MAX_N:expr,
            $ECP:ident,
            $bls12381_decompress:ident,
            $add_p:ident,
            $test_bls12381_decompress:ident,
            $test_bls12381_decompress_many_points:ident,
            $test_bls12381_decompress_incorrect_input:ident
        ) => {
            #[test]
            fn $test_bls12381_decompress() {
                let mut rnd = get_rnd();

                for _ in 0..100 {
                    let p1 = $GOperations::get_random_curve_point(&mut rnd);
                    let res1 = $GOperations::decompress_p(vec![p1.clone()]);

                    assert_eq!(res1, $serialize_uncompressed_g(&p1));

                    let p1_neg = p1.mul(&Big::new_int(-1));
                    let res1_neg = $GOperations::decompress_p(vec![p1_neg.clone()]);

                    assert_eq!(res1[0..$POINT_LEN], res1_neg[0..$POINT_LEN]);
                    assert_ne!(res1[$POINT_LEN..], res1_neg[$POINT_LEN..]);
                    assert_eq!(res1_neg, $serialize_uncompressed_g(&p1_neg));
                }

                let zero1 = $ECP::new();
                let res1 = $GOperations::decompress_p(vec![zero1.clone()]);

                assert_eq!(res1, $serialize_uncompressed_g(&zero1));
            }

            #[test]
            fn $test_bls12381_decompress_many_points() {
                let mut rnd = get_rnd();

                for i in 0..10 {
                    let n: usize =
                        if i == 0 { $MAX_N } else { (thread_rng().next_u32() as usize) % $MAX_N };

                    let mut p1s: Vec<$ECP> = vec![];
                    let mut res2: Vec<u8> = vec![];
                    for i in 0..n {
                        p1s.push($GOperations::get_random_curve_point(&mut rnd));
                        res2.append(&mut $serialize_uncompressed_g(&p1s[i]).to_vec());
                    }
                    let res1 = $GOperations::decompress_p(p1s.clone());
                    assert_eq!(res1, res2);

                    let mut p1s: Vec<$ECP> = vec![];
                    let mut res2: Vec<u8> = vec![];
                    for i in 0..n {
                        p1s.push($GOperations::get_random_g_point(&mut rnd));
                        res2.append(&mut $serialize_uncompressed_g(&p1s[i]).to_vec());
                    }
                    let res1 = $GOperations::decompress_p(p1s.clone());
                    assert_eq!(res1, res2);
                }
            }

            #[test]
            fn $test_bls12381_decompress_incorrect_input() {
                let mut rnd = get_rnd();

                let mut logic_builder = VMLogicBuilder::default();
                let mut logic = logic_builder.build();

                // Incorrect encoding of the point at infinity
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 0x80 | 0x40;
                zero[$POINT_LEN - 1] = 1;

                let input = logic.internal_mem_write(zero.as_slice());
                let res = logic.$bls12381_decompress(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 0x40;

                let input = logic.internal_mem_write(zero.as_slice());
                let res = logic.$bls12381_decompress(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                let p = $GOperations::get_random_curve_point(&mut rnd);
                let mut p_ser = $serialize_g(&p);
                p_ser[0] ^= 0x80;

                let input = logic.internal_mem_write(p_ser.as_slice());
                let res = logic.$bls12381_decompress(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);

                //Point with a coordinate larger than 'p'.
                let p = $GOperations::get_random_curve_point(&mut rnd);

                let input = logic.internal_mem_write($add_p(&p).as_slice());
                let res = logic.$bls12381_decompress(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 1);
            }
        };
    }

    test_bls12381_decompress!(
        G1Operations,
        serialize_uncompressed_g1,
        serialize_g1,
        48,
        500,
        ECP,
        bls12381_p1_decompress,
        add_p_x,
        test_bls12381_p1_decompress,
        test_bls12381_p1_decompress_many_points,
        test_bls12381_p1_decompress_incorrect_input
    );

    test_bls12381_decompress!(
        G2Operations,
        serialize_uncompressed_g2,
        serialize_g2,
        96,
        250,
        ECP2,
        bls12381_p2_decompress,
        add2_p_x,
        test_bls12381_p2_decompress,
        test_bls12381_p2_decompress_many_points,
        test_bls12381_p2_decompress_incorrect_input
    );

    fn add_p_x(point: &ECP) -> [u8; 48] {
        let mut xbig = point.getx();
        xbig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_g1(&point);
        xbig.to_byte_array(&mut p_ser[0..48], 0);
        p_ser[0] |= 0x80;

        p_ser
    }

    fn add2_p_x(point: &ECP2) -> [u8; 96] {
        let mut xabig = point.getx().geta();
        xabig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_g2(&point);
        xabig.to_byte_array(&mut p_ser[0..96], 48);

        p_ser
    }

    #[test]
    fn test_bls12381_pairing_check_one_point() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p1 = G1Operations::get_random_g_point(&mut rnd);
            let p2 = G2Operations::get_random_g_point(&mut rnd);

            let zero1 = ECP::new();
            let zero2 = ECP2::new();

            let mut r = pair::initmp();
            pair::another(&mut r, &zero2, &p1);
            let mut v = pair::miller(&r);

            v = pair::fexp(&v);
            assert!(v.is_unity());

            assert_eq!(pairing_check(vec![zero1.clone()], vec![zero2.clone()]), 0);
            assert_eq!(pairing_check(vec![zero1.clone()], vec![p2.clone()]), 0);
            assert_eq!(pairing_check(vec![p1.clone()], vec![zero2.clone()]), 0);
            assert_eq!(pairing_check(vec![p1.clone()], vec![p2.clone()]), 2);
        }
    }

    #[test]
    fn test_bls12381_pairing_check_two_points() {
        let mut rnd = get_rnd();

        for _ in 0..100 {
            let p1 = G1Operations::get_random_g_point(&mut rnd);
            let p2 = G2Operations::get_random_g_point(&mut rnd);

            let p1_neg = p1.mul(&Big::new_int(-1));
            let p2_neg = p2.mul(&Big::new_int(-1));

            assert_eq!(
                pairing_check(vec![p1.clone(), p1_neg.clone()], vec![p2.clone(), p2.clone()]),
                0
            );
            assert_eq!(
                pairing_check(vec![p1.clone(), p1.clone()], vec![p2.clone(), p2_neg.clone()]),
                0
            );
            assert_eq!(
                pairing_check(vec![p1.clone(), p1.clone()], vec![p2.clone(), p2.clone()]),
                2
            );

            let mut s1 = Big::random(&mut rnd);
            s1.mod2m(32 * 8);

            let mut s2 = Big::random(&mut rnd);
            s2.mod2m(32 * 8);

            assert_eq!(
                pairing_check(vec![p1.mul(&s1), p1_neg.mul(&s2)], vec![p2.mul(&s2), p2.mul(&s1)]),
                0
            );
            assert_eq!(
                pairing_check(vec![p1.mul(&s1), p1.mul(&s2)], vec![p2.mul(&s2), p2_neg.mul(&s1)]),
                0
            );
            assert_eq!(
                pairing_check(
                    vec![p1.mul(&s1), p1.mul(&s2)],
                    vec![p2_neg.mul(&s2), p2_neg.mul(&s1)]
                ),
                2
            );
        }
    }

    #[test]
    fn test_bls12381_pairing_check_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 105;
        let r = Big::from_string(P.to_string());

        for i in 0..10 {
            let n: usize =
                if i == 0 { MAX_N } else { (thread_rng().next_u32() as usize) % MAX_N } + 1;

            let mut scalars_1: Vec<Big> = vec![];
            let mut scalars_2: Vec<Big> = vec![];

            let g1: ECP = ECP::generator();
            let g2: ECP2 = ECP2::generator();

            let mut g1s: Vec<ECP> = vec![];
            let mut g2s: Vec<ECP2> = vec![];

            for i in 0..n {
                scalars_1.push(Big::random(&mut rnd));
                scalars_2.push(Big::random(&mut rnd));

                scalars_1[i].rmod(&r);
                scalars_2[i].rmod(&r);

                g1s.push(g1.mul(&scalars_1[i]));
                g2s.push(g2.mul(&scalars_2[i]));
            }

            assert_eq!(pairing_check(g1s.clone(), g2s.clone()), 2);

            for i in 0..n {
                let mut p2 = g2.mul(&scalars_1[i]);
                p2.neg();

                g1s.push(g1.mul(&scalars_2[i]));
                g2s.push(p2);
            }

            assert_eq!(pairing_check(g1s, g2s), 0);
        }
    }

    #[test]
    fn test_bls12381_pairing_incorrect_input_point() {
        let mut rnd = get_rnd();

        let p1_not_from_g1 = G1Operations::get_random_not_g_curve_point(&mut rnd);
        let p2 = G2Operations::get_random_g_point(&mut rnd);

        let p1 = G1Operations::get_random_g_point(&mut rnd);
        let p2_not_from_g2 = G2Operations::get_random_not_g_curve_point(&mut rnd);

        assert_eq!(pairing_check(vec![p1_not_from_g1.clone()], vec![p2.clone()]), 1);
        assert_eq!(pairing_check(vec![p1.clone()], vec![p2_not_from_g2.clone()]), 1);

        // Incorrect encoding of the point at infinity
        let mut zero = vec![0u8; 96];
        zero[0] = 64;
        zero[95] = 1;
        assert_eq!(pairing_check_vec(zero.clone(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
        let mut zero = vec![0u8; 96];
        zero[0] = 192;
        assert_eq!(pairing_check_vec(zero.clone(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        let p = G1Operations::get_random_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] |= 0x80;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // G1 point not on the curve
        let p = G1Operations::get_random_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // G2 point not on the curve
        let p = G2Operations::get_random_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[191] ^= 0x01;

        assert_eq!(pairing_check_vec(serialize_uncompressed_g1(&p1).to_vec(), p_ser.to_vec()), 1);

        // not G1 point
        let p = G1Operations::get_random_not_g_curve_point(&mut rnd);
        let p_ser = serialize_uncompressed_g1(&p);

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // not G2 point
        let p = G2Operations::get_random_not_g_curve_point(&mut rnd);
        let p_ser = serialize_uncompressed_g2(&p);

        assert_eq!(pairing_check_vec(serialize_uncompressed_g1(&p1).to_vec(), p_ser.to_vec()), 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = G1Operations::get_random_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = G1Operations::get_random_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_uncompressed_g1(&p);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);
    }

    #[test]
    fn test_bls12381_empty_input() {
        assert_eq!(get_zero(96), get_g1_multiexp_many_points(&vec![]));
        assert_eq!(get_zero(192), get_g2_multiexp_many_points(&vec![]));
        assert_eq!(map_fp_to_g1(vec![]).len(), 0);
        assert_eq!(map_fp2_to_g2(vec![]).len(), 0);
        assert_eq!(pairing_check(vec![], vec![]), 0);
        assert_eq!(G1Operations::decompress_p(vec![]).len(), 0);
        assert_eq!(G2Operations::decompress_p(vec![]).len(), 0);
    }

    // EIP-2537 tests
    macro_rules! eip2537_tests {
        (
            $file_path:expr,
            $test_name:ident,
            $item_size:expr,
            $transform_input:ident,
            $run_bls_fn:ident,
            $check_res:ident
        ) => {
            #[test]
            fn $test_name() {
                let input_csv = fs::read($file_path).unwrap();
                let mut reader = csv::Reader::from_reader(input_csv.as_slice());
                for record in reader.records() {
                    let record = record.unwrap();

                    let mut logic_builder = VMLogicBuilder::default();
                    let mut logic = logic_builder.build();

                    let bytes_input = hex::decode(&record[0]).unwrap();
                    let k = bytes_input.len() / $item_size;
                    let mut bytes_input_fix: Vec<Vec<u8>> = vec![];
                    for i in 0..k {
                        bytes_input_fix.push($transform_input(
                            bytes_input[i * $item_size..(i + 1) * $item_size].to_vec(),
                        ));
                    }

                    let input = logic.internal_mem_write(&bytes_input_fix.concat());
                    let res = $run_bls_fn(input, &mut logic);
                    $check_res(&record[1], res);
                }
            }
        };
    }

    fn fix_eip2537_pairing_input(input: Vec<u8>) -> Vec<u8> {
        vec![
            fix_eip2537_g1(input[..128].to_vec()).to_vec(),
            fix_eip2537_g2(input[128..].to_vec()).to_vec(),
        ]
        .concat()
    }

    fn fix_eip2537_fp(fp: Vec<u8>) -> Vec<u8> {
        fp[16..].to_vec()
    }

    fn fix_eip2537_fp2(fp2: Vec<u8>) -> Vec<u8> {
        vec![fp2[64 + 16..].to_vec(), fp2[16..64].to_vec()].concat()
    }

    macro_rules! fix_eip2537_input {
        ($namespace_name:ident, $fix_eip2537_fp:ident) => {
            mod $namespace_name {
                use crate::logic::tests::bls12381::tests::$fix_eip2537_fp;

                pub fn fix_eip2537_g(g: Vec<u8>) -> Vec<u8> {
                    let mut res = vec![
                        $fix_eip2537_fp(g[..g.len() / 2].to_vec()),
                        $fix_eip2537_fp(g[g.len() / 2..].to_vec()),
                    ]
                    .concat();

                    if g == vec![0; g.len()] {
                        res[0] |= 0x40;
                    }

                    return res;
                }

                pub fn fix_eip2537_sum_input(input: Vec<u8>) -> Vec<u8> {
                    vec![
                        vec![0u8],
                        fix_eip2537_g(input[..input.len() / 2].to_vec()),
                        vec![0u8],
                        fix_eip2537_g(input[input.len() / 2..].to_vec()),
                    ]
                    .concat()
                }

                pub fn fix_eip2537_mul_input(input: Vec<u8>) -> Vec<u8> {
                    vec![
                        fix_eip2537_g(input[..(input.len() - 32)].to_vec()),
                        input[(input.len() - 32)..].to_vec().into_iter().rev().collect(),
                    ]
                    .concat()
                }

                pub fn cmp_output_g(output: &str, res: Vec<u8>) {
                    let bytes_output = fix_eip2537_g(hex::decode(output).unwrap());
                    assert_eq!(res, bytes_output);
                }
            }
        };
    }

    fix_eip2537_input!(fix_eip2537_g1_namespace, fix_eip2537_fp);
    use fix_eip2537_g1_namespace::cmp_output_g as cmp_output_g1;
    use fix_eip2537_g1_namespace::fix_eip2537_g as fix_eip2537_g1;
    use fix_eip2537_g1_namespace::fix_eip2537_mul_input as fix_eip2537_mul_g1_input;
    use fix_eip2537_g1_namespace::fix_eip2537_sum_input as fix_eip2537_sum_g1_input;

    fix_eip2537_input!(fix_eip2537_g2_namespace, fix_eip2537_fp2);
    use fix_eip2537_g2_namespace::cmp_output_g as cmp_output_g2;
    use fix_eip2537_g2_namespace::fix_eip2537_g as fix_eip2537_g2;
    use fix_eip2537_g2_namespace::fix_eip2537_mul_input as fix_eip2537_mul_g2_input;
    use fix_eip2537_g2_namespace::fix_eip2537_sum_input as fix_eip2537_sum_g2_input;

    fn check_pairing_res(output: &str, res: u64) {
        if output == "0000000000000000000000000000000000000000000000000000000000000000" {
            assert_eq!(res, 2);
        } else if output == "0000000000000000000000000000000000000000000000000000000000000001" {
            assert_eq!(res, 0);
        } else {
            assert_eq!(res, 1);
        }
    }

    fn error_check(output: &str, res: u64) {
        if !output.contains("padded BE encoding are NOT zeroes") {
            assert_eq!(res, 1)
        }
    }

    macro_rules! run_bls12381_function_raw {
        ($fn_name_raw:ident, $fn_name_return_value_only:ident, $bls_fn_name:ident) => {
            #[allow(unused)]
            fn $fn_name_raw(input: MemSlice, logic: &mut TestVMLogic) -> Vec<u8> {
                let res = logic.$bls_fn_name(input.len, input.ptr, 0).unwrap();
                assert_eq!(res, 0);
                logic.registers().get_for_free(0).unwrap().to_vec()
            }

            #[allow(unused)]
            fn $fn_name_return_value_only(input: MemSlice, logic: &mut TestVMLogic) -> u64 {
                logic.$bls_fn_name(input.len, input.ptr, 0).unwrap()
            }
        };
    }

    run_bls12381_function_raw!(run_map_fp_to_g1, map_fp_to_g1_return_value, bls12381_map_fp_to_g1);
    run_bls12381_function_raw!(run_map_fp2_to_g2, map_fp2tog2_return_value, bls12381_map_fp2_to_g2);
    run_bls12381_function_raw!(run_sum_g1, sum_g1_return_value, bls12381_p1_sum);
    run_bls12381_function_raw!(run_sum_g2, sum_g2_return_value, bls12381_p2_sum);
    run_bls12381_function_raw!(run_multiexp_g1, multiexp_g1_return_value, bls12381_p1_multiexp);
    run_bls12381_function_raw!(run_multiexp_g2, multiexp_g2_return_value, bls12381_p2_multiexp);
    run_bls12381_function_raw!(decompress_g1, decompress_g1_return_value, bls12381_p1_decompress);
    run_bls12381_function_raw!(decompress_g2, decompress_g2_return_value, bls12381_p2_decompress);
    fn run_pairing_check_raw(input: MemSlice, logic: &mut TestVMLogic) -> u64 {
        logic.bls12381_pairing_check(input.len, input.ptr).unwrap()
    }

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/pairing.csv",
        test_bls12381_pairing_test_vectors,
        384,
        fix_eip2537_pairing_input,
        run_pairing_check_raw,
        check_pairing_res
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/fp_to_g1.csv",
        test_bls12381_fp_to_g1_test_vectors,
        64,
        fix_eip2537_fp,
        run_map_fp_to_g1,
        cmp_output_g1
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/fp2_to_g2.csv",
        test_bls12381_fp2_to_g2_test_vectors,
        128,
        fix_eip2537_fp2,
        run_map_fp2_to_g2,
        cmp_output_g2
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g1_add.csv",
        test_bls12381_g1_add_test_vectors,
        256,
        fix_eip2537_sum_g1_input,
        run_sum_g1,
        cmp_output_g1
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g2_add.csv",
        test_bls12381_g2_add_test_vectors,
        512,
        fix_eip2537_sum_g2_input,
        run_sum_g2,
        cmp_output_g2
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g1_mul.csv",
        test_bls12381_g1_mul_test_vectors,
        160,
        fix_eip2537_mul_g1_input,
        run_multiexp_g1,
        cmp_output_g1
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g2_mul.csv",
        test_bls12381_g2_mul_test_vectors,
        288,
        fix_eip2537_mul_g2_input,
        run_multiexp_g2,
        cmp_output_g2
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g1_multiexp.csv",
        test_bls12381_g1_multiexp_test_vectors,
        160,
        fix_eip2537_mul_g1_input,
        run_multiexp_g1,
        cmp_output_g1
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/g2_multiexp.csv",
        test_bls12381_g2_multiexp_test_vectors,
        288,
        fix_eip2537_mul_g2_input,
        run_multiexp_g2,
        cmp_output_g2
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/pairing_error.csv",
        test_bls12381_pairing_error_test_vectors,
        384,
        fix_eip2537_pairing_input,
        run_pairing_check_raw,
        check_pairing_res
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/multiexp_g1_error.csv",
        test_bls12381_g1_multiexp_error_test_vectors,
        160,
        fix_eip2537_mul_g1_input,
        multiexp_g1_return_value,
        error_check
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/multiexp_g2_error.csv",
        test_bls12381_g2_multiexp_error_test_vectors,
        288,
        fix_eip2537_mul_g2_input,
        multiexp_g2_return_value,
        error_check
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/fp_to_g1_error.csv",
        test_bls12381_fp_to_g1_error_test_vectors,
        64,
        fix_eip2537_fp,
        map_fp_to_g1_return_value,
        error_check
    );

    eip2537_tests!(
        "src/logic/tests/bls12381_test_vectors/fp2_to_g2_error.csv",
        test_bls12381_fp2_to_g2_error_test_vectors,
        128,
        fix_eip2537_fp2,
        map_fp2tog2_return_value,
        error_check
    );
}

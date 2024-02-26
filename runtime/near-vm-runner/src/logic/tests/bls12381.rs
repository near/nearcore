mod tests {
    use std::fs;
    use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
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

    const P: &str = "1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab";

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
        for i in 0..100 {
            raw[i] = i as u8
        }

        rnd.seed(100, &raw);

        rnd
    }

    fn decompress_p1(p1: Vec<ECP>) -> Vec<u8> {
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

    fn decompress_p2(p2: Vec<ECP2>) -> Vec<u8> {
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

    fn get_g1_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8]) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8]) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g1_inverse(p: &[u8]) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![vec![1], p.to_vec()];

        let input = logic.internal_mem_write(buffer.concat().as_slice());
        let res = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 0);
        logic.registers().get_for_free(0).unwrap().to_vec()
    }

    fn get_g2_inverse(p: &[u8]) -> Vec<u8> {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

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

    fn fix_eip2537_fp(fp: Vec<u8>) -> Vec<u8> {
        fp[16..].to_vec()
    }


    fn fix_eip2537_fp2(fp2: Vec<u8>) -> Vec<u8> {
        vec![fp2[64 + 16..].to_vec(), fp2[16..64].to_vec()].concat()
    }

    fn fix_eip2537_g1(g1: Vec<u8>) -> Vec<u8> {
        let mut res = vec![fix_eip2537_fp(g1[..64].to_vec()), fix_eip2537_fp(g1[64..].to_vec())].concat();
        if res == vec![0; 192] {
            res[0] |= 0x40;
        }

        return res;
    }

    fn fix_eip2537_g2(g2: Vec<u8>) -> Vec<u8> {
        let mut res = vec![fix_eip2537_fp2(g2[..128].to_vec()), fix_eip2537_fp2(g2[128..].to_vec())].concat();
        if res == vec![0; 192] {
            res[0] |= 0x40;
        }

        return res;
    }

    //==== TESTS FOR G1_SUM
    macro_rules! test_bls12381_sum {
        (
            $test_bls12381_sum_edge_cases:ident,
            $POINT_LEN:expr,
            $get_sum:ident,
            $get_random_g_point:ident,
            $get_random_curve_point:ident,
            $serialize_uncompressed:ident,
            $test_bls12381_sum:ident,
            $deserialize:ident,
            $subgroup_check:ident,
            $test_bls12381_sum_not_g_points:ident,
            $get_random_not_g_curve_point:ident,
            $test_bls12381_sum_inverse:ident,
            $get_inverse:ident,
            $test_bls12381_sum_many_points:ident,
            $MAX_N:expr,
            $get_g_sum_many_points:ident,
            $check_multipoint_g_sum:ident,
            $point_type:ident
        ) => {
            #[test]
            fn $test_bls12381_sum_edge_cases() {
                // 0 + 0
                let mut zero: [u8; $POINT_LEN] = [0; $POINT_LEN];
                zero[0] = 64;
                let got = $get_sum(0, & zero, 0, & zero);
                assert_eq!(zero.to_vec(), got);

                // 0 + P = P + 0 = P
                let mut rnd = get_rnd();
                for _ in 0..10 {
                    let p = $get_random_g_point( & mut rnd);
                    let p_ser = $serialize_uncompressed( & p);

                    let got = $get_sum(0, & zero, 0, & p_ser);
                    assert_eq!(p_ser.to_vec(), got);

                    let got = $get_sum(0, & p_ser, 0, &zero);
                    assert_eq!(p_ser.to_vec(), got);
                }

                // P + (-P) = (-P) + P =  0
                for _ in 0..10 {
                    let mut p = $get_random_curve_point( & mut rnd);
                    let p_ser = $serialize_uncompressed( & p);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed( & p);

                    let got = $get_sum(0, & p_neg_ser, 0, & p_ser);
                    assert_eq!(zero.to_vec(), got);

                    let got = $get_sum(0, & p_ser, 0, & p_neg_ser);
                    assert_eq!(zero.to_vec(), got);
                }

                // P + P
                for _ in 0..10 {
                    let p = $get_random_curve_point( & mut rnd);
                    let p_ser = $serialize_uncompressed( & p);

                    let pmul2 = p.mul( & Big::from_bytes( & [2]));
                    let pmul2_ser = $serialize_uncompressed( & pmul2);

                    let got = $get_sum(0, & p_ser, 0, &p_ser);
                    assert_eq!(pmul2_ser.to_vec(), got);
                }

                // P + (-(P + P))
                for _ in 0..10 {
                    let mut p = $get_random_curve_point( & mut rnd);
                    let p_ser = $serialize_uncompressed( & p);

                    let mut pmul2 = p.mul( & Big::from_bytes( & [2]));
                    pmul2.neg();
                    let pmul2_neg_ser = $serialize_uncompressed( & pmul2);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed( & p);
                    let got = $get_sum(0, & p_ser, 0, & pmul2_neg_ser);
                    assert_eq!(p_neg_ser.to_vec(), got);
                }
            }

            #[test]
            fn $test_bls12381_sum() {
                let mut rnd = get_rnd();

                for _ in 0..100 {
                    let mut p = $get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let q = $get_random_curve_point(&mut rnd);
                    let q_ser = $serialize_uncompressed(&q);

                    // P + Q = Q + P
                    let got1 = $get_sum(0, &p_ser, 0, &q_ser);
                    let got2 = $get_sum(0, &q_ser, 0, &p_ser);
                    assert_eq!(got1, got2);

                    // compare with library results
                    p.add(&q);
                    let library_sum = $serialize_uncompressed(&p);

                    assert_eq!(library_sum.to_vec(), got1);
                }

                for _ in 0..100 {
                   let p = $get_random_g_point(&mut rnd);
                   let p_ser = $serialize_uncompressed(&p);

                   let q = $get_random_g_point(&mut rnd);
                   let q_ser = $serialize_uncompressed(&q);

                   let got1 = $get_sum(0, &p_ser, 0, &q_ser);

                   let result_point = $deserialize(&got1).unwrap();
                   assert!($subgroup_check(&result_point));
                }
            }

            #[test]
            fn $test_bls12381_sum_not_g_points() {
                let mut rnd = get_rnd();

                //points not from G
                for _ in 0..100 {
                    let mut p = $get_random_not_g_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let q = $get_random_not_g_curve_point(&mut rnd);
                    let q_ser = $serialize_uncompressed(&q);

                    // P + Q = Q + P
                    let got1 = $get_sum(0, &p_ser, 0, &q_ser);
                    let got2 = $get_sum(0, &q_ser, 0, &p_ser);
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
                    let p = $get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    // P - P = - P + P = 0
                    let got1 = $get_sum(1, &p_ser, 0, &p_ser);
                    let got2 = $get_sum(0, &p_ser, 1, &p_ser);
                    assert_eq!(got1, got2);
                    assert_eq!(got1, zero.to_vec());

                    // -(-P)
                    let p_inv = $get_inverse(&p_ser);
                    let p_inv_inv = $get_inverse(p_inv.as_slice());

                    assert_eq!(p_ser.to_vec(), p_inv_inv);
                }

                // P in G => -P in G
                for _ in 0..10 {
                    let p = $get_random_g_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $get_inverse(&p_ser);

                    let result_point = $deserialize(&p_inv).unwrap();
                    assert!($subgroup_check(&result_point));
                }

                // Random point check with library
                for _ in 0..10 {
                    let mut p = $get_random_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $get_inverse(&p_ser);

                    p.neg();
                    let p_neg_ser = $serialize_uncompressed(&p);

                    assert_eq!(p_neg_ser.to_vec(), p_inv);
                }

                // Not from G points
                for _ in 0..10 {
                    let mut p = $get_random_not_g_curve_point(&mut rnd);
                    let p_ser = $serialize_uncompressed(&p);

                    let p_inv = $get_inverse(&p_ser);

                    p.neg();

                    let p_neg_ser = $serialize_uncompressed(&p);

                    assert_eq!(p_neg_ser.to_vec(), p_inv);
                }

                // -0
                let zero_inv = $get_inverse(&zero);
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
                        points.push((rnd.getbyte() % 2, $get_random_g_point(&mut rnd)));
                    }

                    let res1 = $get_g_sum_many_points(&points);
                    let sum = $deserialize(&res1).unwrap();

                    assert!($subgroup_check(&sum));
                }
            }
        }
    }

    test_bls12381_sum!(test_bls12381_p1_sum_edge_cases, 96, get_g1_sum, get_random_g1_point, get_random_g1_curve_point, serialize_uncompressed_g1, test_bls12381_p1_sum, deserialize_g1, subgroup_check_g1, test_bls12381_p1_sum_not_g1_points, get_random_not_g1_curve_point, test_bls12381_p1_sum_inverse, get_g1_inverse, test_bls12381_p1_sum_many_points, 676, get_g1_sum_many_points, check_multipoint_g1_sum, ECP);
    test_bls12381_sum!(test_bls12381_p2_sum_edge_cases, 192, get_g2_sum, get_random_g2_point, get_random_g2_curve_point, serialize_uncompressed_g2, test_bls12381_p2_sum, deserialize_g2, subgroup_check_g2, test_bls12381_p2_sum_not_g2_points, get_random_not_g2_curve_point, test_bls12381_p2_sum_inverse, get_g2_inverse, test_bls12381_p2_sum_many_points, 338, get_g2_sum_many_points, check_multipoint_g2_sum, ECP2);

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
        ybig.add(&Big::from_string(P.to_string()));
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
        yabig.add(&Big::from_string(P.to_string()));
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
            n.mod2m(32 * 8);

            let res1 = get_g1_multiexp(&vec![(n.clone(), p.clone())]);
            let res2 = p.mul(&n);

            assert_eq!(res1, serialize_uncompressed_g1(&res2));
        }
    }

    #[test]
    fn test_bls12381_p1_multiexp_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 500;

        for i in 0..10 {
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

            let mut res2 = ECP::new();

            let mut points: Vec<(Big, ECP)> = vec![];
            for i in 0..n {
                let mut scalar = Big::random(&mut rnd);
                scalar.mod2m(32 * 8);
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

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_uncompressed_g1(&p);
        ybig.to_byte_array(&mut p_ser[0..96], 48);

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
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
            n.mod2m(32 * 8);

            let res1 = get_g2_multiexp(&vec![(n.clone(), p.clone())]);
            let res2 = p.mul(&n);

            assert_eq!(res1, serialize_uncompressed_g2(&res2));
        }
    }

    #[test]
    fn test_bls12381_p2_multiexp_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 250;

        for i in 0..10 {
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

            let mut res2 = ECP2::new();

            let mut points: Vec<(Big, ECP2)> = vec![];
            for i in 0..n {
                let mut scalar = Big::random(&mut rnd);
                scalar.mod2m(32 * 8);
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

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        // Point not on the curve
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[191] ^= 0x01;

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[0] ^= 0x20;

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g2_curve_point(&mut rnd);
        let mut yabig = p.gety().geta();
        yabig.add(&Big::from_string(P.to_string()));
        let mut p_ser = serialize_uncompressed_g2(&p);
        yabig.to_byte_array(&mut p_ser[0..192], 96 + 48);

        let input =
            logic.internal_mem_write(vec![p_ser.to_vec(), zero_scalar.clone()].concat().as_slice());
        let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
        assert_eq!(res, 1);
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
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

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
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

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

        let p = hex::decode(P.to_string()).unwrap();

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
            let res1 = decompress_p1(vec![p1.clone()]);

            assert_eq!(res1, serialize_uncompressed_g1(&p1));

            let p1_neg = p1.mul(&Big::new_int(-1));
            let res1_neg = decompress_p1(vec![p1_neg.clone()]);

            assert_eq!(res1[0..48], res1_neg[0..48]);
            assert_ne!(res1[48..], res1_neg[48..]);
            assert_eq!(res1_neg, serialize_uncompressed_g1(&p1_neg));

        }

        let zero1 = ECP::new();
        let res1 = decompress_p1(vec![zero1.clone()]);

        assert_eq!(res1, serialize_uncompressed_g1(&zero1));
    }

    #[test]
    fn test_bls12381_p1_decompress_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 500;

        for i in 0..10 {
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

            let mut p1s: Vec<ECP> = vec![];
            let mut res2: Vec<u8> = vec![];
            for i in 0..n {
                p1s.push(get_random_g1_curve_point(&mut rnd));
                res2.append(&mut serialize_uncompressed_g1(&p1s[i]).to_vec());
            }
            let res1 = decompress_p1(p1s.clone());
            assert_eq!(res1, res2);

            let mut p1s: Vec<ECP> = vec![];
            let mut res2: Vec<u8> = vec![];
            for i in 0..n {
                p1s.push(get_random_g1_point(&mut rnd));
                res2.append(&mut serialize_uncompressed_g1(&p1s[i]).to_vec());
            }
            let res1 = decompress_p1(p1s.clone());
            assert_eq!(res1, res2);
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
        xbig.add(&Big::from_string(P.to_string()));
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
            let res1 = decompress_p2(vec![p2.clone()]);

            assert_eq!(res1, serialize_uncompressed_g2(&p2));

            let p2_neg = p2.mul(&Big::new_int(-1));
            let res2_neg = decompress_p2(vec![p2_neg.clone()]);

            assert_eq!(res1[0..96], res2_neg[0..96]);
            assert_ne!(res1[96..], res2_neg[96..]);
            assert_eq!(res2_neg, serialize_uncompressed_g2(&p2_neg));
        }

        let zero2 = ECP2::new();
        let res1 = decompress_p2(vec![zero2.clone()]);

        assert_eq!(res1, serialize_uncompressed_g2(&zero2));
    }

    #[test]
    fn test_bls12381_p2_decompress_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 250;

        for i in 0..10 {
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            };

            let mut p2s: Vec<ECP2> = vec![];
            let mut res2: Vec<u8> = vec![];
            for i in 0..n {
                p2s.push(get_random_g2_curve_point(&mut rnd));
                res2.append(&mut serialize_uncompressed_g2(&p2s[i]).to_vec());
            }
            let res1 = decompress_p2(p2s.clone());
            assert_eq!(res1, res2);

            let mut p2s: Vec<ECP2> = vec![];
            let mut res2: Vec<u8> = vec![];
            for i in 0..n {
                p2s.push(get_random_g2_point(&mut rnd));
                res2.append(&mut serialize_uncompressed_g2(&p2s[i]).to_vec());
            }
            let res1 = decompress_p2(p2s.clone());
            assert_eq!(res1, res2);
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
        xabig.add(&Big::from_string(P.to_string()));
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
            let p1 = get_random_g1_point(&mut rnd);
            let p2 = get_random_g2_point(&mut rnd);

            let p1_neg = p1.mul(&Big::new_int(-1));
            let p2_neg = p2.mul(&Big::new_int(-1));

            assert_eq!(pairing_check(vec![p1.clone(), p1_neg.clone()], vec![p2.clone(), p2.clone()]), 0);
            assert_eq!(pairing_check(vec![p1.clone(), p1.clone()], vec![p2.clone(), p2_neg.clone()]), 0);
            assert_eq!(pairing_check(vec![p1.clone(), p1.clone()], vec![p2.clone(), p2.clone()]), 2);

            let mut s1 = Big::random(&mut rnd);
            s1.mod2m(32 * 8);

            let mut s2 = Big::random(&mut rnd);
            s2.mod2m(32 * 8);

            assert_eq!(pairing_check(vec![p1.mul(&s1), p1_neg.mul(&s2)], vec![p2.mul(&s2), p2.mul(&s1)]), 0);
            assert_eq!(pairing_check(vec![p1.mul(&s1), p1.mul(&s2)], vec![p2.mul(&s2), p2_neg.mul(&s1)]), 0);
            assert_eq!(pairing_check(vec![p1.mul(&s1), p1.mul(&s2)], vec![p2_neg.mul(&s2), p2_neg.mul(&s1)]), 2);
        }
    }

    #[test]
    fn test_bls12381_pairing_check_many_points() {
        let mut rnd = get_rnd();

        const MAX_N: usize = 105;
        let r = Big::from_string(P.to_string());

        for i in 0..10 {
            let n: usize = if i == 0 {
                MAX_N
            } else {
                (thread_rng().next_u32() as usize) % MAX_N
            } + 1;

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

        let p1_not_from_g1 = get_random_not_g1_curve_point(&mut rnd);
        let p2 = get_random_g2_point(&mut rnd);

        let p1 = get_random_g1_point(&mut rnd);
        let p2_not_from_g2 = get_random_not_g2_curve_point(&mut rnd);

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

        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] |= 0x80;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // G1 point not on the curve
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[95] ^= 0x01;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // G2 point not on the curve
        let p = get_random_g2_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g2(&p);
        p_ser[191] ^= 0x01;

        assert_eq!(pairing_check_vec(serialize_uncompressed_g1(&p1).to_vec(), p_ser.to_vec()), 1);

        // not G1 point
        let p = get_random_not_g1_curve_point(&mut rnd);
        let p_ser = serialize_uncompressed_g1(&p);

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        // not G2 point
        let p = get_random_not_g2_curve_point(&mut rnd);
        let p_ser = serialize_uncompressed_g2(&p);

        assert_eq!(pairing_check_vec(serialize_uncompressed_g1(&p1).to_vec(), p_ser.to_vec()), 1);

        //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut p_ser = serialize_uncompressed_g1(&p);
        p_ser[0] ^= 0x20;

        assert_eq!(pairing_check_vec(p_ser.to_vec(), serialize_uncompressed_g2(&p2).to_vec()), 1);

        //Erroneous coding of field elements resulting in a correct element on the curve modulo p.
        let p = get_random_g1_curve_point(&mut rnd);
        let mut ybig = p.gety();
        ybig.add(&Big::from_string(P.to_string()));
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

    #[test]
    fn test_bls12381_empty_input() {
        let mut zero1: [u8; 96] = [0; 96];
        zero1[0] |= 0x40;
        let empty_multiexp1 = get_g1_multiexp_many_points(&vec![]);
        assert_eq!(zero1, empty_multiexp1.as_slice());

        let mut zero2: [u8; 192] = [0; 192];
        zero2[0] |= 0x40;
        let empty_multiexp2 = get_g2_multiexp_many_points(&vec![]);
        assert_eq!(zero2, empty_multiexp2.as_slice());

        let map_fp_res = map_fp_to_g1(vec![]);
        assert_eq!(map_fp_res.len(), 0);

        let map_fp2_res = map_fp2_to_g2(vec![]);
        assert_eq!(map_fp2_res.len(), 0);

        assert_eq!(pairing_check(vec![], vec![]), 0);

        let decompress_p1_res = decompress_p1(vec![]);
        assert_eq!(decompress_p1_res.len(), 0);

        let decompress_p2_res = decompress_p2(vec![]);
        assert_eq!(decompress_p2_res.len(), 0);
    }

    #[test]
    fn test_bls12381_p1_multiexp_invariants_checks() {
        let mut zero1: [u8; 96] = [0; 96];
        zero1[0] |= 0x40;

        let mut rnd = get_rnd();
        let r = Big::from_string("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001".to_string());

        for _ in 0..10 {
            let p = get_random_g1_point(&mut rnd);

            // group_order * P = 0
            let res = get_g1_multiexp(&vec![(r.clone(), p.clone())]);
            assert_eq!(res.as_slice(), zero1);

            let mut scalar = Big::random(&mut rnd);
            scalar.mod2m(32 * 7);

            // (scalar + group_order) * P = scalar * P
            let res1 = get_g1_multiexp(&vec![(scalar.clone(), p.clone())]);
            scalar.add(&r);
            let res2 = get_g1_multiexp(&vec![(scalar.clone(), p.clone())]);
            assert_eq!(res1, res2);

            // P + P + ... + P = N * P
            let n = rnd.getbyte();
            let res1 = get_g1_multiexp(&vec![(Big::new_int(1), p.clone()); n as usize]);
            let res2 = get_g1_multiexp(&vec![(Big::new_int(n.clone() as isize), p.clone())]);
            assert_eq!(res1, res2);

            // 0 * P = 0
            let res1 = get_g1_multiexp(&vec![(Big::new_int(0), p.clone())]);
            assert_eq!(res1, zero1);

            // 1 * P = P
            let res1 = get_g1_multiexp(&vec![(Big::new_int(1), p.clone())]);
            assert_eq!(res1, serialize_uncompressed_g1(&p));
        }
    }


    #[test]
    fn test_bls12381_p2_multiexp_invariants_checks() {
        let mut zero2: [u8; 192] = [0; 192];
        zero2[0] |= 0x40;

        let mut rnd = get_rnd();
        let r = Big::from_string("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001".to_string());

        for _ in 0..10 {
            let p = get_random_g2_point(&mut rnd);

            // group_order * P = 0
            let res = get_g2_multiexp(&vec![(r.clone(), p.clone())]);
            assert_eq!(res.as_slice(), zero2);

            let mut scalar = Big::random(&mut rnd);
            scalar.mod2m(32 * 7);

            // (scalar + group_order) * P = scalar * P
            let res1 = get_g2_multiexp(&vec![(scalar.clone(), p.clone())]);
            scalar.add(&r);
            let res2 = get_g2_multiexp(&vec![(scalar.clone(), p.clone())]);
            assert_eq!(res1, res2);

            // P + P + ... + P = N * P
            let n = rnd.getbyte();
            let res1 = get_g2_multiexp(&vec![(Big::new_int(1), p.clone()); n as usize]);
            let res2 = get_g2_multiexp(&vec![(Big::new_int(n.clone() as isize), p.clone())]);
            assert_eq!(res1, res2);

            //0 * P = O
            let res1 = get_g2_multiexp(&vec![(Big::new_int(0), p.clone())]);
            assert_eq!(res1, zero2);

            // 1 * P = P
            let res1 = get_g2_multiexp(&vec![(Big::new_int(1), p.clone())]);
            assert_eq!(res1, serialize_uncompressed_g2(&p));
        }
    }

    // Memory limits tests
    #[test]
    #[should_panic]
    fn test_bls12381_p1_multiexp_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; (96 + 32) * 1000];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p2_multiexp_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; (192 + 32) * 500];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bls12381_map_fp_to_g1_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 48 * 2000];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bls12381_map_fp2_to_g2_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 96 * 1000];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
    }


    #[test]
    #[should_panic]
    fn test_bls12381_pairing_check_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 288 * 500];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bls12381_p1_decompress_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 48 * 2000];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p1_decompress(input.len, input.ptr, 0).unwrap();
    }


    // EIP-2537 tests
    #[test]
    #[should_panic]
    fn test_bls12381_p2_decompress_too_big_input() {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let buffer = vec![0u8; 96 * 1000];

        let input = logic.internal_mem_write(buffer.as_slice());
        logic.bls12381_p2_decompress(input.len, input.ptr, 0).unwrap();
    }

    #[test]
    fn test_bls12381_pairing_test_vectors() {
        let pairing_csv = fs::read("src/logic/tests/bls12381_test_vectors/pairing.csv").unwrap();
        let mut reader = csv::Reader::from_reader(pairing_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/384;
            let mut bytes_input_fix: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fix.push(fix_eip2537_g1(bytes_input[i * 384 .. i * 384 + 128].to_vec()));
                bytes_input_fix.push(fix_eip2537_g2(bytes_input[i * 384 + 128 .. (i + 1) * 384].to_vec()));
            }

            let input = logic.internal_mem_write(&bytes_input_fix.concat());
            let res = logic.bls12381_pairing_check(input.len, input.ptr).unwrap();

            if &record[1] == "0000000000000000000000000000000000000000000000000000000000000000" {
                assert_eq!(res, 2);
            } else {
                assert_eq!(res, 0);
            }
        }
    }

    #[test]
    fn test_bls12381_fp_to_g1_test_vectors() {
        let fp_to_g1_csv = fs::read("src/logic/tests/bls12381_test_vectors/fp_to_g1.csv").unwrap();
        let mut reader = csv::Reader::from_reader(fp_to_g1_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = fix_eip2537_fp(hex::decode(&record[0]).unwrap());

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g1(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_fp2_to_g2_test_vectors() {
        let fp2_to_g2_csv = fs::read("src/logic/tests/bls12381_test_vectors/fp2_to_g2.csv").unwrap();
        let mut reader = csv::Reader::from_reader(fp2_to_g2_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = fix_eip2537_fp2(hex::decode(&record[0]).unwrap());

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g2(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g1_add_test_vectors() {
        let g1_add_csv = fs::read("src/logic/tests/bls12381_test_vectors/g1_add.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g1_add_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let bytes_input = vec![vec![0u8], fix_eip2537_g1(bytes_input[..128].to_vec()), vec![0u8], fix_eip2537_g1(bytes_input[128..].to_vec())].concat();

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_p1_sum(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g1(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g2_add_test_vectors() {
        let g2_add_csv = fs::read("src/logic/tests/bls12381_test_vectors/g2_add.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g2_add_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let bytes_input = vec![vec![0u8], fix_eip2537_g2(bytes_input[..256].to_vec()), vec![0u8], fix_eip2537_g2(bytes_input[256..].to_vec())].concat();

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_p2_sum(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g2(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g1_mul_test_vectors() {
        let g1_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/g1_mul.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g1_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let bytes_input = vec![fix_eip2537_g1(bytes_input[..128].to_vec()), bytes_input[128..].to_vec().into_iter().rev().collect()].concat();

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g1(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g2_mul_test_vectors() {
        let g2_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/g2_mul.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g2_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let bytes_input = vec![fix_eip2537_g2(bytes_input[..256].to_vec()), bytes_input[256..].to_vec().into_iter().rev().collect()].concat();

            let input = logic.internal_mem_write(&bytes_input);
            let _ = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g2(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g1_multiexp_test_vectors() {
        let g1_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/g1_multiexp.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g1_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/(128 + 32);

            let mut bytes_input_fixed: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fixed.push(fix_eip2537_g1(bytes_input[i * (128 + 32)..i * (128 + 32) + 128].to_vec()));
                bytes_input_fixed.push(bytes_input[i * (128 + 32) + 128.. (i + 1) * (128 + 32)].to_vec().into_iter().rev().collect());
            }
            let input = logic.internal_mem_write(&bytes_input_fixed.concat());
            let _ = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g1(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_g2_multiexp_test_vectors() {
        let g2_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/g2_multiexp.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g2_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/(256 + 32);

            let mut bytes_input_fixed: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fixed.push(fix_eip2537_g2(bytes_input[i * (256 + 32)..i * (256 + 32) + 256].to_vec()));
                bytes_input_fixed.push(bytes_input[i * (256 + 32) + 256.. (i + 1) * (256 + 32)].to_vec().into_iter().rev().collect());
            }
            let input = logic.internal_mem_write(&bytes_input_fixed.concat());
            let _ = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();
            let res = logic.registers().get_for_free(0).unwrap().to_vec();

            let bytes_output = fix_eip2537_g2(hex::decode(&record[1]).unwrap());
            assert_eq!(res, bytes_output);
        }
    }

    #[test]
    fn test_bls12381_pairing_error_test_vectors() {
        let pairing_csv = fs::read("src/logic/tests/bls12381_test_vectors/pairing_error.csv").unwrap();
        let mut reader = csv::Reader::from_reader(pairing_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/384;
            let mut bytes_input_fix: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fix.push(fix_eip2537_g1(bytes_input[i * 384 .. i * 384 + 128].to_vec()));
                bytes_input_fix.push(fix_eip2537_g2(bytes_input[i * 384 + 128 .. (i + 1) * 384].to_vec()));
            }

            let input = logic.internal_mem_write(&bytes_input_fix.concat());
            let res = logic.bls12381_pairing_check(input.len, input.ptr).unwrap();
            assert_eq!(res, 1);
        }
    }

    #[test]
    fn test_bls12381_fp_to_g1_error_test_vectors() {
        let fp_to_g1_csv = fs::read("src/logic/tests/bls12381_test_vectors/fp_to_g1_error.csv").unwrap();
        let mut reader = csv::Reader::from_reader(fp_to_g1_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input_raw = hex::decode(&record[0]).unwrap();
            if bytes_input_raw[0..16] != vec![0; 16] {
                continue;
            }

            let bytes_input = fix_eip2537_fp(bytes_input_raw);

            let input = logic.internal_mem_write(&bytes_input);
            let res = logic.bls12381_map_fp_to_g1(input.len, input.ptr, 0).unwrap();

            assert_eq!(res, 1);
        }
    }

    #[test]
    fn test_bls12381_fp2_to_g2_error_test_vectors() {
        let fp2_to_g2_csv = fs::read("src/logic/tests/bls12381_test_vectors/fp2_to_g2_error.csv").unwrap();
        let mut reader = csv::Reader::from_reader(fp2_to_g2_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input_raw = hex::decode(&record[0]).unwrap();
            if bytes_input_raw[0..16] != vec![0; 16] {
                continue;
            }
            let bytes_input = fix_eip2537_fp2(bytes_input_raw);

            let input = logic.internal_mem_write(&bytes_input);
            let res = logic.bls12381_map_fp2_to_g2(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 1);
        }
    }

    #[test]
    fn test_bls12381_g1_multiexp_error_test_vectors() {
        let g1_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/multiexp_g1_error.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g1_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/(128 + 32);

            let mut bytes_input_fixed: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fixed.push(fix_eip2537_g1(bytes_input[i * (128 + 32)..i * (128 + 32) + 128].to_vec()));
                bytes_input_fixed.push(bytes_input[i * (128 + 32) + 128.. (i + 1) * (128 + 32)].to_vec().into_iter().rev().collect());
            }
            let input = logic.internal_mem_write(&bytes_input_fixed.concat());
            let res = logic.bls12381_p1_multiexp(input.len, input.ptr, 0).unwrap();

            assert_eq!(res, 1);
        }
    }

    #[test]
    fn test_bls12381_g2_multiexp_error_test_vectors() {
        let g2_mul_csv = fs::read("src/logic/tests/bls12381_test_vectors/multiexp_g2_error.csv").unwrap();
        let mut reader = csv::Reader::from_reader(g2_mul_csv.as_slice());
        for record in reader.records() {
            let record = record.unwrap();

            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();

            let bytes_input = hex::decode(&record[0]).unwrap();
            let k = bytes_input.len()/(256 + 32);

            let mut bytes_input_fixed: Vec<Vec<u8>> = vec![];
            for i in 0..k {
                bytes_input_fixed.push(fix_eip2537_g2(bytes_input[i * (256 + 32)..i * (256 + 32) + 256].to_vec()));
                bytes_input_fixed.push(bytes_input[i * (256 + 32) + 256.. (i + 1) * (256 + 32)].to_vec().into_iter().rev().collect());
            }
            let input = logic.internal_mem_write(&bytes_input_fixed.concat());
            let res = logic.bls12381_p2_multiexp(input.len, input.ptr, 0).unwrap();

            assert_eq!(res, 1);
        }
    }
}

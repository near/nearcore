mod tests {
    use crate::logic::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
    use crate::logic::MemSlice;
    use ark_bls12_381::{Bls12_381, Fq, Fq2, Fr, G1Affine, G2Affine};
    use ark_ec::hashing::{curve_maps::wb::WBMap, map_to_curve_hasher::MapToCurve};
    use ark_ec::{bls12::Bls12Config, pairing::Pairing, AffineRepr, CurveGroup};
    use ark_ff::{Field, PrimeField};
    use ark_serialize::{
        CanonicalDeserialize, CanonicalSerialize, CanonicalSerializeWithFlags, EmptyFlags,
    };
    use ark_std::{One, Zero};
    use bolero::{generator, TypeGenerator};
    use rand::{seq::SliceRandom, thread_rng};
    use std::{fs, ops::Add, ops::Mul, ops::Neg, str::FromStr};

    const P: &str = "4002409555221667393417789825735904156556882819939007885332058136124031650490837864442687629129015664037894272559787";
    const P_HEX: &str = "1a0111ea397fe69a4b1ba7b6434bacd764774b84f38512bf6730d2a0f6b0f6241eabfffeb153ffffb9feffffffffaaab";
    const P_MINUS_1: &str = "4002409555221667393417789825735904156556882819939007885332058136124031650490837864442687629129015664037894272559786";
    const R: &str = "52435875175126190479447740508185965837690552500527637822603658699938581184513";
    const R_MINUS_1: &str = "73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000000";

    const MAX_N_PAIRING: usize = 15;

    macro_rules! run_bls12381_fn {
        ($fn_name:ident, $buffer:expr, $expected_res:expr) => {{
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();
            let input = logic.internal_mem_write($buffer.concat().as_slice());
            let res = logic.$fn_name(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, $expected_res);
        }};
        ($fn_name:ident, $buffer:expr) => {{
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build();
            let input = logic.internal_mem_write($buffer.concat().as_slice());
            let res = logic.$fn_name(input.len, input.ptr, 0).unwrap();
            assert_eq!(res, 0);
            logic.registers().get_for_free(0).unwrap().to_vec()
        }};
    }

    struct G1Operations;
    struct G2Operations;

    #[derive(Debug)]
    pub struct FP {
        pub p: Fq,
    }

    impl TypeGenerator for FP {
        fn generate<D: bolero::Driver>(driver: &mut D) -> Option<FP> {
            let fq_ser: [u8; 48] = <[u8; 48]>::generate(driver)?;
            Some(FP { p: Fq::from_random_bytes(&fq_ser)? })
        }
    }

    #[derive(Debug)]
    pub struct FP2 {
        pub p: Fq2,
    }

    impl TypeGenerator for FP2 {
        fn generate<D: bolero::Driver>(driver: &mut D) -> Option<FP2> {
            let c0: FP = FP::generate(driver)?;
            let c1: FP = FP::generate(driver)?;

            Some(FP2 { p: Fq2::new(c0.p, c1.p) })
        }
    }

    #[derive(Debug)]
    pub struct Scalar {
        pub p: Fr,
    }

    impl TypeGenerator for Scalar {
        fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Scalar> {
            let raw = <[u8; 32]>::generate(driver)?;

            Some(Scalar { p: Fr::from_random_bytes(&raw)? })
        }
    }

    impl G1Operations {
        const POINT_LEN: usize = 96;
        const MAX_N_SUM: usize = 675;
        const MAX_N_MULTIEXP: usize = 100;
        const MAX_N_MAP: usize = 150;
        const MAX_N_DECOMPRESS: usize = 500;

        fn serialize_fp(fq: &Fq) -> Vec<u8> {
            let mut result = [0u8; 48];
            let rep = fq.into_bigint();
            for i in 0..6 {
                result[i * 8..(i + 1) * 8].copy_from_slice(&rep.0[5 - i].to_be_bytes());
            }
            result.to_vec()
        }
    }

    impl G2Operations {
        const POINT_LEN: usize = 192;
        const MAX_N_SUM: usize = 338;
        const MAX_N_MULTIEXP: usize = 50;
        const MAX_N_MAP: usize = 75;
        const MAX_N_DECOMPRESS: usize = 250;

        fn serialize_fp(fq: &Fq2) -> Vec<u8> {
            let c1_bytes = G1Operations::serialize_fp(&fq.c1);
            let c0_bytes = G1Operations::serialize_fp(&fq.c0);
            [c1_bytes, c0_bytes].concat()
        }
    }

    macro_rules! impl_goperations {
        (
            $GOperations:ident,
            $Fq:ident,
            $FP:ident,
            $EPoint:ident,
            $GPoint:ident,
            $EnotGPoint:ident,
            $GConfig:ident,
            $GAffine:ident,
            $add_p_y:ident,
            $bls12381_decompress:ident,
            $bls12381_sum:ident,
            $bls12381_multiexp:ident,
            $bls12381_map_fp_to_g:ident
        ) => {
            #[derive(Debug)]
            pub struct $EPoint {
                pub p: $GAffine,
            }

            impl TypeGenerator for $EPoint {
                fn generate<D: bolero::Driver>(driver: &mut D) -> Option<$EPoint> {
                    let x: $FP = $FP::generate(driver)?;
                    let greatest: bool = bool::generate(driver)?;
                    Some($EPoint { p: $GAffine::get_point_from_x_unchecked(x.p, greatest)? })
                }
            }

            #[derive(Debug)]
            pub struct $GPoint {
                pub p: $GAffine,
            }

            impl TypeGenerator for $GPoint {
                fn generate<D: bolero::Driver>(driver: &mut D) -> Option<$GPoint> {
                    let curve_point = $EPoint::generate(driver)?;
                    Some($GPoint { p: curve_point.p.clear_cofactor() })
                }
            }

            #[derive(Debug)]
            pub struct $EnotGPoint {
                pub p: $GAffine,
            }

            impl TypeGenerator for $EnotGPoint {
                fn generate<D: bolero::Driver>(driver: &mut D) -> Option<$EnotGPoint> {
                    let p = $EPoint::generate(driver)?;
                    if p.p.is_in_correct_subgroup_assuming_on_curve() {
                        return None;
                    }
                    Some($EnotGPoint { p: p.p })
                }
            }

            impl $GOperations {
                fn check_multipoint_sum(ps: &Vec<(bool, $EPoint)>) {
                    let mut res3 = $GAffine::identity();

                    let mut points: Vec<(u8, $GAffine)> = vec![];
                    for i in 0..ps.len() {
                        points.push((ps[i].0 as u8, ps[i].1.p));

                        let mut current_point = points[i].1.clone();
                        if points[i].0 == 1 {
                            current_point = current_point.neg();
                        }

                        res3 = res3.add(&current_point).into();
                    }

                    let res1 = Self::get_sum_many_points(&points);

                    points.shuffle(&mut thread_rng());
                    let res2 = Self::get_sum_many_points(&points);
                    assert_eq!(res1, res2);

                    assert_eq!(res1, Self::serialize_uncompressed_g(&res3).to_vec());
                }

                fn decompress_p(ps: Vec<$GAffine>) -> Vec<u8> {
                    let mut ps_vec: Vec<Vec<u8>> = vec![vec![]];
                    for i in 0..ps.len() {
                        ps_vec.push(Self::serialize_g(&ps[i]));
                    }

                    run_bls12381_fn!($bls12381_decompress, ps_vec)
                }

                fn get_sum(p_sign: u8, p: &[u8], q_sign: u8, q: &[u8]) -> Vec<u8> {
                    let buffer = vec![vec![p_sign], p.to_vec(), vec![q_sign], q.to_vec()];
                    run_bls12381_fn!($bls12381_sum, buffer)
                }

                fn get_inverse(p: &[u8]) -> Vec<u8> {
                    let buffer = vec![vec![1], p.to_vec()];
                    run_bls12381_fn!($bls12381_sum, buffer)
                }

                fn get_sum_many_points(points: &Vec<(u8, $GAffine)>) -> Vec<u8> {
                    let mut buffer: Vec<Vec<u8>> = vec![];
                    for i in 0..points.len() {
                        buffer.push(vec![points[i].0]);
                        buffer.push(Self::serialize_uncompressed_g(&points[i].1).to_vec());
                    }
                    run_bls12381_fn!($bls12381_sum, buffer)
                }

                fn get_multiexp(points: &Vec<(Fr, $GAffine)>) -> Vec<u8> {
                    let mut buffer: Vec<Vec<u8>> = vec![];
                    for i in 0..points.len() {
                        buffer.push(Self::serialize_uncompressed_g(&points[i].1).to_vec());

                        let mut n_vec: [u8; 32] = [0u8; 32];
                        points[i].0.serialize_with_flags(n_vec.as_mut_slice(), EmptyFlags).unwrap();
                        buffer.push(n_vec.to_vec());
                    }

                    run_bls12381_fn!($bls12381_multiexp, buffer)
                }

                fn get_multiexp_small(points: &Vec<(u8, $GAffine)>) -> Vec<u8> {
                    let mut buffer: Vec<Vec<u8>> = vec![];
                    for i in 0..points.len() {
                        buffer.push(Self::serialize_uncompressed_g(&points[i].1).to_vec());
                        let mut n_vec: [u8; 32] = [0u8; 32];
                        n_vec[0] = points[i].0;
                        buffer.push(n_vec.to_vec());
                    }

                    run_bls12381_fn!($bls12381_multiexp, buffer)
                }

                fn get_multiexp_many_points(points: &Vec<(u8, $GAffine)>) -> Vec<u8> {
                    let mut buffer: Vec<Vec<u8>> = vec![];
                    for i in 0..points.len() {
                        buffer.push(Self::serialize_uncompressed_g(&points[i].1).to_vec());
                        if points[i].0 == 0 {
                            buffer.push(vec![vec![1], vec![0; 31]].concat());
                        } else {
                            buffer
                                .push(hex::decode(R_MINUS_1).unwrap().into_iter().rev().collect());
                        }
                    }

                    run_bls12381_fn!($bls12381_multiexp, buffer)
                }

                fn map_fp_to_g(fps: Vec<$Fq>) -> Vec<u8> {
                    let mut fp_vec: Vec<Vec<u8>> = vec![];

                    for i in 0..fps.len() {
                        fp_vec.push(Self::serialize_fp(&fps[i]));
                    }

                    run_bls12381_fn!($bls12381_map_fp_to_g, fp_vec)
                }

                fn get_incorrect_points(p: &$EPoint) -> Vec<Vec<u8>> {
                    let mut res: Vec<Vec<u8>> = vec![];

                    // Incorrect encoding of the point at infinity
                    let mut zero = get_zero(Self::POINT_LEN);
                    zero[Self::POINT_LEN - 1] = 1;
                    res.push(zero);

                    // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
                    let mut zero = vec![0u8; Self::POINT_LEN];
                    zero[0] = 192;
                    res.push(zero);

                    let mut p_ser = Self::serialize_uncompressed_g(&p.p);
                    p_ser[0] |= 0x80;
                    res.push(p_ser.to_vec());

                    // Point not on the curve
                    let mut p_ser = Self::serialize_uncompressed_g(&p.p);
                    p_ser[Self::POINT_LEN - 1] ^= 0x01;
                    res.push(p_ser.to_vec());

                    //Erroneous coding of field elements, resulting in a correct point on the curve if only the suffix is considered.
                    let mut p_ser = Self::serialize_uncompressed_g(&p.p);
                    p_ser[0] ^= 0x20;
                    res.push(p_ser.to_vec());

                    let p_ser = $add_p_y(&p.p).to_vec();
                    res.push(p_ser);

                    res
                }

                fn map_to_curve_g(fp: $Fq) -> $GAffine {
                    let wbmap =
                        WBMap::<<ark_bls12_381::Config as Bls12Config>::$GConfig>::new().unwrap();
                    let res = wbmap.map_to_curve(fp).unwrap();
                    if res.infinity {
                        return $GAffine::identity();
                    }

                    $GAffine::new_unchecked(res.x, res.y)
                }

                fn serialize_uncompressed_g(p: &$GAffine) -> Vec<u8> {
                    let mut serialized = vec![0u8; Self::POINT_LEN];
                    p.serialize_with_mode(serialized.as_mut_slice(), ark_serialize::Compress::No)
                        .unwrap();

                    serialized
                }

                fn serialize_g(p: &$GAffine) -> Vec<u8> {
                    let mut serialized = vec![0u8; Self::POINT_LEN / 2];
                    p.serialize_with_mode(serialized.as_mut_slice(), ark_serialize::Compress::Yes)
                        .unwrap();

                    serialized
                }

                fn deserialize_g(p: Vec<u8>) -> $GAffine {
                    $GAffine::deserialize_with_mode(
                        p.as_slice(),
                        ark_serialize::Compress::No,
                        ark_serialize::Validate::No,
                    )
                    .unwrap()
                }
            }
        };
    }

    impl_goperations!(
        G1Operations,
        Fq,
        FP,
        E1Point,
        G1Point,
        EnotG1Point,
        G1Config,
        G1Affine,
        add_p_y,
        bls12381_p1_decompress,
        bls12381_p1_sum,
        bls12381_g1_multiexp,
        bls12381_map_fp_to_g1
    );
    impl_goperations!(
        G2Operations,
        Fq2,
        FP2,
        E2Point,
        G2Point,
        EnotG2Point,
        G2Config,
        G2Affine,
        add2_p_y,
        bls12381_p2_decompress,
        bls12381_p2_sum,
        bls12381_g2_multiexp,
        bls12381_map_fp2_to_g2
    );

    fn get_zero(point_len: usize) -> Vec<u8> {
        let mut zero1 = vec![0; point_len];
        zero1[0] |= 0x40;
        zero1
    }

    fn pairing_check(p1s: Vec<G1Affine>, p2s: Vec<G2Affine>) -> u64 {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();

        let mut buffer: Vec<Vec<u8>> = vec![];
        for i in 0..p1s.len() {
            buffer.push(G1Operations::serialize_uncompressed_g(&p1s[i]).to_vec());
            buffer.push(G2Operations::serialize_uncompressed_g(&p2s[i]).to_vec());
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

    macro_rules! test_bls12381_sum {
        (
            $GOp:ident,
            $GPoint:ident,
            $EPoint:ident,
            $EnotGPoint:ident,
            $GAffine:ident,
            $bls12381_sum:ident,
            $check_sum:ident,
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
                let zero = get_zero($GOp::POINT_LEN);
                assert_eq!(zero.to_vec(), $GOp::get_sum(0, &zero, 0, &zero));

                // 0 + P = P + 0 = P
                bolero::check!().with_type().for_each(|p: &$GPoint| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);
                    assert_eq!(p_ser.to_vec(), $GOp::get_sum(0, &zero, 0, &p_ser));
                    assert_eq!(p_ser.to_vec(), $GOp::get_sum(0, &p_ser, 0, &zero));
                });

                // P + P
                // P + (-P) = (-P) + P =  0
                // P + (-(P + P))
                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);

                    let pmul2 = p.p.mul(Fr::from(2));
                    let pmul2_ser = $GOp::serialize_uncompressed_g(&pmul2.into_affine());
                    assert_eq!(pmul2_ser.to_vec(), $GOp::get_sum(0, &p_ser, 0, &p_ser));

                    let pneg = p.p.neg();
                    let p_neg_ser = $GOp::serialize_uncompressed_g(&pneg);

                    assert_eq!(zero.to_vec(), $GOp::get_sum(0, &p_neg_ser, 0, &p_ser));
                    assert_eq!(zero.to_vec(), $GOp::get_sum(0, &p_ser, 0, &p_neg_ser));

                    let pmul2neg = pmul2.neg();
                    let pmul2_neg = $GOp::serialize_uncompressed_g(&pmul2neg.into_affine());
                    assert_eq!(p_neg_ser.to_vec(), $GOp::get_sum(0, &p_ser, 0, &pmul2_neg));
                });
            }

            fn $check_sum(p: $GAffine, q: $GAffine) {
                let p_ser = $GOp::serialize_uncompressed_g(&p);
                let q_ser = $GOp::serialize_uncompressed_g(&q);

                // P + Q = Q + P
                let got1 = $GOp::get_sum(0, &p_ser, 0, &q_ser);
                let got2 = $GOp::get_sum(0, &q_ser, 0, &p_ser);
                assert_eq!(got1, got2);

                // compare with library results
                let psum = p.add(&q);
                let library_sum = $GOp::serialize_uncompressed_g(&psum.into_affine());

                assert_eq!(library_sum.to_vec(), got1);

                let p_inv = $GOp::get_inverse(&library_sum);
                let pneg = psum.neg();
                let p_neg_ser = $GOp::serialize_uncompressed_g(&pneg.into_affine());

                assert_eq!(p_neg_ser.to_vec(), p_inv);
            }

            #[test]
            fn $test_bls12381_sum() {
                bolero::check!().with_type().for_each(|(p, q): &($EPoint, $EPoint)| {
                    $check_sum(p.p, q.p);
                });

                bolero::check!().with_type().for_each(|(p, q): &($GPoint, $GPoint)| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);
                    let q_ser = $GOp::serialize_uncompressed_g(&q.p);

                    let got1 = $GOp::get_sum(0, &p_ser, 0, &q_ser);

                    let result_point = $GOp::deserialize_g(got1);
                    assert!(result_point.is_in_correct_subgroup_assuming_on_curve());
                });
            }

            #[test]
            fn $test_bls12381_sum_not_g_points() {
                //points not from G
                bolero::check!().with_type().for_each(|(p, q): &($EnotGPoint, $EnotGPoint)| {
                    $check_sum(p.p, q.p);
                });
            }

            #[test]
            fn $test_bls12381_sum_inverse() {
                let zero = get_zero($GOp::POINT_LEN);

                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);

                    // P - P = - P + P = 0
                    let got1 = $GOp::get_sum(1, &p_ser, 0, &p_ser);
                    let got2 = $GOp::get_sum(0, &p_ser, 1, &p_ser);
                    assert_eq!(got1, got2);
                    assert_eq!(got1, zero.to_vec());

                    // -(-P)
                    let p_inv = $GOp::get_inverse(&p_ser);
                    let p_inv_inv = $GOp::get_inverse(p_inv.as_slice());

                    assert_eq!(p_ser.to_vec(), p_inv_inv);
                });

                // P in G => -P in G
                bolero::check!().with_type().for_each(|p: &$GPoint| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);

                    let p_inv = $GOp::get_inverse(&p_ser);

                    let result_point = $GOp::deserialize_g(p_inv);
                    assert!(result_point.is_in_correct_subgroup_assuming_on_curve());
                });

                // -0
                let zero_inv = $GOp::get_inverse(&zero);
                assert_eq!(zero.to_vec(), zero_inv);
            }

            #[test]
            fn $test_bls12381_sum_many_points() {
                let zero = get_zero($GOp::POINT_LEN);
                //empty input
                let res = $GOp::get_sum_many_points(&vec![]);
                assert_eq!(zero.to_vec(), res);

                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<(bool, $EPoint)>>().with().len(0usize..$GOp::MAX_N_SUM),
                    )
                    .for_each(|ps: &Vec<(bool, $EPoint)>| {
                        $GOp::check_multipoint_sum(ps);
                    });

                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<(bool, $GPoint)>>().with().len(0usize..$GOp::MAX_N_SUM),
                    )
                    .for_each(|ps: &Vec<(bool, $GPoint)>| {
                        let mut points: Vec<(u8, $GAffine)> = vec![];
                        for i in 0..ps.len() {
                            points.push((ps[i].0 as u8, ps[i].1.p));
                        }

                        let res1 = $GOp::get_sum_many_points(&points);
                        let sum = $GOp::deserialize_g(res1);

                        assert!(sum.is_in_correct_subgroup_assuming_on_curve());
                    });
            }

            #[test]
            fn $test_bls12381_crosscheck_sum_and_multiexp() {
                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<(bool, $GPoint)>>()
                            .with()
                            .len(0usize..=$GOp::MAX_N_MULTIEXP),
                    )
                    .for_each(|ps: &Vec<(bool, $GPoint)>| {
                        let mut points: Vec<(u8, $GAffine)> = vec![];
                        for i in 0..ps.len() {
                            points.push((ps[i].0 as u8, ps[i].1.p));
                        }

                        let res1 = $GOp::get_sum_many_points(&points);
                        let res2 = $GOp::get_multiexp_many_points(&points);
                        assert_eq!(res1, res2);
                    });
            }

            #[test]
            fn $test_bls12381_sum_incorrect_input() {
                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    let mut test_vecs: Vec<Vec<Vec<u8>>> = $GOp::get_incorrect_points(p)
                        .into_iter()
                        .map(|test| vec![vec![0u8], test])
                        .collect();

                    // Incorrect sign encoding
                    test_vecs.push(vec![vec![2u8], get_zero($GOp::POINT_LEN)]);

                    for i in 0..test_vecs.len() {
                        run_bls12381_fn!($bls12381_sum, test_vecs[i], 1);
                    }
                });
            }
        };
    }

    test_bls12381_sum!(
        G1Operations,
        G1Point,
        E1Point,
        EnotG1Point,
        G1Affine,
        bls12381_p1_sum,
        check_sum_p1,
        test_bls12381_p1_sum_edge_cases_fuzzer,
        test_bls12381_p1_sum_fuzzer,
        test_bls12381_p1_sum_not_g1_points_fuzzer,
        test_bls12381_p1_sum_inverse_fuzzer,
        test_bls12381_p1_sum_many_points_fuzzer,
        test_bls12381_p1_crosscheck_sum_and_multiexp_fuzzer,
        test_bls12381_p1_sum_incorrect_input_fuzzer
    );
    test_bls12381_sum!(
        G2Operations,
        G2Point,
        E2Point,
        EnotG2Point,
        G2Affine,
        bls12381_p2_sum,
        check_sum_p2,
        test_bls12381_p2_sum_edge_cases_fuzzer,
        test_bls12381_p2_sum_fuzzer,
        test_bls12381_p2_sum_not_g2_points_fuzzer,
        test_bls12381_p2_sum_inverse_fuzzer,
        test_bls12381_p2_sum_many_points_fuzzer,
        test_bls12381_p2_crosscheck_sum_and_multiexp_fuzzer,
        test_bls12381_p2_sum_incorrect_input_fuzzer
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
            $GOp:ident,
            $GPoint:ident,
            $EPoint:ident,
            $EnotGPoint:ident,
            $GAffine:ident,
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
                bolero::check!()
                    .with_generator((
                        generator::gen::<$GPoint>(),
                        generator::gen::<usize>().with().bounds(0..=200),
                    ))
                    .for_each(|(p, n): &($GPoint, usize)| {
                        let points: Vec<(u8, $GAffine)> = vec![(0, p.p.clone()); *n];
                        let res1 = $GOp::get_sum_many_points(&points);
                        let res2 = $GOp::get_multiexp_small(&vec![(*n as u8, p.p.clone())]);

                        assert_eq!(res1, res2);
                        let res3 = p.p.mul(Fr::from(*n as u64));
                        assert_eq!(res1, $GOp::serialize_uncompressed_g(&res3.into()));
                    });

                bolero::check!().with_type().for_each(|(p, n): &($GPoint, Scalar)| {
                    let res1 = $GOp::get_multiexp(&vec![(n.p.clone(), p.p.clone())]);
                    let res2 = p.p.mul(&n.p);

                    assert_eq!(res1, $GOp::serialize_uncompressed_g(&res2.into()));
                });
            }

            #[test]
            fn $test_bls12381_multiexp_many_points() {
                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<(Scalar, $GPoint)>>()
                            .with()
                            .len(0usize..=$GOp::MAX_N_MULTIEXP),
                    )
                    .for_each(|ps: &Vec<(Scalar, $GPoint)>| {
                        let mut res2 = $GAffine::identity();
                        let mut points: Vec<(Fr, $GAffine)> = vec![];
                        for i in 0..ps.len() {
                            points.push((ps[i].0.p, ps[i].1.p));
                            res2 = res2.add(&points[i].1.mul(&points[i].0)).into();
                        }

                        let res1 = $GOp::get_multiexp(&points);
                        assert_eq!(res1, $GOp::serialize_uncompressed_g(&res2.into()));
                    });
            }

            #[test]
            fn $test_bls12381_multiexp_incorrect_input() {
                let zero_scalar = vec![0u8; 32];
                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    let test_vecs: Vec<Vec<Vec<u8>>> = $GOp::get_incorrect_points(p)
                        .into_iter()
                        .map(|test| vec![test, zero_scalar.clone()])
                        .collect();

                    for i in 0..test_vecs.len() {
                        run_bls12381_fn!($bls12381_multiexp, test_vecs[i], 1);
                    }
                });

                //points not from G
                bolero::check!().with_type().for_each(|p: &$EnotGPoint| {
                    let p_ser = $GOp::serialize_uncompressed_g(&p.p);
                    run_bls12381_fn!($bls12381_multiexp, vec![p_ser, zero_scalar.clone()], 1);
                });
            }

            #[test]
            fn $test_bls12381_multiexp_invariants_checks() {
                let zero1 = get_zero($GOp::POINT_LEN);
                let r = Fr::from_str(R).unwrap();

                bolero::check!()
                    .with_generator((
                        generator::gen::<$GPoint>(),
                        generator::gen::<Scalar>(),
                        generator::gen::<usize>().with().bounds(0..$GOp::MAX_N_MULTIEXP),
                    ))
                    .for_each(|(p, scalar, n): &($GPoint, Scalar, usize)| {
                        // group_order * P = 0
                        let res = $GOp::get_multiexp(&vec![(r.clone(), p.p.clone())]);
                        assert_eq!(res.as_slice(), zero1);

                        // (scalar + group_order) * P = scalar * P
                        let res1 = $GOp::get_multiexp(&vec![(scalar.p.clone(), p.p.clone())]);
                        let scalar_plus_r = scalar.p.add(&r);
                        let res2 = $GOp::get_multiexp(&vec![(scalar_plus_r.clone(), p.p.clone())]);
                        assert_eq!(res1, res2);

                        // P + P + ... + P = N * P
                        let res1 = $GOp::get_multiexp(&vec![(Fr::one(), p.p.clone()); *n]);
                        let res2 = $GOp::get_multiexp(&vec![(Fr::from(*n as u8), p.p.clone())]);
                        assert_eq!(res1, res2);

                        // 0 * P = 0
                        let res1 = $GOp::get_multiexp(&vec![(Fr::zero(), p.p.clone())]);
                        assert_eq!(res1, zero1);

                        // 1 * P = P
                        let res1 = $GOp::get_multiexp(&vec![(Fr::one(), p.p.clone())]);
                        assert_eq!(res1, $GOp::serialize_uncompressed_g(&p.p));
                    });
            }
        };
    }

    test_bls12381_multiexp!(
        G1Operations,
        G1Point,
        E1Point,
        EnotG1Point,
        G1Affine,
        bls12381_g1_multiexp,
        bls12381_p1_sum,
        test_bls12381_g1_multiexp_mul_fuzzer,
        test_bls12381_g1_multiexp_many_points_fuzzer,
        test_bls12381_g1_multiexp_incorrect_input_fuzzer,
        test_bls12381_g1_multiexp_invariants_checks_fuzzer,
        test_bls12381_error_g1_encoding
    );
    test_bls12381_multiexp!(
        G2Operations,
        G2Point,
        E2Point,
        EnotG2Point,
        G2Affine,
        bls12381_g2_multiexp,
        bls12381_p2_sum,
        test_bls12381_g2_multiexp_mul_fuzzer,
        test_bls12381_g2_multiexp_many_points_fuzzer,
        test_bls12381_g2_multiexp_incorrect_input_fuzzer,
        test_bls12381_g2_multiexp_invariants_checks_fuzzer,
        test_bls12381_error_g2_encoding
    );

    fn add_p_y(point: &G1Affine) -> Vec<u8> {
        let mut ybig: Fq = *point.y().unwrap();
        ybig = ybig.add(&Fq::from_str(P).unwrap());
        let p_ser = G1Operations::serialize_uncompressed_g(&point);
        let mut y_ser: Vec<u8> = vec![0u8; 48];
        ybig.serialize_with_flags(y_ser.as_mut_slice(), EmptyFlags).unwrap();

        [p_ser[..48].to_vec(), y_ser].concat()
    }

    fn add2_p_y(point: &G2Affine) -> Vec<u8> {
        let mut yabig = (*point.y().unwrap()).c1;
        yabig = yabig.add(&Fq::from_str(P).unwrap());
        let p_ser = G2Operations::serialize_uncompressed_g(&point);
        let mut y_ser: Vec<u8> = vec![0u8; 48];
        yabig.serialize_with_flags(y_ser.as_mut_slice(), EmptyFlags).unwrap();

        [p_ser[..96 + 48].to_vec(), y_ser].concat()
    }

    macro_rules! test_bls12381_map_fp_to_g {
        (
            $GOp:ident,
            $map_to_curve_g:ident,
            $Fq:ident,
            $FP:ident,
            $check_map_fp:ident,
            $test_bls12381_map_fp_to_g:ident,
            $test_bls12381_map_fp_to_g_many_points:ident
        ) => {
            fn $check_map_fp(fp: $Fq) {
                let res1 = $GOp::map_fp_to_g(vec![fp.clone()]);

                let mut res2 = $GOp::map_to_curve_g(fp);
                res2 = res2.clear_cofactor();

                assert_eq!(res1, $GOp::serialize_uncompressed_g(&res2));
            }

            #[test]
            fn $test_bls12381_map_fp_to_g() {
                bolero::check!().with_type().for_each(|fp: &$FP| {
                    $check_map_fp(fp.p);
                });
            }

            #[test]
            fn $test_bls12381_map_fp_to_g_many_points() {
                bolero::check!()
                    .with_generator(bolero::gen::<Vec<$FP>>().with().len(0usize..=$GOp::MAX_N_MAP))
                    .for_each(|fps: &Vec<$FP>| {
                        let mut fps_fq: Vec<$Fq> = vec![];
                        let mut res2_mul: Vec<u8> = vec![];
                        for i in 0..fps.len() {
                            fps_fq.push(fps[i].p);
                            let mut res2 = $GOp::map_to_curve_g(fps[i].p.clone());
                            res2 = res2.clear_cofactor();

                            res2_mul.append(&mut $GOp::serialize_uncompressed_g(&res2));
                        }

                        let res1 = $GOp::map_fp_to_g(fps_fq);
                        assert_eq!(res1, res2_mul);
                    });
            }
        };
    }

    test_bls12381_map_fp_to_g!(
        G1Operations,
        map_to_curve_g1,
        Fq,
        FP,
        check_map_fp,
        test_bls12381_map_fp_to_g1_fuzzer,
        test_bls12381_map_fp_to_g1_many_points_fuzzer
    );

    test_bls12381_map_fp_to_g!(
        G2Operations,
        map_to_curve_g2,
        Fq2,
        FP2,
        check_map_fp2,
        test_bls12381_map_fp2_to_g2_fuzzer,
        test_bls12381_map_fp2_to_g2_many_points_fuzzer
    );

    #[test]
    fn test_bls12381_map_fp_to_g1_edge_cases() {
        check_map_fp(Fq::ZERO);
        check_map_fp(Fq::from_str(P_MINUS_1).unwrap());
    }

    #[test]
    fn test_bls12381_map_fp_to_g1_incorrect_input() {
        let p = hex::decode(P_HEX).unwrap();
        run_bls12381_fn!(bls12381_map_fp_to_g1, [p], 1);
    }

    #[test]
    fn test_bls12381_map_fp2_to_g2_incorrect_input() {
        let p = hex::decode(P_HEX).unwrap();
        run_bls12381_fn!(bls12381_map_fp2_to_g2, [p.clone(), vec![0u8; 48]], 1);
        run_bls12381_fn!(bls12381_map_fp2_to_g2, [vec![0u8; 48], p], 1);
    }

    macro_rules! test_bls12381_decompress {
        (
            $GOp:ident,
            $GPoint:ident,
            $EPoint:ident,
            $GAffine:ident,
            $POINT_LEN:expr,
            $bls12381_decompress:ident,
            $add_p:ident,
            $test_bls12381_decompress:ident,
            $test_bls12381_decompress_many_points:ident,
            $test_bls12381_decompress_incorrect_input:ident
        ) => {
            #[test]
            fn $test_bls12381_decompress() {
                bolero::check!().with_type().for_each(|p1: &$GPoint| {
                    let res1 = $GOp::decompress_p(vec![p1.p.clone()]);
                    assert_eq!(res1, $GOp::serialize_uncompressed_g(&p1.p));

                    let p1_neg = p1.p.mul(&Fr::from(-1));
                    let res1_neg = $GOp::decompress_p(vec![p1_neg.clone().into()]);

                    assert_eq!(res1[0..$POINT_LEN], res1_neg[0..$POINT_LEN]);
                    assert_ne!(res1[$POINT_LEN..], res1_neg[$POINT_LEN..]);
                    assert_eq!(res1_neg, $GOp::serialize_uncompressed_g(&p1_neg.into()));
                });

                let zero1 = $GAffine::identity();
                let res1 = $GOp::decompress_p(vec![zero1.clone()]);

                assert_eq!(res1, $GOp::serialize_uncompressed_g(&zero1));
            }

            #[test]
            fn $test_bls12381_decompress_many_points() {
                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<$EPoint>>().with().len(0usize..=$GOp::MAX_N_DECOMPRESS),
                    )
                    .for_each(|es: &Vec<$EPoint>| {
                        let mut p1s: Vec<$GAffine> = vec![];
                        let mut res2: Vec<u8> = vec![];
                        for i in 0..es.len() {
                            p1s.push(es[i].p);
                            res2.append(&mut $GOp::serialize_uncompressed_g(&p1s[i]).to_vec());
                        }
                        let res1 = $GOp::decompress_p(p1s.clone());
                        assert_eq!(res1, res2);
                    });

                bolero::check!()
                    .with_generator(
                        bolero::gen::<Vec<$GPoint>>().with().len(0usize..=$GOp::MAX_N_DECOMPRESS),
                    )
                    .for_each(|gs: &Vec<$GPoint>| {
                        let mut p1s: Vec<$GAffine> = vec![];
                        let mut res2: Vec<u8> = vec![];
                        for i in 0..gs.len() {
                            p1s.push(gs[i].p);
                            res2.append(&mut $GOp::serialize_uncompressed_g(&p1s[i]).to_vec());
                        }
                        let res1 = $GOp::decompress_p(p1s.clone());
                        assert_eq!(res1, res2);
                    });
            }

            #[test]
            fn $test_bls12381_decompress_incorrect_input() {
                // Incorrect encoding of the point at infinity
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 0x80 | 0x40;
                zero[$POINT_LEN - 1] = 1;
                run_bls12381_fn!($bls12381_decompress, vec![zero], 1);

                // Erroneous coding of field elements with an incorrect extra bit in the decompressed encoding.
                let mut zero = vec![0u8; $POINT_LEN];
                zero[0] = 0x40;
                run_bls12381_fn!($bls12381_decompress, vec![zero], 1);

                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    let mut p_ser = $GOp::serialize_g(&p.p);
                    p_ser[0] ^= 0x80;
                    run_bls12381_fn!($bls12381_decompress, vec![p_ser], 1);
                });

                //Point with a coordinate larger than 'p'.
                bolero::check!().with_type().for_each(|p: &$EPoint| {
                    run_bls12381_fn!($bls12381_decompress, vec![$add_p(&p.p)], 1);
                });
            }
        };
    }

    test_bls12381_decompress!(
        G1Operations,
        G1Point,
        E1Point,
        G1Affine,
        48,
        bls12381_p1_decompress,
        add_p_x,
        test_bls12381_p1_decompress_fuzzer,
        test_bls12381_p1_decompress_many_points_fuzzer,
        test_bls12381_p1_decompress_incorrect_input_fuzzer
    );

    test_bls12381_decompress!(
        G2Operations,
        G2Point,
        E2Point,
        G2Affine,
        96,
        bls12381_p2_decompress,
        add2_p_x,
        test_bls12381_p2_decompress_fuzzer,
        test_bls12381_p2_decompress_many_points_fuzzer,
        test_bls12381_p2_decompress_incorrect_input_fuzzer
    );

    fn add_p_x(point: &G1Affine) -> Vec<u8> {
        let mut p_ser = G1Operations::serialize_g(&point);
        p_ser[0] |= 0x1f;
        p_ser
    }

    fn add2_p_x(point: &G2Affine) -> Vec<u8> {
        let mut p_ser = G2Operations::serialize_g(&point);
        p_ser[0] |= 0x1f;
        p_ser
    }

    #[test]
    fn test_bls12381_pairing_check_one_point_fuzzer() {
        bolero::check!().with_type().for_each(|(p1, p2): &(G1Point, G2Point)| {
            let zero1 = G1Affine::zero();
            let zero2 = G2Affine::zero();

            let v = Bls12_381::pairing(p1.p, zero2);
            assert!(v.is_zero());

            assert_eq!(pairing_check(vec![zero1], vec![zero2]), 0);
            assert_eq!(pairing_check(vec![zero1], vec![p2.p]), 0);
            assert_eq!(pairing_check(vec![p1.p], vec![zero2]), 0);
            assert_eq!(pairing_check(vec![p1.p], vec![p2.p]), 2);
        });
    }

    #[test]
    fn test_bls12381_pairing_check_two_points_fuzzer() {
        bolero::check!().with_type().for_each(
            |(p1, p2, s1, s2): &(G1Point, G2Point, Scalar, Scalar)| {
                let p1_neg = p1.p.neg();
                let p2_neg = p2.p.neg();

                assert_eq!(
                    pairing_check(
                        vec![p1.p, p1_neg],
                        vec![p2.p, p2.p]
                    ),
                    0
                );
                assert_eq!(
                    pairing_check(
                        vec![p1.p, p1.p],
                        vec![p2.p, p2_neg]
                    ),
                    0
                );
                assert_eq!(
                    pairing_check(
                        vec![p1.p, p1.p],
                        vec![p2.p, p2.p]
                    ),
                    2
                );

                assert_eq!(
                    pairing_check(
                        vec![p1.p.mul(&s1.p).into(), p1_neg.mul(&s2.p).into()],
                        vec![p2.p.mul(&s2.p).into(), p2.p.mul(&s1.p).into()]
                    ),
                    0
                );
                assert_eq!(
                    pairing_check(
                        vec![p1.p.mul(&s1.p).into(), p1.p.mul(&s2.p).into()],
                        vec![p2.p.mul(&s2.p).into(), p2_neg.mul(&s1.p).into()]
                    ),
                    0
                );
                assert_eq!(
                    pairing_check(
                        vec![p1.p.mul(&s1.p).into(), p1.p.mul(&s2.p).into()],
                        vec![p2_neg.mul(&s2.p).into(), p2_neg.mul(&s1.p).into()]
                    ),
                    2
                );
            },
        );
    }

    #[test]
    fn test_bls12381_pairing_check_many_points_fuzzer() {
        bolero::check!()
            .with_generator(
                bolero::gen::<Vec<(Scalar, Scalar)>>().with().len(0usize..MAX_N_PAIRING),
            )
            .for_each(|scalars: &Vec<(Scalar, Scalar)>| {
                let mut scalars_1: Vec<Fr> = vec![];
                let mut scalars_2: Vec<Fr> = vec![];

                let g1: G1Affine = G1Affine::generator();
                let g2: G2Affine = G2Affine::generator();

                let mut g1s: Vec<G1Affine> = vec![];
                let mut g2s: Vec<G2Affine> = vec![];

                let mut scalar_res = Fr::from(0);

                for i in 0..scalars.len() {
                    scalars_1.push(scalars[i].0.p);
                    scalars_2.push(scalars[i].1.p);

                    scalar_res = scalar_res.add(&scalars_1[i].mul(&scalars_2[i]));

                    g1s.push(g1.mul(&scalars_1[i]).into());
                    g2s.push(g2.mul(&scalars_2[i]).into());
                }

                if !scalar_res.is_zero() {
                    assert_eq!(pairing_check(g1s.clone(), g2s.clone()), 2);
                } else {
                    assert_eq!(pairing_check(g1s.clone(), g2s.clone()), 0);
                }

                for i in 0..scalars.len() {
                    let mut p2 = g2.mul(&scalars_1[i]).into_affine();
                    p2 = p2.neg();

                    g1s.push(g1.mul(&scalars_2[i]).into());
                    g2s.push(p2);
                }

                assert_eq!(pairing_check(g1s, g2s), 0);
            });
    }

    #[test]
    fn test_bls12381_pairing_incorrect_input_point_fuzzer() {
        bolero::check!().with_type().for_each(
            |(p1_not_from_g1, p2, p1, p2_not_from_g2, curve_p1, curve_p2): &(
                EnotG1Point,
                G2Point,
                G1Point,
                EnotG2Point,
                E1Point,
                E2Point,
            )| {
                assert_eq!(pairing_check(vec![p1_not_from_g1.p], vec![p2.p]), 1);
                assert_eq!(pairing_check(vec![p1.p], vec![p2_not_from_g2.p]), 1);

                let p1_ser = G1Operations::serialize_uncompressed_g(&p1.p).to_vec();
                let p2_ser = G2Operations::serialize_uncompressed_g(&p2.p).to_vec();
                let test_vecs: Vec<Vec<u8>> = G1Operations::get_incorrect_points(curve_p1);
                for i in 0..test_vecs.len() {
                    assert_eq!(pairing_check_vec(test_vecs[i].clone(), p2_ser.clone()), 1);
                }

                let test_vecs: Vec<Vec<u8>> = G2Operations::get_incorrect_points(curve_p2);
                for i in 0..test_vecs.len() {
                    assert_eq!(pairing_check_vec(p1_ser.clone(), test_vecs[i].clone()), 1);
                }

                // not G1 point
                let p_ser = G1Operations::serialize_uncompressed_g(&p1_not_from_g1.p);
                assert_eq!(
                    pairing_check_vec(
                        p_ser.to_vec(),
                        G2Operations::serialize_uncompressed_g(&p2.p).to_vec()
                    ),
                    1
                );

                // not G2 point
                let p_ser = G2Operations::serialize_uncompressed_g(&p2_not_from_g2.p);
                assert_eq!(
                    pairing_check_vec(
                        G1Operations::serialize_uncompressed_g(&p1.p).to_vec(),
                        p_ser.to_vec()
                    ),
                    1
                );
            },
        );
    }

    #[test]
    fn test_bls12381_empty_input() {
        assert_eq!(get_zero(96), G1Operations::get_multiexp_many_points(&vec![]));
        assert_eq!(get_zero(192), G2Operations::get_multiexp_many_points(&vec![]));
        assert_eq!(G1Operations::map_fp_to_g(vec![]).len(), 0);
        assert_eq!(G2Operations::map_fp_to_g(vec![]).len(), 0);
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
        [
            fix_eip2537_g1(input[..128].to_vec()).to_vec(),
            fix_eip2537_g2(input[128..].to_vec()).to_vec(),
        ]
        .concat()
    }

    fn fix_eip2537_fp(fp: Vec<u8>) -> Vec<u8> {
        fp[16..].to_vec()
    }

    fn fix_eip2537_fp2(fp2: Vec<u8>) -> Vec<u8> {
        [fp2[64 + 16..].to_vec(), fp2[16..64].to_vec()].concat()
    }

    macro_rules! fix_eip2537_input {
        ($namespace_name:ident, $fix_eip2537_fp:ident) => {
            mod $namespace_name {
                use crate::logic::tests::bls12381::tests::$fix_eip2537_fp;

                pub fn fix_eip2537_g(g: Vec<u8>) -> Vec<u8> {
                    let mut res = [
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

    macro_rules! run_bls12381_fn_raw {
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

    run_bls12381_fn_raw!(run_map_fp_to_g1, map_fp_to_g1_return_value, bls12381_map_fp_to_g1);
    run_bls12381_fn_raw!(run_map_fp2_to_g2, map_fp2tog2_return_value, bls12381_map_fp2_to_g2);
    run_bls12381_fn_raw!(run_sum_g1, sum_g1_return_value, bls12381_p1_sum);
    run_bls12381_fn_raw!(run_sum_g2, sum_g2_return_value, bls12381_p2_sum);
    run_bls12381_fn_raw!(run_multiexp_g1, multiexp_g1_return_value, bls12381_g1_multiexp);
    run_bls12381_fn_raw!(run_multiexp_g2, multiexp_g2_return_value, bls12381_g2_multiexp);
    run_bls12381_fn_raw!(decompress_g1, decompress_g1_return_value, bls12381_p1_decompress);
    run_bls12381_fn_raw!(decompress_g2, decompress_g2_return_value, bls12381_p2_decompress);
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

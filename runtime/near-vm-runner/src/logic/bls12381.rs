use crate::logic::{HostError, VMLogicError};
use std::ptr::null;

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

const BLS_BOOL_SIZE: usize = 1;
const BLS_SCALAR_SIZE: usize = 32;
const BLS_FP_SIZE: usize = 48;
const BLS_FP2_SIZE: usize = 96;
const BLS_P1_SIZE: usize = 96;
const BLS_P2_SIZE: usize = 192;
const BLS_P1_COMPRESS_SIZE: usize = 48;
const BLS_P2_COMPRESS_SIZE: usize = 96;

#[macro_export]
macro_rules! bls12381_impl {
    (
        $doc:expr,
        $fn_name:ident,
        $ITEM_SIZE:expr,
        $bls12381_base:ident,
        $bls12381_element:ident,
        $impl_fn_name:ident
    ) => {
        #[doc = $doc]
        #[cfg(feature = "protocol_feature_bls12381")]
        pub fn $fn_name(
            &mut self,
            value_len: u64,
            value_ptr: u64,
            register_id: u64,
        ) -> Result<u64> {
            self.gas_counter.pay_base($bls12381_base)?;

            let elements_count = value_len / $ITEM_SIZE;
            self.gas_counter.pay_per($bls12381_element, elements_count as u64)?;

            let data = get_memory_or_register!(self, value_ptr, value_len)?;
            let (status, res) = super::bls12381::$impl_fn_name(&data)?;

            if status != 0 {
                return Ok(status);
            }

            self.registers.set(
                &mut self.gas_counter,
                &self.config.limit_config,
                register_id,
                res.as_slice(),
            )?;

            Ok(0)
        }
    };
}

#[macro_export]
macro_rules! bls12381_fn {
    (
        $p_sum:ident,
        $g_multiexp:ident,
        $p_decompress:ident,
        $map_fp_to_g:ident,
        $BLS_P_SIZE:ident,
        $BLS_FP_SIZE:ident,
        $BLS_P_COMPRESS_SIZE:ident,
        $blst_p:ident,
        $blst_p_affine:ident,
        $blst_p_deserialize:ident,
        $blst_p_from_affine:ident,
        $blst_p_cneg:ident,
        $blst_p_add_or_double:ident,
        $blst_p_to_affine:ident,
        $blst_p_affine_serialize:ident,
        $blst_p_in_g:ident,
        $blst_p_mult:ident,
        $read_fp_point:ident,
        $blst_map_to_g:ident,
        $PubKeyOrSig:ident,
        $parse_p:ident,
        $serialize_p:ident,
        $bls12381_p:expr,
        $bls12381_map_fp_to_g:expr
    ) => {
        fn $parse_p(point_data: &[u8]) -> Option<blst::$blst_p> {
            if point_data[0] & 0x80 != 0 {
                return None;
            }

            let mut pk_aff = blst::$blst_p_affine::default();
            let error_code = unsafe { blst::$blst_p_deserialize(&mut pk_aff, point_data.as_ptr()) };
            if error_code != blst::BLST_ERROR::BLST_SUCCESS {
                return None;
            }

            let mut pk = blst::$blst_p::default();
            unsafe {
                blst::$blst_p_from_affine(&mut pk, &pk_aff);
            }
            Some(pk)
        }

        fn $serialize_p(res_pk: &blst::$blst_p) -> Vec<u8> {
            let mut res_affine = blst::$blst_p_affine::default();

            unsafe {
                blst::$blst_p_to_affine(&mut res_affine, res_pk);
            }

            let mut res = [0u8; $BLS_P_SIZE];
            unsafe {
                blst::$blst_p_affine_serialize(res.as_mut_ptr(), &res_affine);
            }

            res.to_vec()
        }

        pub(super) fn $p_sum(data: &[u8]) -> Result<(u64, Vec<u8>)> {
            const ITEM_SIZE: usize = BLS_BOOL_SIZE + $BLS_P_SIZE;
            check_input_size(data, ITEM_SIZE, &format!("{}_sum", $bls12381_p))?;

            let mut res_pk = blst::$blst_p::default();

            for item_data in data.chunks_exact(ITEM_SIZE) {
                let (sign_data, point_data) = item_data.split_at(BLS_BOOL_SIZE);
                debug_assert_eq!(point_data.len(), $BLS_P_SIZE);

                let mut pk = match $parse_p(point_data) {
                    Some(pk) => pk,
                    None => { return Ok((1, vec![])) }
                };

                let sign = sign_data[0];

                if sign == 1 {
                    unsafe {
                        blst::$blst_p_cneg(&mut pk, true);
                    }
                } else if sign != 0 {
                    return Ok((1, vec![]));
                }

                unsafe {
                    blst::$blst_p_add_or_double(&mut res_pk, &res_pk, &pk);
                }
            }

            Ok((0, $serialize_p(&res_pk)))
        }

        pub(super) fn $g_multiexp(data: &[u8]) -> Result<(u64, Vec<u8>)> {
            const ITEM_SIZE: usize = $BLS_P_SIZE + BLS_SCALAR_SIZE;
            check_input_size(data, ITEM_SIZE, &format!("{}_multiexp", $bls12381_p))?;

            let mut res_pk = blst::$blst_p::default();

            for item_data in data.chunks_exact(ITEM_SIZE) {
                let (point_data, scalar_data) = item_data.split_at($BLS_P_SIZE);
                debug_assert_eq!(scalar_data.len(), BLS_SCALAR_SIZE);

                let pk = match $parse_p(point_data) {
                    Some(pk) => pk,
                    None => { return Ok((1, vec![])) }
                };

                if unsafe { blst::$blst_p_in_g(&pk) } != true {
                    return Ok((1, vec![]));
                }

                let mut pk_mul = blst::$blst_p::default();
                unsafe {
                    blst::$blst_p_mult(&mut pk_mul, &pk, scalar_data.as_ptr(), BLS_SCALAR_SIZE * 8);
                }

                unsafe {
                    blst::$blst_p_add_or_double(&mut res_pk, &res_pk, &pk_mul);
                }
            }

            Ok((0, $serialize_p(&res_pk)))
        }

        pub(super) fn $p_decompress(data: &[u8]) -> Result<(u64, Vec<u8>)> {
            const ITEM_SIZE: usize = $BLS_P_COMPRESS_SIZE;
            check_input_size(data, ITEM_SIZE, &format!("{}_decompress", $bls12381_p))?;
            let elements_count = data.len() / ITEM_SIZE;

            let mut res = Vec::<u8>::with_capacity(elements_count * $BLS_P_SIZE);

            for item_data in data.chunks_exact(ITEM_SIZE) {
                let pk_res = blst::min_pk::$PubKeyOrSig::uncompress(item_data);
                let pk_ser = if let Ok(pk) = pk_res {
                    pk.serialize()
                } else {
                    return Ok((1, vec![]));
                };

                res.extend_from_slice(pk_ser.as_slice());
            }

            Ok((0, res))
        }

        pub(super) fn $map_fp_to_g(data: &[u8]) -> Result<(u64, Vec<u8>)> {
            const ITEM_SIZE: usize = $BLS_FP_SIZE;
            check_input_size(data, ITEM_SIZE, $bls12381_map_fp_to_g)?;
            let elements_count: usize = data.len() / ITEM_SIZE;

            let mut res_concat: Vec<u8> = Vec::with_capacity($BLS_P_SIZE * elements_count);

            for item_data in data.chunks_exact(ITEM_SIZE) {
                let fp_point = match $read_fp_point(item_data) {
                    Some(fp_point) => fp_point,
                    None => return Ok((1, vec![])),
                };

                let mut g_point = blst::$blst_p::default();
                unsafe {
                    blst::$blst_map_to_g(&mut g_point, &fp_point, null());
                }

                let mut res = $serialize_p(&g_point);
                res_concat.append(&mut res);
            }

            Ok((0, res_concat))
        }
    };
}

bls12381_fn!(
    p1_sum,
    g1_multiexp,
    p1_decompress,
    map_fp_to_g1,
    BLS_P1_SIZE,
    BLS_FP_SIZE,
    BLS_P1_COMPRESS_SIZE,
    blst_p1,
    blst_p1_affine,
    blst_p1_deserialize,
    blst_p1_from_affine,
    blst_p1_cneg,
    blst_p1_add_or_double,
    blst_p1_to_affine,
    blst_p1_affine_serialize,
    blst_p1_in_g1,
    blst_p1_mult,
    read_fp_point,
    blst_map_to_g1,
    PublicKey,
    parse_p1,
    serialize_p1,
    "bls12381_p1",
    "bls12381_map_fp_to_g1"
);

bls12381_fn!(
    p2_sum,
    g2_multiexp,
    p2_decompress,
    map_fp2_to_g2,
    BLS_P2_SIZE,
    BLS_FP2_SIZE,
    BLS_P2_COMPRESS_SIZE,
    blst_p2,
    blst_p2_affine,
    blst_p2_deserialize,
    blst_p2_from_affine,
    blst_p2_cneg,
    blst_p2_add_or_double,
    blst_p2_to_affine,
    blst_p2_affine_serialize,
    blst_p2_in_g2,
    blst_p2_mult,
    read_fp2_point,
    blst_map_to_g2,
    Signature,
    parse_p2,
    serialize_p2,
    "bls12381_p2",
    "bls12381_map_fp2_to_g2"
);

pub(super) fn pairing_check(data: &[u8]) -> Result<u64> {
    const ITEM_SIZE: usize = BLS_P1_SIZE + BLS_P2_SIZE;
    check_input_size(data, ITEM_SIZE, "bls12381_pairing_check")?;
    let elements_count = data.len() / ITEM_SIZE;

    let mut blst_g1_list: Vec<blst::blst_p1_affine> =
        vec![blst::blst_p1_affine::default(); elements_count];
    let mut blst_g2_list: Vec<blst::blst_p2_affine> =
        vec![blst::blst_p2_affine::default(); elements_count];

    for (i, item_data) in data.chunks_exact(ITEM_SIZE).enumerate() {
        let (point1_data, point2_data) = item_data.split_at(BLS_P1_SIZE);
        debug_assert_eq!(point2_data.len(), BLS_P2_SIZE);

        if point1_data[0] & 0x80 != 0 {
            return Ok(1);
        }

        let error_code =
            unsafe { blst::blst_p1_deserialize(&mut blst_g1_list[i], point1_data.as_ptr()) };

        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok(1);
        }

        let g1_check = unsafe { blst::blst_p1_affine_in_g1(&blst_g1_list[i]) };
        if g1_check == false {
            return Ok(1);
        }

        if point2_data[0] & 0x80 != 0 {
            return Ok(1);
        }

        let error_code =
            unsafe { blst::blst_p2_deserialize(&mut blst_g2_list[i], point2_data.as_ptr()) };
        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok(1);
        }

        let g2_check = unsafe { blst::blst_p2_affine_in_g2(&blst_g2_list[i]) };
        if g2_check == false {
            return Ok(1);
        }
    }

    let mut pairing_fp12 = blst::blst_fp12::default();
    for i in 0..elements_count {
        pairing_fp12 *= blst::blst_fp12::miller_loop(&blst_g2_list[i], &blst_g1_list[i]);
    }
    pairing_fp12 = pairing_fp12.final_exp();

    let pairing_res = unsafe { blst::blst_fp12_is_one(&pairing_fp12) };

    if pairing_res {
        Ok(0)
    } else {
        Ok(2)
    }
}

fn read_fp_point(item_data: &[u8]) -> Option<blst::blst_fp> {
    let mut fp_point = blst::blst_fp::default();
    unsafe {
        blst::blst_fp_from_bendian(&mut fp_point, item_data.as_ptr());
    }

    let mut fp_row: [u8; BLS_FP_SIZE] = [0u8; BLS_FP_SIZE];
    unsafe {
        blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &fp_point);
    }

    for j in 0..BLS_FP_SIZE {
        if fp_row[j] != item_data[j] {
            return None;
        }
    }

    Some(fp_point)
}

fn read_fp2_point(item_data: &[u8]) -> Option<blst::blst_fp2> {
    let mut c_fp1 = [blst::blst_fp::default(); 2];

    unsafe {
        blst::blst_fp_from_bendian(&mut c_fp1[1], item_data[..BLS_FP_SIZE].as_ptr());
        blst::blst_fp_from_bendian(&mut c_fp1[0], item_data[BLS_FP_SIZE..].as_ptr());
    }

    let mut fp_row: [u8; BLS_FP_SIZE] = [0u8; BLS_FP_SIZE];
    unsafe {
        blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &c_fp1[0]);
    }

    for j in BLS_FP_SIZE..BLS_FP2_SIZE {
        if fp_row[j - BLS_FP_SIZE] != item_data[j] {
            return None;
        }
    }

    unsafe {
        blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &c_fp1[1]);
    }

    for j in 0..BLS_FP_SIZE {
        if fp_row[j] != item_data[j] {
            return None;
        }
    }

    Some(blst::blst_fp2 { fp: c_fp1 })
}

fn check_input_size(data: &[u8], item_size: usize, fn_name: &str) -> Result<()> {
    if data.len() % item_size != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                        "Incorrect input length for {}: {} is not divisible by {}",
                        fn_name,
                        data.len(),
                        item_size
                    ),
        }
            .into());
    }

    Ok(())
}

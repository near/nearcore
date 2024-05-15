use crate::logic::{HostError, VMLogicError};
use std::ptr::null;

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

const BLS_BOOL_SIZE: usize = 1;
const BLS_SCALAR_SIZE: usize = 32;
const BLS_FP_SIZE: usize = 48;
const BLS_FP2_SIZE: usize = 96;
const BLS_P1_SIZE: usize = 96;
const BLS_P2_SIZE: usize = 192;

pub(super) fn p1_sum(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_BOOL_SIZE + BLS_P1_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p1_sum: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let mut res_pk = blst::blst_p1::default();

    for item_data in data.chunks_exact(ITEM_SIZE) {
        let (sign_data, point_data) = item_data.split_at(BLS_BOOL_SIZE);
        debug_assert_eq!(point_data.len(), BLS_P1_SIZE);

        if point_data[0] & 0x80 != 0 {
            return Ok((1, vec![]));
        }

        let mut pk_aff = blst::blst_p1_affine::default();
        let error_code = unsafe { blst::blst_p1_deserialize(&mut pk_aff, point_data.as_ptr()) };

        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok((1, vec![]));
        }

        let mut pk = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_from_affine(&mut pk, &pk_aff);
        }

        if unsafe { blst::blst_p1_in_g1(&pk) } != true {
            return Ok((1, vec![]));
        }

        let sign = sign_data[0];
        if sign == 1 {
            unsafe {
                blst::blst_p1_cneg(&mut pk, true);
            }
        } else if sign != 0 {
            return Ok((1, vec![]));
        }

        unsafe {
            blst::blst_p1_add_or_double(&mut res_pk, &res_pk, &pk);
        }
    }

    let mut res_affine = blst::blst_p1_affine::default();

    unsafe {
        blst::blst_p1_to_affine(&mut res_affine, &res_pk);
    }

    let mut res = [0u8; BLS_P1_SIZE];
    unsafe {
        blst::blst_p1_affine_serialize(res.as_mut_ptr(), &res_affine);
    }

    Ok((0, res.to_vec()))
}

pub(super) fn p2_sum(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_BOOL_SIZE + BLS_P2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p2_sum: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }
    let mut res_pk = blst::blst_p2::default();

    for item_data in data.chunks_exact(ITEM_SIZE) {
        let (sign_data, point_data) = item_data.split_at(BLS_BOOL_SIZE);
        debug_assert_eq!(point_data.len(), BLS_P2_SIZE);

        if point_data[0] & 0x80 != 0 {
            return Ok((1, vec![]));
        }

        let mut pk_aff = blst::blst_p2_affine::default();
        let error_code = unsafe { blst::blst_p2_deserialize(&mut pk_aff, point_data.as_ptr()) };

        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok((1, vec![]));
        }

        let mut pk = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_from_affine(&mut pk, &pk_aff);
        }

        if unsafe { blst::blst_p2_in_g2(&pk) } != true {
            return Ok((1, vec![]));
        }

        let sign = sign_data[0];
        if sign == 1 {
            unsafe {
                blst::blst_p2_cneg(&mut pk, true);
            }
        } else if sign != 0 {
            return Ok((1, vec![]));
        }

        unsafe {
            blst::blst_p2_add_or_double(&mut res_pk, &res_pk, &pk);
        }
    }

    let mut res_affine = blst::blst_p2_affine::default();

    unsafe {
        blst::blst_p2_to_affine(&mut res_affine, &res_pk);
    }

    let mut res = [0u8; BLS_P2_SIZE];
    unsafe {
        blst::blst_p2_affine_serialize(res.as_mut_ptr(), &res_affine);
    }

    Ok((0, res.to_vec()))
}

pub(super) fn p1_multiexp(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_SCALAR_SIZE + BLS_P1_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p1_multiexp: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let mut res_pk = blst::blst_p1::default();

    for item_data in data.chunks_exact(ITEM_SIZE) {
        let (point_data, scalar_data) = item_data.split_at(BLS_P1_SIZE);
        debug_assert_eq!(scalar_data.len(), BLS_SCALAR_SIZE);

        if point_data[0] & 0x80 != 0 {
            return Ok((1, vec![]));
        }

        let mut pk_aff = blst::blst_p1_affine::default();
        let error_code = unsafe { blst::blst_p1_deserialize(&mut pk_aff, point_data.as_ptr()) };

        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok((1, vec![]));
        }

        let mut pk = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_from_affine(&mut pk, &pk_aff);
        }

        if unsafe { blst::blst_p1_in_g1(&pk) } != true {
            return Ok((1, vec![]));
        }

        let mut pk_mul = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_unchecked_mult(
                &mut pk_mul,
                &pk,
                scalar_data.as_ptr(),
                BLS_SCALAR_SIZE * 8,
            );
        }

        unsafe {
            blst::blst_p1_add_or_double(&mut res_pk, &res_pk, &pk_mul);
        }
    }

    let mut res_affine = blst::blst_p1_affine::default();

    unsafe {
        blst::blst_p1_to_affine(&mut res_affine, &res_pk);
    }

    let mut res = [0u8; BLS_P1_SIZE];
    unsafe {
        blst::blst_p1_affine_serialize(res.as_mut_ptr(), &res_affine);
    }

    Ok((0, res.to_vec()))
}

pub(super) fn p2_multiexp(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_SCALAR_SIZE + BLS_P2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p2_multiexp: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let mut res_pk = blst::blst_p2::default();
    for item_data in data.chunks_exact(ITEM_SIZE) {
        let (point_data, scalar_data) = item_data.split_at(BLS_P2_SIZE);
        debug_assert_eq!(scalar_data.len(), BLS_SCALAR_SIZE);

        if point_data[0] & 0x80 != 0 {
            return Ok((1, vec![]));
        }

        let mut pk_aff = blst::blst_p2_affine::default();
        let error_code = unsafe { blst::blst_p2_deserialize(&mut pk_aff, point_data.as_ptr()) };

        if error_code != blst::BLST_ERROR::BLST_SUCCESS {
            return Ok((1, vec![]));
        }

        let mut pk = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_from_affine(&mut pk, &pk_aff);
        }

        if unsafe { blst::blst_p2_in_g2(&pk) } != true {
            return Ok((1, vec![]));
        }

        let mut pk_mul = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_unchecked_mult(
                &mut pk_mul,
                &pk,
                scalar_data.as_ptr(),
                BLS_SCALAR_SIZE * 8,
            );
        }

        unsafe {
            blst::blst_p2_add_or_double(&mut res_pk, &res_pk, &pk_mul);
        }
    }

    let mut mul_res_affine = blst::blst_p2_affine::default();

    unsafe {
        blst::blst_p2_to_affine(&mut mul_res_affine, &res_pk);
    }

    let mut res = [0u8; BLS_P2_SIZE];
    unsafe {
        blst::blst_p2_affine_serialize(res.as_mut_ptr(), &mul_res_affine);
    }

    Ok((0, res.to_vec()))
}

pub(super) fn map_fp_to_g1(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_FP_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_map_fp_to_g1: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let mut fp_point = blst::blst_fp::default();

    let elements_count: usize = data.len() / ITEM_SIZE;

    let mut res_concat: Vec<u8> = Vec::with_capacity(BLS_P1_SIZE * elements_count);

    for item_data in data.chunks_exact(ITEM_SIZE) {
        unsafe {
            blst::blst_fp_from_bendian(&mut fp_point, item_data.as_ptr());
        }

        let mut fp_row: [u8; BLS_FP_SIZE] = [0u8; BLS_FP_SIZE];
        unsafe {
            blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &fp_point);
        }

        for j in 0..BLS_FP_SIZE {
            if fp_row[j] != item_data[j] {
                return Ok((1, vec![]));
            }
        }

        let mut g1_point = blst::blst_p1::default();
        unsafe {
            blst::blst_map_to_g1(&mut g1_point, &fp_point, null());
        }

        let mut mul_res_affine = blst::blst_p1_affine::default();

        unsafe {
            blst::blst_p1_to_affine(&mut mul_res_affine, &g1_point);
        }

        let mut res = [0u8; BLS_P1_SIZE];
        unsafe {
            blst::blst_p1_affine_serialize(res.as_mut_ptr(), &mul_res_affine);
        }

        res_concat.append(&mut res.to_vec());
    }

    Ok((0, res_concat))
}

pub(super) fn map_fp2_to_g2(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = BLS_FP2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_map_fp2_to_g2: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let elements_count: usize = data.len() / ITEM_SIZE;

    let mut res_concat: Vec<u8> = Vec::with_capacity(BLS_P2_SIZE * elements_count);

    for item_data in data.chunks_exact(ITEM_SIZE) {
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
                return Ok((1, vec![]));
            }
        }

        unsafe {
            blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &c_fp1[1]);
        }

        for j in 0..BLS_FP_SIZE {
            if fp_row[j] != item_data[j] {
                return Ok((1, vec![]));
            }
        }

        let fp2_point: blst::blst_fp2 = blst::blst_fp2 { fp: c_fp1 };

        let mut g2_point = blst::blst_p2::default();
        unsafe {
            blst::blst_map_to_g2(&mut g2_point, &fp2_point, null());
        }

        let mut mul_res_affine = blst::blst_p2_affine::default();

        unsafe {
            blst::blst_p2_to_affine(&mut mul_res_affine, &g2_point);
        }

        let mut res = [0u8; BLS_P2_SIZE];
        unsafe {
            blst::blst_p2_affine_serialize(res.as_mut_ptr(), &mul_res_affine);
        }

        res_concat.append(&mut res.to_vec());
    }

    Ok((0, res_concat))
}

pub(super) fn pairing_check(data: &[u8]) -> Result<u64> {
    const ITEM_SIZE: usize = BLS_P1_SIZE + BLS_P2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_pairing_check: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let elements_count = data.len() / ITEM_SIZE;

    let mut blst_g1_list: Vec<blst::blst_p1_affine> =
        vec![blst::blst_p1_affine::default(); elements_count];
    let mut blst_g2_list: Vec<blst::blst_p2_affine> =
        vec![blst::blst_p2_affine::default(); elements_count];

    for (item_data, i) in data.chunks_exact(ITEM_SIZE).zip(0..elements_count) {
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

pub(super) fn p1_decompress(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = 48;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p1_decompress: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let elements_count = data.len() / ITEM_SIZE;
    let mut res = Vec::<u8>::with_capacity(elements_count * BLS_P1_SIZE);

    for item_data in data.chunks_exact(ITEM_SIZE) {
        let pk_res = blst::min_pk::PublicKey::uncompress(item_data);
        let pk_ser = if let Ok(pk) = pk_res {
            pk.serialize()
        } else {
            return Ok((1, vec![]));
        };

        res.extend_from_slice(pk_ser.as_slice());
    }

    Ok((0, res))
}

pub(super) fn p2_decompress(data: &[u8]) -> Result<(u64, Vec<u8>)> {
    const ITEM_SIZE: usize = 96;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p2_decompress: {} is not divisible by {}",
                data.len(),
                ITEM_SIZE
            ),
        }
        .into());
    }

    let elements_count = data.len() / ITEM_SIZE;
    let mut res = Vec::<u8>::with_capacity(elements_count * BLS_P2_SIZE);

    for item_data in data.chunks_exact(ITEM_SIZE) {
        let sig_res = blst::min_pk::Signature::uncompress(item_data);
        let sig_ser = if let Ok(sig) = sig_res {
            sig.serialize()
        } else {
            return Ok((1, vec![]));
        };

        res.extend_from_slice(sig_ser.as_slice());
    }

    Ok((0, res))
}

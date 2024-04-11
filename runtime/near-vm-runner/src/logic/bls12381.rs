use crate::logic::{HostError, VMLogicError};
use std::ptr::null;

pub type Result<T, E = VMLogicError> = ::std::result::Result<T, E>;

pub(super) fn p1_sum(
    data: &[u8]
) -> Result<(u64, [u8; 96])> {
    const BLS_BOOL_SIZE: usize = 1;
    const BLS_P1_SIZE: usize = 96;
    const ITEM_SIZE: usize = BLS_BOOL_SIZE + BLS_P1_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p1_sum: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }
            .into());
    }

    let mut res_pk = blst::blst_p1::default();
    let elements_count = data.len() / ITEM_SIZE;

    for i in 0..elements_count {
        let mut pk_aff = blst::blst_p1_affine::default();
        let error_code = unsafe {
            blst::blst_p1_deserialize(
                &mut pk_aff,
                data[i * (BLS_BOOL_SIZE + BLS_P1_SIZE) + BLS_BOOL_SIZE
                    ..(i + BLS_BOOL_SIZE) * ITEM_SIZE]
                    .as_ptr(),
            )
        };

        if (error_code != blst::BLST_ERROR::BLST_SUCCESS)
            || (data[i * ITEM_SIZE + BLS_BOOL_SIZE] & 0x80 != 0)
        {
            return Ok((1, [0; 96]));
        }

        let mut pk = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_from_affine(&mut pk, &pk_aff);
        }

        let sign = data[i * ITEM_SIZE];
        if sign == 1 {
            unsafe {
                blst::blst_p1_cneg(&mut pk, true);
            }
        } else if sign != 0 {
            return Ok((1, [0; 96]));
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

    Ok((0, res))
}

pub(super) fn p2_sum(
    data: &[u8]
) -> Result<(u64, [u8; 192])>  {
    const BLS_BOOL_SIZE: usize = 1;
    const BLS_P2_SIZE: usize = 192;
    const ITEM_SIZE: usize = BLS_BOOL_SIZE + BLS_P2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p2_sum: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }
            .into());
    }
    let mut res_pk = blst::blst_p2::default();

    let elements_count = data.len() / ITEM_SIZE;

    for i in 0..elements_count {
        let mut pk_aff = blst::blst_p2_affine::default();
        let error_code = unsafe {
            blst::blst_p2_deserialize(
                &mut pk_aff,
                data[i * ITEM_SIZE + BLS_BOOL_SIZE..(i + 1) * ITEM_SIZE].as_ptr(),
            )
        };

        if (error_code != blst::BLST_ERROR::BLST_SUCCESS)
            || (data[i * ITEM_SIZE + BLS_BOOL_SIZE] & 0x80 != 0)
        {
            return Ok((1, [0u8; 192]));
        }

        let mut pk = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_from_affine(&mut pk, &pk_aff);
        }

        let sign = data[i * ITEM_SIZE];
        if sign == 1 {
            unsafe {
                blst::blst_p2_cneg(&mut pk, true);
            }
        } else if sign != 0 {
            return Ok((1, [0u8; 192]));
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

    Ok((0, res))
}

pub(super) fn p1_multiexp(
    data: &[u8]
) -> Result<(u64, [u8; 96])>  {
    const BLS_SCALAR_SIZE: usize = 32;
    const BLS_P1_SIZE: usize = 96;
    const ITEM_SIZE: usize = BLS_SCALAR_SIZE + BLS_P1_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p1_multiexp: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }
            .into());
    }

    let elements_count = data.len() / ((BLS_P1_SIZE + BLS_SCALAR_SIZE) as usize);

    let mut res_pk = blst::blst_p1::default();

    for i in 0..elements_count {
        let mut pk_aff = blst::blst_p1_affine::default();
        let error_code = unsafe {
            blst::blst_p1_deserialize(
                &mut pk_aff,
                data[i * ITEM_SIZE..(i * ITEM_SIZE + BLS_P1_SIZE)].as_ptr(),
            )
        };

        if (error_code != blst::BLST_ERROR::BLST_SUCCESS) || (data[i * ITEM_SIZE] & 0x80 != 0) {
            return Ok((1, [0u8; 96]));
        }

        let mut pk = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_from_affine(&mut pk, &pk_aff);
        }

        let mut pk_mul = blst::blst_p1::default();
        unsafe {
            blst::blst_p1_unchecked_mult(
                &mut pk_mul,
                &pk,
                data[(i * ITEM_SIZE + BLS_P1_SIZE)..((i + 1) * ITEM_SIZE)].as_ptr(),
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

    Ok((0, res))
}

pub(super) fn p2_multiexp(
    data: &[u8]
) -> Result<(u64, [u8; 192])>  {
    const BLS_SCALAR_SIZE: usize = 32;
    const BLS_P2_SIZE: usize = 192;
    const ITEM_SIZE: usize = BLS_SCALAR_SIZE + BLS_P2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_p2_multiexp: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }
            .into());
    }

    let elements_count = data.len() / ITEM_SIZE;

    let mut res_pk = blst::blst_p2::default();
    for i in 0..elements_count {
        let mut pk_aff = blst::blst_p2_affine::default();
        let error_code = unsafe {
            blst::blst_p2_deserialize(
                &mut pk_aff,
                data[i * ITEM_SIZE..(i * ITEM_SIZE + BLS_P2_SIZE)].as_ptr(),
            )
        };

        if (error_code != blst::BLST_ERROR::BLST_SUCCESS) || (data[i * ITEM_SIZE] & 0x80 != 0) {
            return Ok((1, [0u8; 192]));
        }

        let mut pk = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_from_affine(&mut pk, &pk_aff);
        }

        let mut pk_mul = blst::blst_p2::default();
        unsafe {
            blst::blst_p2_unchecked_mult(
                &mut pk_mul,
                &pk,
                data[(i * ITEM_SIZE + BLS_P2_SIZE)..(i + 1) * ITEM_SIZE].as_ptr(),
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

    Ok((0, res))
}

pub(super) fn map_fp_to_g1(
    data: &[u8]
) -> Result<(u64, Vec<u8>)> {
    const BLS_P1_SIZE: usize = 96;
    const BLS_FP_SIZE: usize = 48;
    const ITEM_SIZE: usize = BLS_FP_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_map_fp_to_g1: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }
            .into());
    }

    let mut fp_point = blst::blst_fp::default();

    let elements_count: usize = data.len() / ITEM_SIZE;

    let mut res_concat: Vec<u8> = vec![];

    for i in 0..elements_count {
        unsafe {
            blst::blst_fp_from_bendian(
                &mut fp_point,
                data[i * ITEM_SIZE..(i + 1) * ITEM_SIZE].as_ptr(),
            );
        }

        let mut fp_row: [u8; BLS_FP_SIZE] = [0u8; BLS_FP_SIZE];
        unsafe {
            blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &fp_point);
        }

        for j in 0..BLS_FP_SIZE {
            if fp_row[j] != data[i * BLS_FP_SIZE + j] {
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

pub(super) fn map_fp2_to_g2(
    data: &[u8]
) -> Result<(u64, Vec<u8>)> {
    const BLS_P2_SIZE: usize = 192;
    const BLS_FP_SIZE: usize = 48;
    const BLS_FP2_SIZE: usize = 96;
    const ITEM_SIZE: usize = BLS_FP2_SIZE;

    if data.len() % ITEM_SIZE != 0 {
        return Err(HostError::BLS12381InvalidInput {
            msg: format!(
                "Incorrect input length for bls12381_map_fp2_to_g2: {} is not divisible by {}",
                data.len(), ITEM_SIZE
            ),
        }.into());
    }

    let elements_count: usize = data.len() / ITEM_SIZE;

    let mut res_concat: Vec<u8> = vec![];

    for i in 0..elements_count {
        let mut c_fp1 = [blst::blst_fp::default(); 2];

        unsafe {
            blst::blst_fp_from_bendian(
                &mut c_fp1[1],
                data[i * ITEM_SIZE..i * ITEM_SIZE + BLS_FP_SIZE].as_ptr(),
            );
            blst::blst_fp_from_bendian(
                &mut c_fp1[0],
                data[i * ITEM_SIZE + BLS_FP_SIZE..(i + 1) * ITEM_SIZE].as_ptr(),
            );
        }

        let mut fp_row: [u8; BLS_FP_SIZE] = [0u8; BLS_FP_SIZE];
        unsafe {
            blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &c_fp1[0]);
        }

        for j in BLS_FP_SIZE..BLS_FP2_SIZE {
            if fp_row[j - BLS_FP_SIZE] != data[BLS_FP2_SIZE * i + j] {
                return Ok((1, vec![]));
            }
        }

        unsafe {
            blst::blst_bendian_from_fp(fp_row.as_mut_ptr(), &c_fp1[1]);
        }

        for j in 0..BLS_FP_SIZE {
            if fp_row[j] != data[BLS_FP2_SIZE * i + j] {
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
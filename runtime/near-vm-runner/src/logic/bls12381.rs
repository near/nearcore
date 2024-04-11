use crate::logic::{HostError, VMLogicError};

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

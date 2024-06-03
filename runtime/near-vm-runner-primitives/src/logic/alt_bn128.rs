use super::{HostError, VMLogicError};
use bn::Group;

const BOOL_SIZE: usize = 1;
const SCALAR_SIZE: usize = 256 / 8;
const POINT_SIZE: usize = SCALAR_SIZE * 2;

pub(super) struct InvalidInput {
    pub(super) msg: String,
}

impl InvalidInput {
    fn new(msg: &str, bad_value: &[u8]) -> InvalidInput {
        let msg = format!("{msg}: {bad_value:X?}");
        InvalidInput { msg }
    }
}

impl From<InvalidInput> for VMLogicError {
    fn from(err: InvalidInput) -> Self {
        HostError::AltBn128InvalidInput { msg: err.msg }.into()
    }
}

pub(super) fn split_elements<const ELEMENT_SIZE: usize>(
    data: &[u8],
) -> Result<&[[u8; ELEMENT_SIZE]], InvalidInput> {
    stdx::as_chunks_exact(data).map_err(|e| InvalidInput { msg: e.to_string() })
}

const G1_MULTIEXP_ELEMENT_SIZE: usize = POINT_SIZE + SCALAR_SIZE;

pub(super) fn g1_multiexp(
    elements: &[[u8; G1_MULTIEXP_ELEMENT_SIZE]],
) -> Result<[u8; POINT_SIZE], InvalidInput> {
    let elements: Vec<(bn::G1, bn::Fr)> = elements
        .iter()
        .map(|chunk| {
            let (g1, fr) = stdx::split_array(chunk);
            let g1 = decode_g1(g1)?;
            let fr = decode_fr(fr)?;
            Ok((g1, fr))
        })
        .collect::<Result<Vec<_>, InvalidInput>>()?;

    let res = bn::G1::multiexp(&elements);

    Ok(encode_g1(res))
}

const G1_SUM_ELEMENT_SIZE: usize = BOOL_SIZE + POINT_SIZE;

pub(super) fn g1_sum(
    elements: &[[u8; G1_SUM_ELEMENT_SIZE]],
) -> Result<[u8; POINT_SIZE], InvalidInput> {
    let elements: Vec<(bool, bn::G1)> = {
        elements
            .iter()
            .map(|chunk| {
                let (sign, g1) = stdx::split_array(chunk);
                let sign = decode_bool(sign)?;
                let g1 = decode_g1(g1)?;
                Ok((sign, g1))
            })
            .collect::<Result<Vec<_>, InvalidInput>>()?
    };

    let res = elements
        .iter()
        .fold(bn::G1::zero(), |acc, &(sign, x)| if sign { acc - x } else { acc + x });

    Ok(encode_g1(res))
}

const PAIRING_CHECK_ELEMENT_SIZE: usize = POINT_SIZE + POINT_SIZE * 2;

pub(super) fn pairing_check(
    elements: &[[u8; PAIRING_CHECK_ELEMENT_SIZE]],
) -> Result<bool, InvalidInput> {
    let elements: Vec<(bn::G1, bn::G2)> = elements
        .iter()
        .map(|chunk| {
            let (g1, g2) = stdx::split_array(chunk);
            let g1 = decode_g1(g1)?;
            let g2 = decode_g2(g2)?;
            Ok((g1, g2))
        })
        .collect::<Result<Vec<_>, InvalidInput>>()?;

    let res = bn::pairing_batch(&elements) == bn::Gt::one();

    Ok(res)
}

fn encode_g1(val: bn::G1) -> [u8; POINT_SIZE] {
    let (x, y) = bn::AffineG1::from_jacobian(val)
        .map(|p| (p.x(), p.y()))
        .unwrap_or_else(|| (bn::Fq::zero(), bn::Fq::zero()));
    let x = encode_fq(x);
    let y = encode_fq(y);
    stdx::join_array(x, y)
}

fn encode_fq(val: bn::Fq) -> [u8; SCALAR_SIZE] {
    encode_u256(val.into_u256())
}

fn encode_u256(val: bn::arith::U256) -> [u8; SCALAR_SIZE] {
    let [lo, hi] = val.0;
    stdx::join_array(lo.to_le_bytes(), hi.to_le_bytes())
}

fn decode_g1(raw: &[u8; POINT_SIZE]) -> Result<bn::G1, InvalidInput> {
    let (x, y) = stdx::split_array(raw);
    let x = decode_fq(x)?;
    let y = decode_fq(y)?;
    if x.is_zero() && y.is_zero() {
        Ok(bn::G1::zero())
    } else {
        bn::AffineG1::new(x, y)
            .map_err(|_err| InvalidInput::new("invalid g1", raw))
            .map(bn::G1::from)
    }
}

fn decode_fq(raw: &[u8; SCALAR_SIZE]) -> Result<bn::Fq, InvalidInput> {
    let val = decode_u256(raw);
    bn::Fq::from_u256(val).map_err(|_| InvalidInput::new("invalid fq", raw))
}

fn decode_g2(raw: &[u8; 2 * POINT_SIZE]) -> Result<bn::G2, InvalidInput> {
    let (x, y) = stdx::split_array(raw);
    let x = decode_fq2(x)?;
    let y = decode_fq2(y)?;
    if x.is_zero() && y.is_zero() {
        Ok(bn::G2::zero())
    } else {
        bn::AffineG2::new(x, y)
            .map_err(|_err| InvalidInput::new("invalid g2", raw))
            .map(bn::G2::from)
    }
}

fn decode_fq2(raw: &[u8; 2 * SCALAR_SIZE]) -> Result<bn::Fq2, InvalidInput> {
    let (real, imaginary) = stdx::split_array(raw);
    let real = decode_fq(real)?;
    let imaginary = decode_fq(imaginary)?;
    Ok(bn::Fq2::new(real, imaginary))
}

fn decode_fr(raw: &[u8; SCALAR_SIZE]) -> Result<bn::Fr, InvalidInput> {
    let val = decode_u256(raw);
    bn::Fr::new(val).ok_or_else(|| InvalidInput::new("invalid fr", raw))
}

fn decode_u256(raw: &[u8; SCALAR_SIZE]) -> bn::arith::U256 {
    let (lo, hi) = stdx::split_array(raw);
    let lo = u128::from_le_bytes(*lo);
    let hi = u128::from_le_bytes(*hi);
    bn::arith::U256([lo, hi])
}

fn decode_bool(raw: &[u8; BOOL_SIZE]) -> Result<bool, InvalidInput> {
    match raw {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(InvalidInput::new("invalid bool", raw)),
    }
}

use bn::arith::U256;
use bn::{pairing_batch, AffineG1, AffineG2, Fq, Fq2, Fr, Group, GroupError, Gt, G1, G2};
use borsh::{BorshDeserialize, BorshSerialize};
use std::io::{self, Error, ErrorKind, Write};

use crate::HostError;

const POINT_IS_NOT_ON_THE_CURVE: &str = "point is not on the curve";
const POINT_IS_NOT_IN_THE_SUBGROUP: &str = "point is not in the subgroup";
const NOT_IN_FIELD: &str = "integer is not less than modulus";

pub fn ilog2(mut n: u64) -> u64 {
    assert!(n > 0);
    let mut r = 0;
    if n >> 32 != 0 {
        n >>= 32;
        r += 32;
    }
    if n >> 16 != 0 {
        n >>= 16;
        r += 16;
    }
    if n >> 8 != 0 {
        n >>= 8;
        r += 8;
    }
    if n >> 4 != 0 {
        n >>= 4;
        r += 4;
    }
    if n >> 2 != 0 {
        n >>= 2;
        r += 2;
    }
    if n >> 1 != 0 {
        r += 1;
    }
    r
}

pub fn alt_bn128_g1_multiexp_sublinear_complexity_estimate(n_bytes: u64, discount: u64) -> u64 {
    // details of computation alt_bn128 parameters are available at https://gist.github.com/snjax/90e8b3e7f5c16a983a5f6347d1d28bde
    const A: u64 = 85158;
    const B: u64 = 15119;
    const C: u64 = 682573;
    const MULTIEXP_ITEM_SIZE: u64 = std::mem::size_of::<(G1, Fr)>() as u64;

    // A+C*n/(log2(n)+4) - B*n - discount

    let n = (n_bytes + MULTIEXP_ITEM_SIZE - 1) / MULTIEXP_ITEM_SIZE;
    let mut res = A + if n == 0 {
        0
    } else {
        // set linear complexity growth for n > 32768
        let l = std::cmp::min(ilog2(n), 15);
        (n * (l + 3) + (1 << (1 + l))) * C / ((l + 4) * (l + 5))
    };

    if res < B * n + discount {
        return 0;
    }
    res -= B * n + discount;
    res
}

#[derive(Copy, Clone)]
struct WrapU256(pub U256);

#[derive(Copy, Clone)]
struct WrapFr(pub Fr);

#[derive(Copy, Clone)]
struct WrapFq(pub Fq);

#[derive(Copy, Clone)]
struct WrapFq2(pub Fq2);

#[derive(Copy, Clone)]
struct WrapG1(pub G1);

#[derive(Copy, Clone)]
struct WrapG2(pub G2);

impl BorshSerialize for WrapU256 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        self.0 .0.serialize(writer)
    }
}

impl BorshDeserialize for WrapU256 {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let value = <[u128; 2]>::deserialize(buf)?;
        Ok(WrapU256(U256(value)))
    }
}

impl BorshSerialize for WrapFr {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        WrapU256(self.0.into_u256()).serialize(writer)
    }
}

impl BorshDeserialize for WrapFr {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let num = WrapU256::deserialize(buf)?.0;
        Fr::new(num)
            .ok_or_else(|| Error::new(ErrorKind::InvalidData, NOT_IN_FIELD))
            .map(|r| WrapFr(r))
    }
}

impl BorshSerialize for WrapFq {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        WrapU256(self.0.into_u256()).serialize(writer)
    }
}

impl BorshDeserialize for WrapFq {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let num = WrapU256::deserialize(buf)?.0;
        Fq::from_u256(num)
            .map_err(|_| Error::new(ErrorKind::InvalidData, NOT_IN_FIELD))
            .map(|r| WrapFq(r))
    }
}

impl BorshSerialize for WrapFq2 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        WrapFq(self.0.real()).serialize(writer)?;
        WrapFq(self.0.imaginary()).serialize(writer)
    }
}

impl BorshDeserialize for WrapFq2 {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let re = WrapFq::deserialize(buf)?.0;
        let im = WrapFq::deserialize(buf)?.0;

        Ok(WrapFq2(Fq2::new(re, im)))
    }
}

impl BorshSerialize for WrapG1 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        match AffineG1::from_jacobian(self.0) {
            Some(p) => {
                WrapFq(p.x()).serialize(writer)?;
                WrapFq(p.y()).serialize(writer)?;
            }
            None => {
                WrapFq(Fq::zero()).serialize(writer)?;
                WrapFq(Fq::zero()).serialize(writer)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for WrapG1 {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, io::Error> {
        let x = WrapFq::deserialize(buf)?.0;
        let y = WrapFq::deserialize(buf)?.0;
        if x.is_zero() && y.is_zero() {
            Ok(WrapG1(G1::zero()))
        } else {
            AffineG1::new(x, y)
                .map_err(|e| match e {
                    GroupError::NotOnCurve => {
                        io::Error::new(ErrorKind::InvalidData, POINT_IS_NOT_ON_THE_CURVE)
                    }
                    GroupError::NotInSubgroup => {
                        io::Error::new(ErrorKind::InvalidData, POINT_IS_NOT_IN_THE_SUBGROUP)
                    }
                })
                .map(|p| WrapG1(p.into()))
        }
    }
}

impl BorshSerialize for WrapG2 {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        match AffineG2::from_jacobian(self.0) {
            Some(p) => {
                WrapFq2(p.x()).serialize(writer)?;
                WrapFq2(p.y()).serialize(writer)?;
            }
            None => {
                WrapFq2(Fq2::zero()).serialize(writer)?;
                WrapFq2(Fq2::zero()).serialize(writer)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for WrapG2 {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, io::Error> {
        let x = WrapFq2::deserialize(buf)?.0;
        let y = WrapFq2::deserialize(buf)?.0;
        if x.is_zero() && y.is_zero() {
            Ok(WrapG2(G2::zero()))
        } else {
            AffineG2::new(x, y)
                .map_err(|e| match e {
                    GroupError::NotOnCurve => {
                        io::Error::new(ErrorKind::InvalidData, POINT_IS_NOT_ON_THE_CURVE)
                    }
                    GroupError::NotInSubgroup => {
                        io::Error::new(ErrorKind::InvalidData, POINT_IS_NOT_IN_THE_SUBGROUP)
                    }
                })
                .map(|p| WrapG2(p.into()))
        }
    }
}

/// Computes multiexp on alt_bn128 curve using Pippenger's algorithm
/// \sum_i mul_i g_{1 i} should be equal result.
///
/// # Arguments
///
/// * `data` - slice of (g1:G1, fr:Fr), where
///     G1 is point (x:Fq, y:Fq) on alt_bn128,
///     alt_bn128 is Y^2 = X^3 + 3 curve over Fq,
///     Fq is LE-serialized u256 number lesser than 21888242871839275222246405745257275088696311157297823662689037894645226208583
///     Fr is LE-serialized u256 number lesser than 21888242871839275222246405745257275088548364400416034343698204186575808495617
///
/// # Errors
///
/// If point coordinates are not on curve, point is not in the subgroup, scalar
/// is not in the field or data are wrong serialized, for example,
/// `data.len()%std::mem::sizeof::<(G1,Fr)>()!=0`, the function returns `AltBn128DeserializationError`.
///
/// If `borsh::BorshSerialize` returns error during serialization, the function
/// returns `AltBn128SerializationError`.
///
/// # Example
/// ```
/// # use near_vm_logic::alt_bn128::alt_bn128_g1_multiexp;
/// # use base64;
///
/// let multiexp_data = "AgAAAOzTRBYFMdAMNTUnUW2wNUYLmsNMgKQUC12+o1wVU7QSxF/il/WRT3I1bJqPaWKBbGqehkYu0QS7ct2nz52CRCn3EXSIf0p4ORYJ7mRmZLWtUyGrqlKl/4DNx2kHDEUrET+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsD2H5fx6TkvPtG6iZSiHT1Ih1TDyGsHTrOzFWN3hx0FwAaB2tgYeH+WuEKReDHNFmxyi8v597Ji5NP4PU8bZXkGQ==";
/// let multiexp_result_data = "qoK67D1yppH5iP0qhCrD8Ms+idcZtEry4EegUtSpIylhCyZNbRQ0xVdRe9hQBxZIovzCMwFRMAdcZ5FB+QA6Lg==";
/// let multiexp = base64::decode(multiexp_data).unwrap();
/// let multiexp_result = base64::decode(multiexp_result_data).unwrap();
/// let result = alt_bn128_g1_multiexp(&multiexp).unwrap();
/// assert_eq!(multiexp_result, result);
///
/// ```
pub fn alt_bn128_g1_multiexp(data: &[u8]) -> crate::logic::Result<Vec<u8>> {
    let items = <Vec<(WrapG1, WrapFr)>>::try_from_slice(data)
        .map_err(|e| HostError::AltBn128DeserializationError { msg: format!("{}", e) })?
        .into_iter()
        .map(|e| (e.0 .0, e.1 .0))
        .collect::<Vec<_>>();
    let result = WrapG1(G1::multiexp(&items))
        .try_to_vec()
        .map_err(|e| HostError::AltBn128SerializationError { msg: format!("{}", e) })?;
    Ok(result)
}

/// Computes sum for signed g1 group elements on alt_bn128 curve
/// \sum_i (-1)^{sign_i} g_{1 i} should be equal result.
///
/// # Arguments
///
/// * `data` - slice of (is_negative_sign:bool, g1:G1), where
///     bool is serialized as byte, 0 for false and 1 for true,
///     G1 is point (x:Fq, y:Fq) on alt_bn128,
///     alt_bn128 is Y^2 = X^3 + 3 curve over Fq,
///     Fq is LE-serialized u256 number lesser than 21888242871839275222246405745257275088696311157297823662689037894645226208583
///
/// # Errors
///
/// If point coordinates are not on curve, point is not in the subgroup, scalar
/// is not in the field or data are wrong serialized, for example,
/// `data.len()%std::mem::sizeof::<(bool, G1)>()!=0`, the function returns `AltBn128DeserializationError`.
///
/// If `borsh::BorshSerialize` returns error during serialization, the function
/// returns `AltBn128SerializationError`.///
/// # Example
/// ```
/// # use near_vm_logic::alt_bn128::alt_bn128_g1_sum;
/// # use base64;
///
/// let sum_data = "AgAAAADs00QWBTHQDDU1J1FtsDVGC5rDTICkFAtdvqNcFVO0EsRf4pf1kU9yNWyaj2ligWxqnoZGLtEEu3Ldp8+dgkQpAT+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsDb34dEXKnY0BG4EoWCfaLdEAFcmAKKBbqXEqkAlbaTDA=";
/// let sum_result_data = "6I9NGC6Ikzk7Xw/CIippAtOEsTx4TodcXRjzzu5TLh4EIPsrWPsfnQMtqKfMMF+SHgSphZseRKyej9jTVCT8Aw==";
/// let sum = base64::decode(sum_data).unwrap();
/// let sum_result = base64::decode(sum_result_data).unwrap();
/// let result = alt_bn128_g1_sum(&sum).unwrap();
/// assert_eq!(sum_result, result);
///
/// ```
pub fn alt_bn128_g1_sum(data: &[u8]) -> crate::logic::Result<Vec<u8>> {
    let items = <Vec<(bool, WrapG1)>>::try_from_slice(data)
        .map_err(|e| HostError::AltBn128DeserializationError { msg: format!("{}", e) })?
        .into_iter()
        .map(|e| (e.0, e.1 .0))
        .collect::<Vec<_>>();

    let mut acc = G1::zero();
    for &(sign, e) in items.iter() {
        if sign {
            acc = acc - e;
        } else {
            acc = acc + e;
        }
    }
    let result = WrapG1(acc)
        .try_to_vec()
        .map_err(|e| HostError::AltBn128SerializationError { msg: format!("{}", e) })?;
    Ok(result)
}

/// Computes pairing check on alt_bn128 curve.
/// \sum_i e(g_{1 i}, g_{2 i}) should be equal zero (in additive notation), e(g1, g2) is Ate pairing
///
/// # Arguments
///
/// * `data` - slice of (g1:G1, g2:G2), where
///     G2 is Fr-ordered subgroup point (x:Fq2, y:Fq2) on alt_bn128 twist,
///     alt_bn128 twist is Y^2 = X^3 + 3/(i+9) curve over Fq2
///     Fq2 is complex field element (re: Fq, im: Fq)
///     G1 is point (x:Fq, y:Fq) on alt_bn128,
///     alt_bn128 is Y^2 = X^3 + 3 curve over Fq
///     Fq is LE-serialized u256 number lesser than 21888242871839275222246405745257275088696311157297823662689037894645226208583
///     Fr is LE-serialized u256 number lesser than 21888242871839275222246405745257275088548364400416034343698204186575808495617
///
/// # Errors
///
/// If point coordinates are not on curve, point is not in the subgroup, scalar
/// is not in the field or data are wrong serialized, for example,
/// `data.len()%std::mem::sizeof::<(G1,G2)>()!=0`, the function returns `AltBn128DeserializationError`.
///
/// If `borsh::BorshSerialize` returns error during serialization, the function
/// returns `AltBn128SerializationError`.///
/// # Example
/// ```
/// # use near_vm_logic::alt_bn128::alt_bn128_pairing_check;
/// # use base64;
///
/// let pairs_data = "AgAAAHUK2WNxTupDt1oaOshWw3squNVY4PgSyGwGtQYcEWMHJIY1c8C0A3FM466TMq5PSpfDrArT0hpcdfZB7ahoEAQBGgPbBg3Bc03mGw3y1sMJ1WOHDKDKcoevKnSsT+oaKdRvwIF8cDlrJvTm3vAkQe6FvBMrlDvNKKGzreRYqecdEUOjM6W7ZSz6GERlXIDLvjNVCSs6iES0XG65qGuBLR67FmQRS13YfRfUC7rHzAGMhQtSLEHeFBowGoTcGdVdGU+wBJWX8wuD/el5Jt4PdnXI1q/pgrXBp/+ZqfDP6xwfU0pFswaWSENKpoJTUnN7b9DdQCvt1brrBzj7s1/pnxdtrVVnCKXr4tpPSHis+xRTecmMYqr2edoTcyqHPO8eIDGqq8zExaCeqC8Xbot73t71Yn3QRiduupL+Qrl2A04gL7PFXU/wzE7shdWtdV4/mkRZ7IoA9/LU9SH5ACP26QB8VsaiyTYTGsRL/kdG7jMCF7mYi4ZBa4Fy9C/78FDBFw==";
/// let pairs = base64::decode(pairs_data).unwrap();
/// let pairs_result = alt_bn128_pairing_check(&pairs).unwrap();
/// assert!(pairs_result);
///
/// ```
pub fn alt_bn128_pairing_check(data: &[u8]) -> crate::logic::Result<bool> {
    let items = <Vec<(WrapG1, WrapG2)>>::try_from_slice(data)
        .map_err(|e| HostError::AltBn128DeserializationError { msg: format!("{}", e) })?
        .into_iter()
        .map(|e| (e.0 .0, e.1 .0))
        .collect::<Vec<_>>();
    Ok(pairing_batch(&items) == Gt::one())
}

use crate::error::{Error, UserError};
use ethabi::{ethereum_types::U256, Address, ParamType, Token};

const INVALID_ABI_DATA: Error = Error::User(UserError::InvalidAbiEncodedData);

pub fn abi_decode<T, const N: usize>(types: &[ParamType; N], data: &[u8]) -> Result<T, Error>
where
    T: AbiTuple<N>,
{
    let tokens = data_to_tokens(types, data)?;
    T::try_from_token(tokens)
}

pub trait AbiTuple<const N: usize>: Sized {
    fn try_from_token(tokens: [Token; N]) -> Result<Self, Error>;
}

impl<T> AbiTuple<1> for (T,)
where
    T: TryFromToken,
{
    fn try_from_token(tokens: [Token; 1]) -> Result<Self, Error> {
        let (t,) = tokens.into();
        T::try_from_token(t).map(|t| (t,))
    }
}

impl<T1, T2> AbiTuple<2> for (T1, T2)
where
    T1: TryFromToken,
    T2: TryFromToken,
{
    fn try_from_token(tokens: [Token; 2]) -> Result<Self, Error> {
        let (t1, t2) = tokens.into();
        Ok((T1::try_from_token(t1)?, T2::try_from_token(t2)?))
    }
}

impl<T1, T2, T3> AbiTuple<3> for (T1, T2, T3)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
{
    fn try_from_token(tokens: [Token; 3]) -> Result<Self, Error> {
        let (t1, t2, t3) = tokens.into();
        Ok((T1::try_from_token(t1)?, T2::try_from_token(t2)?, T3::try_from_token(t3)?))
    }
}

impl<T1, T2, T3, T4> AbiTuple<4> for (T1, T2, T3, T4)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
    T4: TryFromToken,
{
    fn try_from_token(tokens: [Token; 4]) -> Result<Self, Error> {
        let (t1, t2, t3, t4) = tokens.into();
        Ok((
            T1::try_from_token(t1)?,
            T2::try_from_token(t2)?,
            T3::try_from_token(t3)?,
            T4::try_from_token(t4)?,
        ))
    }
}

impl<T1, T2, T3, T4, T5> AbiTuple<5> for (T1, T2, T3, T4, T5)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
    T4: TryFromToken,
    T5: TryFromToken,
{
    fn try_from_token(tokens: [Token; 5]) -> Result<Self, Error> {
        let (t1, t2, t3, t4, t5) = tokens.into();
        Ok((
            T1::try_from_token(t1)?,
            T2::try_from_token(t2)?,
            T3::try_from_token(t3)?,
            T4::try_from_token(t4)?,
            T5::try_from_token(t5)?,
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6> AbiTuple<6> for (T1, T2, T3, T4, T5, T6)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
    T4: TryFromToken,
    T5: TryFromToken,
    T6: TryFromToken,
{
    fn try_from_token(tokens: [Token; 6]) -> Result<Self, Error> {
        let (t1, t2, t3, t4, t5, t6) = tokens.into();
        Ok((
            T1::try_from_token(t1)?,
            T2::try_from_token(t2)?,
            T3::try_from_token(t3)?,
            T4::try_from_token(t4)?,
            T5::try_from_token(t5)?,
            T6::try_from_token(t6)?,
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> AbiTuple<7> for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
    T4: TryFromToken,
    T5: TryFromToken,
    T6: TryFromToken,
    T7: TryFromToken,
{
    fn try_from_token(tokens: [Token; 7]) -> Result<Self, Error> {
        let (t1, t2, t3, t4, t5, t6, t7) = tokens.into();
        Ok((
            T1::try_from_token(t1)?,
            T2::try_from_token(t2)?,
            T3::try_from_token(t3)?,
            T4::try_from_token(t4)?,
            T5::try_from_token(t5)?,
            T6::try_from_token(t6)?,
            T7::try_from_token(t7)?,
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> AbiTuple<8> for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: TryFromToken,
    T2: TryFromToken,
    T3: TryFromToken,
    T4: TryFromToken,
    T5: TryFromToken,
    T6: TryFromToken,
    T7: TryFromToken,
    T8: TryFromToken,
{
    fn try_from_token(tokens: [Token; 8]) -> Result<Self, Error> {
        let (t1, t2, t3, t4, t5, t6, t7, t8) = tokens.into();
        Ok((
            T1::try_from_token(t1)?,
            T2::try_from_token(t2)?,
            T3::try_from_token(t3)?,
            T4::try_from_token(t4)?,
            T5::try_from_token(t5)?,
            T6::try_from_token(t6)?,
            T7::try_from_token(t7)?,
            T8::try_from_token(t8)?,
        ))
    }
}

trait TryFromToken: Sized {
    fn try_from_token(t: Token) -> Result<Self, Error>;
}

impl TryFromToken for u8 {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        const U8_MAX: U256 = U256([u8::MAX as u64, 0, 0, 0]);
        let x = t.into_uint().ok_or(INVALID_ABI_DATA)?;
        if x <= U8_MAX {
            Ok(x.low_u32() as u8)
        } else {
            Err(INVALID_ABI_DATA)
        }
    }
}

impl TryFromToken for u32 {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        const U32_MAX: U256 = U256([u32::MAX as u64, 0, 0, 0]);
        let x = t.into_uint().ok_or(INVALID_ABI_DATA)?;
        if x <= U32_MAX {
            Ok(x.low_u32())
        } else {
            Err(INVALID_ABI_DATA)
        }
    }
}

impl TryFromToken for u64 {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        const U64_MAX: U256 = U256([u64::MAX, 0, 0, 0]);
        let x = t.into_uint().ok_or(INVALID_ABI_DATA)?;
        if x <= U64_MAX {
            Ok(x.low_u64())
        } else {
            Err(INVALID_ABI_DATA)
        }
    }
}

impl TryFromToken for u128 {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        const U128_MAX: U256 = U256([u64::MAX, u64::MAX, 0, 0]);
        let x = t.into_uint().ok_or(INVALID_ABI_DATA)?;
        if x <= U128_MAX {
            Ok(x.low_u128())
        } else {
            Err(INVALID_ABI_DATA)
        }
    }
}

impl TryFromToken for bool {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        t.into_bool().ok_or(INVALID_ABI_DATA)
    }
}

impl TryFromToken for String {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        t.into_string().ok_or(INVALID_ABI_DATA)
    }
}

impl TryFromToken for Address {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        t.into_address().ok_or(INVALID_ABI_DATA)
    }
}

impl TryFromToken for Vec<u8> {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        t.into_bytes().ok_or(INVALID_ABI_DATA)
    }
}

impl TryFromToken for Vec<String> {
    fn try_from_token(t: Token) -> Result<Self, Error> {
        let elems = t.into_array().ok_or(INVALID_ABI_DATA)?;
        elems.into_iter().map(String::try_from_token).collect()
    }
}

fn data_to_tokens<const N: usize>(
    types: &[ParamType; N],
    data: &[u8],
) -> Result<[ethabi::Token; N], Error> {
    let result = ethabi::decode(types.as_slice(), data).map_err(|_| INVALID_ABI_DATA)?;
    result.try_into().map_err(|_| INVALID_ABI_DATA)
}

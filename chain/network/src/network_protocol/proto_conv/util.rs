/// Proto conversion utilities.
use crate::decode_budget::decode_borsh;
use borsh::BorshDeserialize;
use protobuf::MessageField as MF;
use std::io;

#[derive(thiserror::Error, Debug)]
#[error("[{idx}]: {source}")]
pub struct ParseVecError<E> {
    idx: usize,
    #[source]
    source: E,
}

/// Borsh-deserializes `T` from `bytes`, rejecting inputs larger than `limit`
/// bytes before decoding. This caps the encoded size only: the memory the
/// decoded value takes is bounded by the decode budget; see
/// [`crate::decode_budget`]. `limit` must sit above the largest legitimate
/// encoding of `T` and well below the peer-frame cap.
/// Returns `io::Error` so it composes with the borsh-based proto decode sites.
pub fn try_from_slice_with_limit<T: BorshDeserialize>(bytes: &[u8], limit: usize) -> io::Result<T> {
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("encoded size {} exceeds the limit of {limit} bytes", bytes.len()),
        ));
    }
    decode_borsh(bytes)
}

pub fn try_from_slice<'a, X, Y: TryFrom<&'a X>>(
    xs: &'a [X],
) -> Result<Vec<Y>, ParseVecError<Y::Error>> {
    let mut ys = vec![];
    for (idx, x) in xs.iter().enumerate() {
        ys.push(x.try_into().map_err(|source| ParseVecError { idx, source })?);
    }
    Ok(ys)
}

#[derive(thiserror::Error, Debug)]
pub enum ParseRequiredError<E> {
    #[error("missing, while required")]
    Missing,
    #[error(transparent)]
    Other(E),
}

pub fn try_from_optional<'a, X, Y: TryFrom<&'a X>>(x: &'a MF<X>) -> Result<Option<Y>, Y::Error> {
    x.as_ref().map(|x| x.try_into()).transpose()
}

pub fn try_from_required<'a, X, Y: TryFrom<&'a X>>(
    x: &'a MF<X>,
) -> Result<Y, ParseRequiredError<Y::Error>> {
    x.as_ref().ok_or(ParseRequiredError::Missing)?.try_into().map_err(ParseRequiredError::Other)
}

pub fn map_from_required<'a, X, Y, E>(
    x: &'a MF<X>,
    f: impl FnOnce(&'a X) -> Result<Y, E>,
) -> Result<Y, ParseRequiredError<E>> {
    f(x.as_ref().ok_or(ParseRequiredError::Missing)?).map_err(ParseRequiredError::Other)
}

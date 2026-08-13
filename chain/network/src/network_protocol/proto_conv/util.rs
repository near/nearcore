/// Proto conversion utilities.
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
/// before decoding so a maliciously inflated peer payload cannot force a large
/// allocation at decode time. Use this for any peer-supplied borsh blob whose
/// decoded form can be much larger than its wire size; `limit` must sit above
/// the largest legitimate encoding of `T` and well below the peer-frame cap.
/// Returns `io::Error` so it composes with the borsh-based proto decode sites.
pub fn try_from_slice_with_limit<T: BorshDeserialize>(bytes: &[u8], limit: usize) -> io::Result<T> {
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("encoded size {} exceeds the limit of {limit} bytes", bytes.len()),
        ));
    }
    T::try_from_slice(bytes)
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

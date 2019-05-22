use crate::serialize::{from_base64, to_base64};
use std::convert::TryFrom;

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base64Encoded:
    for<'a> TryFrom<&'a [u8], Error = Box<std::error::Error>> + ToBytes
{
    fn from_base64(s: &str) -> Result<Self, Box<std::error::Error>> {
        let bytes = from_base64(s)?;
        Self::try_from(&bytes)
    }

    fn to_base64(&self) -> String {
        to_base64(&self.to_bytes())
    }
}

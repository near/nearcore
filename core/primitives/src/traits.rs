use std::convert::TryFrom;

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base58Encoded:
    for<'a> TryFrom<&'a [u8], Error = Box<std::error::Error>> + ToBytes
{
    fn from_base58(s: &str) -> Result<Self, Box<std::error::Error>> {
        let bytes = bs58::decode(s).into_vec()?;
        Self::try_from(&bytes)
    }

    fn to_base58(&self) -> String {
        let bytes = self.to_bytes();
        bs58::encode(bytes).into_string()
    }
}

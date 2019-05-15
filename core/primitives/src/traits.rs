use std::convert::TryFrom;

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base64Encoded:
    for<'a> TryFrom<&'a [u8], Error = String> + ToBytes
{
    fn from_base64(s: &str) -> Result<Self, String> {
        let bytes = base64::decode(s).map_err(|e| format!("{}", e))?;
        Self::try_from(&bytes)
    }

    fn to_base64(&self) -> String {
        base64::encode(&self.to_bytes())
    }
}

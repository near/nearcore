/// FromBytes is like TryFrom<Vec<u8>>
pub trait FromBytes: Sized {
    fn from_bytes(bytes: &Vec<u8>) -> Result<Self, Box<std::error::Error>>;
}

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base58Encoded: FromBytes + ToBytes {
    fn from_base58(s: &String) -> Result<Self, Box<std::error::Error>> {
        let bytes = bs58::decode(s).into_vec()?;
        Self::from_bytes(&bytes)
    }

    fn to_base58(&self) -> String {
        let bytes = self.to_bytes();
        bs58::encode(bytes).into_string()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct TryFromSliceError(pub(crate) ());

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseSignatureError {
    #[error("invalid signature data: {0}")]
    InvalidData(String),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseKeyError {
    #[error("unknown curve kind: {0}")]
    UnknownCurve(String),
    #[error("invalid key length: {0}")]
    InvalidLength(usize),
    #[error("invalid key data: {0}")]
    InvalidData(String),
}

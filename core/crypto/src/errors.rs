#[derive(Debug, Copy, Clone)]
pub struct TryFromSliceError(pub(crate) ());

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseSignatureError {
    #[error("invalid signature data: {0}")]
    InvalidData(String),
}

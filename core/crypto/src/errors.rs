use near_account_id::AccountId;

#[derive(Debug, Clone, thiserror::Error)]
#[error("expected the input of {expected} bytes, but {received} was given")]
pub struct InvalidLength {
    pub expected: usize,
    pub received: usize,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseKeyTypeError {
    #[error("unknown key type '{unknown_key_type}'")]
    UnknownKeyType { unknown_key_type: String },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseKeyError {
    #[error("unknown key type '{unknown_key_type}'")]
    UnknownKeyType { unknown_key_type: String },
    #[error("invalid key length: {0}")]
    InvalidLength(#[from] InvalidLength),
    #[error("invalid key data: {error_message}")]
    InvalidData { error_message: String },
}

impl From<ParseKeyTypeError> for ParseKeyError {
    fn from(err: ParseKeyTypeError) -> Self {
        match err {
            ParseKeyTypeError::UnknownKeyType { unknown_key_type } => {
                Self::UnknownKeyType { unknown_key_type }
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseSignatureError {
    #[error("unknown key type '{unknown_key_type}'")]
    UnknownKeyType { unknown_key_type: String },
    #[error("invalid signature length: {0}")]
    InvalidLength(#[from] InvalidLength),
    #[error("invalid signature data: {error_message}")]
    InvalidData { error_message: String },
}

impl From<ParseKeyTypeError> for ParseSignatureError {
    fn from(err: ParseKeyTypeError) -> Self {
        match err {
            ParseKeyTypeError::UnknownKeyType { unknown_key_type } => {
                Self::UnknownKeyType { unknown_key_type }
            }
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ImplicitPublicKeyError {
    #[error("'{account_id}' is not a NEAR-implicit account")]
    AccountIsNotNearImplicit { account_id: AccountId },
}

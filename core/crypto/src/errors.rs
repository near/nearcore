use near_account_id::AccountId;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseKeyTypeError {
    #[error("unknown key type '{unknown_key_type}'")]
    UnknownKeyType { unknown_key_type: String },
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseKeyError {
    #[error("unknown key type '{unknown_key_type}'")]
    UnknownKeyType { unknown_key_type: String },
    #[error("invalid key length: expected the input of {expected_length} bytes, but {received_length} was given")]
    InvalidLength { expected_length: usize, received_length: usize },
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
    #[error("invalid signature length: expected the input of {expected_length} bytes, but {received_length} was given")]
    InvalidLength { expected_length: usize, received_length: usize },
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
    #[error("'{account_id}' is not an implicit account")]
    AccountIsNotImplicit { account_id: AccountId },
}

use std::fmt;

/// An error occurred when parsing an invalid Account ID with [`AccountId::validate`](crate::AccountId::validate).
#[derive(Debug)]
pub struct ParseAccountError(pub(crate) ParseErrorKind);

impl ParseAccountError {
    pub fn kind(&self) -> &ParseErrorKind {
        &self.0
    }
}

impl std::error::Error for ParseAccountError {}
impl fmt::Display for ParseAccountError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A list of errors that occur when parsing an invalid Account ID.
#[derive(Debug)]
pub enum ParseErrorKind {
    TooLong,
    TooShort,
    Invalid,
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseErrorKind::TooLong => write!(f, "the value is too long for account ID"),
            ParseErrorKind::TooShort => write!(f, "the value is too short for account ID"),
            ParseErrorKind::Invalid => write!(f, "the value has invalid characters for account ID"),
        }
    }
}

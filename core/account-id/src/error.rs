use std::fmt;

/// An error occurred when parsing an invalid Account ID with [`AccountId::validate`](crate::AccountId::validate).
#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub struct ParseAccountError(pub(crate) ParseErrorKind, pub(crate) String);

impl ParseAccountError {
    pub fn kind(&self) -> &ParseErrorKind {
        &self.0
    }

    pub fn get_account_id(self) -> String {
        self.1
    }
}

impl std::error::Error for ParseAccountError {}
impl fmt::Display for ParseAccountError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}]: {}", self.1, self.0)
    }
}

/// A list of errors that occur when parsing an invalid Account ID.
#[derive(Eq, Hash, Clone, Debug, PartialEq)]
pub enum ParseErrorKind {
    TooLong,
    TooShort,
    Invalid,
}

impl ParseErrorKind {
    pub fn is_too_long(&self) -> bool {
        matches!(self, ParseErrorKind::TooLong)
    }

    pub fn is_too_short(&self) -> bool {
        matches!(self, ParseErrorKind::TooShort)
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, ParseErrorKind::Invalid)
    }
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

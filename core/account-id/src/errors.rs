use std::fmt;

/// An error which can be returned when parsing a NEAR Account ID.
#[derive(Eq, Clone, Debug, PartialEq)]
pub struct ParseAccountError(pub(crate) ParseErrorKind);

impl ParseAccountError {
    /// Returns the specific cause why parsing the Account ID failed.
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
#[non_exhaustive]
#[derive(Eq, Clone, Debug, PartialEq)]
pub enum ParseErrorKind {
    TooLong,
    TooShort,
    Invalid,
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseErrorKind::TooLong => write!(f, "the account ID is too long"),
            ParseErrorKind::TooShort => write!(f, "the account ID is too short"),
            _ => write!(f, "the account ID has an invalid format"),
        }
    }
}

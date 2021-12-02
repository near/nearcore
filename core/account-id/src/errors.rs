use std::fmt;

/// An error occurred when parsing an invalid Account ID with [`AccountId::validate`](crate::AccountId::validate).
#[derive(Eq, Clone, Debug, PartialEq)]
pub struct ParseAccountError(pub(crate) ParseErrorKind, pub(crate) String);

impl ParseAccountError {
    /// Returns the corresponding [`ParseErrorKind`] for this error.
    pub fn kind(&self) -> &ParseErrorKind {
        &self.0
    }

    /// Returns the corresponding [`AccountId`](crate::AccountId) for this error.
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
#[non_exhaustive]
#[derive(Eq, Clone, Debug, PartialEq)]
pub enum ParseErrorKind {
    TooLong,
    TooShort,
    Invalid,
}

impl ParseErrorKind {
    /// Returns `true` if the Account ID was too long.
    pub fn is_too_long(&self) -> bool {
        matches!(self, ParseErrorKind::TooLong)
    }

    /// Returns `true` if the Account ID was too short.
    pub fn is_too_short(&self) -> bool {
        matches!(self, ParseErrorKind::TooShort)
    }

    /// Returns `true` if the Account ID was marked invalid.
    ///
    /// This can happen for the following reasons:
    ///
    /// - An invalid character was detected (could be an uppercase character, symbol or space).
    /// - Separators immediately follow each other.
    /// - Separators begin or end the Account ID.
    pub fn is_invalid(&self) -> bool {
        matches!(self, ParseErrorKind::Invalid)
    }
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

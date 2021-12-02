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
///
/// Also see [Error kind precedence](crate::AccountId#error-kind-precedence).
#[non_exhaustive]
#[derive(Eq, Clone, Debug, PartialEq)]
pub enum ParseErrorKind {
    /// The Account ID is too long.
    ///
    /// Returned if the `AccountId` is longer than [`AccountId::MAX_LEN`](crate::AccountId::MAX_LEN).
    TooLong,
    /// The Account ID is too short.
    ///
    /// Returned if the `AccountId` is shorter than [`AccountId::MIN_LEN`](crate::AccountId::MIN_LEN).
    TooShort,
    /// The Account ID has a redundant separator.
    ///
    /// This variant would be returned if the Account ID either begins with,
    /// ends with or has separators immediately following each other.
    ///
    /// Cases: `jane.`, `angela__moss`, `tyrell..wellick`
    RedundantSeparator,
    /// The Account ID contains an invalid character.
    ///
    /// This variant would be returned if the Account ID contains an upper-case character, non-separating symbol or space.
    ///
    /// Cases: `ƒelicia.near`, `user@app.com`, `Emily.near`.
    InvalidChar,
}

impl fmt::Display for ParseErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseErrorKind::TooLong => write!(f, "the Account ID is too long"),
            ParseErrorKind::TooShort => write!(f, "the Account ID is too short"),
            ParseErrorKind::RedundantSeparator => {
                write!(f, "the Account ID has a redundant separator")
            }
            _ => write!(f, "the Account ID contains an invalid character"),
        }
    }
}

use core::fmt;

/// An error that occurred during parsing or hashing.
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub enum Error {
    /// A data overflow error.
    HashDataOverflow,
    /// Too many rounds error.
    TooManyRounds {
        /// Max rounds allowed.
        max: u32,
        /// Actual round value.
        actual: u32,
    },
}

// This prints better looking error messages if `unwrap` is called.
impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match *self {
            HashDataOverflow => f.debug_tuple("HashDataOverflow").finish(),
            TooManyRounds { ref max, ref actual } => {
                f.debug_struct("TooManyRounds").field("max", max).field("actual", actual).finish()
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match *self {
            HashDataOverflow => write!(f, "Hash data length overflow."),
            TooManyRounds { ref max, ref actual } => {
                write!(f, "Too many rounds. Expected fewer than {}, got {}.", max, actual)
            }
        }
    }
}

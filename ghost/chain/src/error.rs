use std::fmt::{self, Display};

use failure::{Backtrace, Context, Fail};

#[derive(Debug, Fail)]
pub struct Error {
    inner: Context<ErrorKind>,
}

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum ErrorKind {
    /// The block doesn't fit anywhere in our chain.
    #[fail(display = "Block is unfit: {}", _0)]
    Unfit(String),
    /// Orphan block.
    #[fail(display = "Orphan")]
    Orphan,
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cause = match self.cause() {
            Some(c) => format!("{}", c),
            None => String::from("Unknown"),
        };
        let backtrace = match self.backtrace() {
            Some(b) => format!("{}", b),
            None => String::from("Unknown"),
        };
        let output = format!("{} \n Cause: {} \n Backtrace: {}", self.inner, cause, backtrace);
        Display::fmt(&output, f)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

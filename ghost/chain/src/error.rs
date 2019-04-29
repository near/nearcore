use std::fmt::{self, Display};
use std::io;

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
    /// Peer abusively sending us an old block we already have
    #[fail(display = "Old Block")]
    OldBlock,
    /// Invalid block time given current time.
    #[fail(display = "Invalid Block Time")]
    InvalidBlockTime,
    /// Block height is invalid (not previous + 1).
    #[fail(display = "Invalid Block Height")]
    InvalidBlockHeight,
    /// Invalid block proposed signature.
    #[fail(display = "Invalid Block Proposer Signature")]
    InvalidBlockProposer,
    /// Invalid block confirmation signature.
    #[fail(display = "Invalid Block Confirmation Signature")]
    InvalidBlockConfirmation,
    /// Invalid block weight.
    #[fail(display = "Invalid Block Weight")]
    InvalidBlockWeight,
    /// Invalid state root hash.
    #[fail(display = "Invalid State Root Hash")]
    InvalidStateRoot,
    /// IO Error.
    #[fail(display = "IO Error: {}", _0)]
    IOErr(String),
    /// Not found record in the DB.
    #[fail(display = "DB Not Found Error: {}", _0)]
    DBNotFoundErr(String),
    /// Anything else
    #[fail(display = "Other Error: {}", _0)]
    Other(String),
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

impl Error {
    pub fn kind(&self) -> ErrorKind {
        self.inner.get_context().clone()
    }

    pub fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }

    pub fn is_bad_data(&self) -> bool {
        match self.kind() {
            ErrorKind::Unfit(_)
            | ErrorKind::Orphan
            | ErrorKind::IOErr(_)
            | ErrorKind::Other(_)
            | ErrorKind::DBNotFoundErr(_) => false,
            ErrorKind::InvalidBlockTime
            | ErrorKind::InvalidBlockHeight
            | ErrorKind::OldBlock
            | ErrorKind::InvalidBlockProposer
            | ErrorKind::InvalidBlockConfirmation
            | ErrorKind::InvalidBlockWeight
            | ErrorKind::InvalidStateRoot => true,
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error { inner: Context::new(ErrorKind::IOErr(error.to_string())) }
    }
}

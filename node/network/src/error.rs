use std::{error, fmt, io};
use substrate_network_libp2p::Error as libp2pError;

#[derive(Debug)]
pub struct Error {
    inner: String,
}

impl Error {
    pub fn new(msg: &str) -> Error {
        Error { inner: msg.to_string() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        &self.inner
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        use std::error::Error;
        self::Error::new(error.description())
    }
}

impl From<libp2pError> for Error {
    fn from(error: libp2pError) -> Self {
        Error::new(error.description())
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidPartMessage,
    InvalidChunkPartId,
    InvalidChunkShardId,
    InvalidMerkleProof,
    InvalidChunkSignature,
    InvalidChunkHeader,
    InvalidChunk,
    UnknownChunk,
    KnownPart,
    ChainError(near_chain::Error),
    IOError(std::io::Error),
    PoolError(near_pool::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl From<near_chain::Error> for Error {
    fn from(err: near_chain::Error) -> Self {
        Error::ChainError(err)
    }
}

impl From<near_pool::Error> for Error {
    fn from(err: near_pool::Error) -> Self {
        Error::PoolError(err)
    }
}

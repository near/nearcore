use chrono::{DateTime, Utc};
use near_primitives::sharding::ChunkHash;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Debug)]
pub enum Error {
    InvalidPartMessage,
    InvalidChunkPartId,
    InvalidChunkShardId,
    InvalidMerkleProof,
    InvalidChunkSignature,
    InvalidChunkHeader,
    InvalidChunk,
    DuplicateChunkHeight,
    UnknownChunk,
    KnownPart,
    ChainError(near_chain::Error),
    IOError(std::io::Error),
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

pub trait TimeProvider {
    fn now(&self) -> DateTime<Utc>;
}

pub struct StandardTimeProvider;

impl TimeProvider for StandardTimeProvider {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Default)]
pub struct ExpiryIndex {
    elements: BinaryHeap<Reverse<(DateTime<Utc>, ChunkHash)>>,
}

impl ExpiryIndex {
    pub fn insert(&mut self, expiry_timestamp: DateTime<Utc>, chunk_hash: ChunkHash) {
        self.elements.push(Reverse((expiry_timestamp, chunk_hash)))
    }

    pub fn pop_before(&mut self, cut_off: &DateTime<Utc>) -> Option<(DateTime<Utc>, ChunkHash)> {
        let (min_timestamp, _) = &self.elements.peek()?.0;
        if min_timestamp < cut_off {
            self.elements.pop().map(|r| r.0)
        } else {
            None
        }
    }
}

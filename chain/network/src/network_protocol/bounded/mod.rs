//! Bounded Borsh decoding of peer-supplied collections.
//!
//! Each decoder reads the `u32` length prefix and rejects it if it exceeds
//! `MAX`, before any element is decoded. It then puts the prefix back in
//! front of the reader and delegates to the stock Borsh implementation, so
//! input within the limit decodes exactly as plain Borsh would.
//!
//! The limit counts elements only. It does not bound strings or vectors
//! nested inside them.
//!
//! The wire format is unchanged. Attach a decoder to a field with
//! `#[borsh(deserialize_with = "bounded::vec::<_, _, 512>")]`.

// Step 2 wires these decoders into network message fields and removes this.
#![allow(dead_code)]

use borsh::BorshDeserialize;
use borsh::error::ERROR_ZST_FORBIDDEN;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::io::{self, IoSliceMut, Read};
use std::mem::size_of;

#[cfg(test)]
mod tests;

/// Peer-supplied collection length exceeded its limit. Carried inside
/// `io::Error` (kind `InvalidData`) so it composes with Borsh.
#[derive(Debug, thiserror::Error)]
#[error("collection length {len} exceeds the limit of {max}")]
pub(crate) struct LengthLimitExceeded {
    pub len: u32,
    pub max: u32,
}

/// True if `err` was produced by a bounded decoder rejecting a length.
pub(crate) fn is_length_limit_exceeded(err: &io::Error) -> bool {
    err.get_ref().is_some_and(|inner| inner.is::<LengthLimitExceeded>())
}

pub(crate) fn vec<R: Read, T: BorshDeserialize, const MAX: u32>(
    reader: &mut R,
) -> io::Result<Vec<T>> {
    check_zst::<T>()?;
    decode::<R, _, MAX>(reader)
}

pub(crate) fn hash_set<R, T, H, const MAX: u32>(reader: &mut R) -> io::Result<HashSet<T, H>>
where
    R: Read,
    T: BorshDeserialize + Eq + Hash + Ord,
    H: BuildHasher + Default,
{
    check_zst::<T>()?;
    decode::<R, _, MAX>(reader)
}

pub(crate) fn hash_map<R, K, V, H, const MAX: u32>(reader: &mut R) -> io::Result<HashMap<K, V, H>>
where
    R: Read,
    K: BorshDeserialize + Eq + Hash + Ord,
    V: BorshDeserialize,
    H: BuildHasher + Default,
{
    check_zst::<K>()?;
    decode::<R, _, MAX>(reader)
}

pub(crate) fn btree_set<R, T, const MAX: u32>(reader: &mut R) -> io::Result<BTreeSet<T>>
where
    R: Read,
    T: BorshDeserialize + Ord,
{
    check_zst::<T>()?;
    decode::<R, _, MAX>(reader)
}

pub(crate) fn btree_map<R, K, V, const MAX: u32>(reader: &mut R) -> io::Result<BTreeMap<K, V>>
where
    R: Read,
    K: BorshDeserialize + Ord,
    V: BorshDeserialize,
{
    check_zst::<K>()?;
    decode::<R, _, MAX>(reader)
}

/// Borsh rejects zero-sized elements (and map keys) before reading the
/// length. Doing the same here keeps errors and consumed bytes identical.
fn check_zst<T>() -> io::Result<()> {
    if size_of::<T>() == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, ERROR_ZST_FORBIDDEN));
    }
    Ok(())
}

/// Checks the length prefix against `MAX`, then decodes the collection with
/// stock Borsh from the replayed prefix followed by the rest of `reader`.
fn decode<R: Read, C: BorshDeserialize, const MAX: u32>(reader: &mut R) -> io::Result<C> {
    let len = u32::deserialize_reader(reader)?;
    if len > MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            LengthLimitExceeded { len, max: MAX },
        ));
    }
    C::deserialize_reader(&mut Replay { prefix: &len.to_le_bytes(), reader })
}

/// Serves `prefix`, then the caller's reader. Unlike `Read::chain`, it passes
/// every method a reader can override through to the caller's reader, whose
/// implementation then decides, for example, how many bytes a failed call
/// consumes. Borsh reads the whole prefix before decoding any element, so
/// element decoders always reach the caller's reader directly.
struct Replay<'a, R> {
    prefix: &'a [u8],
    reader: &'a mut R,
}

impl<R: Read> Read for Replay<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.prefix.is_empty() {
            return self.reader.read(buf);
        }
        self.prefix.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        if self.prefix.is_empty() {
            return self.reader.read_exact(buf);
        }
        let (head, tail) = buf.split_at_mut(buf.len().min(self.prefix.len()));
        self.prefix.read_exact(head)?;
        if tail.is_empty() {
            return Ok(());
        }
        self.reader.read_exact(tail)
    }

    fn read_vectored(&mut self, buffers: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        if self.prefix.is_empty() {
            return self.reader.read_vectored(buffers);
        }
        self.prefix.read_vectored(buffers)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        if self.prefix.is_empty() {
            return self.reader.read_to_end(buf);
        }
        (&mut self.prefix).chain(&mut *self.reader).read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        if self.prefix.is_empty() {
            return self.reader.read_to_string(buf);
        }
        (&mut self.prefix).chain(&mut *self.reader).read_to_string(buf)
    }
}

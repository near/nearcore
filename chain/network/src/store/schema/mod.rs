use crate::types as primitives;
/// Schema module defines a type-safe access to the DB.
/// It is a concise definition of key and value types
/// of the DB columns. For high level access see store.rs.
use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time;
use near_crypto::Signature;
use near_primitives::account::id::AccountId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_store::DBCol;
use std::io;
use std::sync::Arc;

#[cfg(test)]
mod testonly;
#[cfg(test)]
mod tests;

pub struct AccountIdFormat;
impl Format for AccountIdFormat {
    type T = AccountId;
    fn encode<W: io::Write>(a: &AccountId, w: &mut W) -> io::Result<()> {
        w.write_all(a.as_ref().as_bytes())
    }
    fn decode(a: &[u8]) -> Result<AccountId, Error> {
        std::str::from_utf8(a).map_err(invalid_data)?.parse().map_err(invalid_data)
    }
}

/// A Borsh representation of the primitives::ConnectionInfo.
#[derive(BorshSerialize, BorshDeserialize)]
pub(super) struct ConnectionInfoRepr {
    peer_info: primitives::PeerInfo,
    /// UNIX timestamps in nanos.
    time_established: u64,
    time_connected_until: u64,
}

impl BorshRepr for ConnectionInfoRepr {
    type T = primitives::ConnectionInfo;
    fn to_repr(s: &primitives::ConnectionInfo) -> Self {
        Self {
            peer_info: s.peer_info.clone(),
            time_established: s.time_established.unix_timestamp_nanos() as u64,
            time_connected_until: s.time_connected_until.unix_timestamp_nanos() as u64,
        }
    }

    fn from_repr(s: Self) -> Result<primitives::ConnectionInfo, Error> {
        Ok(primitives::ConnectionInfo {
            peer_info: s.peer_info,
            time_established: time::Utc::from_unix_timestamp_nanos(s.time_established as i128)
                .map_err(invalid_data)?,
            time_connected_until: time::Utc::from_unix_timestamp_nanos(
                s.time_connected_until as i128,
            )
            .map_err(invalid_data)?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(super) struct EdgeRepr {
    key: (PeerId, PeerId),
    nonce: u64,
    signature0: Signature,
    signature1: Signature,
    removal_info: Option<(bool, Signature)>,
}

impl BorshRepr for EdgeRepr {
    type T = primitives::Edge;

    fn to_repr(e: &Self::T) -> Self {
        Self {
            key: e.key().clone(),
            nonce: e.nonce(),
            signature0: e.signature0().clone(),
            signature1: e.signature1().clone(),
            removal_info: e.removal_info().cloned(),
        }
    }
    fn from_repr(e: Self) -> Result<Self::T, Error> {
        Ok(primitives::Edge::new(e.key.0, e.key.1, e.nonce, e.signature0, e.signature1)
            .with_removal_info(e.removal_info))
    }
}

/////////////////////////////////////////////
// Columns

pub(super) struct AccountAnnouncements;
impl Column for AccountAnnouncements {
    const COL: DBCol = DBCol::AccountAnnouncements;
    type Key = AccountIdFormat;
    type Value = Borsh<AnnounceAccount>;
}

pub(super) struct RecentOutboundConnections;
impl Column for RecentOutboundConnections {
    const COL: DBCol = DBCol::RecentOutboundConnections;
    type Key = Borsh<()>;
    type Value = Vec<ConnectionInfoRepr>;
}

pub(super) struct PeerComponent;
impl Column for PeerComponent {
    const COL: DBCol = DBCol::PeerComponent;
    type Key = Borsh<PeerId>;
    type Value = Borsh<u64>;
}

pub(super) struct ComponentEdges;
impl Column for ComponentEdges {
    const COL: DBCol = DBCol::ComponentEdges;
    type Key = U64LE;
    type Value = Vec<EdgeRepr>;
}

pub(super) struct LastComponentNonce;
impl Column for LastComponentNonce {
    const COL: DBCol = DBCol::LastComponentNonce;
    type Key = Borsh<()>;
    type Value = Borsh<u64>;
}

////////////////////////////////////////////////////
// Storage

pub type Error = io::Error;
fn invalid_data(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(io::ErrorKind::InvalidData, e)
}

pub trait Format {
    type T;
    /// Encode should write encoded <a> to <w>.
    /// Errors may come only from calling methods of <w>.
    fn encode<W: io::Write>(a: &Self::T, w: &mut W) -> io::Result<()>;
    fn decode(a: &[u8]) -> io::Result<Self::T>;
}

fn to_vec<F: Format>(a: &F::T) -> Vec<u8> {
    let mut out = Vec::new();
    F::encode(a, &mut out).unwrap();
    out
}

/// BorshRepr defines an isomorphism between T and Self,
/// where Self implements serialization to Borsh.
/// Format trait is automatically derived for BorshRepr instances,
/// by first converting T to Self and then serializing to Borsh
/// (Format::decode analogically).
pub trait BorshRepr: BorshSerialize + BorshDeserialize {
    type T;
    fn to_repr(a: &Self::T) -> Self;
    fn from_repr(s: Self) -> Result<Self::T, Error>;
}

impl<R: BorshRepr> Format for R {
    type T = R::T;
    fn encode<W: io::Write>(a: &Self::T, w: &mut W) -> io::Result<()> {
        R::to_repr(a).serialize(w)
    }
    fn decode(a: &[u8]) -> io::Result<Self::T> {
        R::from_repr(R::try_from_slice(a)?)
    }
}

/// This is a wrapper which doesn't change the borsh encoding.
/// It automatically derives BorshRepr by using the trivial embedding
/// as the isomorphism (therefore the derived Format of Borsh<T> is
/// just borsh serialization of T).
#[derive(BorshSerialize, BorshDeserialize)]
pub struct Borsh<T: BorshSerialize + BorshDeserialize>(T);

impl<T: BorshSerialize + BorshDeserialize + Clone> BorshRepr for Borsh<T> {
    type T = T;
    fn to_repr(a: &T) -> Self {
        Self(a.clone())
    }
    fn from_repr(a: Self) -> Result<T, Error> {
        Ok(a.0)
    }
}

/// Combinator which derives BorshRepr for Vec<R>, given
/// BorshRepr for R.
impl<R: BorshRepr> BorshRepr for Vec<R> {
    type T = Vec<R::T>;
    fn to_repr(a: &Self::T) -> Vec<R> {
        a.iter().map(R::to_repr).collect()
    }
    fn from_repr(a: Vec<R>) -> Result<Self::T, Error> {
        a.into_iter().map(R::from_repr).collect()
    }
}

// Little endian representation for u64.
pub struct U64LE;
impl Format for U64LE {
    type T = u64;
    fn encode<W: io::Write>(a: &u64, w: &mut W) -> io::Result<()> {
        w.write_all(&a.to_le_bytes())
    }
    fn decode(a: &[u8]) -> Result<u64, Error> {
        a.try_into().map(u64::from_le_bytes).map_err(invalid_data)
    }
}

/// Column is a type-safe specification of the DB column.
/// It defines how to encode/decode keys and values stored in the column.
pub trait Column {
    const COL: DBCol;
    type Key: Format;
    type Value: Format;
}

/// A type-safe wrapper of the near_store::Store.
#[derive(Clone)]
pub struct Store(std::sync::Arc<dyn near_store::db::Database>);

/// A type-safe wrapper of the near_store::StoreUpdate.
#[derive(Default)]
pub struct StoreUpdate(near_store::db::DBTransaction);

impl Store {
    pub fn new_update(&mut self) -> StoreUpdate {
        Default::default()
    }
    pub fn commit(&mut self, update: StoreUpdate) -> Result<(), Error> {
        self.0.write(update.0)
    }

    pub fn get<C: Column>(
        &self,
        k: &<C::Key as Format>::T,
    ) -> Result<Option<<C::Value as Format>::T>, Error> {
        debug_assert!(!C::COL.is_rc());
        let v = self.0.get_raw_bytes(C::COL, to_vec::<C::Key>(k).as_ref())?;
        Ok(match v {
            Some(v) => Some(C::Value::decode(&v)?),
            None => None,
        })
    }
}

impl From<Arc<dyn near_store::db::Database>> for Store {
    fn from(db: Arc<dyn near_store::db::Database>) -> Self {
        Self(db)
    }
}

impl StoreUpdate {
    pub fn set<C: Column>(&mut self, k: &<C::Key as Format>::T, v: &<C::Value as Format>::T) {
        self.0.set(C::COL, to_vec::<C::Key>(k), to_vec::<C::Value>(v))
    }
    pub fn delete<C: Column>(&mut self, k: &<C::Key as Format>::T) {
        self.0.delete(C::COL, to_vec::<C::Key>(k))
    }
}

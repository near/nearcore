/// Schema module defines a type-safe access to the DB.
/// It is a concise definition of key and value types
/// of the DB columns. For high level access see store.rs.
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_network_primitives::time;
use near_network_primitives::types as primitives;
use near_primitives::account::id::AccountId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_store::DBCol;
use std::io;

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

#[derive(BorshSerialize, BorshDeserialize)]
enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    /// UNIX timestamps in nanos.
    Banned(primitives::ReasonForBan, u64),
}

impl From<primitives::KnownPeerStatus> for KnownPeerStatus {
    fn from(s: primitives::KnownPeerStatus) -> Self {
        match s {
            primitives::KnownPeerStatus::Unknown => Self::Unknown,
            primitives::KnownPeerStatus::NotConnected => Self::NotConnected,
            primitives::KnownPeerStatus::Connected => Self::Connected,
            primitives::KnownPeerStatus::Banned(r, t) => {
                Self::Banned(r, t.unix_timestamp_nanos() as u64)
            }
        }
    }
}

impl From<KnownPeerStatus> for primitives::KnownPeerStatus {
    fn from(s: KnownPeerStatus) -> primitives::KnownPeerStatus {
        match s {
            KnownPeerStatus::Unknown => primitives::KnownPeerStatus::Unknown,
            KnownPeerStatus::NotConnected => primitives::KnownPeerStatus::NotConnected,
            KnownPeerStatus::Connected => primitives::KnownPeerStatus::Connected,
            KnownPeerStatus::Banned(r, t) => primitives::KnownPeerStatus::Banned(
                r,
                time::Utc::from_unix_timestamp_nanos(t as i128).unwrap(),
            ),
        }
    }
}

/// A Borsh representation of the primitives::KnownPeerState.
/// TODO: Currently primitives::KnownPeerState implements Borsh serialization
/// directly, but eventually direct serialization should be removed
/// so that the storage format doesn't leak to the business logic.
/// TODO: Currently primitives::KnownPeerState is identical
/// to the KnownPeerStateRepr, but in the following PR the
/// timestamp type (currently u64), will be replaced with time::Utc.
#[derive(BorshSerialize, BorshDeserialize)]
pub struct KnownPeerStateRepr {
    peer_info: primitives::PeerInfo,
    status: KnownPeerStatus,
    /// UNIX timestamps in nanos.
    first_seen: u64,
    last_seen: u64,
}

impl BorshRepr for KnownPeerStateRepr {
    type T = primitives::KnownPeerState;
    fn to_repr(s: &primitives::KnownPeerState) -> Self {
        Self {
            peer_info: s.peer_info.clone(),
            status: s.status.clone().into(),
            first_seen: s.first_seen.unix_timestamp_nanos() as u64,
            last_seen: s.last_seen.unix_timestamp_nanos() as u64,
        }
    }

    fn from_repr(s: Self) -> Result<primitives::KnownPeerState, Error> {
        Ok(primitives::KnownPeerState {
            peer_info: s.peer_info,
            status: s.status.into(),
            first_seen: time::Utc::from_unix_timestamp_nanos(s.first_seen as i128)
                .map_err(invalid_data)?,
            last_seen: time::Utc::from_unix_timestamp_nanos(s.last_seen as i128)
                .map_err(invalid_data)?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct EdgeRepr {
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

pub struct AccountAnnouncements;
impl Column for AccountAnnouncements {
    const COL: DBCol = DBCol::AccountAnnouncements;
    type Key = AccountIdFormat;
    type Value = Borsh<AnnounceAccount>;
}

pub struct Peers;
impl Column for Peers {
    const COL: DBCol = DBCol::Peers;
    type Key = Borsh<PeerId>;
    type Value = KnownPeerStateRepr;
}

pub struct PeerComponent;
impl Column for PeerComponent {
    const COL: DBCol = DBCol::PeerComponent;
    type Key = Borsh<PeerId>;
    type Value = Borsh<u64>;
}

pub struct ComponentEdges;
impl Column for ComponentEdges {
    const COL: DBCol = DBCol::ComponentEdges;
    type Key = U64LE;
    type Value = Vec<EdgeRepr>;
}

pub struct LastComponentNonce;
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
pub struct Store(near_store::Store);

/// A type-safe wrapper of the near_store::StoreUpdate.
pub struct StoreUpdate(near_store::StoreUpdate);

impl Store {
    pub fn new(s: near_store::Store) -> Store {
        Store(s)
    }
    pub fn new_update(&mut self) -> StoreUpdate {
        StoreUpdate(self.0.store_update())
    }
    pub fn iter<C: Column>(
        &self,
    ) -> impl Iterator<Item = Result<(<C::Key as Format>::T, <C::Value as Format>::T), Error>> + '_
    {
        self.0.iter(C::COL).map(|(k, v)| Ok((C::Key::decode(&k)?, C::Value::decode(&v)?)))
    }
    pub fn get<C: Column>(
        &self,
        k: &<C::Key as Format>::T,
    ) -> Result<Option<<C::Value as Format>::T>, Error> {
        let v = self.0.get(C::COL, to_vec::<C::Key>(k).as_ref())?;
        Ok(match v {
            Some(v) => Some(C::Value::decode(&v)?),
            None => None,
        })
    }
}

impl StoreUpdate {
    pub fn set<C: Column>(&mut self, k: &<C::Key as Format>::T, v: &<C::Value as Format>::T) {
        self.0.set(C::COL, to_vec::<C::Key>(k).as_ref(), to_vec::<C::Value>(v).as_ref())
    }
    pub fn delete<C: Column>(&mut self, k: &<C::Key as Format>::T) {
        self.0.delete(C::COL, to_vec::<C::Key>(k).as_ref())
    }
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit()
    }
}

/// Schema module defines a type-safe access to the DB.
/// It is a concise definition of key and value types
/// of the DB columns. For high level access see store.rs.
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::Signature;
use near_network_primitives::time;
use near_network_primitives::types as primitives;
use near_primitives::account::id::{AccountId, ParseAccountError};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_store::DBCol;
use thiserror::Error;

pub struct AccountIdFormat;
impl Format for AccountIdFormat {
    type T = AccountId;
    fn to_vec(a: &AccountId) -> Vec<u8> {
        a.as_ref().as_bytes().into()
    }
    fn from_slice(a: &[u8]) -> Result<AccountId, Error> {
        std::str::from_utf8(a).map_err(Error::Utf8)?.parse().map_err(Error::AccountId)
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

#[derive(BorshSerialize, BorshDeserialize)]
pub struct KnownPeerState {
    peer_info: primitives::PeerInfo,
    status: KnownPeerStatus,
    /// UNIX timestamps in nanos.
    first_seen: u64,
    last_seen: u64,
}

impl BorshRepr for KnownPeerState {
    type T = primitives::KnownPeerState;
    fn to_repr(s: &primitives::KnownPeerState) -> Self {
        Self {
            peer_info: s.peer_info.clone(),
            status: s.status.clone().into(),
            first_seen: s.first_seen.unix_timestamp_nanos() as u64,
            last_seen: s.last_seen.unix_timestamp_nanos() as u64,
        }
    }

    fn from_repr(s: KnownPeerState) -> Result<primitives::KnownPeerState, Error> {
        Ok(primitives::KnownPeerState {
            peer_info: s.peer_info,
            status: s.status.into(),
            first_seen: time::Utc::from_unix_timestamp_nanos(s.first_seen as i128)
                .map_err(Error::Time)?,
            last_seen: time::Utc::from_unix_timestamp_nanos(s.last_seen as i128)
                .map_err(Error::Time)?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct Edge {
    key: (PeerId, PeerId),
    nonce: u64,
    signature0: Signature,
    signature1: Signature,
    removal_info: Option<(bool, Signature)>,
}

impl BorshRepr for Edge {
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
    type Value = KnownPeerState;
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
    type Value = Vec<Edge>;
}

pub struct LastComponentNonce;
impl Column for LastComponentNonce {
    const COL: DBCol = DBCol::LastComponentNonce;
    type Key = Borsh<()>;
    type Value = Borsh<u64>;
}

////////////////////////////////////////////////////
// Storage

// Parsing error.
#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Utf8(std::str::Utf8Error),
    #[error(transparent)]
    IO(std::io::Error),
    #[error(transparent)]
    Slice(std::array::TryFromSliceError),
    #[error(transparent)]
    AccountId(ParseAccountError),
    #[error(transparent)]
    Time(time::error::ComponentRange),
}

pub trait Format {
    type T;
    fn to_vec(a: &Self::T) -> Vec<u8>;
    fn from_slice(a: &[u8]) -> Result<Self::T, Error>;
}

pub trait BorshRepr: BorshSerialize + BorshDeserialize {
    type T;
    fn to_repr(a: &Self::T) -> Self;
    fn from_repr(s: Self) -> Result<Self::T, Error>;
}

impl<R: BorshRepr> Format for R {
    type T = R::T;
    fn to_vec(a: &Self::T) -> Vec<u8> {
        R::to_repr(a).try_to_vec().unwrap()
    }
    fn from_slice(a: &[u8]) -> Result<Self::T, Error> {
        R::from_repr(R::try_from_slice(a).map_err(Error::IO)?)
    }
}

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

impl<R: BorshRepr> BorshRepr for Vec<R> {
    type T = Vec<R::T>;
    fn to_repr(a: &Self::T) -> Vec<R> {
        a.iter().map(R::to_repr).collect()
    }
    fn from_repr(a: Vec<R>) -> Result<Self::T, Error> {
        a.into_iter().map(R::from_repr).collect()
    }
}

pub struct U64LE;
impl Format for U64LE {
    type T = u64;
    fn to_vec(a: &u64) -> Vec<u8> {
        a.to_le_bytes().into()
    }
    fn from_slice(a: &[u8]) -> Result<u64, Error> {
        a.try_into().map(u64::from_le_bytes).map_err(Error::Slice)
    }
}

pub trait Column {
    const COL: DBCol;
    type Key: Format;
    type Value: Format;
}

#[derive(Clone)]
pub struct Store(near_store::Store);

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
        self.0.iter(C::COL).map(|(k, v)| Ok((C::Key::from_slice(&k)?, C::Value::from_slice(&v)?)))
    }
    pub fn get<C: Column>(
        &self,
        k: &<C::Key as Format>::T,
    ) -> Result<Option<<C::Value as Format>::T>, Error> {
        let v = self.0.get(C::COL, C::Key::to_vec(k).as_ref()).map_err(Error::IO)?;
        Ok(match v {
            Some(v) => Some(C::Value::from_slice(&v)?),
            None => None,
        })
    }
}

impl StoreUpdate {
    pub fn set<C: Column>(&mut self, k: &<C::Key as Format>::T, v: &<C::Value as Format>::T) {
        self.0.set(C::COL, C::Key::to_vec(k).as_ref(), C::Value::to_vec(v).as_ref())
    }
    pub fn delete<C: Column>(&mut self, k: &<C::Key as Format>::T) {
        self.0.delete(C::COL, C::Key::to_vec(k).as_ref())
    }
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit().map_err(Error::IO)
    }
}

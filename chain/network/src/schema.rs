/// Schema module defines a type-safe access to the DB.
/// It is a concise definition of key and value types
/// of the DB columns. For high level access see store.rs.
use borsh::{BorshDeserialize, BorshSerialize};
use near_network_primitives::time;
use near_network_primitives::types as primitives;
use near_primitives::account::id::{AccountId, ParseAccountError};
use near_primitives::network::{AnnounceAccount, PeerId};
use near_store::DBCol;
use thiserror::Error;

// RoutingTableView storage schema.

pub struct AccountIdFormat;
impl Format<AccountId> for AccountIdFormat {
    fn to_vec(a: &AccountId) -> Vec<u8> {
        a.as_ref().as_bytes().into()
    }
    fn from_slice(a: &[u8]) -> Result<AccountId, Error> {
        std::str::from_utf8(a).map_err(Error::Utf8)?.parse().map_err(Error::AccountId)
    }
}

pub struct AccountAnnouncements;
impl Column for AccountAnnouncements {
    const COL: DBCol = DBCol::AccountAnnouncements;
    type Key = AccountId;
    type KeyFormat = AccountIdFormat;
    type Value = AnnounceAccount;
    type ValueFormat = Borsh;
}

// PeerStore storage schema.

#[derive(BorshSerialize, BorshDeserialize)]
enum KnownPeerStatus {
    Unknown,
    NotConnected,
    Connected,
    /// UNIX timestamps in nanos.
    Banned(primitives::ReasonForBan, u64),
}

impl KnownPeerStatus {
    fn new(s: primitives::KnownPeerStatus) -> Self {
        match s {
            primitives::KnownPeerStatus::Unknown => Self::Unknown,
            primitives::KnownPeerStatus::NotConnected => Self::NotConnected,
            primitives::KnownPeerStatus::Connected => Self::Connected,
            primitives::KnownPeerStatus::Banned(r, t) => {
                Self::Banned(r, t.unix_timestamp_nanos() as u64)
            }
        }
    }
    fn parse(self) -> primitives::KnownPeerStatus {
        match self {
            Self::Unknown => primitives::KnownPeerStatus::Unknown,
            Self::NotConnected => primitives::KnownPeerStatus::NotConnected,
            Self::Connected => primitives::KnownPeerStatus::Connected,
            Self::Banned(r, t) => primitives::KnownPeerStatus::Banned(
                r,
                time::Utc::from_unix_timestamp_nanos(t as i128).unwrap(),
            ),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct KnownPeerState {
    peer_info: primitives::PeerInfo,
    status: KnownPeerStatus,
    /// UNIX timestamps in nanos.
    first_seen: u64,
    last_seen: u64,
}

impl KnownPeerState {
    fn new(s: &primitives::KnownPeerState) -> Self {
        Self {
            peer_info: s.peer_info.clone(),
            status: KnownPeerStatus::new(s.status.clone()),
            first_seen: s.first_seen.unix_timestamp_nanos() as u64,
            last_seen: s.last_seen.unix_timestamp_nanos() as u64,
        }
    }

    fn parse(self) -> Result<primitives::KnownPeerState, Error> {
        Ok(primitives::KnownPeerState {
            peer_info: self.peer_info,
            status: self.status.parse(),
            first_seen: time::Utc::from_unix_timestamp_nanos(self.first_seen as i128)
                .map_err(Error::Time)?,
            last_seen: time::Utc::from_unix_timestamp_nanos(self.last_seen as i128)
                .map_err(Error::Time)?,
        })
    }
}

pub struct KnownPeerStateFormat;
impl Format<primitives::KnownPeerState> for KnownPeerStateFormat {
    fn to_vec(a: &primitives::KnownPeerState) -> Vec<u8> {
        Borsh::to_vec(&KnownPeerState::new(a))
    }
    fn from_slice(a: &[u8]) -> Result<primitives::KnownPeerState, Error> {
        <Borsh as Format<KnownPeerState>>::from_slice(a)?.parse()
    }
}

pub struct Peers;
impl Column for Peers {
    const COL: DBCol = DBCol::Peers;
    type Key = PeerId;
    type KeyFormat = Borsh;
    type Value = primitives::KnownPeerState;
    type ValueFormat = KnownPeerStateFormat;
}

// Routing storage schema

pub struct PeerComponent;
impl Column for PeerComponent {
    const COL: DBCol = DBCol::PeerComponent;
    type Key = PeerId;
    type KeyFormat = Borsh;
    type Value = u64;
    type ValueFormat = Borsh;
}

pub struct ComponentEdges;
impl Column for ComponentEdges {
    const COL: DBCol = DBCol::ComponentEdges;
    type Key = u64;
    type KeyFormat = U64LE;
    type Value = Vec<primitives::Edge>;
    type ValueFormat = Borsh;
}

pub struct LastComponentNonce;
impl Column for LastComponentNonce {
    const COL: DBCol = DBCol::LastComponentNonce;
    type Key = ();
    type KeyFormat = Borsh;
    type Value = u64;
    type ValueFormat = Borsh;
}

////////////////////////////////////////////////////

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

pub trait Format<T> {
    fn to_vec(a: &T) -> Vec<u8>;
    fn from_slice(a: &[u8]) -> Result<T, Error>;
}

pub struct U64LE;
impl Format<u64> for U64LE {
    fn to_vec(a: &u64) -> Vec<u8> {
        a.to_le_bytes().into()
    }
    fn from_slice(a: &[u8]) -> Result<u64, Error> {
        a.try_into().map(u64::from_le_bytes).map_err(Error::Slice)
    }
}

pub struct Borsh;
impl<T: BorshSerialize + BorshDeserialize> Format<T> for Borsh {
    fn to_vec(a: &T) -> Vec<u8> {
        a.try_to_vec().unwrap()
    }
    fn from_slice(a: &[u8]) -> Result<T, Error> {
        T::try_from_slice(a).map_err(Error::IO)
    }
}

pub trait Column {
    const COL: DBCol;
    type Key;
    type KeyFormat: Format<Self::Key>;
    type Value;
    type ValueFormat: Format<Self::Value>;
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
    pub fn iter<C: Column>(&self) -> impl Iterator<Item = Result<(C::Key, C::Value), Error>> + '_ {
        self.0
            .iter(C::COL)
            .map(|(k, v)| Ok((C::KeyFormat::from_slice(&k)?, C::ValueFormat::from_slice(&v)?)))
    }
    pub fn get<C: Column>(&self, k: &C::Key) -> Result<Option<C::Value>, Error> {
        let v = self.0.get(C::COL, C::KeyFormat::to_vec(k).as_ref()).map_err(Error::IO)?;
        Ok(match v {
            Some(v) => Some(C::ValueFormat::from_slice(&v)?),
            None => None,
        })
    }
}

impl StoreUpdate {
    pub fn set<C: Column>(&mut self, k: &C::Key, v: &C::Value) {
        self.0.set(C::COL, C::KeyFormat::to_vec(k).as_ref(), C::ValueFormat::to_vec(v).as_ref())
    }
    pub fn delete<C: Column>(&mut self, k: &C::Key) {
        self.0.delete(C::COL, C::KeyFormat::to_vec(k).as_ref())
    }
    pub fn commit(self) -> Result<(), Error> {
        self.0.commit().map_err(Error::IO)
    }
}

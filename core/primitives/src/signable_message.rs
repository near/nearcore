use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::AccountId;

// These numbers are picked to be compatible with the current protocol and how
// transactions are defined in it. Introducing this is no protocol change. This
// is just a forward-looking implementation detail of meta transactions.
//
// We plan to establish a standard with NEP-461 that makes this an official
// specification in the wider ecosystem. Note that NEP-461 should not change the
// protocol in any way, unless we have to change meta transaction implementation
// details to adhere to the future standard.
//
// TODO: consider making these public once there is an approved standard.
const MIN_ON_CHAIN_DISCRIMINANT: u32 = 1 << 30;
const MAX_ON_CHAIN_DISCRIMINANT: u32 = 1 << 31 - 1;
const MIN_OFF_CHAIN_DISCRIMINANT: u32 = 1 << 31 - 1;
const MAX_OFF_CHAIN_DISCRIMINANT: u32 = u32::MAX;

// NEPs currently included in the scheme
const NEP_366_META_TRANSACTIONS: u32 = 366;

/// Used to distinguish message types that are sign by account keys, to avoid an
/// abuse of signed messages as something else.
///
/// This prefix must be be at the first four bytes of a message body that is
/// signed under this signature scheme.
///
/// The scheme is a draft introduced to avoid security issues with the
/// implementation of meta transactions (NEP-366) but will eventually be
/// standardized with NEP-461 that solves the problem more generally.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct MessageDiscriminant {
    /// The unique prefix, serialized in little-endian by borsh.
    discriminant: u32,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct SignableMessage<'a, T> {
    pub discriminant: MessageDiscriminant,
    pub msg: &'a T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SignableMessageType {
    /// A delegate action, intended for a relayer to included it in an action list of a transaction.
    DelegateAction,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ReadDiscriminantError {
    #[error("does not fit any known categories")]
    UnknownMessageType,
    #[error("NEP {0} does not have a known on-chain use")]
    UnknownOnChainNep(u32),
    #[error("NEP {0} does not have a known off-chain use")]
    UnknownOffChainNep(u32),
    #[error("discriminant is in the range for transactions")]
    TransactionFound,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CreateDiscriminantError {
    #[error("nep number {0} is too big")]
    NepTooLarge(u32),
}

impl<'a, T> SignableMessage<'a, T> {
    pub fn new(msg: &'a T, ty: SignableMessageType) -> Self {
        let discriminant = ty.into();
        Self { discriminant, msg }
    }
}

impl MessageDiscriminant {
    /// Create a discriminant for an on-chain actionable message that was introduced in the specified NEP.
    ///
    /// Allows creating discriminants currently unknown in this crate, which can
    /// be useful to prototype new standards. For example, when the client
    /// project still relies on an older version of this crate while nightly
    /// nearcore already supports a new NEP.
    pub fn new_on_chain(nep: u32) -> Result<Self, CreateDiscriminantError> {
        // unchecked arithmetic: these are constants
        if nep > MAX_ON_CHAIN_DISCRIMINANT - MIN_ON_CHAIN_DISCRIMINANT {
            Err(CreateDiscriminantError::NepTooLarge(nep))
        } else {
            Ok(Self {
                // unchecked arithmetic: just checked range
                discriminant: MIN_ON_CHAIN_DISCRIMINANT + nep,
            })
        }
    }

    /// Create a discriminant for an off-chain message that was introduced in the specified NEP.
    ///
    /// Allows creating discriminants currently unknown in this crate, which can
    /// be useful to prototype new standards. For example, when the client
    /// project still relies on an older version of this crate while nightly
    /// nearcore already supports a new NEP.
    pub fn new_off_chain(nep: u32) -> Result<Self, CreateDiscriminantError> {
        // unchecked arithmetic: these are constants
        if nep > MAX_OFF_CHAIN_DISCRIMINANT - MIN_OFF_CHAIN_DISCRIMINANT {
            Err(CreateDiscriminantError::NepTooLarge(nep))
        } else {
            Ok(Self {
                // unchecked arithmetic: just checked range
                discriminant: MIN_OFF_CHAIN_DISCRIMINANT + nep,
            })
        }
    }

    /// Returns the raw integer value of the discriminant as an integer value.
    pub fn raw_discriminant(&self) -> u32 {
        self.discriminant
    }

    /// Whether this discriminant marks a traditional `SignedTransaction`.
    pub fn is_transaction(&self) -> bool {
        // Backwards compatibility with transaction that were defined before this standard:
        // Transaction begins with `AccountId`, which is just a `String` in
        // borsh serialization, which starts with the length of the underlying
        // byte vector in little endian u32.
        // Currently allowed AccountIds are between 2 and 64 bytes.
        self.discriminant >= AccountId::MIN_LEN as u32
            && self.discriminant <= AccountId::MAX_LEN as u32
    }

    /// If this discriminant marks a message intended for on-chain use, return
    /// the NEP in which the message type was introduced.
    pub fn on_chain_nep(&self) -> Option<u32> {
        if self.discriminant < MIN_ON_CHAIN_DISCRIMINANT
            || self.discriminant > MAX_ON_CHAIN_DISCRIMINANT
        {
            None
        } else {
            // unchecked arithmetic: just checked it is in range
            let nep = self.discriminant - MIN_ON_CHAIN_DISCRIMINANT;
            Some(nep)
        }
    }

    /// If this discriminant marks a message intended for off-chain use, return
    /// the NEP in which the message type was introduced.
    pub fn off_chain_nep(&self) -> Option<u32> {
        if self.discriminant < MIN_OFF_CHAIN_DISCRIMINANT
            || self.discriminant > MAX_OFF_CHAIN_DISCRIMINANT
        {
            None
        } else {
            // unchecked arithmetic: just checked it is in range
            let nep = self.discriminant - MIN_OFF_CHAIN_DISCRIMINANT;
            Some(nep)
        }
    }
}

impl TryFrom<MessageDiscriminant> for SignableMessageType {
    type Error = ReadDiscriminantError;

    fn try_from(discriminant: MessageDiscriminant) -> Result<Self, Self::Error> {
        if discriminant.is_transaction() {
            Err(Self::Error::TransactionFound)
        } else if let Some(nep) = discriminant.on_chain_nep() {
            match nep {
                NEP_366_META_TRANSACTIONS => Ok(Self::DelegateAction),
                _ => Err(Self::Error::UnknownOnChainNep(nep)),
            }
        } else if let Some(nep) = discriminant.off_chain_nep() {
            Err(Self::Error::UnknownOffChainNep(nep))
        } else {
            Err(Self::Error::UnknownMessageType)
        }
    }
}

impl From<SignableMessageType> for MessageDiscriminant {
    fn from(ty: SignableMessageType) -> Self {
        // unwrapping here is ok, we know the constant NEP numbers used are in range
        match ty {
            SignableMessageType::DelegateAction => {
                MessageDiscriminant::new_on_chain(NEP_366_META_TRANSACTIONS).unwrap()
            }
        }
    }
}

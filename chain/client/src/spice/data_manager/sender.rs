use near_crypto::PublicKey;
use near_primitives::spice::partial_data::{SpicePartialData, SpiceVerifiedPartialData};
use near_primitives::types::AccountId;

/// The sender of a message whose signature verified, with the key that verified it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AuthenticatedSender {
    account_id: AccountId,
    key: PublicKey,
}

impl AuthenticatedSender {
    /// Checks the signature of `data` against `keys`; `None` if no key verifies it.
    pub(super) fn verify(
        data: SpicePartialData,
        keys: &[PublicKey],
    ) -> Option<(Self, SpiceVerifiedPartialData)> {
        let (verified, key) = data.into_verified_with_key(keys)?;
        let sender = Self { account_id: verified.sender.clone(), key };
        Some((sender, verified))
    }

    pub(super) fn account_id(&self) -> &AccountId {
        &self.account_id
    }

    pub(super) fn key(&self) -> &PublicKey {
        &self.key
    }
}

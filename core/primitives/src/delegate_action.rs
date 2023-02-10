//! DelegateAction is a type of action to support meta transactions.
//!
//! NEP: https://github.com/near/NEPs/pull/366
//! This is the module containing the types introduced for delegate actions.

pub use self::private_non_delegate_action::NonDelegateAction;
use crate::hash::{hash, CryptoHash};
use crate::transaction::Action;
use crate::types::{AccountId, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::types::BlockHeight;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

/// This is an index number of Action::Delegate in Action enumeration
const ACTION_DELEGATE_NUMBER: u8 = 8;
/// This action allows to execute the inner actions behalf of the defined sender.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct DelegateAction {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    ///
    /// With the meta transactions MVP defined in NEP-366, nested
    /// DelegateActions are not allowed. A separate type is used to enforce it.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce to ensure that the same delegate action is not sent twice by a
    /// relayer and should match for given account's `public_key`.
    /// After this action is processed it will increment.
    pub nonce: Nonce,
    /// The maximal height of the block in the blockchain below which the given DelegateAction is valid.
    pub max_block_height: BlockHeight,
    /// Public key used to sign this delegated action.
    pub public_key: PublicKey,
}

#[cfg_attr(feature = "protocol_feature_nep366_delegate_action", derive(BorshDeserialize))]
#[derive(BorshSerialize, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SignedDelegateAction {
    pub delegate_action: DelegateAction,
    pub signature: Signature,
}

#[cfg(not(feature = "protocol_feature_nep366_delegate_action"))]
impl borsh::de::BorshDeserialize for SignedDelegateAction {
    fn deserialize(_buf: &mut &[u8]) -> ::core::result::Result<Self, borsh::maybestd::io::Error> {
        return Err(Error::new(ErrorKind::InvalidInput, "Delegate action isn't supported"));
    }
}

impl SignedDelegateAction {
    pub fn verify(&self) -> bool {
        let delegate_action = &self.delegate_action;
        let hash = delegate_action.get_hash();
        let public_key = &delegate_action.public_key;

        self.signature.verify(hash.as_ref(), public_key)
    }
}

#[cfg(feature = "protocol_feature_nep366_delegate_action")]
impl From<SignedDelegateAction> for Action {
    fn from(delegate_action: SignedDelegateAction) -> Self {
        Self::Delegate(delegate_action)
    }
}

impl DelegateAction {
    pub fn get_actions(&self) -> Vec<Action> {
        self.actions.iter().map(|a| a.clone().into()).collect()
    }

    pub fn get_hash(&self) -> CryptoHash {
        let bytes = self.try_to_vec().expect("Failed to deserialize");
        hash(&bytes)
    }
}

/// A small private module to protect the private fields inside `NonDelegateAction`.
mod private_non_delegate_action {
    use super::*;

    /// This is Action which mustn't contain DelegateAction.
    ///
    /// This struct is needed to avoid the recursion when Action/DelegateAction is deserialized.
    ///
    /// Important: Don't make the inner Action public, this must only be constructed
    /// through the correct interface that ensures the inner Action is actually not
    /// a delegate action. That would break an assumption of this type, which we use
    /// in several places. For example, borsh de-/serialization relies on it. If the
    /// invariant is broken, we may end up with a `Transaction` or `Receipt` that we
    /// can serialize but deserializing it back causes a parsing error.
    #[derive(Serialize, BorshSerialize, Deserialize, PartialEq, Eq, Clone, Debug)]
    pub struct NonDelegateAction(Action);

    impl From<NonDelegateAction> for Action {
        fn from(action: NonDelegateAction) -> Self {
            action.0
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("attempted to construct NonDelegateAction from Action::Delegate")]
    pub struct IsDelegateAction;

    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    impl TryFrom<Action> for NonDelegateAction {
        type Error = IsDelegateAction;

        fn try_from(action: Action) -> Result<Self, IsDelegateAction> {
            if matches!(action, Action::Delegate(_)) {
                Err(IsDelegateAction)
            } else {
                Ok(Self(action))
            }
        }
    }

    impl borsh::de::BorshDeserialize for NonDelegateAction {
        fn deserialize(
            buf: &mut &[u8],
        ) -> ::core::result::Result<Self, borsh::maybestd::io::Error> {
            if buf.is_empty() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Failed to deserialize DelegateAction",
                ));
            }
            match buf[0] {
                ACTION_DELEGATE_NUMBER => Err(Error::new(
                    ErrorKind::InvalidInput,
                    "DelegateAction mustn't contain a nested one",
                )),
                _ => Ok(Self(borsh::BorshDeserialize::deserialize(buf)?)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    use crate::transaction::CreateAccountAction;
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    use near_crypto::KeyType;

    /// A serialized `Action::Delegate(SignedDelegateAction)` for testing.
    ///
    /// We want this to be parseable and accepted by protocol versions with meta
    /// transactions enabled. But it should fail either in parsing or in
    /// validation when this is included in a receipt for a block of an earlier
    /// version. For now, it just fails to parse, as a test below checks.
    const DELEGATE_ACTION_HEX: &str = concat!(
        "0803000000616161030000006262620100000000010000000000000002000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000"
    );

    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn create_delegate_action(actions: Vec<Action>) -> Action {
        Action::Delegate(SignedDelegateAction {
            delegate_action: DelegateAction {
                sender_id: "aaa".parse().unwrap(),
                receiver_id: "bbb".parse().unwrap(),
                actions: actions
                    .iter()
                    .map(|a| NonDelegateAction::try_from(a.clone()).unwrap())
                    .collect(),
                nonce: 1,
                max_block_height: 2,
                public_key: PublicKey::empty(KeyType::ED25519),
            },
            signature: Signature::empty(KeyType::ED25519),
        })
    }

    #[test]
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn test_delegate_action_deserialization() {
        // Expected an error. Buffer is empty
        assert_eq!(
            NonDelegateAction::try_from_slice(Vec::new().as_ref()).map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );

        let delegate_action = create_delegate_action(Vec::<Action>::new());
        let serialized_non_delegate_action = delegate_action.try_to_vec().expect("Expect ok");

        // Expected Action::Delegate has not been moved in enum Action
        assert_eq!(serialized_non_delegate_action[0], ACTION_DELEGATE_NUMBER);

        // Expected a nested DelegateAction error
        assert_eq!(
            NonDelegateAction::try_from_slice(&serialized_non_delegate_action)
                .map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );

        let delegate_action =
            create_delegate_action(vec![Action::CreateAccount(CreateAccountAction {})]);
        let serialized_delegate_action = delegate_action.try_to_vec().expect("Expect ok");

        // Valid action
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).expect("Expect ok"),
            delegate_action
        );
    }

    /// Check that we will not accept delegate actions with the feature
    /// disabled.
    ///
    /// This test is to ensure that while working on meta transactions, we don't
    /// accientally start accepting delegate actions in receipts. Otherwise, a
    /// malicious validator could create receipts that include delegate actions
    /// and other nodes will accept such a receipt.
    ///
    /// TODO: Before stabilizing "protocol_feature_nep366_delegate_action" we
    /// have to replace this rest with a test that checks that we discard
    /// delegate actions for earlier versions somewhere in validation.
    #[test]
    #[cfg(not(feature = "protocol_feature_nep366_delegate_action"))]
    fn test_delegate_action_deserialization() {
        let serialized_delegate_action = hex::decode(DELEGATE_ACTION_HEX).expect("invalid hex");

        // DelegateAction isn't supported
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).map_err(|e| e.kind()),
            Err(ErrorKind::InvalidInput)
        );
    }

    /// Check that the hard-coded delegate action is valid.
    #[test]
    #[cfg(feature = "protocol_feature_nep366_delegate_action")]
    fn test_delegate_action_deserialization_hard_coded() {
        use crate::transaction::CreateAccountAction;

        let serialized_delegate_action = hex::decode(DELEGATE_ACTION_HEX).expect("invalid hex");
        // The hex data is the same as the one we create below.
        let delegate_action =
            create_delegate_action(vec![Action::CreateAccount(CreateAccountAction {})]);

        // Valid action
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).expect("Expect ok"),
            delegate_action
        );
    }
}

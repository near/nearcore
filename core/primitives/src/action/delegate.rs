//! DelegateAction is a type of action to support meta transactions.
//!
//! NEP: <https://github.com/near/NEPs/pull/366>
//! This is the module containing the types introduced for delegate actions.

use super::Action;
use crate::signable_message::{SignableMessage, SignableMessageType};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature, Signer};
use near_primitives_core::hash::{CryptoHash, hash};
use near_primitives_core::types::BlockHeight;
use near_primitives_core::types::{AccountId, Nonce};
use near_schema_checker_lib::ProtocolSchema;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Read};

/// This is an index number of Action::Delegate in Action enumeration
const ACTION_DELEGATE_NUMBER: u8 = 8;
/// This action allows to execute the inner actions behalf of the defined sender.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Clone,
    Debug,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SignedDelegateAction {
    pub delegate_action: DelegateAction,
    pub signature: Signature,
}

impl SignedDelegateAction {
    pub fn verify(&self) -> bool {
        let delegate_action = &self.delegate_action;
        let hash = delegate_action.get_nep461_hash();
        let public_key = &delegate_action.public_key;

        self.signature.verify(hash.as_ref(), public_key)
    }

    pub fn sign(singer: &Signer, delegate_action: DelegateAction) -> Self {
        let signature = singer.sign(delegate_action.get_nep461_hash().as_bytes());
        Self { delegate_action, signature }
    }
}

impl From<SignedDelegateAction> for Action {
    fn from(delegate_action: SignedDelegateAction) -> Self {
        Self::Delegate(Box::new(delegate_action))
    }
}

impl DelegateAction {
    pub fn get_actions(&self) -> Vec<Action> {
        self.actions.iter().map(|a| a.clone().into()).collect()
    }

    /// Delegate action hash used for NEP-461 signature scheme which tags
    /// different messages before hashing
    ///
    /// For more details, see: [NEP-461](https://github.com/near/NEPs/pull/461)
    pub fn get_nep461_hash(&self) -> CryptoHash {
        let signable = SignableMessage::new(&self, SignableMessageType::DelegateAction);
        let bytes = borsh::to_vec(&signable).expect("Failed to deserialize");
        hash(&bytes)
    }
}

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
#[derive(Serialize, BorshSerialize, Deserialize, PartialEq, Eq, Clone, Debug, ProtocolSchema)]
pub struct NonDelegateAction(Action);

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for NonDelegateAction {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NonDelegateAction".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Get the actual Action schema by calling its json_schema method directly
        // This gives us the full schema with the oneOf array, not just a reference
        let mut action_schema = Action::json_schema(generator);

        // Find and filter the oneOf array directly on the Schema object
        if let Some(one_of) = action_schema.get_mut("oneOf") {
            if let Some(arr) = one_of.as_array_mut() {
                // Remove the Delegate variant
                arr.retain(|variant| {
                    !variant
                        .get("properties")
                        .and_then(|p| p.as_object())
                        .map(|p| p.contains_key("Delegate"))
                        .unwrap_or(false)
                });
            }
        }

        // Update description to be more client-friendly
        action_schema.insert("description".to_string(), serde_json::json!(
            "An Action that can be included in a transaction or receipt, excluding delegate actions. \
            This type represents all possible action types except DelegateAction to prevent \
            infinite recursion in meta-transactions."
        ));

        action_schema
    }
}

/// A small private module to protect the private fields inside `NonDelegateAction`.
mod private_non_delegate_action {
    use super::*;

    impl From<NonDelegateAction> for Action {
        fn from(action: NonDelegateAction) -> Self {
            action.0
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("attempted to construct NonDelegateAction from Action::Delegate")]
    pub struct IsDelegateAction;

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
        fn deserialize_reader<R: Read>(rd: &mut R) -> ::core::result::Result<Self, Error> {
            match u8::deserialize_reader(rd)? {
                ACTION_DELEGATE_NUMBER => Err(Error::new(
                    ErrorKind::InvalidInput,
                    "DelegateAction mustn't contain a nested one",
                )),
                n => borsh::de::EnumExt::deserialize_variant(rd, n).map(Self),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::CreateAccountAction;
    use near_crypto::KeyType;

    /// A serialized `Action::Delegate(SignedDelegateAction)` for testing.
    ///
    /// We want this to be parsable and accepted by protocol versions with meta
    /// transactions enabled. But it should fail either in parsing or in
    /// validation when this is included in a receipt for a block of an earlier
    /// version. For now, it just fails to parse, as a test below checks.
    const DELEGATE_ACTION_HEX: &str = concat!(
        "0803000000616161030000006262620100000000010000000000000002000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000000000000000000000"
    );

    fn create_delegate_action(actions: Vec<Action>) -> Action {
        Action::Delegate(Box::new(SignedDelegateAction {
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
        }))
    }

    #[test]
    fn test_delegate_action_deserialization() {
        // Expected an error. Buffer is empty
        assert_eq!(
            NonDelegateAction::try_from_slice(Vec::new().as_ref()).map_err(|e| e.kind()),
            Err(ErrorKind::InvalidData)
        );

        let delegate_action = create_delegate_action(Vec::<Action>::new());
        let serialized_non_delegate_action = borsh::to_vec(&delegate_action).expect("Expect ok");

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
        let serialized_delegate_action = borsh::to_vec(&delegate_action).expect("Expect ok");

        // Valid action
        assert_eq!(
            Action::try_from_slice(&serialized_delegate_action).expect("Expect ok"),
            delegate_action
        );
    }

    /// Check that the hard-coded delegate action is valid.
    #[test]
    fn test_delegate_action_deserialization_hard_coded() {
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

    #[cfg(feature = "schemars")]
    #[test]
    fn test_non_delegate_action_json_schema_excludes_delegate() {
        use schemars::{JsonSchema, SchemaGenerator};

        // Create a schema generator
        let mut generator = SchemaGenerator::default();

        // Generate the NonDelegateAction schema
        let non_delegate_schema = NonDelegateAction::json_schema(&mut generator);

        // Convert to JSON for inspection
        let schema_json = serde_json::to_value(&non_delegate_schema).unwrap();

        // Get the oneOf array
        let one_of = schema_json
            .get("oneOf")
            .expect("NonDelegateAction schema must have oneOf")
            .as_array()
            .expect("NonDelegateAction oneOf must be an array");

        // Verify that none of the variants have a Delegate property
        for variant in one_of {
            if let Some(properties) = variant.get("properties") {
                if let Some(props_obj) = properties.as_object() {
                    assert!(
                        !props_obj.contains_key("Delegate"),
                        "NonDelegateAction schema should not contain Delegate variant"
                    );
                }
            }
        }

        // Verify that the Action schema (for comparison) does include Delegate
        let action_schema = Action::json_schema(&mut generator);
        let action_json = serde_json::to_value(&action_schema).unwrap();

        // Action MUST have a oneOf array
        let action_one_of = action_json
            .get("oneOf")
            .expect("Action schema must have oneOf")
            .as_array()
            .expect("Action oneOf must be an array");

        // Count how many variants have Delegate
        let delegate_count = action_one_of
            .iter()
            .filter(|variant| {
                variant
                    .get("properties")
                    .and_then(|p| p.as_object())
                    .map(|p| p.contains_key("Delegate"))
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(delegate_count, 1, "Action schema should contain exactly one Delegate variant");

        // NonDelegateAction should have one less variant than Action
        assert_eq!(
            one_of.len(),
            action_one_of.len() - 1,
            "NonDelegateAction should have one less variant than Action (excluding Delegate)"
        );
    }
}

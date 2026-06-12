//! DelegateAction is a type of action to support meta transactions.
//!
//! NEP: <https://github.com/near/NEPs/pull/366>
//! This is the module containing the types introduced for delegate actions.

use super::Action;
use crate::signable_message::{SignableMessage, SignableMessageType};
use crate::transaction::TransactionNonce;
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
/// This is an index number of Action::DelegateV2 in Action enumeration
const ACTION_DELEGATE_V2_NUMBER: u8 = 14;
/// Borsh discriminants of the delegate actions, rejected by `NonDelegateAction`
/// so a delegate action can't be nested. Must list exactly the variants for
/// which `Action::is_delegate` is true; cross-checked in
/// `test_delegate_variant_encodings_match`.
const DELEGATE_VARIANT_NUMBERS: [u8; 2] = [ACTION_DELEGATE_NUMBER, ACTION_DELEGATE_V2_NUMBER];
/// JSON-schema variant names of the delegate actions, excluded from
/// `NonDelegateAction`'s schema. Must list exactly the variants for which
/// `Action::is_delegate` is true; cross-checked in
/// `test_delegate_variant_encodings_match`.
#[cfg(feature = "schemars")]
const DELEGATE_VARIANT_NAMES: [&str; 2] = ["Delegate", "DelegateV2"];
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

/// Delegate action with gas key support: `nonce` selects either the access
/// key's nonce or one of a gas key's parallel nonces by index, mirroring
/// `TransactionV1`.
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
pub struct DelegateActionV2 {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce of the signing key, advanced when this action is processed. For
    /// a gas key it also selects which of the parallel nonces to advance.
    pub nonce: TransactionNonce,
    /// The maximal height of the block in the blockchain below which the given DelegateActionV2 is valid.
    pub max_block_height: BlockHeight,
    /// Public key used to sign this delegated action.
    pub public_key: PublicKey,
}

impl DelegateActionV2 {
    pub fn get_actions(&self) -> Vec<Action> {
        self.actions.iter().map(|a| a.clone().into()).collect()
    }
}

/// Versions of the delegate action carried by `Action::DelegateV2`. New
/// versions add a variant here rather than a new `Action` variant. The variant
/// is part of the signed payload, so a signature can't be ambiguous across
/// versions.
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
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum VersionedDelegateActionPayload {
    V2(DelegateActionV2) = 0,
}

impl VersionedDelegateActionPayload {
    pub fn public_key(&self) -> &PublicKey {
        match self {
            VersionedDelegateActionPayload::V2(delegate_action) => &delegate_action.public_key,
        }
    }

    pub fn get_actions(&self) -> Vec<Action> {
        match self {
            VersionedDelegateActionPayload::V2(delegate_action) => delegate_action.get_actions(),
        }
    }

    /// Delegate action hash used for NEP-461 signature scheme which tags
    /// different messages before hashing
    ///
    /// For more details, see: [NEP-461](https://github.com/near/NEPs/pull/461)
    pub fn get_nep461_hash(&self) -> CryptoHash {
        let signable = SignableMessage::new(&self, SignableMessageType::DelegateActionV2);
        let bytes = borsh::to_vec(&signable).expect("failed to serialize");
        hash(&bytes)
    }
}

impl From<DelegateActionV2> for VersionedDelegateActionPayload {
    fn from(delegate_action: DelegateActionV2) -> Self {
        VersionedDelegateActionPayload::V2(delegate_action)
    }
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
pub struct VersionedSignedDelegateAction {
    pub delegate_action: VersionedDelegateActionPayload,
    pub signature: Signature,
}

impl VersionedSignedDelegateAction {
    pub fn verify(&self) -> bool {
        let hash = self.delegate_action.get_nep461_hash();
        self.signature.verify(hash.as_ref(), self.delegate_action.public_key())
    }

    pub fn sign(signer: &Signer, delegate_action: VersionedDelegateActionPayload) -> Self {
        let signature = signer.sign(delegate_action.get_nep461_hash().as_bytes());
        Self { delegate_action, signature }
    }
}

impl From<VersionedSignedDelegateAction> for Action {
    fn from(action: VersionedSignedDelegateAction) -> Self {
        Self::DelegateV2(Box::new(action))
    }
}

/// Convenience wrapper for common logic accessing fields on delegate actions
/// of different versions.
#[derive(Clone, Copy, Debug)]
pub enum VersionedDelegateActionRef<'a> {
    V1(&'a DelegateAction),
    V2(&'a DelegateActionV2),
}

impl VersionedDelegateActionRef<'_> {
    pub fn sender_id(&self) -> &AccountId {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => &delegate_action.sender_id,
            VersionedDelegateActionRef::V2(delegate_action) => &delegate_action.sender_id,
        }
    }

    pub fn receiver_id(&self) -> &AccountId {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => &delegate_action.receiver_id,
            VersionedDelegateActionRef::V2(delegate_action) => &delegate_action.receiver_id,
        }
    }

    pub fn actions(&self) -> &[NonDelegateAction] {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => &delegate_action.actions,
            VersionedDelegateActionRef::V2(delegate_action) => &delegate_action.actions,
        }
    }

    pub fn get_actions(&self) -> Vec<Action> {
        self.actions().iter().map(|a| a.clone().into()).collect()
    }

    pub fn nonce(&self) -> TransactionNonce {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => {
                TransactionNonce::from_nonce(delegate_action.nonce)
            }
            VersionedDelegateActionRef::V2(delegate_action) => delegate_action.nonce,
        }
    }

    pub fn max_block_height(&self) -> BlockHeight {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => delegate_action.max_block_height,
            VersionedDelegateActionRef::V2(delegate_action) => delegate_action.max_block_height,
        }
    }

    pub fn public_key(&self) -> &PublicKey {
        match self {
            VersionedDelegateActionRef::V1(delegate_action) => &delegate_action.public_key,
            VersionedDelegateActionRef::V2(delegate_action) => &delegate_action.public_key,
        }
    }
}

impl<'a> From<&'a DelegateAction> for VersionedDelegateActionRef<'a> {
    fn from(delegate_action: &'a DelegateAction) -> Self {
        VersionedDelegateActionRef::V1(delegate_action)
    }
}

impl<'a> From<&'a DelegateActionV2> for VersionedDelegateActionRef<'a> {
    fn from(delegate_action: &'a DelegateActionV2) -> Self {
        VersionedDelegateActionRef::V2(delegate_action)
    }
}

impl<'a> From<&'a VersionedDelegateActionPayload> for VersionedDelegateActionRef<'a> {
    fn from(delegate_action: &'a VersionedDelegateActionPayload) -> Self {
        match delegate_action {
            VersionedDelegateActionPayload::V2(delegate_action) => {
                VersionedDelegateActionRef::V2(delegate_action)
            }
        }
    }
}

/// Convenience wrapper for common logic accessing signed delegate actions of
/// different versions.
#[derive(Clone, Copy, Debug)]
pub enum VersionedSignedDelegateActionRef<'a> {
    V1(&'a SignedDelegateAction),
    V2(&'a VersionedSignedDelegateAction),
}

impl VersionedSignedDelegateActionRef<'_> {
    pub fn delegate_action(&self) -> VersionedDelegateActionRef<'_> {
        match self {
            VersionedSignedDelegateActionRef::V1(signed) => (&signed.delegate_action).into(),
            VersionedSignedDelegateActionRef::V2(signed) => (&signed.delegate_action).into(),
        }
    }

    pub fn verify(&self) -> bool {
        match self {
            VersionedSignedDelegateActionRef::V1(signed) => signed.verify(),
            VersionedSignedDelegateActionRef::V2(signed) => signed.verify(),
        }
    }
}

impl<'a> From<&'a SignedDelegateAction> for VersionedSignedDelegateActionRef<'a> {
    fn from(signed: &'a SignedDelegateAction) -> Self {
        VersionedSignedDelegateActionRef::V1(signed)
    }
}

impl<'a> From<&'a VersionedSignedDelegateAction> for VersionedSignedDelegateActionRef<'a> {
    fn from(signed: &'a VersionedSignedDelegateAction) -> Self {
        VersionedSignedDelegateActionRef::V2(signed)
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
        if let Some(one_of) = action_schema.get_mut("oneOf")
            && let Some(arr) = one_of.as_array_mut()
        {
            // Remove every delegate-style variant; none may be nested.
            arr.retain(|variant| {
                !variant
                    .get("properties")
                    .and_then(|p| p.as_object())
                    .map(|p| DELEGATE_VARIANT_NAMES.iter().any(|name| p.contains_key(*name)))
                    .unwrap_or(false)
            });
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
    #[error(
        "attempted to construct NonDelegateAction from a delegate action (Delegate or DelegateV2)"
    )]
    pub struct IsDelegateAction;

    impl TryFrom<Action> for NonDelegateAction {
        type Error = IsDelegateAction;

        fn try_from(action: Action) -> Result<Self, IsDelegateAction> {
            if action.is_delegate() { Err(IsDelegateAction) } else { Ok(Self(action)) }
        }
    }

    impl borsh::de::BorshDeserialize for NonDelegateAction {
        fn deserialize_reader<R: Read>(rd: &mut R) -> ::core::result::Result<Self, Error> {
            match u8::deserialize_reader(rd)? {
                n if DELEGATE_VARIANT_NUMBERS.contains(&n) => Err(Error::new(
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
    use near_crypto::{InMemorySigner, KeyType};

    #[test]
    fn test_signed_delegate_action_v2_verify() {
        let signer = InMemorySigner::test_signer(&"alice.near".parse().unwrap());
        let delegate_action = DelegateActionV2 {
            sender_id: "alice.near".parse().unwrap(),
            receiver_id: "bob.near".parse().unwrap(),
            actions: vec![],
            nonce: TransactionNonce::from_nonce_and_index(1, 3),
            max_block_height: 1000,
            public_key: signer.public_key(),
        };
        let signed = VersionedSignedDelegateAction::sign(&signer, delegate_action.clone().into());
        assert!(signed.verify());

        // A signature bound to nonce index 3 must not verify for another index.
        let forged = VersionedSignedDelegateAction {
            delegate_action: DelegateActionV2 {
                nonce: TransactionNonce::from_nonce_and_index(1, 4),
                ..delegate_action.clone()
            }
            .into(),
            signature: signed.signature,
        };
        assert!(!forged.verify());

        // A signature under the V1 message discriminant must not verify for a
        // V2 action; V1 and V2 signing domains are disjoint.
        let versioned = VersionedDelegateActionPayload::from(delegate_action);
        let v1_tagged_signature =
            SignableMessage::new(&versioned, SignableMessageType::DelegateAction).sign(&signer);
        let forged = VersionedSignedDelegateAction {
            delegate_action: versioned,
            signature: v1_tagged_signature,
        };
        assert!(!forged.verify());
    }

    #[test]
    fn test_delegate_action_v2_borsh_roundtrip() {
        let action: Action = VersionedSignedDelegateAction {
            delegate_action: DelegateActionV2 {
                sender_id: "alice.near".parse().unwrap(),
                receiver_id: "bob.near".parse().unwrap(),
                actions: vec![],
                nonce: TransactionNonce::from_nonce_and_index(1, 7),
                max_block_height: 1000,
                public_key: PublicKey::empty(KeyType::ED25519),
            }
            .into(),
            signature: Signature::empty(KeyType::ED25519),
        }
        .into();
        let bytes = borsh::to_vec(&action).unwrap();
        assert_eq!(bytes[0], ACTION_DELEGATE_V2_NUMBER);
        assert_eq!(Action::try_from_slice(&bytes).unwrap(), action);
    }

    #[test]
    fn test_delegate_variant_encodings_match() {
        let delegate_v2: Action = VersionedSignedDelegateAction {
            delegate_action: DelegateActionV2 {
                sender_id: "alice.near".parse().unwrap(),
                receiver_id: "bob.near".parse().unwrap(),
                actions: vec![],
                nonce: TransactionNonce::from_nonce_and_index(1, 0),
                max_block_height: 1000,
                public_key: PublicKey::empty(KeyType::ED25519),
            }
            .into(),
            signature: Signature::empty(KeyType::ED25519),
        }
        .into();
        let delegates = [create_delegate_action(vec![]), delegate_v2];

        for action in &delegates {
            assert!(action.is_delegate());
            let bytes = borsh::to_vec(action).unwrap();
            assert!(DELEGATE_VARIANT_NUMBERS.contains(&bytes[0]));
            // NonDelegateAction refuses it via both the typed and borsh paths.
            assert!(NonDelegateAction::try_from(action.clone()).is_err());
            assert_eq!(
                NonDelegateAction::try_from_slice(&bytes).map_err(|e| e.kind()),
                Err(ErrorKind::InvalidInput)
            );
            #[cfg(feature = "schemars")]
            assert!(DELEGATE_VARIANT_NAMES.contains(&action.as_ref()));
        }
        assert_eq!(DELEGATE_VARIANT_NUMBERS.len(), delegates.len());
        #[cfg(feature = "schemars")]
        assert_eq!(DELEGATE_VARIANT_NAMES.len(), delegates.len());

        // A non-delegate is accepted and absent from the reject set.
        let non_delegate = Action::CreateAccount(CreateAccountAction {});
        assert!(!non_delegate.is_delegate());
        let bytes = borsh::to_vec(&non_delegate).unwrap();
        assert!(!DELEGATE_VARIANT_NUMBERS.contains(&bytes[0]));
        assert!(NonDelegateAction::try_from(non_delegate).is_ok());
    }

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

        // No delegate-style variant may appear.
        let has_delegate_variant = |variant: &serde_json::Value| {
            variant
                .get("properties")
                .and_then(|p| p.as_object())
                .map(|p| DELEGATE_VARIANT_NAMES.iter().any(|name| p.contains_key(*name)))
                .unwrap_or(false)
        };
        assert!(
            !one_of.iter().any(has_delegate_variant),
            "NonDelegateAction schema must not contain any delegate variant"
        );

        // Action (for comparison) includes all delegate variants.
        let action_schema = Action::json_schema(&mut generator);
        let action_json = serde_json::to_value(&action_schema).unwrap();
        let action_one_of = action_json
            .get("oneOf")
            .expect("Action schema must have oneOf")
            .as_array()
            .expect("Action oneOf must be an array");

        let delegate_count = action_one_of.iter().filter(|v| has_delegate_variant(v)).count();
        assert_eq!(delegate_count, DELEGATE_VARIANT_NAMES.len());

        // NonDelegateAction excludes exactly the delegate variants.
        assert_eq!(one_of.len(), action_one_of.len() - DELEGATE_VARIANT_NAMES.len());
    }
}

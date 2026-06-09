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
use near_primitives_core::types::{AccountId, Nonce, NonceIndex};
use near_schema_checker_lib::ProtocolSchema;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind, Read, Write};

/// This is an index number of Action::Delegate in Action enumeration
const ACTION_DELEGATE_NUMBER: u8 = 8;
/// Version tag prefixed to the borsh encoding of a `DelegateActionV1`.
const DELEGATE_ACTION_V1_TAG: u8 = 1;

/// Fields shared by all delegate action versions.
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
pub struct DelegateActionV0 {
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

/// Delegate action signed by a gas key, /// nonces, selected by `nonce_index`.
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
pub struct DelegateActionV1 {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce for the gas key nonce at `nonce_index`. After this action is
    /// processed the nonce for `nonce_index` increments.
    pub nonce: Nonce,
    /// The maximal height of the block in the blockchain below which the given DelegateAction is valid.
    pub max_block_height: BlockHeight,
    /// Public key used to sign this delegated action.
    pub public_key: PublicKey,
    /// Index of the gas key nonce this action advances.
    pub nonce_index: NonceIndex,
}

/// This action allows to execute the inner actions behalf of the defined sender.
///
/// `V0` is the original NEP-366 form. `V1` additionally carries a `nonce_index`
/// so a gas key can use one of its parallel nonces.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DelegateAction {
    V0(DelegateActionV0),
    V1(DelegateActionV1),
}

impl BorshSerialize for DelegateAction {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            DelegateAction::V0(action) => BorshSerialize::serialize(action, writer),
            DelegateAction::V1(action) => {
                BorshSerialize::serialize(&DELEGATE_ACTION_V1_TAG, writer)?;
                BorshSerialize::serialize(action, writer)
            }
        }
    }
}

impl BorshDeserialize for DelegateAction {
    /// Disambiguate V0 from V1 by the first two bytes, mirroring `Transaction`.
    ///
    /// V0 begins with `sender_id`, a borsh `String` whose 4-byte little-endian
    /// length is at least 2 (the minimum AccountId length), so its second byte
    /// is always 0. V1 is prefixed with the tag byte 1 followed by a struct that
    /// also starts with that length, whose second byte is nonzero. Therefore
    /// `u2 == 0` implies V0, and `u1 == 1` with `u2 != 0` implies V1.
    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let u1 = u8::deserialize_reader(reader)?;
        let u2 = u8::deserialize_reader(reader)?;

        // Erase the chained reader to `&mut dyn Read` before recursing. A
        // delegate action can transitively contain another one (actions ->
        // Action -> Delegate), so monomorphizing the inner deserialize on the
        // chained reader type would recurse without bound.
        if u2 == 0 {
            let prefix = [u1, u2];
            let mut chained = prefix.chain(reader);
            let mut reader: &mut dyn Read = &mut chained;
            return Ok(DelegateAction::V0(DelegateActionV0::deserialize_reader(&mut reader)?));
        }
        if u1 == DELEGATE_ACTION_V1_TAG {
            let prefix = [u2];
            let mut chained = prefix.chain(reader);
            let mut reader: &mut dyn Read = &mut chained;
            return Ok(DelegateAction::V1(DelegateActionV1::deserialize_reader(&mut reader)?));
        }
        Err(Error::new(
            ErrorKind::InvalidData,
            format!("invalid delegate action version tag: {u1}"),
        ))
    }
}

/// Flat serde representation shared by both versions, keeping V0's JSON shape
/// unchanged. `nonce_index` is present iff the value is a V1.
#[derive(Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
struct DelegateActionRepr {
    sender_id: AccountId,
    receiver_id: AccountId,
    actions: Vec<NonDelegateAction>,
    nonce: Nonce,
    max_block_height: BlockHeight,
    public_key: PublicKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nonce_index: Option<NonceIndex>,
}

impl From<&DelegateAction> for DelegateActionRepr {
    fn from(action: &DelegateAction) -> Self {
        DelegateActionRepr {
            sender_id: action.sender_id().clone(),
            receiver_id: action.receiver_id().clone(),
            actions: action.actions().to_vec(),
            nonce: action.nonce(),
            max_block_height: action.max_block_height(),
            public_key: action.public_key().clone(),
            nonce_index: action.nonce_index(),
        }
    }
}

impl From<DelegateActionRepr> for DelegateAction {
    fn from(repr: DelegateActionRepr) -> Self {
        let DelegateActionRepr {
            sender_id,
            receiver_id,
            actions,
            nonce,
            max_block_height,
            public_key,
            nonce_index,
        } = repr;
        match nonce_index {
            None => DelegateAction::V0(DelegateActionV0 {
                sender_id,
                receiver_id,
                actions,
                nonce,
                max_block_height,
                public_key,
            }),
            Some(nonce_index) => DelegateAction::V1(DelegateActionV1 {
                sender_id,
                receiver_id,
                actions,
                nonce,
                max_block_height,
                public_key,
                nonce_index,
            }),
        }
    }
}

impl Serialize for DelegateAction {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        DelegateActionRepr::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DelegateAction {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DelegateActionRepr::deserialize(deserializer).map(DelegateAction::from)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for DelegateAction {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DelegateAction".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        DelegateActionRepr::json_schema(generator)
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
pub struct SignedDelegateAction {
    pub delegate_action: DelegateAction,
    pub signature: Signature,
}

impl SignedDelegateAction {
    pub fn verify(&self) -> bool {
        let delegate_action = &self.delegate_action;
        let hash = delegate_action.get_nep461_hash();
        let public_key = delegate_action.public_key();

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
    pub fn sender_id(&self) -> &AccountId {
        match self {
            DelegateAction::V0(action) => &action.sender_id,
            DelegateAction::V1(action) => &action.sender_id,
        }
    }

    pub fn receiver_id(&self) -> &AccountId {
        match self {
            DelegateAction::V0(action) => &action.receiver_id,
            DelegateAction::V1(action) => &action.receiver_id,
        }
    }

    pub fn public_key(&self) -> &PublicKey {
        match self {
            DelegateAction::V0(action) => &action.public_key,
            DelegateAction::V1(action) => &action.public_key,
        }
    }

    pub fn nonce(&self) -> Nonce {
        match self {
            DelegateAction::V0(action) => action.nonce,
            DelegateAction::V1(action) => action.nonce,
        }
    }

    pub fn max_block_height(&self) -> BlockHeight {
        match self {
            DelegateAction::V0(action) => action.max_block_height,
            DelegateAction::V1(action) => action.max_block_height,
        }
    }

    /// Gas key nonce index for V1, `None` for V0.
    pub fn nonce_index(&self) -> Option<NonceIndex> {
        match self {
            DelegateAction::V0(_) => None,
            DelegateAction::V1(action) => Some(action.nonce_index),
        }
    }

    pub fn actions(&self) -> &[NonDelegateAction] {
        match self {
            DelegateAction::V0(action) => &action.actions,
            DelegateAction::V1(action) => &action.actions,
        }
    }

    pub fn actions_mut(&mut self) -> &mut Vec<NonDelegateAction> {
        match self {
            DelegateAction::V0(action) => &mut action.actions,
            DelegateAction::V1(action) => &mut action.actions,
        }
    }

    pub fn nonce_mut(&mut self) -> &mut Nonce {
        match self {
            DelegateAction::V0(action) => &mut action.nonce,
            DelegateAction::V1(action) => &mut action.nonce,
        }
    }

    pub fn get_actions(&self) -> Vec<Action> {
        self.actions().iter().map(|a| a.clone().into()).collect()
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
            // Remove the Delegate variant
            arr.retain(|variant| {
                !variant
                    .get("properties")
                    .and_then(|p| p.as_object())
                    .map(|p| p.contains_key("Delegate"))
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

    fn delegate_action_v0(actions: Vec<NonDelegateAction>) -> DelegateAction {
        DelegateAction::V0(DelegateActionV0 {
            sender_id: "aaa".parse().unwrap(),
            receiver_id: "bbb".parse().unwrap(),
            actions,
            nonce: 1,
            max_block_height: 2,
            public_key: PublicKey::empty(KeyType::ED25519),
        })
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
        let actions =
            actions.iter().map(|a| NonDelegateAction::try_from(a.clone()).unwrap()).collect();
        Action::Delegate(Box::new(SignedDelegateAction {
            delegate_action: delegate_action_v0(actions),
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

    fn delegate_action_v1(nonce_index: NonceIndex) -> DelegateAction {
        DelegateAction::V1(DelegateActionV1 {
            sender_id: "aaa".parse().unwrap(),
            receiver_id: "bbb".parse().unwrap(),
            actions: vec![],
            nonce: 1,
            max_block_height: 2,
            public_key: PublicKey::empty(KeyType::ED25519),
            nonce_index,
        })
    }

    #[test]
    fn test_delegate_action_version_borsh_roundtrip() {
        for action in [delegate_action_v0(vec![]), delegate_action_v1(7)] {
            let bytes = borsh::to_vec(&action).unwrap();
            assert_eq!(DelegateAction::try_from_slice(&bytes).unwrap(), action);
        }
    }

    #[test]
    fn test_delegate_action_v0_borsh_unchanged_by_versioning() {
        // V0 must serialize identically to the bare inner struct so existing
        // signatures and stored receipts stay valid.
        let DelegateAction::V0(inner) = delegate_action_v0(vec![]) else { unreachable!() };
        assert_eq!(
            borsh::to_vec(&DelegateAction::V0(inner.clone())).unwrap(),
            borsh::to_vec(&inner).unwrap()
        );
    }

    #[test]
    fn test_delegate_action_v1_borsh_is_tagged() {
        let bytes = borsh::to_vec(&delegate_action_v1(0)).unwrap();
        assert_eq!(bytes[0], DELEGATE_ACTION_V1_TAG);
    }

    #[test]
    fn test_delegate_action_serde_roundtrip_and_shape() {
        let v0 = delegate_action_v0(vec![]);
        let v0_json = serde_json::to_value(&v0).unwrap();
        assert!(v0_json.get("nonce_index").is_none(), "V0 JSON must stay flat without nonce_index");
        assert_eq!(serde_json::from_value::<DelegateAction>(v0_json).unwrap(), v0);

        let v1 = delegate_action_v1(7);
        let v1_json = serde_json::to_value(&v1).unwrap();
        assert_eq!(v1_json.get("nonce_index").and_then(|v| v.as_u64()), Some(7));
        assert_eq!(serde_json::from_value::<DelegateAction>(v1_json).unwrap(), v1);
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

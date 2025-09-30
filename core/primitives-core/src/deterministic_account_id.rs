use crate::global_contract::GlobalContractIdentifier;
use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::BTreeMap;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum DeterministicAccountStateInit {
    V1(DeterministicAccountStateInitV1),
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DeterministicAccountStateInitV1 {
    pub code: GlobalContractIdentifier,
    #[serde_as(as = "BTreeMap<Base64, Base64>")]
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, String>"))]
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl DeterministicAccountStateInit {
    pub fn code(&self) -> &GlobalContractIdentifier {
        match self {
            DeterministicAccountStateInit::V1(inner) => &inner.code,
        }
    }

    pub fn data(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        match self {
            DeterministicAccountStateInit::V1(inner) => &inner.data,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            DeterministicAccountStateInit::V1(_) => 1,
        }
    }

    /// Use like `let (code, data) = state_init.take();` to take fields without cloning.
    pub fn take(self) -> (GlobalContractIdentifier, BTreeMap<Vec<u8>, Vec<u8>>) {
        match self {
            DeterministicAccountStateInit::V1(inner) => (inner.code, inner.data),
        }
    }

    /// The length of the serialized structure.
    ///
    /// This length is multiplied by `action_deterministic_state_init_per_byte`
    /// for gas cost calculations of a state initialization.
    pub fn len_bytes(&self) -> usize {
        borsh::object_length(&self).expect("borsh must not fail")
    }
}

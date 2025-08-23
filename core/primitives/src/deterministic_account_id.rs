use crate::action::GlobalContractIdentifier;
use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
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
    strum::AsRefStr,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum DeterministicAccountStateInit {
    V1(DeterministicAccountStateInitV1),
}

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
}

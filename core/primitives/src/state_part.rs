use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use strum::AsRefStr;

use crate::state::PartialState;

const STATE_PARTS_COMPRESSION_LEVEL: i32 = 3;

// to specify a part we always specify both part_id and num_parts together
#[derive(Copy, Clone, Debug)]
pub struct PartId {
    pub idx: u64,
    pub total: u64,
}
impl PartId {
    pub fn new(part_id: u64, num_parts: u64) -> PartId {
        assert!(part_id < num_parts);
        PartId { idx: part_id, total: num_parts }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StatePartV0(pub(crate) Vec<u8>);

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StatePartV1 {
    bytes_compressed: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema, AsRefStr)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum StatePart {
    /// Borsh-serialized trie nodes
    V0(StatePartV0) = 0,
    /// zstd-compressed borsh-serialized trie nodes
    V1(StatePartV1) = 1,
}

impl fmt::Debug for StatePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl StatePartV0 {
    pub fn empty() -> Self {
        Self(vec![])
    }

    fn from_partial_state(partial_state: PartialState) -> Self {
        let bytes =
            borsh::to_vec(&partial_state).expect("serializing partial state should not fail");
        Self(bytes)
    }

    fn to_partial_state(&self) -> borsh::io::Result<PartialState> {
        PartialState::try_from_slice(&self.0)
    }
}

impl StatePartV1 {
    fn from_partial_state(partial_state: PartialState) -> Self {
        let bytes =
            borsh::to_vec(&partial_state).expect("serializing partial state should not fail");
        let bytes_compressed = zstd::encode_all(bytes.as_slice(), STATE_PARTS_COMPRESSION_LEVEL)
            .expect("state part compression should not fail");
        Self { bytes_compressed }
    }

    fn to_partial_state(&self) -> borsh::io::Result<PartialState> {
        let bytes_decompressed = &zstd::decode_all(self.bytes_compressed.as_slice())?;
        PartialState::try_from_slice(bytes_decompressed)
    }
}

impl StatePart {
    pub fn from_partial_state(
        partial_state: PartialState,
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::StatePartsCompression.enabled(protocol_version) {
            Self::V1(StatePartV1::from_partial_state(partial_state))
        } else {
            Self::V0(StatePartV0::from_partial_state(partial_state))
        }
    }

    pub fn to_partial_state(&self) -> borsh::io::Result<PartialState> {
        match self {
            Self::V0(part) => part.to_partial_state(),
            Self::V1(part) => part.to_partial_state(),
        }
    }

    pub fn from_bytes(
        bytes: Vec<u8>,
        protocol_version: ProtocolVersion,
    ) -> borsh::io::Result<Self> {
        if ProtocolFeature::StatePartsVersioning.enabled(protocol_version) {
            BorshDeserialize::try_from_slice(&bytes)
        } else {
            Ok(Self::V0(StatePartV0(bytes)))
        }
    }

    pub fn to_bytes(&self, protocol_version: ProtocolVersion) -> Vec<u8> {
        if ProtocolFeature::StatePartsVersioning.enabled(protocol_version) {
            return borsh::to_vec(self).expect("serializing StatePart should not fail");
        }
        let StatePart::V0(state_part) = self else {
            panic!("{self:?} used without `StatePartsVersioning` feature enabled");
        };
        state_part.0.clone()
    }

    pub fn payload_length(&self) -> usize {
        match self {
            StatePart::V0(part) => part.0.len(),
            StatePart::V1(part) => part.bytes_compressed.len(),
        }
    }
}

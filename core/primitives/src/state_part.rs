use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;

use crate::state::PartialState;
use crate::state_sync::STATE_PART_MEMORY_LIMIT;

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

/// Serialized version of `PartialState`.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StatePartV0(pub(crate) Vec<u8>);

/// Similar to `StatePartV0`, but uses zstd compression.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct StatePartV1 {
    bytes_compressed: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum StatePart {
    /// Borsh-serialized trie nodes
    V0(StatePartV0) = 0,
    /// zstd-compressed borsh-serialized trie nodes
    V1(StatePartV1) = 1,
}

impl StatePartV0 {
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
    fn from_partial_state(partial_state: PartialState, compression_lvl: i32) -> Self {
        let bytes =
            borsh::to_vec(&partial_state).expect("serializing partial state should not fail");
        let bytes_compressed = zstd::encode_all(bytes.as_slice(), compression_lvl)
            .expect("state part compression should not fail");
        Self { bytes_compressed }
    }

    fn to_partial_state(&self) -> borsh::io::Result<PartialState> {
        let part_size_limit = STATE_PART_MEMORY_LIMIT.as_u64();
        let decoder = zstd::stream::read::Decoder::new(self.bytes_compressed.as_slice())?;
        // We add +1 so we can detect when decompressed size exceeds the limit
        let mut decoder_with_limit = std::io::Read::take(decoder, part_size_limit + 1);

        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut decoder_with_limit, &mut decoded)?;
        if decoded.len() > part_size_limit as usize {
            return Err(borsh::io::Error::new(
                borsh::io::ErrorKind::InvalidData,
                "decompression limit exceeded",
            ));
        }
        PartialState::try_from_slice(&decoded)
    }
}

impl StatePart {
    pub fn from_partial_state(
        partial_state: PartialState,
        protocol_version: ProtocolVersion,
        compression_lvl: i32,
    ) -> Self {
        if ProtocolFeature::StatePartsCompression.enabled(protocol_version) {
            Self::V1(StatePartV1::from_partial_state(partial_state, compression_lvl))
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

    /// Construct state part from bytes that are supposed to be result of `to_bytes()`.
    /// That's used to construct state part loaded from disk or network.
    /// Note that this does not validate the data, the validation logic happens in `validate_state_part()`.
    pub fn from_bytes(
        bytes: Vec<u8>,
        protocol_version: ProtocolVersion,
    ) -> borsh::io::Result<Self> {
        if ProtocolFeature::StatePartsCompression.enabled(protocol_version) {
            BorshDeserialize::try_from_slice(&bytes)
        } else {
            Ok(Self::V0(StatePartV0(bytes)))
        }
    }

    pub fn to_bytes(&self, protocol_version: ProtocolVersion) -> Vec<u8> {
        if ProtocolFeature::StatePartsCompression.enabled(protocol_version) {
            return borsh::to_vec(self).expect("serializing StatePart should not fail");
        }
        let StatePart::V0(state_part) = self else {
            panic!("{self:?} used without `StatePartsCompression` feature enabled");
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use near_primitives_core::version::ProtocolFeature;

    use crate::state::PartialState;
    use crate::state_part::StatePart;
    use crate::state_sync::STATE_PART_MEMORY_LIMIT;

    // Some values with low entropy, to benefit from compression.
    fn dummy_partial_state() -> PartialState {
        let dummy_trie_values =
            ["aaaaaaaaaaaaaaaaaaaaaaaaaaa", "xxxxxxxxxxxxxxxxxxxx", "00000000000000000000"]
                .iter()
                .map(|value| Arc::from(value.as_bytes()))
                .collect_vec();
        PartialState::TrieValues(dummy_trie_values)
    }

    #[test]
    fn test_legacy_state_part() {
        let new_protocol_version = ProtocolFeature::StatePartsCompression.protocol_version();
        let old_protocol_version = new_protocol_version - 1;

        let partial_state = dummy_partial_state();
        let state_part_v0 =
            StatePart::from_partial_state(partial_state.clone(), old_protocol_version, 1);
        assert!(matches!(state_part_v0, StatePart::V0(_)));
        let partial_state_reconstructed = state_part_v0.to_partial_state().unwrap();
        assert_eq!(partial_state, partial_state_reconstructed);

        let bytes = state_part_v0.to_bytes(old_protocol_version);
        let state_part_v0_reconstructed =
            StatePart::from_bytes(bytes.clone(), old_protocol_version).unwrap();
        assert_eq!(state_part_v0, state_part_v0_reconstructed);

        // Legacy state parts (without version discriminant) cannot be used for sync to
        // epoch which has `StatePartsCompression` enabled.
        assert!(StatePart::from_bytes(bytes, new_protocol_version).is_err());
    }

    #[test]
    fn test_state_part_compression() {
        let new_protocol_version = ProtocolFeature::StatePartsCompression.protocol_version();
        let old_protocol_version = new_protocol_version - 1;
        let partial_state = dummy_partial_state();

        let state_part_v0 =
            StatePart::from_partial_state(partial_state.clone(), old_protocol_version, 1);
        let state_part_v1 =
            StatePart::from_partial_state(partial_state.clone(), new_protocol_version, 1);
        assert!(state_part_v1.payload_length() < state_part_v0.payload_length());

        let partial_state_reconstructed_from_state_part_v1 =
            state_part_v1.to_partial_state().unwrap();
        assert_eq!(partial_state, partial_state_reconstructed_from_state_part_v1);

        let state_part_v1_bytes = state_part_v1.to_bytes(new_protocol_version);
        let state_part_v1_reconstructed =
            StatePart::from_bytes(state_part_v1_bytes, new_protocol_version).unwrap();
        assert_eq!(state_part_v1, state_part_v1_reconstructed);

        // Compressed state parts are not backward compatible, i.e. cannot be used for sync to
        // epoch which does not have `StatePartsCompression` enabled yet.
        assert!(std::panic::catch_unwind(|| state_part_v1.to_bytes(old_protocol_version)).is_err());
    }

    #[test]
    fn test_state_part_compression_bomb() {
        let part_size_limit = STATE_PART_MEMORY_LIMIT.as_u64() as usize;
        let protocol_version = ProtocolFeature::StatePartsCompression.protocol_version();
        let big_value = Arc::from(vec![b'a'; 2 * part_size_limit].into_boxed_slice());
        let partial_state = PartialState::TrieValues(vec![big_value]);

        let state_part = StatePart::from_partial_state(partial_state, protocol_version, 1);
        assert!(state_part.payload_length() < part_size_limit / 2);

        let decompression_result = state_part.to_partial_state();
        // Although the compressed size is less than half of the limit, after decompression is twice the limit.
        let err = decompression_result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "decompression limit exceeded");
    }
}

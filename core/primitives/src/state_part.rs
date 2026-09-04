use crate::state::{PARTIAL_STATE_HEADER_LEN, PartialState};
use crate::state_sync::STATE_PART_MEMORY_LIMIT;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::MIB;
use near_schema_checker_lib::ProtocolSchema;

/// Upper bound for a decompressed part size.
///
/// Historically, state parts were sent uncompressed and were therefore bounded by
/// `NETWORK_MESSAGE_MAX_SIZE_BYTES` (512 MB), which makes this limit valid.
///
/// `crate::state_sync::STATE_PART_MEMORY_LIMIT` did not work in rare cases, because
/// `find_state_part_boundary()` is only approximate due to limited granularity (it must pick a range of
/// nodes). Therefore, the real limit is `crate::state_sync::STATE_PART_MEMORY_LIMIT` plus the maximum trie
/// node size.
// TODO(#14340): Try to lower the upper bound, e.g. determine the maximum trie node size.
const PART_SIZE_LIMIT: u64 = 512 * MIB;

/// Index of a state part, in the range `0..num_parts`.
pub type StatePartIndex = u64;

/// Lower bound for the `memory_usage` a single part entry contributes.
///
/// `memory_usage_direct` charges `TRIE_COSTS.node_cost` per node and
/// `memory_usage_value` charges it again per value, so entries of both kinds cost
/// at least this much. near-store owns `TRIE_COSTS` and tests the two agree.
pub const MIN_MEMORY_USAGE_PER_PART_ENTRY: u64 = 50;

/// Upper bound for the number of trie values in a decompressed part.
///
/// `PART_SIZE_LIMIT` caps bytes only, and an entry can be four bytes on the wire,
/// so it alone allows over a hundred million entries. Parts are cut by trie
/// memory usage instead, so one holds at most `STATE_PART_MEMORY_LIMIT` divided
/// by `MIN_MEMORY_USAGE_PER_PART_ENTRY`. The doubling covers the approximate part
/// boundary and the boundary proof nodes, which sit outside the split.
///
/// Largest part seen on mainnet or testnet on 2026-08-25, over every shard, was
/// 440,488 entries: 1.4x under the undoubled bound, 2.9x under this one.
const PART_ENTRY_LIMIT: u32 =
    (2 * STATE_PART_MEMORY_LIMIT.0 / MIN_MEMORY_USAGE_PER_PART_ENTRY) as u32;

/// Index of a state part, in the range `0..num_parts`.
#[derive(Copy, Clone, Debug)]
pub struct StatePartId {
    pub index: StatePartIndex,
    pub total: u64,
}

impl StatePartId {
    pub fn new(part_idx: StatePartIndex, num_parts: u64) -> StatePartId {
        assert!(part_idx < num_parts);
        StatePartId { index: part_idx, total: num_parts }
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
    fn to_partial_state(&self) -> borsh::io::Result<PartialState> {
        PartialState::try_from_slice_with_entry_limit(&self.0, PART_ENTRY_LIMIT)
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
        let decoder = zstd::stream::read::Decoder::new(self.bytes_compressed.as_slice())?;
        // We add +1 so we can detect when decompressed size exceeds the limit
        let mut decoder_with_limit = std::io::Read::take(decoder, PART_SIZE_LIMIT + 1);

        // Reject an oversized entry count before the rest of the stream is
        // decompressed into memory.
        let mut header = [0u8; PARTIAL_STATE_HEADER_LEN];
        std::io::Read::read_exact(&mut decoder_with_limit, &mut header)?;
        PartialState::check_entry_limit(&header, PART_ENTRY_LIMIT)?;

        // Seeding with the header keeps the `PART_SIZE_LIMIT + 1` budget accurate.
        let mut decoded = header.to_vec();
        std::io::Read::read_to_end(&mut decoder_with_limit, &mut decoded)?;
        if decoded.len() > PART_SIZE_LIMIT as usize {
            return Err(borsh::io::Error::new(
                borsh::io::ErrorKind::InvalidData,
                "decompression limit exceeded",
            ));
        }
        PartialState::try_from_slice(&decoded)
    }
}

impl StatePart {
    pub fn from_partial_state(partial_state: PartialState, compression_lvl: i32) -> Self {
        Self::V1(StatePartV1::from_partial_state(partial_state, compression_lvl))
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
    pub fn from_bytes(bytes: Vec<u8>) -> borsh::io::Result<Self> {
        BorshDeserialize::try_from_slice(&bytes)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        borsh::to_vec(self).expect("serializing StatePart should not fail")
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
    use crate::state::PartialState;
    use crate::state_part::{PART_ENTRY_LIMIT, PART_SIZE_LIMIT, StatePart, StatePartV0};
    use itertools::Itertools;
    use std::sync::Arc;

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
    fn test_state_part_compression() {
        let partial_state = dummy_partial_state();

        let v0_bytes =
            borsh::to_vec(&partial_state).expect("serializing partial state should not fail");
        let state_part_v0 = StatePart::V0(StatePartV0(v0_bytes));
        let state_part_v1 = StatePart::from_partial_state(partial_state.clone(), 1);
        assert!(state_part_v1.payload_length() < state_part_v0.payload_length());

        let partial_state_reconstructed_from_state_part_v1 =
            state_part_v1.to_partial_state().unwrap();
        assert_eq!(partial_state, partial_state_reconstructed_from_state_part_v1);

        let state_part_v1_bytes = state_part_v1.to_bytes();
        let state_part_v1_reconstructed = StatePart::from_bytes(state_part_v1_bytes).unwrap();
        assert_eq!(state_part_v1, state_part_v1_reconstructed);
    }

    fn partial_state_with_empty_values(num_values: usize) -> PartialState {
        PartialState::TrieValues(vec![Arc::from(&b""[..]); num_values])
    }

    #[test]
    fn test_state_part_entry_count_at_limit_is_accepted() {
        let partial_state = partial_state_with_empty_values(PART_ENTRY_LIMIT as usize);
        let state_part = StatePart::from_partial_state(partial_state, 1);
        assert_eq!(state_part.to_partial_state().unwrap().len(), PART_ENTRY_LIMIT as usize);
    }

    #[test]
    fn test_state_part_entry_count_bomb() {
        let partial_state = partial_state_with_empty_values(PART_ENTRY_LIMIT as usize + 1);
        let state_part = StatePart::from_partial_state(partial_state, 1);
        // The part is well under the byte cap, so only the entry count rejects it.
        assert!(state_part.payload_length() < PART_SIZE_LIMIT as usize);

        let err = state_part.to_partial_state().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "state part entry limit exceeded");
    }

    #[test]
    fn test_state_part_compression_bomb() {
        let big_value = Arc::from(vec![b'a'; 2 * PART_SIZE_LIMIT as usize].into_boxed_slice());
        let partial_state = PartialState::TrieValues(vec![big_value]);

        let state_part = StatePart::from_partial_state(partial_state, 1);
        assert!(state_part.payload_length() < PART_SIZE_LIMIT as usize / 2);

        let decompression_result = state_part.to_partial_state();
        // Although the compressed size is less than half of the limit, after decompression is twice the limit.
        let err = decompression_result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), "decompression limit exceeded");
    }
}

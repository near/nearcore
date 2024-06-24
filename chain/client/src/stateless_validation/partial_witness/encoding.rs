use std::collections::HashMap;
use std::sync::Arc;

use near_primitives::checked_feature;
use near_primitives::reed_solomon::{
    reed_solomon_decode, reed_solomon_encode, reed_solomon_part_length,
};
use near_primitives::stateless_validation::EncodedChunkStateWitness;
use near_vm_runner::logic::ProtocolVersion;
use reed_solomon_erasure::galois_8::ReedSolomon;

/// Ratio of the number of data parts to total parts in the Reed Solomon encoding.
/// The tradeoff here is having a higher ratio is better for handling missing parts and network errors
/// but increases the size of the encoded state witness and the total network bandwidth requirements.
const RATIO_DATA_PARTS_OLD: f32 = 0.8;
const RATIO_DATA_PARTS: f32 = 0.6;

/// Type alias around what ReedSolomon represents data part as.
/// This should help with making the code a bit more understandable.
pub type WitnessPart = Option<Box<[u8]>>;

/// Reed Solomon encoder wrapper for encoding and decoding state witness parts.
pub struct WitnessEncoder {
    /// None corresponds to the case when we are the only validator for the chunk
    /// since ReedSolomon does not support having exactly 1 total part count and
    /// no parity parts.
    rs: Option<ReedSolomon>,
}

impl WitnessEncoder {
    pub fn new(total_parts: usize, protocol_version: ProtocolVersion) -> WitnessEncoder {
        let rs = if total_parts > 1 {
            let data_parts = num_witness_data_parts(total_parts, protocol_version);
            Some(ReedSolomon::new(data_parts, total_parts - data_parts).unwrap())
        } else {
            None
        };
        Self { rs }
    }

    pub fn total_parts(&self) -> usize {
        match self.rs {
            Some(ref rs) => rs.total_shard_count(),
            None => 1,
        }
    }

    pub fn data_parts(&self) -> usize {
        match self.rs {
            Some(ref rs) => rs.data_shard_count(),
            None => 1,
        }
    }

    pub fn encode(&self, witness: &EncodedChunkStateWitness) -> (Vec<WitnessPart>, usize) {
        match self.rs {
            Some(ref rs) => reed_solomon_encode(rs, witness),
            None => {
                (vec![Some(witness.as_slice().to_vec().into_boxed_slice())], witness.size_bytes())
            }
        }
    }

    pub fn decode(
        &self,
        parts: &mut [WitnessPart],
        encoded_length: usize,
    ) -> Result<EncodedChunkStateWitness, std::io::Error> {
        match self.rs {
            Some(ref rs) => reed_solomon_decode(rs, parts, encoded_length),
            None => {
                Ok(EncodedChunkStateWitness::from_boxed_slice(parts[0].as_ref().unwrap().clone()))
            }
        }
    }
}

/// We keep one encoder for each length of chunk_validators to avoid re-creating the encoder.
pub struct WitnessEncoderCache {
    instances: HashMap<(usize, ProtocolVersion), Arc<WitnessEncoder>>,
}

impl WitnessEncoderCache {
    pub fn new() -> Self {
        Self { instances: HashMap::new() }
    }

    pub fn entry(
        &mut self,
        total_parts: usize,
        protocol_version: ProtocolVersion,
    ) -> Arc<WitnessEncoder> {
        self.instances
            .entry((total_parts, protocol_version))
            .or_insert_with(|| Arc::new(WitnessEncoder::new(total_parts, protocol_version)))
            .clone()
    }
}

pub fn witness_part_length(
    encoded_witness_size: usize,
    total_parts: usize,
    protocol_version: ProtocolVersion,
) -> usize {
    reed_solomon_part_length(
        encoded_witness_size,
        num_witness_data_parts(total_parts, protocol_version),
    )
}

fn num_witness_data_parts(total_parts: usize, protocol_version: ProtocolVersion) -> usize {
    let ratio_data_parts =
        if checked_feature!("stable", ChangePartialWitnessDataPartsRequired, protocol_version) {
            RATIO_DATA_PARTS
        } else {
            RATIO_DATA_PARTS_OLD
        };
    std::cmp::max((total_parts as f32 * ratio_data_parts) as usize, 1)
}

use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use reed_solomon_erasure::{galois_8::ReedSolomon, ShardByShard};
use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;

/// Type alias around what ReedSolomon represents data part as.
/// This should help with making the code a bit more understandable.
pub type ReedSolomonPart = Option<Box<[u8]>>;

// Encode function takes a serializable object and returns a tuple of parts and length of encoded data
pub fn reed_solomon_encode<T: BorshSerialize>(
    rs: &ReedSolomon,
    data: T,
) -> (Vec<ReedSolomonPart>, usize) {
    let mut bytes = borsh::to_vec(&data).unwrap();
    let encoded_length = bytes.len();

    let data_parts = rs.data_shard_count();
    let part_length = reed_solomon_part_length(encoded_length, data_parts);

    // Pad the bytes to be a multiple of `part_length`
    // Convert encoded data into `data_shard_count` number of parts and pad with `parity_shard_count` None values
    // with 4 data_parts and 2 parity_parts
    // b'aaabbbcccd' -> [Some(b'aaa'), Some(b'bbb'), Some(b'ccc'), Some(b'd00'), None, None]
    bytes.resize(data_parts * part_length, 0);
    let mut parts = bytes
        .chunks_exact(part_length)
        .map(|chunk| Some(chunk.to_vec().into_boxed_slice()))
        .chain(itertools::repeat_n(None, rs.parity_shard_count()))
        .collect_vec();

    // Fine to unwrap here as we just constructed the parts
    rs.reconstruct(&mut parts).unwrap();

    (parts, encoded_length)
}

// Decode function is the reverse of encode function. It takes parts and length of encoded data
// and returns the deserialized object.
// Return an error if the reed solomon decoding fails or borsh deserialization fails.
pub fn reed_solomon_decode<T: BorshDeserialize>(
    rs: &ReedSolomon,
    parts: &mut [ReedSolomonPart],
    encoded_length: usize,
) -> Result<T, Error> {
    if let Err(err) = rs.reconstruct(parts) {
        return Err(Error::other(err));
    }

    let encoded_data = parts
        .iter()
        .flat_map(|option| option.as_ref().expect("Missing shard").iter())
        .cloned()
        .take(encoded_length)
        .collect_vec();

    T::try_from_slice(&encoded_data)
}

pub fn reed_solomon_part_length(encoded_length: usize, data_parts: usize) -> usize {
    (encoded_length + data_parts - 1) / data_parts
}

pub fn reed_solomon_num_data_parts(total_parts: usize, ratio_data_parts: f64) -> usize {
    std::cmp::max((total_parts as f64 * ratio_data_parts) as usize, 1)
}

pub struct ReedSolomonEncoder {
    /// ReedSolomon does not support having exactly 1 total part count and
    /// no parity parts, so we use None for that
    rs: Option<ReedSolomon>,
}

/// We need separate ReedSolomonEncoder[Serialize|Deserialize] traits instead
/// of just using Borsh directly because of already existing code for handling
/// chunk state witness where we do not use Borsh for single part encoding.
/// This was accidental, but unfortunately we cannot change it because now
/// it is a part of the networking protocol.
pub trait ReedSolomonEncoderSerialize: BorshSerialize {
    fn serialize_single_part(&self) -> std::io::Result<Vec<u8>> {
        borsh::to_vec(self)
    }
}

pub trait ReedSolomonEncoderDeserialize: BorshDeserialize {
    fn deserialize_single_part(data: &[u8]) -> std::io::Result<Self> {
        Self::try_from_slice(data)
    }
}

impl ReedSolomonEncoder {
    pub fn new(total_parts: usize, ratio_data_parts: f64) -> ReedSolomonEncoder {
        let rs = if total_parts > 1 {
            let data_parts = reed_solomon_num_data_parts(total_parts, ratio_data_parts);
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

    pub fn encode<T: ReedSolomonEncoderSerialize>(
        &self,
        data: &T,
    ) -> (Vec<ReedSolomonPart>, usize) {
        match self.rs {
            Some(ref rs) => reed_solomon_encode(rs, data),
            None => {
                let bytes = T::serialize_single_part(&data).unwrap();
                let size = bytes.len();
                (vec![Some(bytes.into_boxed_slice())], size)
            }
        }
    }

    pub fn decode<T: ReedSolomonEncoderDeserialize>(
        &self,
        parts: &mut [ReedSolomonPart],
        encoded_length: usize,
    ) -> Result<T, std::io::Error> {
        match self.rs {
            Some(ref rs) => reed_solomon_decode(rs, parts, encoded_length),
            None => {
                if parts.len() != 1 {
                    return Err(std::io::Error::other(format!(
                        "Expected single part, received {}",
                        parts.len()
                    )));
                }
                let Some(part) = &parts[0] else {
                    return Err(std::io::Error::other("Received part is not expected to be None"));
                };
                T::deserialize_single_part(part.as_ref())
            }
        }
    }

    /// Creates an `IncrementalEncoder` using the same `ReedSolomon` instance
    pub fn incremental_encoder<'a, T: ReedSolomonEncoderSerialize>(
        &'a self,
        data: &T,
    ) -> Result<IncrementalEncoder<'a>, std::io::Error> {
        let rs = self.rs.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "No encoder available")
        })?;
        Ok(IncrementalEncoder::new(rs, data))
    }
}

pub struct ReedSolomonEncoderCache {
    ratio_data_parts: f64,
    instances: HashMap<usize, Arc<ReedSolomonEncoder>>,
}

impl ReedSolomonEncoderCache {
    pub fn new(ratio_data_parts: f64) -> Self {
        Self { ratio_data_parts, instances: HashMap::new() }
    }

    /// Gets an encoder (or adds a new one to the cache if not present) for a
    /// given number of the total parts.
    pub fn entry(&mut self, total_parts: usize) -> Arc<ReedSolomonEncoder> {
        self.instances
            .entry(total_parts)
            .or_insert_with(|| {
                Arc::new(ReedSolomonEncoder::new(total_parts, self.ratio_data_parts))
            })
            .clone()
    }
}

pub struct ReedSolomonPartsTracker<T> {
    parts: Vec<ReedSolomonPart>,
    encoded_length: usize,
    data_parts_present: usize,
    encoder: Arc<ReedSolomonEncoder>,
    total_parts_size: usize,
    phantom: std::marker::PhantomData<T>,
}

pub enum InsertPartResult<T> {
    Accepted,
    PartAlreadyAvailable,
    InvalidPartOrd,
    Decoded(std::io::Result<T>),
}

impl<T: ReedSolomonEncoderDeserialize> ReedSolomonPartsTracker<T> {
    pub fn new(encoder: Arc<ReedSolomonEncoder>, encoded_length: usize) -> Self {
        Self {
            data_parts_present: 0,
            parts: vec![None; encoder.total_parts()],
            total_parts_size: 0,
            encoded_length,
            encoder,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn data_parts_present(&self) -> usize {
        self.data_parts_present
    }

    pub fn total_parts_size(&self) -> usize {
        self.total_parts_size
    }

    pub fn data_parts_required(&self) -> usize {
        self.encoder.data_parts()
    }

    pub fn has_enough_parts(&self) -> bool {
        self.data_parts_present >= self.data_parts_required()
    }

    pub fn encoded_length(&self) -> usize {
        self.encoded_length
    }

    pub fn has_part(&self, part_ord: usize) -> bool {
        self.parts.get(part_ord).is_some_and(|part| part.is_some())
    }

    pub fn insert_part(&mut self, part_ord: usize, part: Box<[u8]>) -> InsertPartResult<T> {
        if part_ord >= self.parts.len() {
            return InsertPartResult::InvalidPartOrd;
        }
        if self.has_part(part_ord) {
            return InsertPartResult::PartAlreadyAvailable;
        }

        self.data_parts_present += 1;
        self.total_parts_size += part.len();
        self.parts[part_ord] = Some(part);

        if self.has_enough_parts() {
            InsertPartResult::Decoded(self.encoder.decode(&mut self.parts, self.encoded_length))
        } else {
            InsertPartResult::Accepted
        }
    }
}

/// Struct for incremental encoding
pub struct IncrementalEncoder<'a> {
    sbs: ShardByShard<'a, reed_solomon_erasure::galois_8::Field>,
    parts: Vec<Box<[u8]>>,
    total_parts: usize,
    data_parts: usize,
    current_input_index: usize,
    // Keep track of how many shards (data + parity) we've returned.
    shards_returned: usize,
    encoded_length: usize,
}

impl<'a> IncrementalEncoder<'a> {
    pub fn new<T: ReedSolomonEncoderSerialize>(rs: &'a ReedSolomon, data: &T) -> Self {
        let mut bytes = borsh::to_vec(&data).unwrap();
        let encoded_length = bytes.len();

        let data_parts = rs.data_shard_count();
        let parity_parts = rs.parity_shard_count();
        let total_parts = rs.total_shard_count();
        let part_length = reed_solomon_part_length(encoded_length, data_parts);

        bytes.resize(data_parts * part_length, 0);

        // Prepare data shards
        let mut parts: Vec<Box<[u8]>> = bytes
            .chunks_exact(part_length)
            .map(|chunk| chunk.to_vec().into_boxed_slice())
            .collect();

        // Prepare parity shards as zero
        for _ in 0..parity_parts {
            parts.push(vec![0; part_length].into_boxed_slice());
        }

        let sbs = ShardByShard::new(&rs);

        Self {
            sbs,
            parts,
            total_parts,
            data_parts,
            current_input_index: 0,
            shards_returned: 0,
            encoded_length,
        }
    }

    /// If we still have data shards left, we call `encode` once per data shard.
    /// After finishing the data shards, parity is ready and the shards get returned in order.
    pub fn encode_next(&mut self) -> Option<ReedSolomonPart> {
        if self.shards_returned >= self.total_parts {
            return None; // All shards returned
        }

        // Process the next shard
        if self.current_input_index < self.data_parts {
            let mut shard_refs: Vec<&mut [u8]> =
                self.parts.iter_mut().map(|p| p.as_mut()).collect();
            // Process one data shard into parity
            if let Err(e) = self.sbs.encode(&mut shard_refs) {
                panic!("Encoding failed: {:?}", e);
            }
            self.current_input_index += 1;
        }

        let part = self.parts[self.shards_returned].clone();
        self.shards_returned += 1;

        Some(Some(part))
    }

    pub fn encoded_length(&self) -> usize {
        self.encoded_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stateless_validation::state_witness::EncodedChunkStateWitness;
    use rand::Rng;

    // Test that the parts generated by the incremental encoder are the same as the parts
    // generated by the normal encoder.
    #[test]
    fn test_reed_solomon_incremental_generated_parts() {
        const SIZE: usize = 5000;
        let total_parts = 50;
        let ratio = 0.6;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..SIZE).map(|_| rng.gen::<u8>()).collect();
        let data = EncodedChunkStateWitness::from(bytes.into_boxed_slice());
        let encoder = ReedSolomonEncoder::new(total_parts, ratio);
        let (bytes_encoded1, n) = encoder.encode(&data);
        let mut incremental_encoder = encoder.incremental_encoder(&data).unwrap();
        let mut bytes_encoded2 = Vec::new();
        for _ in 0..total_parts {
            let part = incremental_encoder.encode_next().unwrap();
            bytes_encoded2.push(Some(part.unwrap()));
        }

        assert_eq!(n, incremental_encoder.encoded_length());
        assert_eq!(bytes_encoded1, bytes_encoded2);
    }
}

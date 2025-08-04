use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use raptorq::{Decoder, EncoderBuilder, EncodingPacket, ObjectTransmissionInformation};
use reed_solomon_erasure::Field;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::HashMap;
use std::io::Error;
use std::sync::Arc;
use tracing::span::EnteredSpan;

/// Type alias around what ReedSolomon represents data part as.
/// This should help with making the code a bit more understandable.
pub type ReedSolomonPart = Option<Box<[u8]>>;

pub const REED_SOLOMON_MAX_PARTS: usize = reed_solomon_erasure::galois_8::Field::ORDER;

// Encode function takes a serializable object and returns a tuple of parts and length of encoded data
pub fn reed_solomon_encode<T: BorshSerialize>(
    rs: &ReedSolomon,
    data: &T,
) -> (Vec<ReedSolomonPart>, usize) {
    let mut bytes = borsh::to_vec(data).unwrap();
    let encoded_length = bytes.len();

    let data_parts = rs.data_shard_count();
    let repair_parts = rs.parity_shard_count() as u32;

    let mtu = (encoded_length + data_parts - 1) / data_parts;

    assert!(mtu <= u16::MAX as usize, "MTU is too large: {}", mtu); // for POC only

    let padded_len = data_parts * mtu;
    bytes.resize(padded_len, 0);

    // let encoder = Encoder::with_defaults(&bytes, mtu as u16);
    let mut encoder = EncoderBuilder::new();
    encoder.set_max_packet_size(mtu as u16);
    encoder.set_decoder_memory_requirement(u64::MAX);
    let encoder = encoder.build(&bytes);

    let config = encoder.get_config();
    let symbol_size = config.symbol_size() as usize;
    let k = (padded_len + symbol_size - 1) / symbol_size;
    let repairs = data_parts + repair_parts as usize - k;

    let parts: Vec<Option<Box<[u8]>>> = encoder
        .get_encoded_packets(repairs as u32)
        .iter()
        .map(|symbol| Some(symbol.serialize().into_boxed_slice()))
        .collect();

    (parts, encoded_length)
}

// Decode function is the reverse of encode function. It takes parts and length of encoded data
// and returns the deserialized object.
// Return an error if the reed solomon decoding fails or borsh deserialization fails.
pub fn reed_solomon_decode<T: BorshDeserialize + BorshSerialize>(
    rs: &ReedSolomon,
    parts: &mut Vec<ReedSolomonPart>,
    encoded_length: usize,
) -> Result<T, Error> {
    let mtu = (encoded_length + rs.data_shard_count() - 1) / rs.data_shard_count();
    let packets = parts
        .iter()
        .filter_map(|part| part.as_ref().map(|part| EncodingPacket::deserialize(&part)))
        .collect_vec();

    let config = ObjectTransmissionInformation::with_defaults(encoded_length as u64, mtu as u16);
    let mut decoder = Decoder::new(config);

    for part in packets {
        let decoded = decoder.decode(part);
        if decoded.is_some() {
            let res = T::try_from_slice(decoded.unwrap().as_slice());
            if res.is_ok() {
                *parts = reed_solomon_encode(rs, res.as_ref().unwrap()).0;
            }
            return res;
        }
    }

    Err(Error::other("Failed to decode part"))
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

    pub fn decode<T: ReedSolomonEncoderDeserialize + ReedSolomonEncoderSerialize>(
        &self,
        parts: &mut Vec<ReedSolomonPart>,
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

impl<T: ReedSolomonEncoderDeserialize + ReedSolomonEncoderSerialize> ReedSolomonPartsTracker<T> {
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

    pub fn insert_part(
        &mut self,
        part_ord: usize,
        part: Box<[u8]>,
        create_decode_span: Option<Box<dyn Fn() -> EnteredSpan>>,
    ) -> InsertPartResult<T> {
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
            let _decode_span = create_decode_span.map(|f| f());
            InsertPartResult::Decoded(self.encoder.decode(&mut self.parts, self.encoded_length))
        } else {
            InsertPartResult::Accepted
        }
    }
}

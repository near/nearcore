use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use reed_solomon_erasure::galois_8::{Field, ReedSolomon};
use reed_solomon_erasure::ReconstructShard;
use std::io::Error;

/// The ttl for a reed solomon instance to control memory usage. This number below corresponds to
/// roughly 60MB of memory usage.
const RS_TTL: u64 = 2 * 1024;

/// Wrapper around reed solomon which occasionally resets the underlying
/// reed solomon instead to work around the memory leak in reed solomon
/// implementation <https://github.com/darrenldl/reed-solomon-erasure/issues/74>
pub struct ReedSolomonWrapper {
    rs: ReedSolomon,
    ttl: u64,
}

impl ReedSolomonWrapper {
    pub fn new(data_shards: usize, parity_shards: usize) -> Self {
        ReedSolomonWrapper {
            rs: ReedSolomon::new(data_shards, parity_shards).unwrap(),
            ttl: RS_TTL,
        }
    }

    // Encode function takes a serializable object and returns a tuple of parts and length of encoded data
    pub fn encode<T: BorshSerialize>(&mut self, data: T) -> (Vec<Option<Box<[u8]>>>, usize) {
        let mut bytes = borsh::to_vec(&data).unwrap();
        let encoded_length = bytes.len();

        let data_parts = self.rs.data_shard_count();
        let part_length = (encoded_length + data_parts - 1) / data_parts;

        // Pad the bytes to be a multiple of `part_length`
        // Convert encoded data into `data_shard_count` number of parts and pad with `parity_shard_count` None values
        // with 4 data_parts and 2 parity_parts
        // b'aaabbbcccd' -> [Some(b'aaa'), Some(b'bbb'), Some(b'ccc'), Some(b'd00'), None, None]
        bytes.resize(data_parts * part_length, 0);
        let mut parts = bytes
            .chunks_exact(part_length)
            .map(|chunk| Some(chunk.to_vec().into_boxed_slice()))
            .chain(itertools::repeat_n(None, self.rs.parity_shard_count()))
            .collect_vec();

        // Fine to unwrap here as we just constructed the parts
        self.reconstruct(&mut parts).unwrap();

        (parts, encoded_length)
    }

    // Decode function is the reverse of encode function. It takes parts and length of encoded data
    // and returns the deserialized object.
    // Return an error if the reed solomon decoding fails or borsh deserialization fails.
    pub fn decode<T: BorshDeserialize>(
        &mut self,
        parts: &mut [Option<Box<[u8]>>],
        encoded_length: usize,
    ) -> Result<T, Error> {
        if let Err(err) = self.reconstruct(parts) {
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

    fn reconstruct<T: ReconstructShard<Field>>(
        &mut self,
        slices: &mut [T],
    ) -> Result<(), reed_solomon_erasure::Error> {
        self.ttl -= 1;
        if self.ttl == 0 {
            *self =
                ReedSolomonWrapper::new(self.rs.data_shard_count(), self.rs.parity_shard_count());
        }
        self.rs.reconstruct(slices)
    }
}

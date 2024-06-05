use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::io::Error;

// Encode function takes a serializable object and returns a tuple of parts and length of encoded data
pub fn reed_solomon_encode<T: BorshSerialize>(
    rs: &ReedSolomon,
    data: T,
) -> (Vec<Option<Box<[u8]>>>, usize) {
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
    parts: &mut [Option<Box<[u8]>>],
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

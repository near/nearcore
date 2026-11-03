use super::SpiceData;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::reed_solomon::ReedSolomonEncoder;
use near_primitives::spice::partial_data::{SpiceDataCommitment, SpiceDataPart};

/// Data erasure-coded into one part per producer, with the commitment the parts verify
/// against.
pub(crate) struct EncodedData {
    pub(crate) commitment: SpiceDataCommitment,
    /// Every part, in ordinal order.
    pub(crate) parts: Vec<SpiceDataPart>,
}

pub(crate) fn encode(encoder: &ReedSolomonEncoder, data: &SpiceData) -> EncodedData {
    let (boxed_parts, encoded_length) = encoder.encode(data);
    let parts: Vec<&[u8]> = boxed_parts.iter().map(|part| part.as_deref().unwrap()).collect();
    let (merkle_root, merkle_proofs) = merklize(&parts);
    // TODO(spice): As an optimization we should be able to avoid serializing data both in
    // encode and to compute hash.
    let data_hash = hash(&borsh::to_vec(&data).unwrap());
    let commitment = SpiceDataCommitment {
        hash: data_hash,
        root: merkle_root,
        encoded_length: encoded_length as u64,
    };
    debug_assert_eq!(boxed_parts.len(), merkle_proofs.len());
    let parts = boxed_parts
        .into_iter()
        .zip(merkle_proofs)
        .enumerate()
        .map(|(part_ord, (boxed_part, merkle_proof))| SpiceDataPart {
            part_ord: part_ord as u64,
            part: boxed_part.unwrap(),
            merkle_proof,
        })
        .collect();
    EncodedData { commitment, parts }
}

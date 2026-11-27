use super::{DataId, SpiceData, max_encoded_length};
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

/// Data longer than any commitment for its id may claim.
#[derive(Debug, thiserror::Error)]
#[error("encoded length {encoded_length} is over its kind's cap {max_encoded_length}")]
pub(crate) struct EncodedLengthTooLarge {
    pub(crate) encoded_length: u64,
    pub(crate) max_encoded_length: u64,
}

/// Encodes `data`, the data of `id`, unless it is longer than any commitment for `id` may claim.
pub(crate) fn encode(
    encoder: &ReedSolomonEncoder,
    id: &DataId,
    data: &SpiceData,
) -> Result<EncodedData, EncodedLengthTooLarge> {
    let (boxed_parts, encoded_length) = encoder.encode(data);
    let encoded_length = encoded_length as u64;
    let max_encoded_length = max_encoded_length(id);
    if encoded_length > max_encoded_length {
        return Err(EncodedLengthTooLarge { encoded_length, max_encoded_length });
    }
    let parts: Vec<&[u8]> = boxed_parts.iter().map(|part| part.as_deref().unwrap()).collect();
    let (merkle_root, merkle_proofs) = merklize(&parts);
    // TODO(spice): As an optimization we should be able to avoid serializing data both in
    // encode and to compute hash.
    let data_hash = hash(&borsh::to_vec(&data).unwrap());
    let commitment = SpiceDataCommitment { hash: data_hash, root: merkle_root, encoded_length };
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
    Ok(EncodedData { commitment, parts })
}

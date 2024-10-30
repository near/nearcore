use near_primitives::reed_solomon::{reed_solomon_num_data_parts, reed_solomon_part_length};

/// Ratio of the number of data parts to total parts in the Reed Solomon encoding.
/// The tradeoff here is having a higher ratio is better for handling missing parts and network errors
/// but increases the size of the encoded state witness and the total network bandwidth requirements.
pub const WITNESS_RATIO_DATA_PARTS: f64 = 0.6;
pub const CONTRACT_DEPLOYS_RATIO_DATA_PARTS: f64 = 0.6;

pub fn witness_part_length(encoded_witness_size: usize, total_parts: usize) -> usize {
    reed_solomon_part_length(
        encoded_witness_size,
        reed_solomon_num_data_parts(total_parts, WITNESS_RATIO_DATA_PARTS),
    )
}

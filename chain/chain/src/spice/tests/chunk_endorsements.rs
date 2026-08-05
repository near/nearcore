use crate::Error;
use crate::stateless_validation::chunk_endorsement::validate_spice_chunk_endorsements_in_header;
use near_primitives::block_header::{BlockHeader, BlockHeaderV7};
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;

/// With spice the chunk endorsements bitmap in the header is unused, so headers must contain an
/// empty bitmap for each shard.
fn header_with(chunk_mask: Vec<bool>, chunk_endorsements: ChunkEndorsementsBitmap) -> BlockHeader {
    let mut header = BlockHeaderV7::default();
    header.inner_rest.chunk_mask = chunk_mask;
    header.inner_rest.chunk_endorsements = chunk_endorsements;
    BlockHeader::BlockHeaderV7(header)
}

#[test]
fn test_empty_bitmap_for_each_shard_is_valid() {
    let header = header_with(vec![true, false], ChunkEndorsementsBitmap::new(2));
    validate_spice_chunk_endorsements_in_header(&header).unwrap();
}

#[test]
fn test_bitmap_with_endorsement_bits_is_invalid() {
    let header = header_with(
        vec![true, false],
        ChunkEndorsementsBitmap::from_endorsements(vec![vec![true], vec![]]),
    );
    let result = validate_spice_chunk_endorsements_in_header(&header);
    assert!(matches!(result, Err(Error::InvalidChunkEndorsementBitmap(_))), "got {:?}", result);
}

#[test]
fn test_bitmap_with_only_false_bits_is_invalid() {
    // The bitmap must be empty; even all-zero bits of non-zero length are rejected.
    let header = header_with(
        vec![true, false],
        ChunkEndorsementsBitmap::from_endorsements(vec![vec![false], vec![]]),
    );
    let result = validate_spice_chunk_endorsements_in_header(&header);
    assert!(matches!(result, Err(Error::InvalidChunkEndorsementBitmap(_))), "got {:?}", result);
}

#[test]
fn test_bitmap_with_wrong_number_of_shards_is_invalid() {
    let header = header_with(vec![true, false], ChunkEndorsementsBitmap::new(3));
    let result = validate_spice_chunk_endorsements_in_header(&header);
    assert!(matches!(result, Err(Error::InvalidChunkEndorsementBitmap(_))), "got {:?}", result);
}

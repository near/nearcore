use super::types::BlockInfo;

struct BloomFilter;

pub struct FlastStorageDelta {
    block: BlockInfo,
    filter: BloomFilter,
}
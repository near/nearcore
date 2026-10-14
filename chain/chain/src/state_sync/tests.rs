use super::{derive_epoch_sync_hash, update_sync_hashes};
use crate::ChainStore;
use crate::test_utils::get_chain_with_genesis;
use near_async::time::Clock;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::EpochId;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_store::adapter::StoreAdapter as _;
use near_store::{DBCol, FINAL_HEAD_KEY};
use std::sync::Arc;

const NUM_SHARDS: usize = 2;
const EPOCH_ID_1: EpochId = EpochId(CryptoHash([1; 32]));
const EPOCH_ID_2: EpochId = EpochId(CryptoHash([2; 32]));

struct HeaderChain {
    chain_store: ChainStore,
    headers: Vec<Arc<BlockHeader>>,
}

impl HeaderChain {
    fn new(protocol_version: ProtocolVersion) -> Self {
        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&Clock::real())
            .protocol_version(protocol_version)
            .shard_layout(ShardLayout::multi_shard(NUM_SHARDS as u64, 1))
            .build();
        let chain = get_chain_with_genesis(Clock::real(), genesis);
        let chain_store = chain.chain_store().clone();
        let genesis_header = chain_store.get_block_header(chain.genesis().hash()).unwrap();
        Self { chain_store, headers: vec![genesis_header] }
    }

    fn non_spice() -> Self {
        let chain = Self::new(PROTOCOL_VERSION.min(ProtocolFeature::Spice.protocol_version() - 1));
        assert!(!chain.headers[0].is_spice());
        chain
    }

    #[cfg(feature = "protocol_feature_spice")]
    fn spice() -> Self {
        let chain = Self::new(ProtocolFeature::Spice.protocol_version());
        assert!(chain.headers[0].is_spice());
        chain
    }

    fn push(&mut self, epoch_id: EpochId, chunk_mask: [bool; NUM_SHARDS]) -> CryptoHash {
        self.push_header(epoch, chunk_mask, /*save=*/ true)
    }

    /// Appends a header the node never stored, as epoch sync leaves gaps.
    #[cfg(feature = "protocol_feature_spice")]
    fn push_unsaved(&mut self, epoch_id: EpochId, chunk_mask: [bool; NUM_SHARDS]) -> CryptoHash {
        self.push_header(epoch, chunk_mask, /*save=*/ false)
    }

    fn push_header(
        &mut self,
        epoch_id: EpochId,
        chunk_mask: [bool; NUM_SHARDS],
        save: bool,
    ) -> CryptoHash {
        let prev = self.headers.last().unwrap().clone();
        let mut header = BlockHeader::clone(&prev);
        header.set_prev_hash(*prev.hash());
        header.set_height(prev.height() + 1);
        header.set_prev_height(prev.height());
        header.set_epoch_id(epoch_id);
        header.set_chunk_mask(chunk_mask.to_vec());
        // Finality two blocks behind, as doomslug reaches it with every block produced.
        let last_final = &self.headers[self.headers.len().saturating_sub(2)];
        header.set_last_final_block(*last_final.hash());
        header.init();
        let block_hash = *header.hash();
        if !save {
            self.headers.push(Arc::new(header));
            return block_hash;
        }
        let mut store_update = self.chain_store.store().store_update();
        store_update.insert_ser(DBCol::BlockHeader, block_hash.as_ref(), &header);
        store_update.set_ser(DBCol::NextBlockHashes, prev.hash().as_ref(), &block_hash);
        store_update.commit();
        // What header processing records for the sync hash.
        let mut store_update = self.chain_store.store().store_update();
        update_sync_hashes(&self.chain_store, &mut store_update, &header).unwrap();
        store_update.commit();
        self.headers.push(Arc::new(header));
        block_hash
    }

    /// Appends `count` headers in `epoch` with a chunk in every shard.
    fn push_full(&mut self, epoch_id: EpochId, count: usize) -> Vec<CryptoHash> {
        (0..count).map(|_| self.push(epoch_id, [true; NUM_SHARDS])).collect()
    }

    /// The sync hash header processing recorded for `epoch`.
    fn recorded_sync_hash(&self, epoch_id: &EpochId) -> Option<CryptoHash> {
        self.chain_store.get_current_epoch_sync_hash(epoch_id)
    }

    fn set_final_head(&self, block_hash: &CryptoHash) {
        let header = self.chain_store.get_block_header(block_hash).unwrap();
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::BlockMisc, FINAL_HEAD_KEY, &Tip::from_header(&header));
        store_update.commit();
    }
}

#[test]
fn test_derive_epoch_sync_hash_after_two_new_chunks_in_every_shard() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::non_spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    let new_epoch = chain.push_full(EPOCH_ID_2, EPOCH_LENGTH);
    chain.set_final_head(new_epoch.last().unwrap());

    assert_eq!(
        derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(),
        Some(new_epoch[3])
    );
}

#[test]
fn test_derive_epoch_sync_hash_missing_chunk_delays_it() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::non_spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    // The epoch's first block, which counts none, then one without shard 1's chunk.
    chain.push(EPOCH_ID_2, [true, true]);
    chain.push(EPOCH_ID_2, [true, false]);
    let rest = chain.push_full(EPOCH_ID_2, EPOCH_LENGTH - 2);
    chain.set_final_head(&rest[3]);

    // Shard 1 reaches its second new chunk only at `rest[1]`.
    assert_eq!(derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(), Some(rest[2]));
}

#[test]
fn test_derive_epoch_sync_hash_none_above_final_head() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::non_spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    let new_epoch = chain.push_full(EPOCH_ID_2, EPOCH_LENGTH);

    // `new_epoch[2]` reaches the threshold, but the block after it is not final yet.
    chain.set_final_head(&new_epoch[2]);
    assert_eq!(derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(), None);

    // now block after it is final.
    chain.set_final_head(&new_epoch[3]);
    assert_eq!(
        derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(),
        Some(new_epoch[3])
    );
}

#[test]
fn test_update_sync_hashes_records_once_final() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::non_spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();

    let new_epoch = chain.push_full(EPOCH_ID_2, 5);
    // `new_epoch[3]` is the sync block, but the last final block is still `new_epoch[2]`.
    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_2), None);
    chain.push_full(EPOCH_ID_2, 1);
    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_2), Some(new_epoch[3]));
    chain.set_final_head(&new_epoch[3]);
    assert_eq!(
        derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(),
        chain.recorded_sync_hash(&EPOCH_ID_2)
    );
}

#[cfg(feature = "protocol_feature_spice")]
#[test]
fn test_derive_epoch_sync_hash_spice_is_epoch_first_block() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    let first = chain.push(EPOCH_ID_2, [true, false]);
    chain.push(EPOCH_ID_2, [false, true]);
    chain.push(EPOCH_ID_2, [true, false]);
    chain.set_final_head(&first);

    assert_eq!(derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(), Some(first));
}

#[cfg(feature = "protocol_feature_spice")]
#[test]
fn test_derive_epoch_sync_hash_spice_none_before_first_block_is_final() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    chain.push_full(EPOCH_ID_2, 3);
    chain.set_final_head(&old_epoch_end);

    assert_eq!(derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(), None);
}

#[cfg(feature = "protocol_feature_spice")]
#[test]
fn test_update_sync_hashes_spice_records_first_block_once_final() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::spice();
    let old_epoch_end = *chain.push_full(EPOCH_ID_1, EPOCH_LENGTH).last().unwrap();
    let first = chain.push(EPOCH_ID_2, [true, false]);
    chain.push(EPOCH_ID_2, [false, true]);

    // The last final block is still in epoch "a".
    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_2), None);
    chain.push(EPOCH_ID_2, [true, false]);
    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_2), Some(first));
    chain.set_final_head(&first);
    assert_eq!(derive_epoch_sync_hash(&chain.chain_store, &old_epoch_end).unwrap(), Some(first));
}

#[cfg(feature = "protocol_feature_spice")]
#[test]
fn test_update_sync_hashes_spice_epoch_after_genesis() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::spice();
    let epoch = chain.push_full(EPOCH_ID_1, EPOCH_LENGTH);

    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_1), Some(epoch[0]));
}

/// A header missing on the way back to the epoch's first block, as epoch sync leaves, means
/// the first block cannot be named, so nothing is recorded.
#[cfg(feature = "protocol_feature_spice")]
#[test]
fn test_update_sync_hashes_spice_none_across_header_gap() {
    const EPOCH_LENGTH: usize = 10;

    let mut chain = HeaderChain::spice();
    chain.push_full(EPOCH_ID_1, EPOCH_LENGTH);
    chain.push_unsaved(EPOCH_ID_2, [true, true]);
    chain.push_full(EPOCH_ID_2, EPOCH_LENGTH - 1);

    assert_eq!(chain.recorded_sync_hash(&EPOCH_ID_2), None);
}

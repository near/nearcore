#![no_main]

use arbitrary::Arbitrary;
use std::sync::Arc;

use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::receipt::Receipt;
use near_chunks::test_utils::ChunkTestFixture;
use near_chunks::ShardsManager;
use near_primitives::utils::MaybeValidated;
use near_async::time::FakeClock;
use near_async::messaging::IntoSender;


#[derive(Debug, Arbitrary, BorshDeserialize, BorshSerialize)]
pub struct ChunkParams {
    orphan_chunk: bool,
    track_all_shards: bool,
    receipts: Vec<Receipt>
}

// looking for crashes here
libfuzzer_sys::fuzz_target!(|params: ChunkParams| {
    let fixture = ChunkTestFixture::new(
        params.orphan_chunk,
        3,
        6,
        6,
        params.track_all_shards,
        params.receipts,
    );
    let mut shards_manager = ShardsManager::new(
        FakeClock::default().clock(),
        Some(fixture.mock_chunk_part_owner.clone()),
        Arc::new(fixture.epoch_manager.clone()),
        fixture.shard_tracker.clone(),
        fixture.mock_network.as_sender(),
        fixture.mock_client_adapter.as_sender(),
        fixture.chain_store.new_read_only_chunks_store(),
        fixture.mock_chain_head.clone(),
        fixture.mock_chain_head.clone(),
    );

    let partial_encoded_chunk = fixture.make_partial_encoded_chunk(fixture.all_part_ords.as_slice());
    let _ = shards_manager.process_partial_encoded_chunk(MaybeValidated::from(partial_encoded_chunk));
});

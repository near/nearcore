use crate::metrics;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use lru::LruCache;
use near_async::time::Clock;
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessAck;
use s3::creds::time::ext::InstantExt as _;
use std::hash::Hash;
use std::num::NonZeroUsize;

/// Limit to the number of witnesses tracked.
///
/// Other witnesses past this number are discarded (perhaps add a blurb on how.)
const CHUNK_STATE_WITNESS_MAX_RECORD_COUNT: usize = 50;

/// Refers to a state witness sent from a chunk producer to a chunk validator.
///
/// Used to map the incoming acknowledgement messages back to the timing information of
/// the originating witness record.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ChunkStateWitnessKey {
    /// Hash of the chunk for which the state witness was generated.
    chunk_hash: ChunkHash,
}

impl ChunkStateWitnessKey {
    pub fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash }
    }
}

struct ChunkStateWitnessRecord {
    /// Size of the witness in bytes.
    witness_size: usize,
    /// Number of validators that the witness is sent to.
    num_validators: usize,
    /// Timestamp of when the chunk producer sends the state witness.
    sent_timestamp: near_async::time::Instant,
}

/// Tracks a collection of state witnesses sent from chunk producers to validators.
///
/// This is currently used to calculate the round-trip time of sending the witness and
/// getting the ack message back, where the ack message is used as a proxy for the endorsement
/// message from validators to block producers to make an estimate of network time for sending
/// witness and receiving the endorsement.
pub struct ChunkStateWitnessTracker {
    witnesses: LruCache<ChunkStateWitnessKey, ChunkStateWitnessRecord>,
    clock: Clock,
}

impl ChunkStateWitnessTracker {
    pub fn new(clock: Clock) -> Self {
        Self {
            witnesses: LruCache::new(
                NonZeroUsize::new(CHUNK_STATE_WITNESS_MAX_RECORD_COUNT).unwrap(),
            ),
            clock,
        }
    }

    /// Adds a new witness message to track.
    pub fn record_witness_sent(
        &mut self,
        chunk_hash: ChunkHash,
        witness_size_in_bytes: usize,
        num_validators: usize,
    ) -> () {
        let key = ChunkStateWitnessKey::new(chunk_hash);
        tracing::trace!(target: "state_witness_tracker", witness_key=?key,
            size=witness_size_in_bytes, "Recording state witness sent.");
        self.witnesses.put(
            key,
            ChunkStateWitnessRecord {
                num_validators,
                witness_size: witness_size_in_bytes,
                sent_timestamp: self.clock.now(),
            },
        );
    }

    /// Handles an ack message for the witness. Calculates the round-trip duration and
    /// records it in the corresponding metric.
    pub fn on_witness_ack_received(&mut self, ack: ChunkStateWitnessAck) -> () {
        let key = ChunkStateWitnessKey { chunk_hash: ack.chunk_hash };
        tracing::trace!(target: "state_witness_tracker", witness_key=?key,
            "Received ack for state witness");
        if let Some(record) = self.witnesses.get_mut(&key) {
            debug_assert!(record.num_validators > 0);

            Self::update_roundtrip_time_metric(record, &self.clock);

            // Cleanup the record if we received the acks from all the validators, otherwise update
            // the number of validators from which we are expecting an ack message.
            let remaining = record.num_validators.saturating_sub(1);
            if remaining > 0 {
                record.num_validators = remaining;
            } else {
                self.witnesses.pop(&key);
            }
        }
    }

    /// Records the roundtrip time in metrics.
    fn update_roundtrip_time_metric(record: &ChunkStateWitnessRecord, clock: &Clock) -> () {
        let received_time = clock.now();
        if received_time > record.sent_timestamp {
            metrics::CHUNK_STATE_WITNESS_NETWORK_ROUNDTRIP_TIME
                .with_label_values(&[witness_size_bucket(record.witness_size)])
                .observe(
                    (received_time.signed_duration_since(record.sent_timestamp)).as_seconds_f64(),
                );
        }
    }

    #[cfg(test)]
    fn get_record_for_witness(
        &mut self,
        witness: &near_primitives::stateless_validation::state_witness::ChunkStateWitness,
    ) -> Option<&ChunkStateWitnessRecord> {
        let key = ChunkStateWitnessKey::new(witness.chunk_header.chunk_hash());
        self.witnesses.get(&key)
    }
}

/// Buckets for state-witness size.
static SIZE_IN_BYTES_TO_BUCKET: &'static [(ByteSize, &str)] = &[
    (ByteSize::kb(1), "<1KB"),
    (ByteSize::kb(10), "1-10KB"),
    (ByteSize::kb(100), "10-100KB"),
    (ByteSize::mb(1), "100KB-1MB"),
    (ByteSize::mb(2), "1-2MB"),
    (ByteSize::mb(3), "2-3MB"),
    (ByteSize::mb(4), "3-4MB"),
    (ByteSize::mb(5), "4-5MB"),
    (ByteSize::mb(10), "5-10MB"),
    (ByteSize::mb(20), "10-20MB"),
];

/// Returns the string representation of the size buckets for a given witness size in bytes.
fn witness_size_bucket(size_in_bytes: usize) -> &'static str {
    for (upper_size, label) in SIZE_IN_BYTES_TO_BUCKET.iter() {
        if size_in_bytes < upper_size.as_u64() as usize {
            return *label;
        }
    }
    ">20MB"
}

#[cfg(test)]
mod state_witness_tracker_tests {
    use super::*;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_primitives::hash::hash;
    use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
    use near_primitives::types::ShardId;

    const NUM_VALIDATORS: usize = 3;

    #[test]
    fn record_and_receive_ack_num_validators_decreased() {
        let witness = dummy_witness();
        let clock = dummy_clock();
        let mut tracker = ChunkStateWitnessTracker::new(clock.clock());

        tracker.record_witness_sent(witness.chunk_header.compute_hash(), 4321, NUM_VALIDATORS);
        clock.advance(Duration::milliseconds(3444));

        // Ack received from all "except for one".
        for _ in 1..NUM_VALIDATORS {
            tracker.on_witness_ack_received(ChunkStateWitnessAck::new(&witness));
        }

        let record = tracker.get_record_for_witness(&witness);
        assert!(record.is_some());
        assert_eq!(record.unwrap().num_validators, 1);
    }

    #[test]
    fn record_and_receive_ack_record_deleted() {
        let witness = dummy_witness();
        let clock = dummy_clock();
        let mut tracker = ChunkStateWitnessTracker::new(clock.clock());

        tracker.record_witness_sent(witness.chunk_header.compute_hash(), 4321, NUM_VALIDATORS);
        clock.advance(Duration::milliseconds(3444));

        // Ack received from all.
        for _ in 1..=NUM_VALIDATORS {
            tracker.on_witness_ack_received(ChunkStateWitnessAck::new(&witness));
        }

        let record = tracker.get_record_for_witness(&witness);
        assert!(record.is_none());
    }

    #[test]
    fn choose_size_bucket() {
        assert_eq!(witness_size_bucket(500), "<1KB");
        assert_eq!(witness_size_bucket(15_000), "10-100KB");
        assert_eq!(witness_size_bucket(250_000), "100KB-1MB");
        assert_eq!(witness_size_bucket(2_500_000), "2-3MB");
        assert_eq!(witness_size_bucket(7_500_000), "5-10MB");
        assert_eq!(witness_size_bucket(25_000_000), ">20MB");
    }

    fn dummy_witness() -> ChunkStateWitness {
        ChunkStateWitness::new_dummy(100, 2 as ShardId, hash("fake hash".as_bytes()))
    }

    fn dummy_clock() -> FakeClock {
        FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap())
    }
}

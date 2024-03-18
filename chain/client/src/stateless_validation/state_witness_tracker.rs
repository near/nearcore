use std::hash::{Hash, Hasher};
use borsh::{BorshDeserialize, BorshSerialize};
use lru::LruCache;
use once_cell::sync::Lazy;

use near_async::time::Clock;
use near_o11y::metrics::{exponential_buckets, HistogramVec, try_create_histogram_vec};
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::{ChunkStateWitness, ChunkStateWitnessAck};

/// This is currently used for metrics computation, so we do not need to keep all the
/// witnesses, so make the capacity 10.
const CHUNK_STATE_WITNESS_MAX_RECORD_COUNT: usize = 5;

static CHUNK_STATE_WITNESS_NETWORK_ROUNDTRIP_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_network_roundtrip_time",
        "Time in seconds between sending state witness through the network to chunk producer \
               and receiving the corresponding ack message",
        &["witness_size_bucket"],
        Some(exponential_buckets(0.01, 2.0, 12).unwrap()),
    )
        .unwrap()
});

/// Refers to a state witness sent to a chunk producer. Used to locate incoming acknowledgement
/// messages back to the timing information of the originating witness.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkStateWitnessKey {
    /// Hash of the chunk for which the state witness was generated.
    pub chunk_hash: ChunkHash,
}

impl Hash for ChunkStateWitnessKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.chunk_hash.hash(state);
    }
}


impl ChunkStateWitnessKey {
    pub fn new(witness: &ChunkStateWitness) -> Self {
        Self {
            chunk_hash: witness.inner.chunk_header.chunk_hash(),
        }
    }
}

pub struct ChunkStateWitnessRecord {
    /// Size of the witness in bytes.
    pub witness_size: usize,
    /// Number of validators that the witness is sent to.
    pub num_validators: usize,
    /// Timestamp of when the chunk producer sends the state witness.
    pub sent_timestamp: near_async::time::Instant,
}

/// Tracks a collection of state witnesses sent from chunk producers to validators.
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
            witnesses: LruCache::new(CHUNK_STATE_WITNESS_MAX_RECORD_COUNT),
            clock,
        }
    }

    /// Adds a new witness message to track.
    pub fn record_witness_sent(&mut self, witness: &ChunkStateWitness, witness_size_in_bytes: usize,
                               num_validators: usize) -> () {
        let key = ChunkStateWitnessKey::new(witness);
        tracing::debug!(target: "state_witness_tracker", "Sent state witness: {:?} of {} bytes",
            witness.inner.chunk_header.chunk_hash(), witness_size_in_bytes);
        self.witnesses.put(key, ChunkStateWitnessRecord {
            num_validators,
            witness_size: witness_size_in_bytes,
            sent_timestamp: self.clock.now(),
        });
    }

    /// Handles an ack message for the witness. Calculates the round-trip duration and
    /// records it in the corresponding metric.
    pub fn on_witness_ack_received(&mut self, ack: ChunkStateWitnessAck) -> () {
        let key = ChunkStateWitnessKey { chunk_hash: ack.chunk_hash };
        tracing::debug!(target: "state_witness_tracker", "Received ack for state witness: {:?}",
            key.chunk_hash);
        if let Some(mut record) = self.witnesses.pop(&key) {
            assert!(record.num_validators > 0);

            Self::update_roundtrip_time_metric(&record, &self.clock);

            // Cleanup the record if we received the acks from all the validators.
            if let Some(remaining) = record.num_validators.checked_sub(1) {
                if remaining > 0 {
                    record.num_validators = remaining;
                    self.witnesses.put(key.clone(), record);
                }
            }
        }
    }

    /// Records the roundtrip time in metrics.
    fn update_roundtrip_time_metric(record: &ChunkStateWitnessRecord, clock: &Clock) -> () {
        let received_time = clock.now();
        if received_time > record.sent_timestamp {
            CHUNK_STATE_WITNESS_NETWORK_ROUNDTRIP_TIME
                .with_label_values(&[witness_size_bucket(record.witness_size)])
                .observe((received_time - record.sent_timestamp).as_seconds_f64());
        }
    }

    #[cfg(test)]
    fn get_record_for_witness(&mut self, witness: &ChunkStateWitness) -> Option<&ChunkStateWitnessRecord> {
        let key = ChunkStateWitnessKey::new(witness);
        return self.witnesses.get(&key);
    }
}


/// Buckets for state-witness size.
// TODO: Use size::Size to represent sizes.
static SIZE_IN_BYTES_TO_BUCKET: &'static [(usize, &str)] = &[
    (1_000, "<1KB"),
    (10_000, "1-10KB"),
    (100_000, "10-100KB"),
    (1_000_000, "100KB-1MB"),
    (5_000_000, "1-5MB"),
    (10_000_000, "5-10MB"),
    (20_000_000, "10-20MB")
];

/// Returns the string representation of the size buckets for a given witness size in bytes.
fn witness_size_bucket(size_in_bytes: usize) -> &'static str {
    for (upper_size, label) in SIZE_IN_BYTES_TO_BUCKET.iter() {
        if size_in_bytes < *upper_size {
            return *label;
        }
    }
    return ">20MB";
}


#[cfg(test)]
mod state_witness_tracker_tests {
    use super::*;
    use near_async::time::{Duration, FakeClock, Utc};
    use near_primitives::hash::hash;
    use near_primitives::types::ShardId;

    const NUM_VALIDATORS: usize = 3;

    #[test]
    fn record_and_receive_ack_num_validators_decreased() {
        let witness = dummy_witness();
        let clock = dummy_clock();
        let mut tracker = ChunkStateWitnessTracker::new(clock.clock());

        tracker.record_witness_sent(&witness, 4321, NUM_VALIDATORS);
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

        tracker.record_witness_sent(&witness, 4321, NUM_VALIDATORS);
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
        assert_eq!(witness_size_bucket(15_000),  "10-100KB");
        assert_eq!(witness_size_bucket(250_000), "100KB-1MB");
        assert_eq!(witness_size_bucket(7_500), "5-10MB");
        assert_eq!(witness_size_bucket(25_000_000), ">20MB");
    }

    fn dummy_witness() -> ChunkStateWitness {
        ChunkStateWitness::new_dummy(100, 2 as ShardId, hash("fake hash".as_bytes()))
    }

    fn dummy_clock() -> FakeClock {
        FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap())
    }
}
use std::collections::HashMap;

use near_primitives::types::ShardId;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};

use super::protocol_upgrade::test_protocol_upgrade;

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_no_missing_chunks() {
    if !ProtocolFeature::BandwidthScheduler.enabled(PROTOCOL_VERSION) {
        // EpochConfig is unavailable on stable
        return;
    }

    test_protocol_upgrade(
        ProtocolFeature::BandwidthScheduler.protocol_version() - 1,
        ProtocolFeature::BandwidthScheduler.protocol_version(),
        HashMap::new(),
    );
}

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_with_missing_chunks_two() {
    if !ProtocolFeature::BandwidthScheduler.enabled(PROTOCOL_VERSION) {
        // EpochConfig is unavailable on stable
        return;
    }

    test_protocol_upgrade(
        ProtocolFeature::BandwidthScheduler.protocol_version() - 1,
        ProtocolFeature::BandwidthScheduler.protocol_version(),
        HashMap::from_iter(
            [
                (ShardId::new(0), 0..0),
                (ShardId::new(1), -2..0),
                (ShardId::new(2), 0..2),
                (ShardId::new(3), -2..2),
            ]
            .into_iter(),
        ),
    );
}

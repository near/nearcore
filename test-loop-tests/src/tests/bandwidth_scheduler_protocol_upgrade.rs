use std::collections::HashMap;

use near_primitives::version::ProtocolFeature;

use super::protocol_upgrade::test_protocol_upgrade;

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_no_missing_chunks() {
    test_protocol_upgrade(
        ProtocolFeature::BandwidthScheduler.protocol_version() - 1,
        ProtocolFeature::BandwidthScheduler.protocol_version(),
        HashMap::new(),
    );
}

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_with_missing_chunks_two() {
    test_protocol_upgrade(
        ProtocolFeature::BandwidthScheduler.protocol_version() - 1,
        ProtocolFeature::BandwidthScheduler.protocol_version(),
        HashMap::from_iter([(0, 0..0), (1, -2..0), (2, 0..2), (3, -2..2)].into_iter()),
    );
}

use std::collections::HashMap;

use super::protocol_upgrade::test_protocol_upgrade;

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_no_missing_chunks() {
    test_protocol_upgrade(
        near_primitives::version::PROTOCOL_VERSION - 1,
        near_primitives::version::PROTOCOL_VERSION,
        HashMap::new(),
    );
}

#[test]
fn test_bandwidth_scheduler_protocol_upgrade_with_missing_chunks_two() {
    test_protocol_upgrade(
        near_primitives::version::PROTOCOL_VERSION - 1,
        near_primitives::version::PROTOCOL_VERSION,
        HashMap::from_iter([(0, 0..0), (1, -2..0), (2, 0..2), (3, -2..2)].into_iter()),
    );
}

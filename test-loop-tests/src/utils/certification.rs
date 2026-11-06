use crate::setup::env::TestLoopEnv;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolFeature;

/// How far the last certified block may trail a node's head.
const MAX_CERTIFICATION_LAG: BlockHeight = 5;

/// Asserts that certification did not stall: on every live node at a spice epoch, the last
/// certified block is within `MAX_CERTIFICATION_LAG` blocks of the node's head.
pub(crate) fn assert_certification_kept_up(env: &TestLoopEnv) {
    for (idx, node_data) in env.node_datas.iter().enumerate() {
        if env.test_loop.is_denylisted(&node_data.identifier) {
            continue;
        }
        let node = env.node(idx);
        if !ProtocolFeature::Spice.enabled(node.protocol_version_at_head()) {
            continue;
        }
        let head = node.head().height;
        let certified = node.last_certified_block_header().height();
        assert!(
            certified + MAX_CERTIFICATION_LAG >= head,
            "certification stalled on {}: head {head}, last certified {certified}",
            node_data.account_id
        );
    }
}

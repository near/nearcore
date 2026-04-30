//! End-of-build mesh seeding for the real-PMA path.
//!
//! Position Y design (see plan.md "T3 design principle" and decision
//! #14): the testloop transport never writes connectivity state on
//! the connect path. Setup pre-populates `state.peers`, `peer_store`,
//! `account_announcements`, and the routing graph on every node so
//! PMA's `monitor_peers_trigger` and `tier1_connect` short-circuit
//! naturally.
//!
//! `populate_full_mesh` runs once at end-of-build, after every node
//! has been wired by `setup_client`. Order doesn't matter — the
//! populate runs after all nodes exist and is N×N symmetric.

use crate::setup::state::NodeExecutionData;
use near_async::time;
use near_network::PeerConnectionInfo;
use near_network::tcp;
use near_network::types::{Edge, PeerType};
use near_primitives::network::AnnounceAccount;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::EpochId;

const MESH_EDGE_NONCE: u64 = 1;

/// Build a signed `Edge` between two peers using each side's
/// `node_key`. Both signatures are present so the edge passes
/// `edge.verify()` inside `add_edges`.
fn build_signed_edge(me: &NodeExecutionData, other: &NodeExecutionData) -> Edge {
    let me_state = me
        .network_state
        .as_ref()
        .expect("populate_full_mesh: real-PMA path requires network_state");
    let other_state = other
        .network_state
        .as_ref()
        .expect("populate_full_mesh: real-PMA path requires network_state");

    let hash = Edge::build_hash(&me.peer_id, &other.peer_id, MESH_EDGE_NONCE);
    let me_sig = me_state.config.node_key.sign(hash.as_ref());
    let other_sig = other_state.config.node_key.sign(hash.as_ref());

    // Edge::new orders signatures to match its peer0 < peer1 convention.
    let (peer0, peer1, sig0, sig1) = if me.peer_id < other.peer_id {
        (me.peer_id.clone(), other.peer_id.clone(), me_sig, other_sig)
    } else {
        (other.peer_id.clone(), me.peer_id.clone(), other_sig, me_sig)
    };
    Edge::new(peer0, peer1, MESH_EDGE_NONCE, sig0, sig1)
}

/// Seeds peer_store, account_announcements, `state.peers`, and the
/// routing graph bidirectionally for every pair of nodes.
///
/// Synchronous: bypasses the async `on_peer_connected` path because
/// it would await `add_edges` through the demux, which is bound to a
/// `FakeClock` that the testloop drives. Calling `block_on` on that
/// path while the testloop thread is held would deadlock. Instead we
/// use `NetworkState::populate_for_testloop` which writes the same
/// connection state synchronously and adds the edge directly to the
/// routing graph (no broadcast needed — every node is populated
/// independently).
pub(crate) fn populate_full_mesh(
    clock: &time::Clock,
    nodes: &[NodeExecutionData],
    test_epoch_id: EpochId,
) {
    // 1. peer_store + account_announcements.
    for me in nodes {
        let Some(me_state) = me.network_state.as_ref() else { continue };
        for other in nodes {
            if other.peer_id == me.peer_id {
                continue;
            }
            // peer_store: status `Connected` so disconnect_and_ban can
            // write peer_ban (peer_store/mod.rs:peer_connected).
            me_state.peer_store.peer_connected(clock, &other.peer_info());

            // account_announcements: bypasses verify_announce_accounts
            // (the gossip ingress path). Each peer announces itself
            // signed under its own validator key.
            let signer = create_test_signer(other.account_id.as_str());
            let announcement = AnnounceAccount::new(&signer, other.peer_id.clone(), test_epoch_id);
            me_state.account_announcements.add_accounts(vec![announcement]);
        }
    }

    // 2. state.peers + routing graph via populate_for_testloop.
    for me in nodes {
        let Some(me_state) = me.network_state.as_ref() else { continue };
        for other in nodes {
            if other.peer_id == me.peer_id {
                continue;
            }
            let edge = build_signed_edge(me, other);

            // Convention: lexicographically lower peer_id is Outbound.
            // Stable rule; testloop doesn't differentiate but
            // production does for some checks.
            let peer_type =
                if me.peer_id < other.peer_id { PeerType::Outbound } else { PeerType::Inbound };

            let info = PeerConnectionInfo {
                peer_info: other.peer_info(),
                tier: tcp::Tier::T2,
                peer_type,
                archival: other.is_archival,
                tracked_shards: vec![],
                owned_account: None,
                established_time: clock.now(),
            };

            me_state.populate_for_testloop(clock, edge, info);
        }
    }
}

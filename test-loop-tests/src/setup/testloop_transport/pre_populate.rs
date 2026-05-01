//! End-of-build mesh seeding for the real-PMA path.
//!
//! Position Y design: the testloop transport never writes connectivity
//! state on the connect path. Setup pre-populates `state.peers`,
//! `peer_store`, `account_announcements`, and the routing graph on
//! every node so PMA's `monitor_peers_trigger` and `tier1_connect`
//! short-circuit naturally.
//!
//! `populate_full_mesh` runs once at end-of-build, after every node
//! has been wired by `setup_client`. `seed_node_into_mesh` is the
//! per-node variant called from `restart_node` / `add_node` so a
//! newly-built node is wired bidirectionally with all existing nodes.
//!
//! Both async: they call `state.on_peer_connected(...).await` which
//! goes through the FakeClock-bound demux. Callers spawn these on the
//! testloop's `FutureSpawner` and `run_until` a completion flag flips.

use super::registry::TestLoopNodeRegistry;
use crate::setup::state::NodeExecutionData;
use near_async::time;
use near_network::tcp;
use near_network::types::{Edge, PeerType};
use near_network::{NetworkTransport, PeerConnectionInfo};
use near_primitives::network::AnnounceAccount;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::EpochId;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Build a signed `Edge` between two peers using each side's
/// `node_key`. Both signatures are present so the edge passes
/// `edge.verify()` inside `add_edges`.
///
/// Nonce is anchored to the (Fake)Clock via `Edge::create_fresh_nonce`
/// — production interprets edge nonces as Unix-timestamp seconds and
/// silently prunes edges older than `PRUNE_EDGES_AFTER` (30 min). A
/// plain monotonic counter starting at 1 looks like 1970-01-01 to the
/// graph and gets dropped by the age filter, leaving the routing
/// table empty.
///
/// `nonce_counter` is incremented by 2 per call: keeps nonces odd
/// (Active edge state) and strictly monotonic across re-seeds (e.g.
/// `restart_node`) so the new edge dominates any previous edge to the
/// same peer.
fn build_signed_edge(
    clock: &time::Clock,
    me: &NodeExecutionData,
    other: &NodeExecutionData,
    nonce_counter: &AtomicU64,
) -> Edge {
    let me_state =
        me.network_state.as_ref().expect("populate: real-PMA path requires network_state");
    let other_state =
        other.network_state.as_ref().expect("populate: real-PMA path requires network_state");

    let base = Edge::create_fresh_nonce(clock);
    let nonce = base + nonce_counter.fetch_add(2, Ordering::Relaxed);
    let hash = Edge::build_hash(&me.peer_id, &other.peer_id, nonce);
    let me_sig = me_state.config.node_key.sign(hash.as_ref());
    let other_sig = other_state.config.node_key.sign(hash.as_ref());

    // Edge::new orders signatures to match its peer0 < peer1 convention.
    let (peer0, peer1, sig0, sig1) = if me.peer_id < other.peer_id {
        (me.peer_id.clone(), other.peer_id.clone(), me_sig, other_sig)
    } else {
        (other.peer_id.clone(), me.peer_id.clone(), other_sig, me_sig)
    };
    Edge::new(peer0, peer1, nonce, sig0, sig1)
}

/// Sync side of the seeding: `peer_store` + `account_announcements`.
fn seed_sync_side(
    me: &NodeExecutionData,
    other: &NodeExecutionData,
    clock: &time::Clock,
    test_epoch_id: EpochId,
) {
    let Some(me_state) = me.network_state.as_ref() else { return };
    // peer_store: status `Connected` so disconnect_and_ban can write
    // peer_ban (peer_store/mod.rs:peer_connected).
    me_state.peer_store.peer_connected(clock, &other.peer_info());

    // account_announcements: bypasses verify_announce_accounts (the
    // gossip ingress path). Each peer announces itself signed under
    // its own validator key.
    let signer = create_test_signer(other.account_id.as_str());
    let announcement = AnnounceAccount::new(&signer, other.peer_id.clone(), test_epoch_id);
    me_state.account_announcements.add_accounts(vec![announcement]);
}

/// Async side: `state.peers` + routing graph via `on_peer_connected`.
async fn seed_async_side(
    me: &NodeExecutionData,
    other: &NodeExecutionData,
    clock: &time::Clock,
    registry: &TestLoopNodeRegistry,
    nonce_counter: &AtomicU64,
) {
    let Some(me_state) = me.network_state.as_ref() else { return };
    let me_transport: Arc<dyn NetworkTransport> =
        registry.get(&me.peer_id).expect("populate: transport missing from registry");
    let edge = build_signed_edge(clock, me, other, nonce_counter);

    // Convention: lexicographically lower peer_id is Outbound. Stable
    // rule; testloop doesn't differentiate but production does for
    // some checks.
    let peer_type = if me.peer_id < other.peer_id { PeerType::Outbound } else { PeerType::Inbound };

    let info = PeerConnectionInfo {
        peer_info: other.peer_info(),
        tier: tcp::Tier::T2,
        peer_type,
        archival: other.is_archival,
        tracked_shards: vec![],
        owned_account: None,
        established_time: clock.now(),
    };

    me_state.on_peer_connected(clock, edge, info, me_transport).await;
}

/// Seeds peer_store, account_announcements, `state.peers`, and the
/// routing graph bidirectionally for every pair of nodes.
pub(crate) async fn populate_full_mesh(
    clock: &time::Clock,
    nodes: &[NodeExecutionData],
    registry: &TestLoopNodeRegistry,
    nonce_counter: &AtomicU64,
    test_epoch_id: EpochId,
) {
    // 1. Sync side first so all peer_store / announcement entries
    //    exist before any on_peer_connected reads them.
    for me in nodes {
        for other in nodes {
            if other.peer_id == me.peer_id {
                continue;
            }
            seed_sync_side(me, other, clock, test_epoch_id);
        }
    }

    // 2. Async side: state.peers + routing graph. Run all pairs
    //    concurrently so the demux's debounce fires once for the
    //    whole batch instead of N²-times sequentially. With 8 nodes,
    //    that's ~100ms of FakeClock vs ~5.4s sequential, which
    //    avoids racing past the warmup deadline.
    let async_seeds = nodes.iter().flat_map(|me| {
        nodes.iter().filter_map(move |other| {
            if other.peer_id == me.peer_id {
                None
            } else {
                Some(seed_async_side(me, other, clock, registry, nonce_counter))
            }
        })
    });
    futures::future::join_all(async_seeds).await;
}

/// Seeds a single newly-built node bidirectionally against all
/// existing nodes. Used by `restart_node` / `add_node` when the
/// real-PMA path is active.
pub(crate) async fn seed_node_into_mesh(
    clock: &time::Clock,
    new_node: &NodeExecutionData,
    existing_nodes: &[NodeExecutionData],
    registry: &TestLoopNodeRegistry,
    nonce_counter: &AtomicU64,
    test_epoch_id: EpochId,
) {
    // Sync side both directions.
    for other in existing_nodes {
        if other.peer_id == new_node.peer_id {
            continue;
        }
        seed_sync_side(new_node, other, clock, test_epoch_id);
        seed_sync_side(other, new_node, clock, test_epoch_id);
    }

    // Async side both directions, run concurrently so the demux's
    // debounce fires once for the batch instead of per-pair.
    let async_seeds =
        existing_nodes.iter().filter(|n| n.peer_id != new_node.peer_id).flat_map(|other| {
            [
                seed_async_side(new_node, other, clock, registry, nonce_counter),
                seed_async_side(other, new_node, clock, registry, nonce_counter),
            ]
        });
    futures::future::join_all(async_seeds).await;
}

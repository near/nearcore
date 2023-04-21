use crate::blacklist;
use crate::broadcast;
use crate::config::NetworkConfig;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{Edge, Encoding, Ping, Pong, RoutedMessageBody, RoutingTableUpdate};
use crate::peer;
use crate::peer::peer_actor::{
    ClosingReason, ConnectionClosedEvent, DROP_DUPLICATED_MESSAGES_PERIOD,
};
use crate::peer_manager;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::Event;
use crate::private_actix::RegisterPeerError;
use crate::store;
use crate::tcp;
use crate::testonly::{abort_on_panic, make_rng, Rng};
use crate::types::PeerMessage;
use crate::types::{PeerInfo, ReasonForBan};
use near_async::time;
use near_primitives::network::PeerId;
use near_store::db::TestDB;
use pretty_assertions::assert_eq;
use rand::seq::IteratorRandom;
use rand::Rng as _;
use std::collections::HashSet;
use std::net::Ipv6Addr;
use std::sync::Arc;

// test routing in a two-node network before and after connecting the nodes
#[tokio::test]
async fn simple() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[]).await;

    tracing::info!(target:"test", "connect the nodes");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[(id1.clone(), vec![id1.clone()])]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[(id0.clone(), vec![id0.clone()])]).await;
}

// test routing for three nodes in a line
#[tokio::test]
async fn three_nodes_path() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "connect three nodes in a line");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;
}

// test routing for three nodes in a line, then test routing after completing the triangle
#[tokio::test]
async fn three_nodes_star() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "connect three nodes in a line");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    tracing::info!(target:"test", "connect {id0} and {id2}");
    pm0.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;
}

// test routing in a network with two connected components having two nodes each,
// then test routing after joining them into a square
#[tokio::test]
async fn join_components() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "create two connected components having two nodes each");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[(id1.clone(), vec![id1.clone()])]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[(id0.clone(), vec![id0.clone()])]).await;

    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[(id3.clone(), vec![id3.clone()])]).await;
    tracing::info!(target:"test", "wait for {id3} routing table");
    pm3.wait_for_routing_table(&[(id2.clone(), vec![id2.clone()])]).await;

    tracing::info!(target:"test", "join the two components into a square");
    pm0.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;
    pm3.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id2.clone()]),
        (id3.clone(), vec![id1.clone(), id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id0.clone(), id3.clone()]),
        (id3.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id1.clone(), vec![id0.clone(), id3.clone()]),
        (id3.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id3} routing table");
    pm3.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone(), id2.clone()]),
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    drop(pm0);
    drop(pm1);
    drop(pm2);
    drop(pm3);
}

// test routing for three nodes in a line, then test dropping the middle node
#[tokio::test]
async fn simple_remove() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "connect 3 nodes in a line");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    tracing::info!(target:"test","stop {id1}");
    drop(pm1);

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[]).await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[]).await;
}

// Awaits until the expected ping is seen in the event stream.
pub async fn wait_for_ping(events: &mut broadcast::Receiver<Event>, want_ping: Ping) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::Ping(ping)) => {
                if ping == want_ping {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
        .await;
}

// Awaits until the expected pong is seen in the event stream.
pub async fn wait_for_pong(events: &mut broadcast::Receiver<Event>, want_pong: Pong) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::Pong(pong)) => {
                if pong == want_pong {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
        .await;
}

// Awaits until RoutedMessageDropped is seen in the event stream.
pub async fn wait_for_message_dropped(events: &mut broadcast::Receiver<Event>) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::RoutedMessageDropped) => Some(()),
            _ => None,
        })
        .await;
}

// test ping in a two-node network
#[tokio::test]
async fn ping_simple() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[(id1.clone(), vec![id1.clone()])]).await;

    // capture event streams before pinging
    let mut pm0_ev = pm0.events.from_now();
    let mut pm1_ev = pm1.events.from_now();

    tracing::info!(target:"test", "send ping from {id0} to {id1}");
    pm0.send_ping(&clock.clock(), 0, id1.clone()).await;

    tracing::info!(target:"test", "await ping at {id1}");
    wait_for_ping(&mut pm1_ev, Ping { nonce: 0, source: id0.clone() }).await;

    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 0, source: id1.clone() }).await;

    drop(pm0);
    drop(pm1);
}

// test ping without a direct connection
#[tokio::test]
async fn ping_jump() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start three nodes");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "connect nodes in a line");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    // capture event streams before pinging
    let mut pm0_ev = pm0.events.from_now();
    let mut pm2_ev = pm2.events.from_now();

    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 0, id2.clone()).await;

    tracing::info!(target:"test", "await ping at {id2}");
    wait_for_ping(&mut pm2_ev, Ping { nonce: 0, source: id0.clone() }).await;

    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 0, source: id2.clone() }).await;
}

// test that ping over an indirect connection with ttl=2 is delivered
#[tokio::test]
async fn test_dont_drop_after_ttl() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start three nodes");
    let mut cfg0 = chain.make_config(rng);
    cfg0.routed_message_ttl = 2;
    let pm0 = start_pm(clock.clock(), TestDB::new(), cfg0, chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "connect nodes in a line");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    // capture event streams before pinging
    let mut pm0_ev = pm0.events.from_now();
    let mut pm2_ev = pm2.events.from_now();

    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 0, id2.clone()).await;

    tracing::info!(target:"test", "await ping at {id2}");
    wait_for_ping(&mut pm2_ev, Ping { nonce: 0, source: id0.clone() }).await;

    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 0, source: id2.clone() }).await;
}

// test that ping over an indirect connection with ttl=1 is dropped
#[tokio::test]
async fn test_drop_after_ttl() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start three nodes");
    let mut cfg0 = chain.make_config(rng);
    cfg0.routed_message_ttl = 1;
    let pm0 = start_pm(clock.clock(), TestDB::new(), cfg0, chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "connect nodes in a line");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    // capture event stream before pinging
    let mut pm1_ev = pm1.events.from_now();

    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 0, id2.clone()).await;

    tracing::info!(target:"test", "await message dropped at {id1}");
    wait_for_message_dropped(&mut pm1_ev).await;
}

// test dropping behavior for duplicate messages
#[tokio::test]
async fn test_dropping_duplicate_messages() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start three nodes");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "connect nodes in a line");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id1.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id1.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    // capture event streams before pinging
    let mut pm0_ev = pm0.events.from_now();
    let mut pm1_ev = pm1.events.from_now();
    let mut pm2_ev = pm2.events.from_now();

    // Send two identical messages. One will be dropped, because the delay between them was less than 50ms.
    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 0, id2.clone()).await;
    tracing::info!(target:"test", "await ping at {id2}");
    wait_for_ping(&mut pm2_ev, Ping { nonce: 0, source: id0.clone() }).await;
    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 0, source: id2.clone() }).await;

    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 0, id2.clone()).await;
    tracing::info!(target:"test", "await message dropped at {id1}");
    wait_for_message_dropped(&mut pm1_ev).await;

    // Send two identical messages but with 300ms delay so they don't get dropped.
    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 1, id2.clone()).await;
    tracing::info!(target:"test", "await ping at {id2}");
    wait_for_ping(&mut pm2_ev, Ping { nonce: 1, source: id0.clone() }).await;
    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 1, source: id2.clone() }).await;

    clock.advance(DROP_DUPLICATED_MESSAGES_PERIOD + time::Duration::milliseconds(1));

    tracing::info!(target:"test", "send ping from {id0} to {id2}");
    pm0.send_ping(&clock.clock(), 1, id2.clone()).await;
    tracing::info!(target:"test", "await ping at {id2}");
    wait_for_ping(&mut pm2_ev, Ping { nonce: 1, source: id0.clone() }).await;
    tracing::info!(target:"test", "await pong at {id0}");
    wait_for_pong(&mut pm0_ev, Pong { nonce: 1, source: id2.clone() }).await;
}

/// Awaits until a ConnectionClosed event with the expected reason is seen in the event stream.
/// This helper function should be used in tests with peer manager instances with
/// `config.outbound_enabled = true`, because it makes the order of spawning connections
/// non-deterministic, so we cannot just wait for the first ConnectionClosed event.
pub(crate) async fn wait_for_connection_closed(
    events: &mut broadcast::Receiver<Event>,
    want_reason: ClosingReason,
) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed(ConnectionClosedEvent {
                stream_id: _,
                reason,
            })) => {
                if reason == want_reason {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
        .await;
}

// Constructs NetworkConfigs for num_nodes nodes, the first num_boot_nodes of which
// are configured as boot nodes for all nodes.
fn make_configs(
    chain: &data::Chain,
    rng: &mut Rng,
    num_nodes: usize,
    num_boot_nodes: usize,
    enable_outbound: bool,
) -> Vec<NetworkConfig> {
    let mut cfgs: Vec<_> = (0..num_nodes).map(|_i| chain.make_config(rng)).collect();
    let boot_nodes: Vec<_> = cfgs[0..num_boot_nodes]
        .iter()
        .map(|c| PeerInfo {
            id: c.node_id(),
            addr: c.node_addr.as_ref().map(|a| **a),
            account_id: None,
        })
        .collect();
    for config in cfgs.iter_mut() {
        config.outbound_disabled = !enable_outbound;
        config.peer_store.boot_nodes = boot_nodes.clone();
    }
    cfgs
}

// test bootstrapping a two-node network with one boot node
#[tokio::test]
async fn from_boot_nodes() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes");
    let cfgs = make_configs(&chain, rng, 2, 1, true);
    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[(id1.clone(), vec![id1.clone()])]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[(id0.clone(), vec![id0.clone()])]).await;
}

// test node 0 blacklisting node 1
#[tokio::test]
async fn blacklist_01() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes with 0 blacklisting 1");
    let mut cfgs = make_configs(&chain, rng, 2, 2, true);
    cfgs[0].peer_store.blacklist =
        [blacklist::Entry::from_addr(**cfgs[1].node_addr.as_ref().unwrap())].into_iter().collect();

    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "wait for the connection to be attempted and rejected");
    wait_for_connection_closed(
        &mut pm0.events.clone(),
        ClosingReason::RejectedByPeerManager(RegisterPeerError::Blacklisted),
    )
    .await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[]).await;
}

// test node 1 blacklisting node 0
#[tokio::test]
async fn blacklist_10() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes with 1 blacklisting 0");
    let mut cfgs = make_configs(&chain, rng, 2, 2, true);
    cfgs[1].peer_store.blacklist =
        [blacklist::Entry::from_addr(**cfgs[0].node_addr.as_ref().unwrap())].into_iter().collect();

    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "wait for the connection to be attempted and rejected");
    wait_for_connection_closed(
        &mut pm1.events.clone(),
        ClosingReason::RejectedByPeerManager(RegisterPeerError::Blacklisted),
    )
    .await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[]).await;
}

// test node 0 blacklisting all nodes
#[tokio::test]
async fn blacklist_all() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start two nodes with 0 blacklisting everything");
    let mut cfgs = make_configs(&chain, rng, 2, 2, true);
    cfgs[0].peer_store.blacklist =
        [blacklist::Entry::from_ip(Ipv6Addr::LOCALHOST.into())].into_iter().collect();

    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();

    tracing::info!(target:"test", "wait for the connection to be attempted and rejected");
    wait_for_connection_closed(
        &mut pm0.events.clone(),
        ClosingReason::RejectedByPeerManager(RegisterPeerError::Blacklisted),
    )
    .await;

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[]).await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[]).await;
}

// Spawn 3 nodes with max peers configured to 2, then allow them to connect to each other in a triangle.
// Spawn a fourth node and see it fail to connect since the first three are at max capacity.
#[tokio::test]
async fn max_num_peers_limit() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "start three nodes with max_num_peers=2");
    let mut cfgs = make_configs(&chain, rng, 4, 4, true);
    for config in cfgs.iter_mut() {
        config.max_num_peers = 2;
        config.ideal_connections_lo = 2;
        config.ideal_connections_hi = 2;
    }

    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), cfgs[2].clone(), chain.clone()).await;

    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id1} routing table");
    pm1.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id2.clone(), vec![id2.clone()]),
    ])
    .await;
    tracing::info!(target:"test", "wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id0.clone(), vec![id0.clone()]),
        (id1.clone(), vec![id1.clone()]),
    ])
    .await;

    let mut pm0_ev = pm0.events.from_now();
    let mut pm1_ev = pm1.events.from_now();
    let mut pm2_ev = pm2.events.from_now();

    tracing::info!(target:"test", "start a fourth node");
    let pm3 = start_pm(clock.clock(), TestDB::new(), cfgs[3].clone(), chain.clone()).await;

    let id3 = pm3.cfg.node_id();

    tracing::info!(target:"test", "wait for {id0} to reject attempted connection");
    wait_for_connection_closed(
        &mut pm0_ev,
        ClosingReason::RejectedByPeerManager(RegisterPeerError::ConnectionLimitExceeded),
    )
    .await;
    tracing::info!(target:"test", "wait for {id1} to reject attempted connection");
    wait_for_connection_closed(
        &mut pm1_ev,
        ClosingReason::RejectedByPeerManager(RegisterPeerError::ConnectionLimitExceeded),
    )
    .await;
    tracing::info!(target:"test", "wait for {id2} to reject attempted connection");
    wait_for_connection_closed(
        &mut pm2_ev,
        ClosingReason::RejectedByPeerManager(RegisterPeerError::ConnectionLimitExceeded),
    )
    .await;

    tracing::info!(target:"test", "wait for {id3} routing table");
    pm3.wait_for_routing_table(&[]).await;

    // These drop() calls fix the place at which we want the values to be dropped,
    // so that they live long enough for the test to succeed.
    // AFAIU (gprusak) NLL (https://blog.rust-lang.org/2022/08/05/nll-by-default.html)
    // this is NOT strictly necessary, as the destructors of these values should
    // be called exactly at the end of the function anyway
    // (unless the problem is even deeper: for example the compiler incorrectly
    // figures out that it can reorder some instructions). However, I add those every time
    // I observe some flakiness that I can't reproduce, just to eliminate the possibility that
    // I just don't understand how NLL works.
    //
    // The proper solution would be to:
    // 1. Make nearcore presubmit run tests with "[panic=abort]" (which is not supported with
    //    "cargo test"), so that the test actually stop if some thread panic.
    // 2. Set some timeout on the test execution (with an enourmous heardoom), so that we actually get some logs
    //    if the presubmit times out (also not supported by "cargo test").
    drop(pm0);
    drop(pm1);
    drop(pm2);
    drop(pm3);
}

/// Test that TTL is handled properly.
#[tokio::test]
async fn ttl() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let mut pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        force_encoding: Some(Encoding::Proto),
    };
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;
    pm.wait_for_routing_table(&[(peer.cfg.id(), vec![peer.cfg.id()])]).await;

    for ttl in 0..5 {
        let msg = RoutedMessageBody::Ping(Ping { nonce: rng.gen(), source: peer.cfg.id() });
        let msg = Box::new(peer.routed_message(msg, peer.cfg.id(), ttl, Some(clock.now_utc())));
        peer.send(PeerMessage::Routed(msg.clone())).await;
        // If TTL is <2, then the message will be dropped (at least 2 hops are required).
        if ttl < 2 {
            pm.events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::RoutedMessageDropped) => Some(()),
                    _ => None,
                })
                .await;
        } else {
            let got = peer
                .events
                .recv_until(|ev| match ev {
                    peer::testonly::Event::Network(PME::MessageProcessed(
                        tcp::Tier::T2,
                        PeerMessage::Routed(msg),
                    )) => Some(msg),
                    _ => None,
                })
                .await;
            assert_eq!(msg.body, got.body);
            assert_eq!(msg.ttl - 1, got.ttl);
        }
    }
}

/// After the initial exchange, all subsequent SyncRoutingTable messages are
/// expected to contain only the diff of the known data.
#[tokio::test]
async fn repeated_data_in_sync_routing_table() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let pm = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    let cfg = peer::testonly::PeerConfig {
        network: chain.make_config(rng),
        chain,
        force_encoding: Some(Encoding::Proto),
    };
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T2).await.unwrap();
    let mut peer = peer::testonly::PeerHandle::start_endpoint(clock.clock(), cfg, stream).await;
    peer.complete_handshake().await;

    let mut edges_got = HashSet::new();
    let mut edges_want = HashSet::new();
    let mut accounts_got = HashSet::new();
    let mut accounts_want = HashSet::new();
    edges_want.insert(peer.edge.clone().unwrap());

    // Gradually increment the amount of data in the system and then broadcast it.
    for i in 0..10 {
        tracing::info!(target: "test", "iteration {i}");
        // Wait for the new data to be broadcasted.
        // Note that in the first iteration we expect just 1 edge, without sending anything before.
        // It is important because the first SyncRoutingTable contains snapshot of all data known to
        // the node (not just the diff), so we expect incremental behavior only after the first
        // SyncRoutingTable.
        while edges_got != edges_want || accounts_got != accounts_want {
            match peer.events.recv().await {
                peer::testonly::Event::Network(PME::MessageProcessed(
                    tcp::Tier::T2,
                    PeerMessage::SyncRoutingTable(got),
                )) => {
                    for a in got.accounts {
                        assert!(!accounts_got.contains(&a), "repeated broadcast: {a:?}");
                        assert!(accounts_want.contains(&a), "unexpected broadcast: {a:?}");
                        accounts_got.insert(a);
                    }
                    for e in got.edges {
                        // TODO(gprusak): Currently there is a race condition between
                        // initial full sync and broadcasting the new connection edge,
                        // which may cause the new connection edge to be broadcasted twice.
                        // Synchronize those 2 events, so that behavior here is deterministic.
                        if &e != peer.edge.as_ref().unwrap() {
                            assert!(!edges_got.contains(&e), "repeated broadcast: {e:?}");
                        }
                        assert!(edges_want.contains(&e), "unexpected broadcast: {e:?}");
                        edges_got.insert(e);
                    }
                }
                // Ignore other messages.
                _ => {}
            }
        }
        // Add more data.
        let key = data::make_secret_key(rng);
        edges_want.insert(data::make_edge(&peer.cfg.network.node_key, &key, 1));
        accounts_want.insert(data::make_announce_account(rng));
        // Send all the data created so far. PeerManager is expected to discard the duplicates.
        peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: edges_want.iter().cloned().collect(),
            accounts: accounts_want.iter().cloned().collect(),
        }))
        .await;
    }
}

/// Awaits for SyncRoutingTable messages until all edges from `want` arrive.
/// Panics if any other edges arrive.
async fn wait_for_edges(
    mut events: broadcast::Receiver<peer::testonly::Event>,
    want: &HashSet<Edge>,
) {
    let mut got = HashSet::new();
    tracing::info!(target: "test", "want edges: {:?}",want.iter().map(|e|e.hash()).collect::<Vec<_>>());
    while &got != want {
        match events.recv().await {
            peer::testonly::Event::Network(PME::MessageProcessed(
                tcp::Tier::T2,
                PeerMessage::SyncRoutingTable(msg),
            )) => {
                tracing::info!(target: "test", "got edges: {:?}",msg.edges.iter().map(|e|e.hash()).collect::<Vec<_>>());
                got.extend(msg.edges);
                assert!(want.is_superset(&got), "want: {:#?}, got: {:#?}", want, got);
            }
            // Ignore other messages.
            _ => {}
        }
    }
}

// After each handshake a full sync of routing table is performed with the peer.
// After a restart, some edges reside in storage. The node shouldn't broadcast
// edges which it learned about before the restart.
#[tokio::test]
async fn no_edge_broadcast_after_restart() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let make_edges = |rng: &mut Rng| {
        vec![
            data::make_edge(&data::make_secret_key(rng), &data::make_secret_key(rng), 1),
            data::make_edge(&data::make_secret_key(rng), &data::make_secret_key(rng), 1),
            data::make_edge_tombstone(&data::make_secret_key(rng), &data::make_secret_key(rng)),
        ]
    };

    // Create a bunch of fresh unreachable edges, then send all the edges created so far.
    let stored_edges = make_edges(rng);

    // We are preparing the initial storage by hand (rather than simulating the restart),
    // because semantics of the RoutingTable protocol are very poorly defined, and it
    // is hard to write a solid test for it without literally assuming the implementation details.
    let store = near_store::db::TestDB::new();
    {
        let mut stored_peers = HashSet::new();
        for e in &stored_edges {
            stored_peers.insert(e.key().0.clone());
            stored_peers.insert(e.key().1.clone());
        }
        let mut store: store::Store = store.clone().into();
        store.push_component(&stored_peers, &stored_edges).unwrap();
    }

    // Start a PeerManager and connect a peer to it.
    let pm =
        peer_manager::testonly::start(clock.clock(), store, chain.make_config(rng), chain.clone())
            .await;
    let peer = pm
        .start_inbound(chain.clone(), chain.make_config(rng))
        .await
        .handshake(&clock.clock())
        .await;
    tracing::info!(target:"test","pm = {}",pm.cfg.node_id());
    tracing::info!(target:"test","peer = {}",peer.cfg.id());
    // Wait for the initial sync, which will contain just 1 edge.
    // Only incremental sync are guaranteed to not contain already known edges.
    wait_for_edges(peer.events.clone(), &[peer.edge.clone().unwrap()].into()).await;

    let fresh_edges = make_edges(rng);
    let mut total_edges = stored_edges.clone();
    total_edges.extend(fresh_edges.iter().cloned());
    let events = peer.events.from_now();
    peer.send(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
        edges: total_edges,
        accounts: vec![],
    }))
    .await;

    // Wait for the fresh edges to be broadcasted back.
    tracing::info!(target: "test", "wait_for_edges(<fresh edges>)");
    wait_for_edges(events, &fresh_edges.into_iter().collect()).await;
}

#[tokio::test]
async fn square() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    tracing::info!(target:"test", "connect 4 nodes in a square");
    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;
    pm1.connect_to(&pm2.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;
    pm3.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    let id0 = pm0.cfg.node_id();
    let id1 = pm1.cfg.node_id();
    let id2 = pm2.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    pm0.wait_for_routing_table(&[
        (id1.clone(), vec![id1.clone()]),
        (id3.clone(), vec![id3.clone()]),
        (id2.clone(), vec![id1.clone(), id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","stop {id1}");
    drop(pm1);
    tracing::info!(target:"test","wait for {id0} routing table");
    pm0.wait_for_routing_table(&[
        (id3.clone(), vec![id3.clone()]),
        (id2.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","wait for {id2} routing table");
    pm2.wait_for_routing_table(&[
        (id3.clone(), vec![id3.clone()]),
        (id0.clone(), vec![id3.clone()]),
    ])
    .await;
    tracing::info!(target:"test","wait for {id3} routing table");
    pm3.wait_for_routing_table(&[
        (id2.clone(), vec![id2.clone()]),
        (id0.clone(), vec![id0.clone()]),
    ])
    .await;
    drop(pm0);
    drop(pm2);
    drop(pm3);
}

#[tokio::test]
async fn fix_local_edges() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let conn = pm
        .start_inbound(chain.clone(), chain.make_config(rng))
        .await
        .handshake(&clock.clock())
        .await;
    // TODO(gprusak): the case when the edge is updated via UpdateNondeRequest is not covered yet,
    // as it requires awaiting for the RPC roundtrip which is not implemented yet.
    let edge1 = data::make_edge(&pm.cfg.node_key, &data::make_secret_key(rng), 1);
    let edge2 = conn
        .edge
        .as_ref()
        .unwrap()
        .remove_edge(conn.cfg.network.node_id(), &conn.cfg.network.node_key);
    let msg = PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(vec![
        edge1.clone(),
        edge2.clone(),
    ]));

    tracing::info!(target:"test", "waiting for fake edges to be processed");
    let mut events = pm.events.from_now();
    conn.send(msg.clone()).await;
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::MessageProcessed(tcp::Tier::T2, got)) if got == msg => Some(()),
            _ => None,
        })
        .await;

    tracing::info!(target:"test","waiting for fake edges to be fixed");
    let mut events = pm.events.from_now();
    pm.fix_local_edges(&clock.clock(), time::Duration::ZERO).await;
    // TODO(gprusak): make fix_local_edges await closing of the connections, so
    // that we don't have to wait for it explicitly here.
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed { .. }) => Some(()),
            _ => None,
        })
        .await;

    tracing::info!(target:"test","checking the consistency");
    pm.check_consistency().await;
    drop(conn);
}

#[tokio::test]
async fn do_not_block_announce_account_broadcast() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let db0 = TestDB::new();
    let db1 = TestDB::new();
    let aa = data::make_announce_account(rng);

    tracing::info!(target:"test", "spawn 2 nodes and announce the account.");
    let pm0 = start_pm(clock.clock(), db0.clone(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), db1.clone(), chain.make_config(rng), chain.clone()).await;
    pm1.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm1.announce_account(aa.clone()).await;
    assert_eq!(&aa.peer_id, &pm0.wait_for_account_owner(&aa.account_id).await);
    drop(pm0);
    drop(pm1);

    tracing::info!(target:"test", "spawn 3 nodes and re-announce the account.");
    // Even though the account was previously announced (pm0 and pm1 have it in DB),
    // the nodes should allow to let the broadcast through.
    let pm0 = start_pm(clock.clock(), db0, chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), db1, chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    pm1.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm2.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm1.announce_account(aa.clone()).await;
    assert_eq!(&aa.peer_id, &pm2.wait_for_account_owner(&aa.account_id).await);
}

/// Check that two archival nodes keep connected after network rebalance. Nodes 0 and 1 are archival nodes, others aren't.
/// Initially connect 2, 3, 4 to 0. Then connect 1 to 0, this connection should persist, even after other nodes tries
/// to connect to node 0 again.
///
/// Do four rounds where 2, 3, 4 tries to connect to 0 and check that connection between 0 and 1 was never dropped.
#[tokio::test]
async fn archival_node() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut cfgs = make_configs(&chain, rng, 5, 5, false);
    for config in cfgs.iter_mut() {
        config.max_num_peers = 3;
        config.ideal_connections_lo = 2;
        config.ideal_connections_hi = 2;
        config.safe_set_size = 1;
        config.minimum_outbound_peers = 0;
    }
    cfgs[0].archive = true;
    cfgs[1].archive = true;

    tracing::info!(target:"test", "start five nodes, the first two of which are archival nodes");
    let pm0 = start_pm(clock.clock(), TestDB::new(), cfgs[0].clone(), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), cfgs[1].clone(), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), cfgs[2].clone(), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), cfgs[3].clone(), chain.clone()).await;
    let pm4 = start_pm(clock.clock(), TestDB::new(), cfgs[4].clone(), chain.clone()).await;

    let id1 = pm1.cfg.node_id();

    // capture pm0 event stream
    let mut pm0_ev = pm0.events.from_now();

    tracing::info!(target:"test", "connect node 2 to node 0");
    pm2.send_outbound_connect(&pm0.peer_info(), tcp::Tier::T2).await;
    tracing::info!(target:"test", "connect node 3 to node 0");
    pm3.send_outbound_connect(&pm0.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "connect node 4 to node 0 and wait for pm0 to close a connection");
    pm4.send_outbound_connect(&pm0.peer_info(), tcp::Tier::T2).await;
    wait_for_connection_closed(&mut pm0_ev, ClosingReason::PeerManagerRequest).await;

    tracing::info!(target:"test", "connect node 1 to node 0 and wait for pm0 to close a connection");
    pm1.send_outbound_connect(&pm0.peer_info(), tcp::Tier::T2).await;
    wait_for_connection_closed(&mut pm0_ev, ClosingReason::PeerManagerRequest).await;

    tracing::info!(target:"test", "check that node 0 and node 1 are still connected");
    pm0.wait_for_direct_connection(id1.clone()).await;

    for _step in 0..10 {
        tracing::info!(target:"test", "[{_step}] select a node which node 0 is not connected to");
        let pm0_connections: HashSet<PeerId> =
            pm0.with_state(|s| async move { s.tier2.load().ready.keys().cloned().collect() }).await;

        let pms = vec![&pm2, &pm3, &pm4];
        let chosen = pms
            .iter()
            .filter(|&pm| !pm0_connections.contains(&pm.cfg.node_id()))
            .choose(rng)
            .unwrap();

        tracing::info!(target:"test", "[{_step}] wait for the chosen node to finish disconnecting from node 0");
        chosen.wait_for_num_connected_peers(0).await;

        tracing::info!(target:"test", "[{_step}] connect the chosen node to node 0 and wait for pm0 to close a connection");
        chosen.send_outbound_connect(&pm0.peer_info(), tcp::Tier::T2).await;
        wait_for_connection_closed(&mut pm0_ev, ClosingReason::PeerManagerRequest).await;

        tracing::info!(target:"test", "[{_step}] check that node 0 and node 1 are still connected");
        pm0.wait_for_direct_connection(id1.clone()).await;
    }

    drop(pm0);
    drop(pm1);
    drop(pm2);
    drop(pm3);
    drop(pm4);
}

/// Awaits for ConnectionClosed event for a given `stream_id`.
async fn wait_for_stream_closed(
    events: &mut broadcast::Receiver<Event>,
    stream_id: tcp::StreamId,
) -> ClosingReason {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                Some(ev.reason)
            }
            _ => None,
        })
        .await
}

/// Check two peers are able to connect again after one peers is banned and unbanned.
#[tokio::test]
async fn connect_to_unbanned_peer() {
    abort_on_panic();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut pm0 =
        start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let mut pm1 =
        start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    tracing::info!(target:"test", "pm0 connects to pm1");
    let stream_id = pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    tracing::info!(target:"test", "pm1 bans pm0");
    let ban_reason = ReasonForBan::BadBlock;
    pm1.disconnect_and_ban(&clock.clock(), &pm0.cfg.node_id(), ban_reason).await;
    wait_for_stream_closed(&mut pm0.events, stream_id).await;
    assert_eq!(
        ClosingReason::Ban(ban_reason),
        wait_for_stream_closed(&mut pm1.events, stream_id).await
    );

    tracing::info!(target:"test", "pm0 fails to reconnect to pm1");
    let got_reason = pm1
        .start_inbound(chain.clone(), pm0.cfg.clone())
        .await
        .manager_fail_handshake(&clock.clock())
        .await;
    assert_eq!(ClosingReason::RejectedByPeerManager(RegisterPeerError::Banned), got_reason);

    tracing::info!(target:"test", "pm1 unbans pm0");
    clock.advance(pm1.cfg.peer_store.ban_window);
    pm1.peer_store_update(&clock.clock()).await;

    tracing::info!(target:"test", "pm0 reconnects to pm1");
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    drop(pm0);
    drop(pm1);
}

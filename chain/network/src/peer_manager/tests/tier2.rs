use crate::broadcast;
use crate::network_protocol::testonly as data;
use crate::peer_manager::network_state::RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::peer_manager_actor::UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL;
use crate::peer_manager::testonly::start as start_pm;
use crate::peer_manager::testonly::Event;
use crate::tcp;
use crate::testonly::make_rng;
use crate::time;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use near_store::db::TestDB;
use std::sync::Arc;

pub async fn wait_for_recent_outbound_connections(
    events: &mut broadcast::Receiver<Event>,
    wanted: Vec<PeerId>,
) {
    events
        .recv_until(|ev| match ev {
            Event::PeerManager(PME::RecentOutboundConnectionsUpdated(updated)) => {
                let got: Vec<PeerId> = updated.iter().map(|c| c.peer_info.id.clone()).collect();
                if got == wanted {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
        .await;
}

#[tokio::test]
async fn test_recent_outbound_connections() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm0 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm1 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm2 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;
    let pm3 = start_pm(clock.clock(), TestDB::new(), chain.make_config(rng), chain.clone()).await;

    let id1 = pm1.cfg.node_id();
    let id3 = pm3.cfg.node_id();

    let mut pm0_ev = pm0.events.from_now();

    // connect pm0 to pm1
    pm0.connect_to(&pm1.peer_info(), tcp::Tier::T2).await;

    // check that the outbound connection to pm1 is stored
    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);
    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);
    wait_for_recent_outbound_connections(&mut pm0_ev, vec![id1.clone()]).await;

    // connect pm2 to pm0, then pm0 to pm3
    pm2.connect_to(&pm0.peer_info(), tcp::Tier::T2).await;
    pm0.connect_to(&pm3.peer_info(), tcp::Tier::T2).await;
    clock.advance(RECENT_OUTBOUND_CONNECTIONS_MIN_DURATION);

    // disconnect pm1 from pm0
    drop(pm1);

    clock.advance(UPDATE_RECENT_OUTBOUND_CONNECTIONS_INTERVAL);
    // expect the two outbound connections, with the disconnected one appearing second
    wait_for_recent_outbound_connections(&mut pm0_ev, vec![id3.clone(), id1.clone()]).await;

    drop(pm0);
    drop(pm2);
    drop(pm3);
}

use crate::network_protocol::testonly as data;
use crate::peer_manager;
use crate::testonly::fake_client::Event as CE;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::testonly::{assert_is_superset, make_rng, Rng, AsSet as _};
use near_primitives::block_header::{ApprovalMessage,ApprovalInner,Approval};
use near_primitives::validator_signer::{ValidatorSigner};
use rand::{Rng as _};
use crate::time;
use near_o11y::testonly::init_test_logger;
use crate::types::{NetworkRequests,NetworkResponses,PeerManagerMessageRequest};
use std::collections::HashSet;
use std::sync::Arc;

fn make_block_approval(rng: &mut Rng, signer: &dyn ValidatorSigner) -> Approval {
    let inner = ApprovalInner::Endorsement(data::make_hash(rng));
    let target_height = rng.gen_range(0..100000);
    Approval{
        signature: signer.sign_approval(&inner,target_height),
        account_id: signer.validator_id().clone(),
        target_height,
        inner,
    } 
}

#[tokio::test]
async fn direct_connections() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let mut pms = vec![];
    for _ in 0..5 {
        pms.push(
            peer_manager::testonly::start(
                clock.clock(),
                near_store::db::TestDB::new(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }

    // Connect peers serially.
    let peer_infos: Vec<_> = pms.iter().map(|pm| pm.peer_info()).collect();
    for i in 0..pms.len() - 1 {
        pms[i].connect_to(&peer_infos[i + 1]).await;
    }

    // Construct ChainInfo with tier1_accounts containing all validators.
    let e = data::make_epoch_id(rng);
    let vs: Vec<_> = pms.iter().map(|pm| pm.cfg.validator.clone().unwrap()).collect();
    let mut chain_info = chain.get_chain_info();
    chain_info.tier1_accounts = Arc::new(
        vs.iter()
            .map(|v| ((e.clone(), v.signer.validator_id().clone()), v.signer.public_key()))
            .collect(),
    );
    let want = vs.iter().map(|v| super::peer_account_data(&e, v)).collect();
    // Send it to all peers.
    for pm in &mut pms {
        pm.set_chain_info(chain_info.clone()).await;
    }
    // Wait for accounts data to propagate.
    for pm in &mut pms {
        pm.wait_for_accounts_data(&want).await;
    }
    let ids: Vec<_> = pms.iter().map(|x| x.cfg.node_id()).collect();
    // Establish TIER1 connections.
    for pm in &mut pms {
        tracing::debug!(target: "test", "starting TIER1 connections from {}",pm.cfg.node_id());
        let mut events = pm.events.from_now();
        let clock = clock.clock();
        let ids = ids.clone();
        pm.with_state(|s| async move {
            // Start the connections.
            let ids = ids.as_set();
            s.tier1_daemon_tick(&clock, s.config.features.tier1.as_ref().unwrap()).await;
            // Wait for all the connections to be established.
            loop {
                let tier1 = s.tier1.load();
                let mut got: HashSet<_> = tier1.ready.keys().collect();
                let id = s.config.node_id();
                got.insert(&id);    
                assert_is_superset(&ids, &got);
                if ids == got {
                    break;
                }
                events
                    .recv_until(|ev| match ev {
                        Event::PeerManager(PME::HandshakeCompleted(_)) => Some(()),
                        _ => None,
                    })
                    .await;
            }
        })
        .await;
    }
    // Send a message over each connection.
    for from in &pms {
        let from_signer = from.cfg.validator.as_ref().unwrap().signer.clone();
        for to in &pms {
            let to_signer = to.cfg.validator.as_ref().unwrap().signer.clone();
            let target = to_signer.validator_id().clone();
            let want = make_block_approval(rng,from_signer.as_ref());
            let req = NetworkRequests::Approval {
                approval_message: ApprovalMessage {approval: want.clone(), target},
            };
            let mut events = to.events.from_now();
            let resp = from.actix.addr.send(PeerManagerMessageRequest::NetworkRequests(req)).await.unwrap();
            assert_eq!(NetworkResponses::NoResponse,resp.as_network_response());
            tracing::debug!(target:"test", "awaiting message {} -> {}",from.cfg.node_id(),to.cfg.node_id());
            let got = events.recv_until(|ev| match ev {
                Event::Client(CE::BlockApproval(got, peer_id)) if peer_id == from.cfg.node_id() => Some(got),
                _ => None,
            }).await;
            tracing::debug!(target:"test", "got {} -> {}",from.cfg.node_id(),to.cfg.node_id());
            assert_eq!(want,got);
        }
    }
    drop(pms);
}

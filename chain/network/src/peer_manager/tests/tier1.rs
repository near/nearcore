use crate::network_protocol::testonly as data;
use crate::network_protocol::{RoutedMessageBody,PeerMessage,PeerAddr};
use crate::config;
use crate::peer_manager;
use crate::tcp;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::peer_manager::testonly::Event;
use crate::testonly::{make_rng, Rng};
use crate::time;
use crate::types::{NetworkRequests, NetworkResponses, PeerManagerMessageRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::block_header::{Approval, ApprovalInner, ApprovalMessage};
use near_primitives::validator_signer::ValidatorSigner;
use rand::Rng as _;
use std::sync::Arc;

/// Constructs a random TIER1 message.
fn make_block_approval(rng: &mut Rng, signer: &dyn ValidatorSigner) -> Approval {
    let inner = ApprovalInner::Endorsement(data::make_hash(rng));
    let target_height = rng.gen_range(0..100000);
    Approval {
        signature: signer.sign_approval(&inner, target_height),
        account_id: signer.validator_id().clone(),
        target_height,
        inner,
    }
}

async fn propagate_accounts_data(
    clock: &time::Clock,
    rng: &mut Rng,
    chain: &data::Chain,
    validators: &[&peer_manager::testonly::ActorHandler],
    all: &[&peer_manager::testonly::ActorHandler],
) {
    // Construct ChainInfo with tier1_accounts set to `validators`.
    let e = data::make_epoch_id(rng);
    let vs: Vec<_> = validators.iter().map(|pm| pm.cfg.validator.clone().unwrap()).collect();
    let account_keys = Arc::new(
        vs.iter()
            .map(|v| ((e.clone(), v.signer.validator_id().clone()), v.signer.public_key()))
            .collect(),
    );
    let mut chain_info = chain.get_chain_info();
    chain_info.tier1_accounts = account_keys;
    
    // Send it to all peers.
    for pm in all {
        pm.set_chain_info(clock,chain_info.clone()).await;
    }
    let want = vs.iter().map(|v| super::peer_account_data(&e, v)).collect();
    // Wait for accounts data to propagate.
    for pm in all {
        pm.wait_for_accounts_data(&want).await;
    }
}

async fn test_clique(clock: &time::Clock, rng: &mut Rng, pms: &[&peer_manager::testonly::ActorHandler]) {
    // Establish TIER1 connections.
    for pm in pms {
        pm.establish_tier1_connections(clock).await;
    }
    // Send a message over each connection.
    for from in pms {
        let from_signer = from.cfg.validator.as_ref().unwrap().signer.clone();
        for to in pms {
            if from.cfg.node_id() == to.cfg.node_id() {
                continue;
            }
            let to_signer = to.cfg.validator.as_ref().unwrap().signer.clone();
            let target = to_signer.validator_id().clone();
            let want = make_block_approval(rng, from_signer.as_ref());
            let req = NetworkRequests::Approval {
                approval_message: ApprovalMessage { approval: want.clone(), target },
            };
            let mut events = to.events.from_now();
            let resp = from
                .actix
                .addr
                .send(PeerManagerMessageRequest::NetworkRequests(req))
                .await
                .unwrap();
            assert_eq!(NetworkResponses::NoResponse, resp.as_network_response());
            tracing::debug!(target:"test", "awaiting message {} -> {}",from.cfg.node_id(),to.cfg.node_id());
            let got = events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::MessageProcessed(
                        tcp::Tier::T1,
                        PeerMessage::Routed(got),
                    )) => Some(got),
                    _ => None,
                })
                .await;
            tracing::debug!(target:"test", "got {} -> {}",from.cfg.node_id(),to.cfg.node_id());
            assert_eq!(from.cfg.node_id(), got.author);
            assert_eq!(RoutedMessageBody::BlockApproval(want), got.body);
        }
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
    let pms : Vec<_> = pms.iter().collect();

    // Connect peers serially.
    let peer_infos: Vec<_> = pms.iter().map(|pm| pm.peer_info()).collect();
    for i in 0..pms.len() - 1 {
        pms[i].connect_to(&peer_infos[i + 1]).await;
    }

    propagate_accounts_data(&clock.clock(),rng,&chain,&pms[..],&pms[..]).await;
    test_clique(&clock.clock(),rng,&pms[..]).await;
}


#[tokio::test]
async fn proxy_connections() {
    init_test_logger();
    let mut rng = make_rng(921853233);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    const N : usize = 5;

    let mut proxies = vec![];
    for _ in 0..N {
        proxies.push(
            peer_manager::testonly::start(
                clock.clock(),
                near_store::db::TestDB::new(),
                chain.make_config(rng),
                chain.clone(),
            )
            .await,
        );
    }
    let proxies : Vec<_> = proxies.iter().collect();
    
    let mut validators = vec![];
    for i in 0..N {
        let mut cfg = chain.make_config(rng);
        cfg.validator.as_mut().unwrap().endpoints = config::ValidatorEndpoints::PublicAddrs(vec![
            PeerAddr{
                peer_id: proxies[i].cfg.node_id(),
                addr: proxies[i].cfg.node_addr.unwrap(),
            }
        ]);
        validators.push(
            peer_manager::testonly::start(
                clock.clock(),
                near_store::db::TestDB::new(),
                cfg,
                chain.clone(),
            )
            .await,
        );
    }
    let validators : Vec<_> = validators.iter().collect();

    // Connect validators and proxies in a star topology. Any connected graph would do.
    let hub = peer_manager::testonly::start(
        clock.clock(),
        near_store::db::TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    ).await;
    for pm in &validators {
        pm.connect_to(&hub.peer_info()).await;
    }
    for pm in &proxies {
        pm.connect_to(&hub.peer_info()).await;
    }

    let mut all = vec![];
    all.extend(validators.clone());
    all.extend(proxies.clone());
    all.push(&hub);
    propagate_accounts_data(&clock.clock(),rng,&chain,&validators[..],&all[..]).await;
    test_clique(&clock.clock(),rng,&validators[..]).await;
}

#[tokio::test]
async fn account_keys_change() {
    // TODO(gprusak)
}

#[tokio::test]
async fn proxy_change() {
    // TODO(gprusak)
}


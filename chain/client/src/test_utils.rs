use std::sync::{Arc, RwLock};

use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, AsyncContext, Context, Recipient};
use chrono::{DateTime, Utc};

use near_chain::test_utils::KeyValueRuntime;
use near_network::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerInfo, PeerManagerActor,
};
use near_primitives::crypto::signature::PublicKey;
use near_primitives::crypto::signer::InMemorySigner;
use near_store::test_utils::create_test_store;
use near_telemetry::TelemetryActor;

use crate::{BlockProducer, ClientActor, ClientConfig, ViewClientActor};
use near_chain::ChainGenesis;

use futures::future;
use futures::future::Future;
use near_network::types::{NetworkInfo, PeerChainInfo};
use std::ops::DerefMut;

pub type NetworkMock = Mocker<PeerManagerActor>;

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
pub fn setup(
    validators: Vec<Vec<&str>>,
    validator_groups: u64,
    account_id: &str,
    skip_sync_wait: bool,
    block_prod_time: u64,
    recipient: Recipient<NetworkRequests>,
    genesis_time: DateTime<Utc>,
) -> (ClientActor, ViewClientActor) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        validators.into_iter().map(|inner| inner.into_iter().map(Into::into).collect()).collect(),
        validator_groups,
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    let chain_genesis = ChainGenesis::new(genesis_time, 1_000_000);
    let telemetry = TelemetryActor::default().start();
    let view_client =
        ViewClientActor::new(store.clone(), chain_genesis.clone(), runtime.clone()).unwrap();
    let client = ClientActor::new(
        ClientConfig::test(skip_sync_wait, block_prod_time),
        store,
        chain_genesis,
        runtime,
        PublicKey::empty().into(),
        recipient,
        Some(signer.into()),
        telemetry,
    )
    .unwrap();
    (client, view_client)
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    mut network_mock: Box<
        dyn FnMut(
            &NetworkRequests,
            &mut Context<NetworkMock>,
            Addr<ClientActor>,
        ) -> NetworkResponses,
    >,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let view_client_addr = Arc::new(RwLock::new(None));
    let view_client_addr1 = view_client_addr.clone();
    let client_addr = ClientActor::create(move |ctx| {
        let client_addr = ctx.address();
        let pm = NetworkMock::mock(Box::new(move |msg, ctx| {
            let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
            let resp = network_mock(msg, ctx, client_addr.clone());
            Box::new(Some(resp))
        }))
        .start();
        let (client, view_client) =
            setup(vec![validators], 1, account_id, skip_sync_wait, 100, pm.recipient(), Utc::now());
        *view_client_addr1.write().unwrap() = Some(view_client.start());
        client
    });
    (client_addr, view_client_addr.clone().read().unwrap().clone().unwrap())
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock_all_validators(
    validators: Vec<Vec<&'static str>>,
    key_pairs: Vec<PeerInfo>,
    validator_groups: u64,
    skip_sync_wait: bool,
    block_prod_time: u64,
    network_mock: Arc<RwLock<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>,
) -> Vec<(Addr<ClientActor>, Addr<ViewClientActor>)> {
    let validators_clone = validators.clone();
    let key_pairs = key_pairs.clone();
    let genesis_time = Utc::now();
    let mut ret = vec![];

    let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
        Arc::new(RwLock::new(vec![]));

    // Lock the connectors so that none of the threads spawned below access them until we overwrite
    //    them at the end of this function
    let mut locked_connectors = connectors.write().unwrap();

    for account_id in validators.iter().flatten().cloned() {
        let view_client_addr = Arc::new(RwLock::new(None));
        let view_client_addr1 = view_client_addr.clone();
        let validators_clone1 = validators_clone.clone();
        let validators_clone2 = validators_clone.clone();
        let key_pairs = key_pairs.clone();
        let connectors1 = connectors.clone();
        let network_mock1 = network_mock.clone();
        let client_addr = ClientActor::create(move |ctx| {
            let _client_addr = ctx.address();
            let pm = NetworkMock::mock(Box::new(move |msg, _ctx| {
                let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
                let (mut resp, perform_default) =
                    network_mock1.write().unwrap().deref_mut()(account_id.to_string(), msg);

                if perform_default {
                    let mut my_key_pair = None;
                    for (i, name) in validators_clone2.iter().flatten().enumerate() {
                        if *name == account_id {
                            my_key_pair = Some(key_pairs[i].clone());
                        }
                    }
                    let my_key_pair = my_key_pair.unwrap();

                    match msg {
                        NetworkRequests::FetchInfo{ .. } => {
                            resp = NetworkResponses::Info ( NetworkInfo {
                                num_active_peers: key_pairs.len(),
                                peer_max_count: key_pairs.len() as u32,
                                most_weight_peers: key_pairs
                                    .iter()
                                    .map(|peer_info| FullPeerInfo {
                                        peer_info: peer_info.clone(),
                                        chain_info: PeerChainInfo {
                                            genesis: Default::default(),
                                            height: 5,
                                            total_weight: 100.into(),
                                        },
                                    })
                                    .collect(),
                                sent_bytes_per_sec: 0,
                                received_bytes_per_sec: 0,
                                routes: None
                            })
                        }
                        NetworkRequests::Block { block } => {
                            for (client, _) in connectors1.write().unwrap().iter() {
                                client.do_send(NetworkClientMessages::Block(
                                    block.clone(),
                                    PeerInfo::random().id,
                                    false,
                                ))
                            }
                        }
                        NetworkRequests::ChunkOnePart { account_id, header_and_part } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == account_id {
                                    connectors1.write().unwrap()[i].0.do_send(
                                        NetworkClientMessages::ChunkOnePart(
                                            header_and_part.clone(),
                                        ),
                                    );
                                }
                            }
                        }
                        NetworkRequests::ChunkPartRequest { account_id, part_request } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == account_id {
                                    connectors1.write().unwrap()[i].0.do_send(
                                        NetworkClientMessages::ChunkPartRequest(
                                            part_request.clone(),
                                            my_key_pair.id,
                                        ),
                                    );
                                }
                            }
                        }
                        NetworkRequests::ChunkOnePartRequest {
                            account_id: their_account_id,
                            part_request,
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == their_account_id {
                                    connectors1.write().unwrap()[i].0.do_send(
                                        NetworkClientMessages::ChunkOnePartRequest(
                                            part_request.clone(),
                                            account_id.to_string(),
                                        ),
                                    );
                                }
                            }
                        }
                        NetworkRequests::ChunkPart { peer_id, part } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                if peer_info.id == *peer_id {
                                    connectors1.write().unwrap()[i]
                                        .0
                                        .do_send(NetworkClientMessages::ChunkPart(part.clone()));
                                }
                            }
                        }
                        NetworkRequests::StateRequest { shard_id, hash, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                if peer_info.id == *peer_id {
                                    let connectors2 = connectors1.clone();
                                    let validators_clone3 = validators_clone2.clone();
                                    actix::spawn(
                                    connectors1.write().unwrap()[i]
                                        .0
                                        .send(NetworkClientMessages::StateRequest(*shard_id, *hash))
                                        .then(move |response| {
                                            let response = response.unwrap();
                                            match response {
                                                NetworkClientResponses::StateResponse(info) =>
                                                    {
                                                        for (i, name) in
                                                            validators_clone3.iter().flatten().enumerate()
                                                            {
                                                                if *name == account_id {
                                                                    connectors2.write().unwrap()[i].0.do_send(
                                                                        NetworkClientMessages::StateResponse(info),
                                                                    );
                                                                    break;
                                                                }
                                                            }
                                                    },
                                                NetworkClientResponses::NoResponse => {},
                                                _ => assert!(false),
                                            }
                                            future::result(Ok(()))
                                        }));
                                }
                            }
                        }
                        NetworkRequests::AnnounceAccount(announce_account) => {
                            println!("LOL {:?}", announce_account);
                        }
                        _ => {}
                    };
                }
                Box::new(Some(resp))
            }))
            .start();
            let (client, view_client) = setup(
                validators_clone1.clone(),
                validator_groups,
                account_id,
                skip_sync_wait,
                block_prod_time,
                pm.recipient(),
                genesis_time,
            );
            *view_client_addr1.write().unwrap() = Some(view_client.start());
            client
        });
        ret.push((client_addr, view_client_addr.clone().read().unwrap().clone().unwrap()));
    }
    *locked_connectors = ret.clone();
    ret
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock(
        validators,
        account_id,
        skip_sync_wait,
        Box::new(|req, _, _| match req {
            NetworkRequests::FetchInfo { .. } => NetworkResponses::Info(NetworkInfo {
                num_active_peers: 0,
                peer_max_count: 0,
                most_weight_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                routes: None,
            }),
            _ => NetworkResponses::NoResponse,
        }),
    )
}

impl BlockProducer {
    pub fn test(seed: &str) -> Self {
        Arc::new(InMemorySigner::from_seed(seed, seed)).into()
    }
}

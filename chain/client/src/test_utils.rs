use std::cmp::max;
use std::collections::{HashSet, VecDeque};
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, AsyncContext, Context, Recipient};
use chrono::{DateTime, Utc};
use futures::future;
use futures::future::Future;
use rand::{thread_rng, Rng};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Chain, ChainGenesis, Provenance, RuntimeAdapter};
use near_chunks::NetworkAdapter;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_network::routing::EdgeInfo;
use near_network::types::{NetworkInfo, PeerChainInfo};
use near_network::{
    FullPeerInfo, NetworkClientMessages, NetworkClientResponses, NetworkRequests, NetworkResponses,
    PeerInfo, PeerManagerActor,
};
use near_primitives::block::{Block, GenesisId, Weight};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockIndex, ShardId, ValidatorId};
use near_store::test_utils::create_test_store;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::{BlockProducer, Client, ClientActor, ClientConfig, ViewClientActor};

pub type NetworkMock = Mocker<PeerManagerActor>;

#[derive(Default)]
pub struct MockNetworkAdapter {
    pub requests: Arc<RwLock<VecDeque<NetworkRequests>>>,
}

impl NetworkAdapter for MockNetworkAdapter {
    fn send(&self, msg: NetworkRequests) {
        self.requests.write().unwrap().push_back(msg);
    }
}

impl MockNetworkAdapter {
    pub fn pop(&self) -> Option<NetworkRequests> {
        self.requests.write().unwrap().pop_front()
    }
}

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
pub fn setup(
    validators: Vec<Vec<&str>>,
    validator_groups: u64,
    num_shards: ShardId,
    epoch_length: u64,
    account_id: &str,
    skip_sync_wait: bool,
    block_prod_time: u64,
    recipient: Recipient<NetworkRequests>,
    tx_validity_period: BlockIndex,
    genesis_time: DateTime<Utc>,
) -> (Block, ClientActor, ViewClientActor) {
    let store = create_test_store();
    let num_validators = validators.iter().map(|x| x.len()).sum();
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        validators.into_iter().map(|inner| inner.into_iter().map(Into::into).collect()).collect(),
        validator_groups,
        num_shards,
        epoch_length,
    ));
    let chain_genesis = ChainGenesis::new(
        "unittest".to_string(),
        genesis_time,
        1_000_000,
        100,
        1_000_000_000,
        0,
        0,
        tx_validity_period,
    );

    let mut chain = Chain::new(store.clone(), runtime.clone(), &chain_genesis).unwrap();
    let genesis_block = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let signer = Arc::new(InMemorySigner::from_seed(account_id, KeyType::ED25519, account_id));
    let telemetry = TelemetryActor::default().start();
    let view_client = ViewClientActor::new(store.clone(), &chain_genesis, runtime.clone()).unwrap();
    let config = ClientConfig::test(skip_sync_wait, block_prod_time, num_validators);
    let client = ClientActor::new(
        config,
        store,
        chain_genesis,
        runtime,
        PublicKey::empty(KeyType::ED25519).into(),
        recipient,
        Some(signer.into()),
        telemetry,
    )
    .unwrap();
    (genesis_block, client, view_client)
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    network_mock: Box<
        dyn FnMut(
            &NetworkRequests,
            &mut Context<NetworkMock>,
            Addr<ClientActor>,
        ) -> NetworkResponses,
    >,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period(validators, account_id, skip_sync_wait, network_mock, 100)
}

pub fn setup_mock_with_validity_period(
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
    validity_period: BlockIndex,
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
        let (_, client, view_client) = setup(
            vec![validators],
            1,
            1,
            5,
            account_id,
            skip_sync_wait,
            100,
            pm.recipient(),
            validity_period,
            Utc::now(),
        );
        *view_client_addr1.write().unwrap() = Some(view_client.start());
        client
    });
    (client_addr, view_client_addr.clone().read().unwrap().clone().unwrap())
}

fn sample_binary(n: u64, k: u64) -> bool {
    thread_rng().gen_range(0, k) <= n
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock_all_validators(
    validators: Vec<Vec<&'static str>>,
    key_pairs: Vec<PeerInfo>,
    validator_groups: u64,
    skip_sync_wait: bool,
    block_prod_time: u64,
    drop_chunks: bool,
    epoch_length: u64,
    network_mock: Arc<RwLock<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>,
) -> (Block, Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>) {
    let validators_clone = validators.clone();
    let key_pairs = key_pairs.clone();
    let genesis_time = Utc::now();
    let mut ret = vec![];

    let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
        Arc::new(RwLock::new(vec![]));

    // Lock the connectors so that none of the threads spawned below access them until we overwrite
    //    them at the end of this function
    let mut locked_connectors = connectors.write().unwrap();

    let announced_accounts = Arc::new(RwLock::new(HashSet::new()));
    let genesis_block = Arc::new(RwLock::new(None));
    let num_shards = validators.iter().map(|x| x.len()).min().unwrap() as ShardId;

    let last_height_weight = Arc::new(RwLock::new(vec![(0, Weight::from(0)); key_pairs.len()]));

    for account_id in validators.iter().flatten().cloned() {
        let view_client_addr = Arc::new(RwLock::new(None));
        let view_client_addr1 = view_client_addr.clone();
        let validators_clone1 = validators_clone.clone();
        let validators_clone2 = validators_clone.clone();
        let genesis_block1 = genesis_block.clone();
        let key_pairs = key_pairs.clone();
        let connectors1 = connectors.clone();
        let network_mock1 = network_mock.clone();
        let announced_accounts1 = announced_accounts.clone();
        let last_height_weight1 = last_height_weight.clone();
        let client_addr = ClientActor::create(move |ctx| {
            let _client_addr = ctx.address();
            let pm = NetworkMock::mock(Box::new(move |msg, _ctx| {
                let msg = msg.downcast_ref::<NetworkRequests>().unwrap();

                let mut guard = network_mock1.write().unwrap();
                let (mut resp, perform_default) = guard.deref_mut()(account_id.to_string(), msg);
                drop(guard);

                if perform_default {
                    let mut my_key_pair = None;
                    let mut my_ord = None;
                    for (i, name) in validators_clone2.iter().flatten().enumerate() {
                        if *name == account_id {
                            my_key_pair = Some(key_pairs[i].clone());
                            my_ord = Some(i);
                        }
                    }
                    let my_key_pair = my_key_pair.unwrap();
                    let my_ord = my_ord.unwrap();
                    let my_account_id = account_id;

                    match msg {
                        NetworkRequests::FetchInfo { .. } => {
                            let last_height_weight1 = last_height_weight1.read().unwrap();
                            let peers: Vec<_> = key_pairs
                                .iter()
                                .take(connectors1.read().unwrap().len())
                                .enumerate()
                                .map(|(i, peer_info)| FullPeerInfo {
                                    peer_info: peer_info.clone(),
                                    chain_info: PeerChainInfo {
                                        genesis_id: GenesisId {
                                            chain_id: "unittest".to_string(),
                                            hash: Default::default(),
                                        },
                                        height: last_height_weight1[i].0,
                                        total_weight: last_height_weight1[i].1,
                                    },
                                    edge_info: EdgeInfo::default(),
                                })
                                .collect();
                            let peers2 = peers.clone();
                            resp = NetworkResponses::Info(NetworkInfo {
                                active_peers: peers,
                                num_active_peers: key_pairs.len(),
                                peer_max_count: key_pairs.len() as u32,
                                most_weight_peers: peers2,
                                sent_bytes_per_sec: 0,
                                received_bytes_per_sec: 0,
                                known_producers: vec![],
                            })
                        }
                        NetworkRequests::Block { block } => {
                            for (client, _) in connectors1.read().unwrap().iter() {
                                client.do_send(NetworkClientMessages::Block(
                                    block.clone(),
                                    PeerInfo::random().id,
                                    false,
                                ))
                            }

                            let mut last_height_weight1 = last_height_weight1.write().unwrap();

                            let my_height_weight = &mut last_height_weight1[my_ord];

                            my_height_weight.0 = max(my_height_weight.0, block.header.inner.height);
                            my_height_weight.1 =
                                max(my_height_weight.1, block.header.inner.total_weight);
                        }
                        NetworkRequests::ChunkPartRequest { account_id, part_request } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == account_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::ChunkPartRequest(
                                                part_request.clone(),
                                                my_key_pair.id.clone(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::ChunkOnePartRequest {
                            account_id: their_account_id,
                            one_part_request,
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == their_account_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::ChunkOnePartRequest(
                                                one_part_request.clone(),
                                                my_key_pair.id.clone(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::ChunkOnePartMessage { account_id, header_and_part } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == account_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::ChunkOnePart(
                                                header_and_part.clone(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::ChunkOnePartResponse { peer_id, header_and_part } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                if peer_info.id == *peer_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::ChunkOnePart(
                                                header_and_part.clone(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::ChunkPart { peer_id, part } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                if peer_info.id == *peer_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::ChunkPart(part.clone()),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::BlockRequest { hash, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let connectors2 = connectors1.clone();
                                    actix::spawn(
                                        connectors1.read().unwrap()[i]
                                            .0
                                            .send(NetworkClientMessages::BlockRequest(*hash))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkClientResponses::Block(block) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(NetworkClientMessages::Block(
                                                                block, peer_id, true,
                                                            ));
                                                    }
                                                    NetworkClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::result(Ok(()))
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                            for (i, peer_info) in key_pairs.iter().enumerate() {
                                let peer_id = peer_id.clone();
                                if peer_info.id == peer_id {
                                    let connectors2 = connectors1.clone();
                                    actix::spawn(
                                        connectors1.read().unwrap()[i]
                                            .0
                                            .send(NetworkClientMessages::BlockHeadersRequest(
                                                hashes.clone(),
                                            ))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkClientResponses::BlockHeaders(
                                                        headers,
                                                    ) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(
                                                                NetworkClientMessages::BlockHeaders(
                                                                    headers, peer_id,
                                                                ),
                                                            );
                                                    }
                                                    NetworkClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::result(Ok(()))
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateRequest {
                            shard_id,
                            hash,
                            need_header,
                            parts_ranges,
                            account_id: target_account_id,
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == target_account_id {
                                    let connectors2 = connectors1.clone();
                                    actix::spawn(
                                        connectors1.read().unwrap()[i]
                                            .0
                                            .send(NetworkClientMessages::StateRequest(
                                                *shard_id,
                                                *hash,
                                                *need_header,
                                                parts_ranges.to_vec(),
                                            ))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkClientResponses::StateResponse(info) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(
                                                            NetworkClientMessages::StateResponse(
                                                                info,
                                                            ),
                                                        );
                                                    }
                                                    NetworkClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::result(Ok(()))
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::AnnounceAccount(announce_account) => {
                            let mut aa = announced_accounts1.write().unwrap();
                            let key = (
                                announce_account.account_id.clone(),
                                announce_account.epoch_id.clone(),
                            );
                            if aa.get(&key).is_none() {
                                aa.insert(key);
                                for (client, _) in connectors1.read().unwrap().iter() {
                                    client.do_send(NetworkClientMessages::AnnounceAccount(vec![
                                        announce_account.clone(),
                                    ]))
                                }
                            }
                        }
                        NetworkRequests::BlockHeaderAnnounce {
                            header: _,
                            approval: Some(approval),
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == &approval.target {
                                    connectors1.read().unwrap()[i].0.do_send(
                                        NetworkClientMessages::BlockApproval(
                                            my_account_id.to_string(),
                                            approval.hash,
                                            approval.signature.clone(),
                                            my_key_pair.id.clone(),
                                        ),
                                    );
                                }
                            }
                        }
                        NetworkRequests::ForwardTx(_, _)
                        | NetworkRequests::Sync { .. }
                        | NetworkRequests::FetchRoutingTable
                        | NetworkRequests::PingTo(_, _)
                        | NetworkRequests::FetchPingPongInfo
                        | NetworkRequests::BanPeer { .. }
                        | NetworkRequests::BlockHeaderAnnounce { .. }
                        | NetworkRequests::TxStatus(_, _, _)
                        | NetworkRequests::Challenge(_) => {}
                        NetworkRequests::RequestUpdateNonce(_, _)
                        | NetworkRequests::ResponseUpdateNonce(_) => {}
                    };
                }
                Box::new(Some(resp))
            }))
            .start();
            let (block, client, view_client) = setup(
                validators_clone1.clone(),
                validator_groups,
                num_shards,
                epoch_length,
                account_id,
                skip_sync_wait,
                block_prod_time,
                pm.recipient(),
                10000,
                genesis_time,
            );
            *view_client_addr1.write().unwrap() = Some(view_client.start());
            *genesis_block1.write().unwrap() = Some(block);
            client
        });
        ret.push((client_addr, view_client_addr.clone().read().unwrap().clone().unwrap()));
    }
    *locked_connectors = ret.clone();
    let value = genesis_block.read().unwrap();
    (value.clone().unwrap(), ret)
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_no_network_with_validity_period(validators, account_id, skip_sync_wait, 100)
}

pub fn setup_no_network_with_validity_period(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    validity_period: BlockIndex,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period(
        validators,
        account_id,
        skip_sync_wait,
        Box::new(|req, _, _| match req {
            NetworkRequests::FetchInfo { .. } => NetworkResponses::Info(NetworkInfo {
                active_peers: vec![],
                num_active_peers: 0,
                peer_max_count: 0,
                most_weight_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
            }),
            _ => NetworkResponses::NoResponse,
        }),
        validity_period,
    )
}

impl BlockProducer {
    pub fn test(seed: &str) -> Self {
        Arc::new(InMemorySigner::from_seed(seed, KeyType::ED25519, seed)).into()
    }
}

pub fn setup_client_with_runtime(
    store: Arc<Store>,
    num_validators: ValidatorId,
    account_id: Option<&str>,
    network_adapter: Arc<dyn NetworkAdapter>,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
) -> Client {
    let block_producer =
        account_id.map(|x| Arc::new(InMemorySigner::from_seed(x, KeyType::ED25519, x)).into());
    let config = ClientConfig::test(true, 10, num_validators);
    Client::new(config, store, chain_genesis, runtime_adapter, network_adapter, block_producer)
        .unwrap()
}

pub fn setup_client(
    store: Arc<Store>,
    validators: Vec<Vec<&str>>,
    validator_groups: u64,
    num_shards: ShardId,
    account_id: Option<&str>,
    network_adapter: Arc<dyn NetworkAdapter>,
    chain_genesis: ChainGenesis,
) -> Client {
    let num_validators = validators.iter().map(|x| x.len()).sum();
    let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        validators.into_iter().map(|inner| inner.into_iter().map(Into::into).collect()).collect(),
        validator_groups,
        num_shards,
        5,
    ));
    setup_client_with_runtime(
        store,
        num_validators,
        account_id,
        network_adapter,
        chain_genesis,
        runtime_adapter,
    )
}

pub struct TestEnv {
    chain_genesis: ChainGenesis,
    validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockNetworkAdapter>>,
    pub clients: Vec<Client>,
}

impl TestEnv {
    pub fn new(chain_genesis: ChainGenesis, num_clients: usize, num_validators: usize) -> Self {
        let validators: Vec<AccountId> =
            (0..num_validators).map(|i| format!("test{}", i)).collect();
        let network_adapters =
            (0..num_clients).map(|_| Arc::new(MockNetworkAdapter::default())).collect::<Vec<_>>();
        let clients = (0..num_clients)
            .map(|i| {
                let store = create_test_store();
                setup_client(
                    store.clone(),
                    vec![validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
                    1,
                    1,
                    Some(&format!("test{}", i)),
                    network_adapters[i].clone(),
                    chain_genesis.clone(),
                )
            })
            .collect();
        TestEnv { chain_genesis, validators, network_adapters, clients }
    }

    pub fn new_with_runtime(
        chain_genesis: ChainGenesis,
        num_clients: usize,
        num_validators: usize,
        runtime_adapters: Vec<Arc<dyn RuntimeAdapter>>,
    ) -> Self {
        let validators: Vec<AccountId> =
            (0..num_validators).map(|i| format!("test{}", i)).collect();
        let network_adapters =
            (0..num_clients).map(|_| Arc::new(MockNetworkAdapter::default())).collect::<Vec<_>>();
        let clients = (0..num_clients)
            .map(|i| {
                let store = create_test_store();
                setup_client_with_runtime(
                    store.clone(),
                    num_validators,
                    Some(&format!("test{}", i)),
                    network_adapters[i].clone(),
                    chain_genesis.clone(),
                    runtime_adapters[i].clone(),
                )
            })
            .collect();
        TestEnv { chain_genesis, validators, network_adapters, clients }
    }

    pub fn process_block(&mut self, id: usize, block: Block, provenance: Provenance) {
        let (mut accepted_blocks, result) = self.clients[id].process_block(block, provenance);
        assert!(result.is_ok(), format!("{:?}", result));
        let more_accepted_blocks = self.clients[id].run_catchup().unwrap();
        accepted_blocks.extend(more_accepted_blocks);
        for accepted_block in accepted_blocks.into_iter() {
            self.clients[id].on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }

    pub fn produce_block(&mut self, id: usize, height: BlockIndex) {
        let block = self.clients[id].produce_block(height, Duration::from_millis(10)).unwrap();
        self.process_block(id, block.unwrap(), Provenance::PRODUCED);
    }

    pub fn send_money(&mut self, id: usize) -> NetworkClientResponses {
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        let tx = SignedTransaction::send_money(
            1,
            "test1".to_string(),
            "test1".to_string(),
            &signer,
            100,
            self.clients[id].chain.head().unwrap().last_block_hash,
        );
        self.clients[id].process_tx(tx)
    }

    pub fn restart(&mut self, id: usize) {
        let store = self.clients[id].chain.store().store().clone();
        self.clients[id] = setup_client(
            store,
            vec![self.validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
            1,
            1,
            Some(&format!("test{}", id)),
            self.network_adapters[id].clone(),
            self.chain_genesis.clone(),
        )
    }
}

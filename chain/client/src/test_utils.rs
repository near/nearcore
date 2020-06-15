use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::{Arc, RwLock};

use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, AsyncContext, Context};
use chrono::{DateTime, Utc};
use futures::{future, FutureExt};
use rand::{thread_rng, Rng};

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode, Provenance, RuntimeAdapter};
use near_chain_configs::ClientConfig;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
#[cfg(feature = "metric_recorder")]
use near_network::recorder::MetricRecorder;
use near_network::routing::EdgeInfo;
use near_network::types::{
    AccountOrPeerIdOrHash, NetworkInfo, NetworkViewClientMessages, NetworkViewClientResponses,
    PeerChainInfo,
};
use near_network::{
    FullPeerInfo, NetworkAdapter, NetworkClientMessages, NetworkClientResponses, NetworkRecipient,
    NetworkRequests, NetworkResponses, PeerInfo, PeerManagerActor,
};
use near_primitives::block::{ApprovalInner, Block, GenesisId};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, NumBlocks, NumSeats, NumShards,
};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{AccountView, QueryRequest, QueryResponseKind};
use near_store::test_utils::create_test_store;
use near_store::Store;
use near_telemetry::TelemetryActor;

use crate::{Client, ClientActor, SyncStatus, ViewClientActor};
use near_network::test_utils::MockNetworkAdapter;
use num_rational::Rational;

pub type NetworkMock = Mocker<PeerManagerActor>;

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
pub fn setup(
    validators: Vec<Vec<&str>>,
    validator_groups: u64,
    num_shards: NumShards,
    epoch_length: BlockHeightDelta,
    account_id: &str,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    network_adapter: Arc<dyn NetworkAdapter>,
    transaction_validity_period: NumBlocks,
    genesis_time: DateTime<Utc>,
) -> (Block, ClientActor, ViewClientActor) {
    let store = create_test_store();
    let num_validator_seats = validators.iter().map(|x| x.len()).sum::<usize>() as NumSeats;
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store,
        validators.into_iter().map(|inner| inner.into_iter().map(Into::into).collect()).collect(),
        validator_groups,
        num_shards,
        epoch_length,
    ));
    let chain_genesis = ChainGenesis::new(
        genesis_time,
        0,
        1_000_000,
        100,
        1_000_000_000,
        3_000_000_000_000_000_000_000_000_000_000_000,
        Rational::from_integer(0),
        Rational::from_integer(0),
        transaction_validity_period,
        epoch_length,
        PROTOCOL_VERSION,
    );
    let doomslug_threshold_mode = if enable_doomslug {
        DoomslugThresholdMode::TwoThirds
    } else {
        DoomslugThresholdMode::NoApprovals
    };
    let mut chain = Chain::new(runtime.clone(), &chain_genesis, doomslug_threshold_mode).unwrap();
    let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap().clone();

    let signer =
        Arc::new(InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, account_id));
    let telemetry = TelemetryActor::default().start();
    let config = ClientConfig::test(
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
    );
    let view_client = ViewClientActor::new(
        Some(signer.validator_id().clone()),
        &chain_genesis,
        runtime.clone(),
        network_adapter.clone(),
        config.clone(),
    )
    .unwrap();

    let client = ClientActor::new(
        config,
        chain_genesis,
        runtime,
        PublicKey::empty(KeyType::ED25519).into(),
        network_adapter,
        Some(signer),
        telemetry,
        enable_doomslug,
    )
    .unwrap();
    (genesis_block, client, view_client)
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    network_mock: Box<
        dyn FnMut(
            &NetworkRequests,
            &mut Context<NetworkMock>,
            Addr<ClientActor>,
        ) -> NetworkResponses,
    >,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period(
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        network_mock,
        100,
    )
}

pub fn setup_mock_with_validity_period(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    mut network_mock: Box<
        dyn FnMut(
            &NetworkRequests,
            &mut Context<NetworkMock>,
            Addr<ClientActor>,
        ) -> NetworkResponses,
    >,
    transaction_validity_period: NumBlocks,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let network_adapter = Arc::new(NetworkRecipient::new());
    let (_, client, view_client) = setup(
        vec![validators],
        1,
        1,
        5,
        account_id,
        skip_sync_wait,
        100,
        200,
        enable_doomslug,
        false,
        network_adapter.clone(),
        transaction_validity_period,
        Utc::now(),
    );
    let client_addr = client.start();
    let view_client_addr = view_client.start();
    let client_addr1 = client_addr.clone();

    let network_actor = NetworkMock::mock(Box::new(move |msg, ctx| {
        let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
        let resp = network_mock(msg, ctx, client_addr1.clone());
        Box::new(Some(resp))
    }))
    .start();

    network_adapter.set_recipient(network_actor.recipient());

    (client_addr, view_client_addr)
}

fn sample_binary(n: u64, k: u64) -> bool {
    thread_rng().gen_range(0, k) <= n
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
///
/// # Arguments
/// * `validators` - a vector or vector of validator names. Each vector is a set of validators for a
///                 particular epoch. E.g. if `validators` has three elements, then the each epoch
///                 with id % 3 == 0 will have the first set of validators, with id % 3 == 1 will
///                 have the second set of validators, and with id % 3 == 2 will have the third
/// * `key_pairs` - a flattened list of key pairs for the `validators`
/// * `validator_groups` - how many groups to split validators into. E.g. say there are four shards,
///                 and four validators in a particular epoch. If `validator_groups == 1`, all vals
///                 will validate all shards. If `validator_groups == 2`, shards 0 and 1 will have
///                 two validators validating them, and shards 2 and 3 will have the remaining two.
///                 If `validator_groups == 4`, each validator will validate a single shard
/// `skip_sync_wait`
/// `block_prod_time` - Minimum block production time, assuming there is enough approvals. The
///                 maximum block production time depends on the value of `tamper_with_fg`, and is
///                 equal to `block_prod_time` if `tamper_with_fg` is `true`, otherwise it is
///                 `block_prod_time * 2`
/// `drop_chunks` - if set to true, 10% of all the chunk messages / requests will be dropped
/// `tamper_with_fg` - if set to true, will split the heights into groups of 100. For some groups
///                 all the approvals will be dropped (thus completely disabling the finality gadget
///                 and introducing severe forkfulness if `block_prod_time` is sufficiently small),
///                 for some groups will keep all the approvals (and test the fg invariants), and
///                 for some will drop 50% of the approvals.
/// `epoch_length` - approximate length of the epoch as measured
///                 by the block heights difference of it's last and first block.
/// `network_mock` - the callback that is called for each message sent. The `mock` is called before
///                 the default processing. `mock` returns `(response, perform_default)`. If
///                 `perform_default` is false, then the message is not processed or broadcasted
///                 further and `response` is returned to the requester immediately. Otherwise
///                 the default action is performed, that might (and likely will) overwrite the
///                 `response` before it is sent back to the requester.
pub fn setup_mock_all_validators(
    validators: Vec<Vec<&'static str>>,
    key_pairs: Vec<PeerInfo>,
    validator_groups: u64,
    skip_sync_wait: bool,
    block_prod_time: u64,
    drop_chunks: bool,
    tamper_with_fg: bool,
    epoch_length: BlockHeightDelta,
    enable_doomslug: bool,
    archive: Vec<bool>,
    network_mock: Arc<RwLock<Box<dyn FnMut(String, &NetworkRequests) -> (NetworkResponses, bool)>>>,
) -> (Block, Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>) {
    let validators_clone = validators.clone();
    let key_pairs = key_pairs;

    let addresses: Vec<_> = (0..key_pairs.len()).map(|i| hash(vec![i as u8].as_ref())).collect();
    let genesis_time = Utc::now();
    let mut ret = vec![];

    let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
        Arc::new(RwLock::new(vec![]));

    // Lock the connectors so that none of the threads spawned below access them until we overwrite
    //    them at the end of this function
    let mut locked_connectors = connectors.write().unwrap();

    let announced_accounts = Arc::new(RwLock::new(HashSet::new()));
    let genesis_block = Arc::new(RwLock::new(None));
    let num_shards = validators.iter().map(|x| x.len()).min().unwrap() as NumShards;

    let last_height = Arc::new(RwLock::new(vec![0; key_pairs.len()]));
    let largest_endorsed_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let largest_skipped_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let hash_to_height = Arc::new(RwLock::new(HashMap::new()));

    for (index, account_id) in validators.into_iter().flatten().enumerate() {
        let view_client_addr = Arc::new(RwLock::new(None));
        let view_client_addr1 = view_client_addr.clone();
        let validators_clone1 = validators_clone.clone();
        let validators_clone2 = validators_clone.clone();
        let genesis_block1 = genesis_block.clone();
        let key_pairs = key_pairs.clone();
        let key_pairs1 = key_pairs.clone();
        let addresses = addresses.clone();
        let connectors1 = connectors.clone();
        let connectors2 = connectors.clone();
        let network_mock1 = network_mock.clone();
        let announced_accounts1 = announced_accounts.clone();
        let last_height1 = last_height.clone();
        let last_height2 = last_height.clone();
        let largest_endorsed_height1 = largest_endorsed_height.clone();
        let largest_skipped_height1 = largest_skipped_height.clone();
        let hash_to_height1 = hash_to_height.clone();
        let archive1 = archive.clone();
        let client_addr = ClientActor::create(move |ctx| {
            let client_addr = ctx.address();
            let pm = NetworkMock::mock(Box::new(move |msg, _ctx| {
                let msg = msg.downcast_ref::<NetworkRequests>().unwrap();

                let mut guard = network_mock1.write().unwrap();
                let (resp, perform_default) = guard.deref_mut()(account_id.to_string(), msg);
                drop(guard);

                if perform_default {
                    let mut my_key_pair = None;
                    let mut my_address = None;
                    let mut my_ord = None;
                    for (i, name) in validators_clone2.iter().flatten().enumerate() {
                        if *name == account_id {
                            my_key_pair = Some(key_pairs[i].clone());
                            my_address = Some(addresses[i].clone());
                            my_ord = Some(i);
                        }
                    }
                    let my_key_pair = my_key_pair.unwrap();
                    let my_address = my_address.unwrap();
                    let my_ord = my_ord.unwrap();

                    {
                        let last_height2 = last_height2.read().unwrap();
                        let peers: Vec<_> = key_pairs1
                            .iter()
                            .take(connectors2.read().unwrap().len())
                            .enumerate()
                            .map(|(i, peer_info)| FullPeerInfo {
                                peer_info: peer_info.clone(),
                                chain_info: PeerChainInfo {
                                    genesis_id: GenesisId {
                                        chain_id: "unittest".to_string(),
                                        hash: Default::default(),
                                    },
                                    height: last_height2[i],
                                    tracked_shards: vec![],
                                },
                                edge_info: EdgeInfo::default(),
                            })
                            .collect();
                        let peers2 = peers.clone();
                        let info = NetworkInfo {
                            active_peers: peers,
                            num_active_peers: key_pairs1.len(),
                            peer_max_count: key_pairs1.len() as u32,
                            highest_height_peers: peers2,
                            sent_bytes_per_sec: 0,
                            received_bytes_per_sec: 0,
                            known_producers: vec![],
                            #[cfg(feature = "metric_recorder")]
                            metric_recorder: MetricRecorder::default(),
                        };
                        client_addr.do_send(NetworkClientMessages::NetworkInfo(info));
                    }

                    match msg {
                        NetworkRequests::Block { block } => {
                            for (client, _) in connectors1.read().unwrap().iter() {
                                client.do_send(NetworkClientMessages::Block(
                                    block.clone(),
                                    PeerInfo::random().id,
                                    false,
                                ))
                            }

                            let mut last_height1 = last_height1.write().unwrap();

                            let my_height = &mut last_height1[my_ord];

                            *my_height = max(*my_height, block.header().height());

                            hash_to_height1
                                .write()
                                .unwrap()
                                .insert(*block.header().hash(), block.header().height());
                        }
                        NetworkRequests::PartialEncodedChunkRequest {
                            account_id: their_account_id,
                            request,
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == their_account_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::PartialEncodedChunkRequest(
                                                request.clone(),
                                                my_address,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                            for (i, address) in addresses.iter().enumerate() {
                                if route_back == address {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::PartialEncodedChunkResponse(
                                                response.clone(),
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        } => {
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == account_id {
                                    if !drop_chunks || !sample_binary(1, 10) {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::PartialEncodedChunk(
                                                partial_encoded_chunk.clone(),
                                            ),
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
                                            .1
                                            .send(NetworkViewClientMessages::BlockRequest(*hash))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::Block(block) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(NetworkClientMessages::Block(
                                                                *block, peer_id, true,
                                                            ));
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
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
                                            .1
                                            .send(NetworkViewClientMessages::BlockHeadersRequest(
                                                hashes.clone(),
                                            ))
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::BlockHeaders(
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
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateRequestHeader {
                            shard_id,
                            sync_hash,
                            target: target_account_id,
                        } => {
                            let target_account_id = match target_account_id {
                                AccountOrPeerIdOrHash::AccountId(x) => x,
                                _ => panic!(),
                            };
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == target_account_id {
                                    let connectors2 = connectors1.clone();
                                    actix::spawn(
                                        connectors1.read().unwrap()[i]
                                            .1
                                            .send(NetworkViewClientMessages::StateRequestHeader {
                                                shard_id: *shard_id,
                                                sync_hash: *sync_hash,
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::StateResponse(
                                                        response,
                                                    ) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(
                                                            NetworkClientMessages::StateResponse(
                                                                *response,
                                                            ),
                                                        );
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateRequestPart {
                            shard_id,
                            sync_hash,
                            part_id,
                            target: target_account_id,
                        } => {
                            let target_account_id = match target_account_id {
                                AccountOrPeerIdOrHash::AccountId(x) => x,
                                _ => panic!(),
                            };
                            for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                if name == target_account_id {
                                    let connectors2 = connectors1.clone();
                                    actix::spawn(
                                        connectors1.read().unwrap()[i]
                                            .1
                                            .send(NetworkViewClientMessages::StateRequestPart {
                                                shard_id: *shard_id,
                                                sync_hash: *sync_hash,
                                                part_id: *part_id,
                                            })
                                            .then(move |response| {
                                                let response = response.unwrap();
                                                match response {
                                                    NetworkViewClientResponses::StateResponse(
                                                        response,
                                                    ) => {
                                                        connectors2.read().unwrap()[my_ord]
                                                            .0
                                                            .do_send(
                                                            NetworkClientMessages::StateResponse(
                                                                *response,
                                                            ),
                                                        );
                                                    }
                                                    NetworkViewClientResponses::NoResponse => {}
                                                    _ => assert!(false),
                                                }
                                                future::ready(())
                                            }),
                                    );
                                }
                            }
                        }
                        NetworkRequests::StateResponse { route_back, response } => {
                            for (i, address) in addresses.iter().enumerate() {
                                if route_back == address {
                                    connectors1.read().unwrap()[i].0.do_send(
                                        NetworkClientMessages::StateResponse(response.clone()),
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
                                for (_, view_client) in connectors1.read().unwrap().iter() {
                                    view_client.do_send(NetworkViewClientMessages::AnnounceAccount(
                                        vec![(announce_account.clone(), None)],
                                    ))
                                }
                            }
                        }
                        NetworkRequests::Approval { approval_message } => {
                            let height_mod = approval_message.approval.target_height % 300;

                            let do_propagate = if tamper_with_fg {
                                if height_mod < 100 {
                                    false
                                } else if height_mod < 200 {
                                    let mut rng = rand::thread_rng();
                                    rng.gen()
                                } else {
                                    true
                                }
                            } else {
                                true
                            };

                            let approval = approval_message.approval.clone();

                            if do_propagate {
                                for (i, name) in validators_clone2.iter().flatten().enumerate() {
                                    if name == &approval_message.target {
                                        connectors1.read().unwrap()[i].0.do_send(
                                            NetworkClientMessages::BlockApproval(
                                                approval.clone(),
                                                my_key_pair.id.clone(),
                                            ),
                                        );
                                    }
                                }
                            }

                            // Verify doomslug invariant
                            match approval.inner {
                                ApprovalInner::Endorsement(parent_hash) => {
                                    assert!(
                                        approval.target_height
                                            > largest_skipped_height1.read().unwrap()[my_ord]
                                    );
                                    largest_endorsed_height1.write().unwrap()[my_ord] =
                                        approval.target_height;

                                    if let Some(prev_height) =
                                        hash_to_height1.read().unwrap().get(&parent_hash).clone()
                                    {
                                        assert_eq!(prev_height + 1, approval.target_height);
                                    }
                                }
                                ApprovalInner::Skip(prev_height) => {
                                    largest_skipped_height1.write().unwrap()[my_ord] =
                                        approval.target_height;
                                    let e = largest_endorsed_height1.read().unwrap()[my_ord];
                                    // `e` is the *target* height of the last endorsement. `prev_height`
                                    // is allowed to be anything >= to the source height, which is e-1.
                                    assert!(
                                        prev_height + 1 >= e,
                                        "New: {}->{}, Old: {}->{}",
                                        prev_height,
                                        approval.target_height,
                                        e - 1,
                                        e
                                    );
                                }
                            };
                        }
                        NetworkRequests::ForwardTx(_, _)
                        | NetworkRequests::Sync { .. }
                        | NetworkRequests::FetchRoutingTable
                        | NetworkRequests::PingTo(_, _)
                        | NetworkRequests::FetchPingPongInfo
                        | NetworkRequests::BanPeer { .. }
                        | NetworkRequests::TxStatus(_, _, _)
                        | NetworkRequests::Query { .. }
                        | NetworkRequests::Challenge(_)
                        | NetworkRequests::RequestUpdateNonce(_, _)
                        | NetworkRequests::ResponseUpdateNonce(_)
                        | NetworkRequests::ReceiptOutComeRequest(_, _) => {}
                    };
                }
                Box::new(Some(resp))
            }))
            .start();
            let network_adapter = NetworkRecipient::new();
            network_adapter.set_recipient(pm.recipient());
            let (block, client, view_client) = setup(
                validators_clone1.clone(),
                validator_groups,
                num_shards,
                epoch_length,
                account_id,
                skip_sync_wait,
                block_prod_time,
                block_prod_time * 3,
                enable_doomslug,
                archive1[index],
                Arc::new(network_adapter),
                10000,
                genesis_time,
            );
            *view_client_addr1.write().unwrap() = Some(view_client.start());
            *genesis_block1.write().unwrap() = Some(block);
            client
        });

        ret.push((client_addr, view_client_addr.clone().read().unwrap().clone().unwrap()));
    }
    hash_to_height.write().unwrap().insert(CryptoHash::default(), 0);
    hash_to_height
        .write()
        .unwrap()
        .insert(*genesis_block.read().unwrap().as_ref().unwrap().header().clone().hash(), 0);
    *locked_connectors = ret.clone();
    let value = genesis_block.read().unwrap();
    (value.clone().unwrap(), ret)
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    enable_doomslug: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_no_network_with_validity_period(
        validators,
        account_id,
        skip_sync_wait,
        100,
        enable_doomslug,
    )
}

pub fn setup_no_network_with_validity_period(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock_with_validity_period(
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        Box::new(|_, _, _| NetworkResponses::NoResponse),
        transaction_validity_period,
    )
}

pub fn setup_client_with_runtime(
    num_validator_seats: NumSeats,
    account_id: Option<&str>,
    enable_doomslug: bool,
    network_adapter: Arc<dyn NetworkAdapter>,
    chain_genesis: ChainGenesis,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
) -> Client {
    let validator_signer = account_id.map(|x| {
        Arc::new(InMemoryValidatorSigner::from_seed(x, KeyType::ED25519, x))
            as Arc<dyn ValidatorSigner>
    });
    let mut config = ClientConfig::test(true, 10, 20, num_validator_seats, false);
    config.epoch_length = chain_genesis.epoch_length;
    let mut client = Client::new(
        config,
        chain_genesis,
        runtime_adapter,
        network_adapter,
        validator_signer,
        enable_doomslug,
    )
    .unwrap();
    client.sync_status = SyncStatus::NoSync;
    client
}

pub fn setup_client(
    store: Arc<Store>,
    validators: Vec<Vec<&str>>,
    validator_groups: u64,
    num_shards: NumShards,
    account_id: Option<&str>,
    enable_doomslug: bool,
    network_adapter: Arc<dyn NetworkAdapter>,
    chain_genesis: ChainGenesis,
) -> Client {
    let num_validator_seats = validators.iter().map(|x| x.len()).sum::<usize>() as NumSeats;
    let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
        store,
        validators.into_iter().map(|inner| inner.into_iter().map(Into::into).collect()).collect(),
        validator_groups,
        num_shards,
        chain_genesis.epoch_length,
    ));
    setup_client_with_runtime(
        num_validator_seats,
        account_id,
        enable_doomslug,
        network_adapter,
        chain_genesis,
        runtime_adapter,
    )
}

pub struct TestEnv {
    pub chain_genesis: ChainGenesis,
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
                    store,
                    vec![validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
                    1,
                    1,
                    Some(&format!("test{}", i)),
                    false,
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
        num_validator_seats: NumSeats,
        runtime_adapters: Vec<Arc<dyn RuntimeAdapter>>,
    ) -> Self {
        let network_adapters: Vec<Arc<MockNetworkAdapter>> =
            (0..num_clients).map(|_| Arc::new(MockNetworkAdapter::default())).collect();
        Self::new_with_runtime_and_network_adapter(
            chain_genesis,
            num_clients,
            num_validator_seats,
            runtime_adapters,
            network_adapters,
        )
    }

    pub fn new_with_runtime_and_network_adapter(
        chain_genesis: ChainGenesis,
        num_clients: usize,
        num_validator_seats: NumSeats,
        runtime_adapters: Vec<Arc<dyn RuntimeAdapter>>,
        network_adapters: Vec<Arc<MockNetworkAdapter>>,
    ) -> Self {
        let validators: Vec<AccountId> =
            (0..num_validator_seats).map(|i| format!("test{}", i)).collect();
        let clients = (0..num_clients)
            .map(|i| {
                setup_client_with_runtime(
                    num_validator_seats,
                    Some(&format!("test{}", i)),
                    false,
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
        let more_accepted_blocks = self.clients[id].run_catchup(&vec![]).unwrap();
        accepted_blocks.extend(more_accepted_blocks);
        for accepted_block in accepted_blocks {
            self.clients[id].on_block_accepted(
                accepted_block.hash,
                accepted_block.status,
                accepted_block.provenance,
            );
        }
    }

    /// Produces block by given client, which kicks of creation of chunk.
    /// Which means that transactions added before this call, will be included in the next block of this validator.
    pub fn produce_block(&mut self, id: usize, height: BlockHeight) {
        let block = self.clients[id].produce_block(height).unwrap();
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
        self.clients[id].process_tx(tx, false, false)
    }

    pub fn query_account(&mut self, account_id: AccountId) -> AccountView {
        let head = self.clients[0].chain.head().unwrap();
        let last_block = self.clients[0].chain.get_block(&head.last_block_hash).unwrap().clone();
        let response = self.clients[0]
            .runtime_adapter
            .query(
                0,
                &last_block.chunks()[0].inner.prev_state_root,
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewAccount(account_view) => account_view,
            _ => panic!("Wrong return value"),
        }
    }

    pub fn query_balance(&mut self, account_id: AccountId) -> Balance {
        self.query_account(account_id).amount
    }

    pub fn restart(&mut self, id: usize) {
        let store = self.clients[id].chain.store().owned_store();
        self.clients[id] = setup_client(
            store,
            vec![self.validators.iter().map(|x| x.as_str()).collect::<Vec<&str>>()],
            1,
            1,
            Some(&format!("test{}", id)),
            false,
            self.network_adapters[id].clone(),
            self.chain_genesis.clone(),
        )
    }
}

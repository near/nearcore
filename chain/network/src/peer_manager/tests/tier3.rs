use crate::broadcast;
use crate::client::{
    StatePartOrHeader, StateRequestHeader, StateRequestPart, StateRequestResponse,
    StateRequestUpdate, StateRequestUpdateMessage,
};
use crate::config::{NetworkConfig, SocketOptions};
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, Handshake, PartialEdgeInfo, PeerMessage, SignedOwnedAccount, SnapshotHostInfo,
    SnapshotHostInfoV1, StateRequestAck, StateResponseInfo, StateResponseInfoV2, SyncSnapshotHosts,
    T2MessageBody, TieredMessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer::testonly::PeerHandle;
use crate::peer_manager;
use crate::peer_manager::connection::PoolError;
use crate::peer_manager::peer_manager_actor::{Event, TIER3_IDLE_TIMEOUT};
use crate::peer_manager::testonly::{ActorHandler, make_chain_info};
use crate::private_messages::RegisterPeerError;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{
    Edge, NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    StateRequestSenderForNetwork,
};
use futures::future::join_all;
use near_async::messaging::{AsyncSender, CanSendAsync, IntoMultiSender, noop};
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::StatePartIndex;
use near_primitives::state_sync::{
    PartOrHeader, ShardStateSyncResponse, ShardStateSyncResponseV2, StateRequestAckBody,
};
use near_primitives::types::ShardId;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::db::TestDB;
use rand::Rng;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

/// Limit on inbound Tier3 connections, kept low so that the tests don't need many file
/// descriptors.
const MAX_INBOUND_TIER3_CONNECTIONS: usize = 8;
/// Number of the requester connections which `start_pm` reserves for validators.
const RESERVED_FOR_VALIDATORS: usize = 2;
/// Number of the requester connections which non-validators can take.
const NON_VALIDATOR_SLOTS: usize = MAX_INBOUND_TIER3_CONNECTIONS - RESERVED_FOR_VALIDATORS;

/// The response which a host started with `start_pm` sends for a state request. The network
/// doesn't look into the response, so it is left empty.
fn state_response(
    shard_id: ShardId,
    sync_hash: CryptoHash,
    part_idx: Option<StatePartIndex>,
) -> StateResponseInfo {
    let state_response = ShardStateSyncResponse::V2(ShardStateSyncResponseV2 {
        header: None,
        part: part_idx.map(|part_idx| (part_idx, vec![])),
    });
    StateResponseInfo::V2(Box::new(StateResponseInfoV2 { shard_id, sync_hash, state_response }))
}

/// Starts a peer manager to which the tests connect. It answers all state requests, and reserves
/// `RESERVED_FOR_VALIDATORS` of its requester connections for validators.
async fn start_pm(
    clock: &time::Clock,
    chain: &Arc<data::Chain>,
    rng: &mut impl Rng,
) -> ActorHandler {
    start_pm_with_reserve(clock, chain, rng, RESERVED_FOR_VALIDATORS as u32).await
}

/// Like `start_pm`, with `reserved` requester connections reserved for validators.
async fn start_pm_with_reserve(
    clock: &time::Clock,
    chain: &Arc<data::Chain>,
    rng: &mut impl Rng,
    reserved: u32,
) -> ActorHandler {
    let mut cfg = chain.make_config(rng);
    cfg.max_inbound_tier3_connections = MAX_INBOUND_TIER3_CONNECTIONS as u32;
    cfg.inbound_tier3_connections_reserved_for_validators = reserved;
    let state_request_sender = StateRequestSenderForNetwork {
        state_request_header: AsyncSender::from_fn(|StateRequestHeader { shard_id, sync_hash }| {
            StateRequestResponse::Respond(StatePartOrHeader(Box::new(state_response(
                shard_id, sync_hash, None,
            ))))
        }),
        state_request_part: AsyncSender::from_fn(
            |StateRequestPart { shard_id, sync_hash, part_idx }| {
                StateRequestResponse::Respond(StatePartOrHeader(Box::new(state_response(
                    shard_id,
                    sync_hash,
                    Some(part_idx),
                ))))
            },
        ),
    };
    peer_manager::testonly::start_with_state_request_sender(
        clock.clone(),
        TestDB::new(),
        cfg,
        chain.clone(),
        state_request_sender,
    )
    .await
}

/// Opens a Tier3 connection to `pm` and sends a handshake as the owner of `key`, with
/// `owned_account` as the proof of ownership of an account key.
async fn connect_tier3(
    pm: &ActorHandler,
    chain: &data::Chain,
    key: &SecretKey,
    clock: &time::Clock,
    encoding: Encoding,
    owned_account: Option<SignedOwnedAccount>,
) -> (tcp::StreamId, Stream) {
    let stream = tcp::Stream::connect(&pm.peer_info(), tcp::Tier::T3, &SocketOptions::default())
        .await
        .unwrap();
    let stream_id = stream.id();
    let port = stream.local_addr.port();
    let peer_id = PeerId::new(key.public_key());
    let mut stream = Stream::new_with_encoding(stream, encoding);
    stream
        .write(&PeerMessage::Tier3Handshake(Handshake {
            protocol_version: PROTOCOL_VERSION,
            oldest_supported_version: PROTOCOL_VERSION,
            sender_peer_id: peer_id.clone(),
            target_peer_id: pm.cfg.node_id(),
            sender_listen_port: Some(port),
            sender_chain_info: chain.get_peer_chain_info(),
            partial_edge_info: PartialEdgeInfo::new(
                &peer_id,
                &pm.cfg.node_id(),
                Edge::create_fresh_nonce(clock),
                key,
            ),
            owned_account,
        }))
        .await;
    (stream_id, stream)
}

/// Waits until the peer manager accepts or rejects the handshake on the stream.
async fn handshake_result(
    events: &mut broadcast::Receiver<Event>,
    stream_id: tcp::StreamId,
) -> Result<(), ClosingReason> {
    events
        .recv_until(|ev| match ev {
            Event::HandshakeCompleted(ev) if ev.stream_id == stream_id => Some(Ok(())),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => Some(Err(ev.reason)),
            _ => None,
        })
        .await
}

/// Makes the proof that the peer with `key` owns the account key of the validator `account`.
/// It is signed using protobuf, which handshakes in either encoding can carry.
fn owned_account(
    account: &NetworkConfig,
    key: &SecretKey,
    clock: &time::Clock,
) -> SignedOwnedAccount {
    let signer = account.validator.signer.get().unwrap();
    data::make_signed_owned_account(&signer, clock, PeerId::new(key.public_key()), Encoding::Proto)
}

/// Connects to `pm` as a new requester, which proves that it owns the account key of the validator
/// `account` if set, and waits for its handshake. Returns the reason why `pm` closed the connection
/// if it rejected the handshake.
async fn try_connect_requester(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    rng: &mut impl Rng,
    encoding: Encoding,
    account: Option<&NetworkConfig>,
) -> Result<(tcp::StreamId, Stream), ClosingReason> {
    let key = data::make_secret_key(rng);
    let owned_account = account.map(|account| owned_account(account, &key, clock));
    let mut events = pm.events.from_now();
    let (stream_id, mut stream) =
        connect_tier3(pm, chain, &key, clock, encoding, owned_account).await;
    handshake_result(&mut events, stream_id).await?;
    let msg = stream.read().await.unwrap();
    assert!(matches!(msg, PeerMessage::Tier3Handshake(_)), "unexpected message: {msg}");
    Ok((stream_id, stream))
}

/// Connects to `pm` as a new requester and waits for its handshake.
async fn connect_requester(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    rng: &mut impl Rng,
    encoding: Encoding,
) -> (tcp::StreamId, Stream) {
    try_connect_requester(pm, chain, clock, rng, encoding, None).await.unwrap()
}

/// Connects to `pm` as a new requester which owns the account key of `validator`, and waits for
/// its handshake.
async fn connect_validator_requester(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    rng: &mut impl Rng,
    encoding: Encoding,
    validator: &NetworkConfig,
) -> (tcp::StreamId, Stream) {
    try_connect_requester(pm, chain, clock, rng, encoding, Some(validator)).await.unwrap()
}

/// Connects to `pm` as a new requester like `try_connect_requester`, and checks that `pm` rejects
/// the handshake because its pool returns `err`.
async fn assert_requester_rejected(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    rng: &mut impl Rng,
    encoding: Encoding,
    account: Option<&NetworkConfig>,
    err: PoolError,
) {
    let result = try_connect_requester(pm, chain, clock, rng, encoding, account).await;
    let want = ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(err));
    assert_eq!(result.err(), Some(want));
}

/// Makes `n` validators.
fn make_validators(chain: &data::Chain, rng: &mut impl Rng, n: usize) -> Vec<NetworkConfig> {
    (0..n).map(|_| chain.make_config(rng)).collect()
}

/// Makes `validators` the validators known to `pm`.
async fn set_validators(pm: &ActorHandler, chain: &data::Chain, validators: &[NetworkConfig]) {
    pm.set_chain_info(make_chain_info(chain, &validators.iter().collect::<Vec<_>>())).await;
}

/// Makes `pm` aware of a snapshot of `shard_id` advertised under `snapshot_hash` by the node
/// with config `host`, with `addr` as the host's address. Without an address, the advertisement
/// is a V1 info, as published by a host on an older release.
async fn advertise_snapshot(
    pm: &ActorHandler,
    host: &NetworkConfig,
    snapshot_hash: CryptoHash,
    shard_id: ShardId,
    addr: Option<SocketAddr>,
) {
    let (epoch_height, shards) = (1, vec![shard_id]);
    let info = match addr {
        Some(addr) => SnapshotHostInfo::new(
            host.node_id(),
            snapshot_hash,
            epoch_height,
            shards,
            addr,
            &host.node_key,
        ),
        None => {
            let hash = CryptoHash::hash_borsh((&snapshot_hash, &epoch_height, &shards));
            SnapshotHostInfo::V1(SnapshotHostInfoV1 {
                peer_id: host.node_id(),
                sync_hash: snapshot_hash,
                epoch_height,
                shards,
                signature: host.node_key.sign(hash.as_ref()),
            })
        }
    };
    let info = Arc::new(info);
    let (inserted, err) = pm
        .with_state(move |s| async move { s.snapshot_hosts.insert(vec![info], HashSet::new).await })
        .await;
    assert_eq!((inserted.len(), err), (1, None));
}

/// Sends a network request to `pm` and returns its response.
async fn send_request(pm: &ActorHandler, request: NetworkRequests) -> NetworkResponses {
    let response: PeerManagerMessageResponse =
        pm.actor.send_async(PeerManagerMessageRequest::NetworkRequests(request)).await.unwrap();
    response.as_network_response()
}

// Verify that a node accepts Tier3 connections from peers which request state from it.
#[tokio::test]
async fn requester_accepted() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        requester_accepted_with_encoding(encoding).await;
    }
}

async fn requester_accepted_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853240);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    connect_requester(&pm, &chain, &clock, rng, encoding).await;
}

// Verify that a node accepts at most MAX_INBOUND_TIER3_CONNECTIONS requesters at a time,
// that hosts answering our own requests aren't limited, and that closing a requester's
// connection makes room for another.
#[tokio::test]
async fn requesters_limited() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        requesters_limited_with_encoding(encoding).await;
    }
}

async fn requesters_limited_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853241);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let validators = make_validators(&chain, rng, RESERVED_FOR_VALIDATORS);
    set_validators(&pm, &chain, &validators).await;
    let mut requesters = vec![];
    for _ in 0..NON_VALIDATOR_SLOTS {
        requesters.push(connect_requester(&pm, &chain, &clock, rng, encoding).await);
    }
    let mut validator_requesters = vec![];
    for validator in &validators {
        validator_requesters
            .push(connect_validator_requester(&pm, &chain, &clock, rng, encoding, validator).await);
    }
    assert_requester_rejected(
        &pm,
        &chain,
        &clock,
        rng,
        encoding,
        None,
        PoolError::TooManyTier3Requesters,
    )
    .await;

    // A host answering our own state request is accepted regardless of the limit.
    let host_key = data::make_secret_key(rng);
    let host = PeerId::new(host_key.public_key());
    let now = clock.now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(host, now);
    })
    .await;
    let mut events = pm.events.from_now();
    let (stream_id, _host_stream) =
        connect_tier3(&pm, &chain, &host_key, &clock, encoding, None).await;
    assert_eq!(handshake_result(&mut events, stream_id).await, Ok(()));

    let (closed_id, closed) = requesters.pop().unwrap();
    let mut events = pm.events.from_now();
    drop(closed);
    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == closed_id => Some(()),
            _ => None,
        })
        .await;
    connect_requester(&pm, &chain, &clock, rng, encoding).await;
}

// Verify that requesters handshaking concurrently stay within their limits: non-validators within
// the slots which aren't reserved, and validators within MAX_INBOUND_TIER3_CONNECTIONS.
#[tokio::test]
async fn concurrent_requesters_limited() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        concurrent_requesters_limited_with_encoding(encoding).await;
    }
}

async fn concurrent_requesters_limited_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853245);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    // Leave room for a few non-validators, then let twice as many handshake at once.
    const FREE: usize = 4;
    let pm = start_pm(&clock, &chain, rng).await;
    let mut requesters = vec![];
    for _ in 0..NON_VALIDATOR_SLOTS - FREE {
        requesters.push(connect_requester(&pm, &chain, &clock, rng, encoding).await);
    }
    let keys: Vec<SecretKey> = (0..2 * FREE).map(|_| data::make_secret_key(rng)).collect();
    let attempts = keys.iter().map(|key| (key, None)).collect();
    let (non_validators, errors) = race_requesters(&pm, &chain, &clock, encoding, attempts).await;
    assert_eq!(non_validators.len(), FREE);
    assert_eq!(errors, vec![PoolError::TooManyTier3Requesters; FREE]);

    // Only the reserved slots are left, and twice as many validators handshake at once.
    let validators = make_validators(&chain, rng, 2 * RESERVED_FOR_VALIDATORS);
    set_validators(&pm, &chain, &validators).await;
    let keys: Vec<SecretKey> =
        (0..2 * RESERVED_FOR_VALIDATORS).map(|_| data::make_secret_key(rng)).collect();
    let attempts = keys
        .iter()
        .zip(&validators)
        .map(|(key, validator)| (key, Some(owned_account(validator, key, &clock))))
        .collect();
    let (accepted_validators, errors) =
        race_requesters(&pm, &chain, &clock, encoding, attempts).await;
    assert_eq!(accepted_validators.len(), RESERVED_FOR_VALIDATORS);
    assert_eq!(errors, vec![PoolError::TooManyTier3Requesters; RESERVED_FOR_VALIDATORS]);
}

/// Lets requesters handshake with `pm` at once, each with a key and an optional proof of ownership
/// of an account key. Returns the connections which `pm` accepted, and the errors with which its
/// pool rejected the others.
async fn race_requesters(
    pm: &ActorHandler,
    chain: &data::Chain,
    clock: &time::Clock,
    encoding: Encoding,
    attempts: Vec<(&SecretKey, Option<SignedOwnedAccount>)>,
) -> (Vec<Stream>, Vec<PoolError>) {
    let events = pm.events.from_now();
    let racing =
        join_all(attempts.into_iter().map(|(key, owned_account)| {
            connect_tier3(pm, chain, key, clock, encoding, owned_account)
        }))
        .await;
    let mut accepted = vec![];
    let mut errors = vec![];
    for (stream_id, stream) in racing {
        match handshake_result(&mut events.clone(), stream_id).await {
            Ok(()) => accepted.push(stream),
            Err(ClosingReason::RejectedByPeerManager(RegisterPeerError::PoolError(err))) => {
                errors.push(err)
            }
            Err(reason) => panic!("unexpected closing reason: {reason}"),
        }
    }
    (accepted, errors)
}

// Verify that non-validators can't take the requester slots reserved for validators, and that
// validators can.
#[tokio::test]
async fn validators_get_reserved_requester_slots() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        validators_get_reserved_requester_slots_with_encoding(encoding).await;
    }
}

async fn validators_get_reserved_requester_slots_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853253);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let validators = make_validators(&chain, rng, RESERVED_FOR_VALIDATORS + 1);
    set_validators(&pm, &chain, &validators).await;
    let mut requesters = vec![];
    for _ in 0..NON_VALIDATOR_SLOTS {
        requesters.push(connect_requester(&pm, &chain, &clock, rng, encoding).await);
    }
    assert_requester_rejected(
        &pm,
        &chain,
        &clock,
        rng,
        encoding,
        None,
        PoolError::TooManyTier3Requesters,
    )
    .await;
    for validator in &validators[..RESERVED_FOR_VALIDATORS] {
        requesters
            .push(connect_validator_requester(&pm, &chain, &clock, rng, encoding, validator).await);
    }
    let validator = &validators[RESERVED_FOR_VALIDATORS];
    assert_requester_rejected(
        &pm,
        &chain,
        &clock,
        rng,
        encoding,
        Some(validator),
        PoolError::TooManyTier3Requesters,
    )
    .await;
}

// Verify that validators can take any of the requester slots, not only the reserved ones.
#[tokio::test]
async fn validators_use_unreserved_requester_slots() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        validators_use_unreserved_requester_slots_with_encoding(encoding).await;
    }
}

async fn validators_use_unreserved_requester_slots_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853254);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let validators = make_validators(&chain, rng, MAX_INBOUND_TIER3_CONNECTIONS + 1);
    set_validators(&pm, &chain, &validators).await;
    let mut requesters = vec![];
    for validator in &validators[..MAX_INBOUND_TIER3_CONNECTIONS] {
        requesters
            .push(connect_validator_requester(&pm, &chain, &clock, rng, encoding, validator).await);
    }
    let validator = &validators[MAX_INBOUND_TIER3_CONNECTIONS];
    for account in [Some(validator), None] {
        assert_requester_rejected(
            &pm,
            &chain,
            &clock,
            rng,
            encoding,
            account,
            PoolError::TooManyTier3Requesters,
        )
        .await;
    }
}

// Verify that the peers which own an account key that isn't a validator's, and the peers which
// don't prove the ownership of any account key, don't get the slots reserved for validators.
#[tokio::test]
async fn non_validator_owned_account_not_reserved() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        non_validator_owned_account_not_reserved_with_encoding(encoding).await;
    }
}

async fn non_validator_owned_account_not_reserved_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853255);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let validators = make_validators(&chain, rng, 1);
    set_validators(&pm, &chain, &validators).await;
    // Half of the slots of non-validators are taken by peers which own the account keys of accounts
    // that `pm` doesn't know as validators, and the other half by peers without account keys.
    let others = make_validators(&chain, rng, NON_VALIDATOR_SLOTS / 2 + 1);
    let mut requesters = vec![];
    for other in &others[..NON_VALIDATOR_SLOTS / 2] {
        let requester =
            try_connect_requester(&pm, &chain, &clock, rng, encoding, Some(other)).await;
        requesters.push(requester.unwrap());
        requesters.push(connect_requester(&pm, &chain, &clock, rng, encoding).await);
    }
    for account in [others.last(), None] {
        assert_requester_rejected(
            &pm,
            &chain,
            &clock,
            rng,
            encoding,
            account,
            PoolError::TooManyTier3Requesters,
        )
        .await;
    }
    connect_validator_requester(&pm, &chain, &clock, rng, encoding, &validators[0]).await;
}

// Verify that the same validator can't connect twice: a second peer which proves that it owns the
// account key of a connected validator is rejected.
#[tokio::test]
async fn validator_cannot_connect_twice() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        validator_cannot_connect_twice_with_encoding(encoding).await;
    }
}

async fn validator_cannot_connect_twice_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853257);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let validators = make_validators(&chain, rng, 1);
    set_validators(&pm, &chain, &validators).await;
    let validator = &validators[0];
    let _requester =
        connect_validator_requester(&pm, &chain, &clock, rng, encoding, validator).await;
    let peer_id = pm.tcp.tier3.load().ready.keys().next().unwrap().clone();
    let account_key = validator.validator.signer.get().unwrap().public_key();
    assert_requester_rejected(
        &pm,
        &chain,
        &clock,
        rng,
        encoding,
        Some(validator),
        PoolError::AlreadyConnectedAccount { peer_id, account_key },
    )
    .await;
}

// Verify that non-validators can take all the requester slots of a host which reserves none of
// them for validators.
#[tokio::test]
async fn non_validators_take_all_slots_without_reserve() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        non_validators_take_all_slots_without_reserve_with_encoding(encoding).await;
    }
}

async fn non_validators_take_all_slots_without_reserve_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853259);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm_with_reserve(&clock, &chain, rng, 0).await;
    let mut requesters = vec![];
    for _ in 0..MAX_INBOUND_TIER3_CONNECTIONS {
        requesters.push(connect_requester(&pm, &chain, &clock, rng, encoding).await);
    }
    let err = PoolError::TooManyTier3Requesters;
    assert_requester_rejected(&pm, &chain, &clock, rng, encoding, None, err).await;
}

// Verify that only validators can connect to a host which reserves all of its requester slots for
// them.
#[tokio::test]
async fn only_validators_connect_with_all_slots_reserved() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        only_validators_connect_with_all_slots_reserved_with_encoding(encoding).await;
    }
}

async fn only_validators_connect_with_all_slots_reserved_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853260);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let all = MAX_INBOUND_TIER3_CONNECTIONS as u32;
    let pm = start_pm_with_reserve(&clock, &chain, rng, all).await;
    let validators = make_validators(&chain, rng, 1);
    set_validators(&pm, &chain, &validators).await;
    let err = PoolError::TooManyTier3Requesters;
    assert_requester_rejected(&pm, &chain, &clock, rng, encoding, None, err).await;
    connect_validator_requester(&pm, &chain, &clock, rng, encoding, &validators[0]).await;
}

// Verify that a host sends the acks and the responses for state header and part requests over
// the Tier3 connection on which the requests arrived.
#[tokio::test]
async fn requests_answered_over_tier3() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        requests_answered_over_tier3_with_encoding(encoding).await;
    }
}

async fn requests_answered_over_tier3_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853242);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = start_pm(&clock, &chain, rng).await;
    let (_, mut stream) = connect_requester(&pm, &chain, &clock, rng, encoding).await;

    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let part_idx = 7;
    for (request, part_or_header, part_idx) in [
        (PeerMessage::StateRequestHeader(shard_id, sync_hash), PartOrHeader::Header, None),
        (
            PeerMessage::StateRequestPart(shard_id, sync_hash, part_idx),
            PartOrHeader::Part { part_idx },
            Some(part_idx),
        ),
    ] {
        stream.write(&request).await;
        let ack = StateRequestAck {
            shard_id,
            sync_hash,
            part_or_header,
            body: StateRequestAckBody::WillRespond,
        };
        assert_eq!(stream.read().await.unwrap(), PeerMessage::StateRequestAck(ack));
        assert_eq!(
            stream.read().await.unwrap(),
            PeerMessage::VersionedStateResponse(state_response(shard_id, sync_hash, part_idx)),
        );
    }
}

/// A client which answers all state requests with `response`.
fn client_answering(response: StateRequestResponse) -> StateRequestSenderForNetwork {
    let header_response = response.clone();
    StateRequestSenderForNetwork {
        state_request_header: AsyncSender::from_fn(move |_: StateRequestHeader| {
            header_response.clone()
        }),
        state_request_part: AsyncSender::from_fn(move |_: StateRequestPart| response.clone()),
    }
}

// Verify that a host acks over the Tier3 connection the state header and part requests which it
// doesn't answer: with Busy if the client is busy, and with Error if the client can't serve the
// request or fails to respond. No response follows the acks.
#[tokio::test]
async fn unanswered_requests_acknowledged_over_tier3() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        unanswered_requests_acknowledged_over_tier3_with_encoding(encoding).await;
    }
}

async fn unanswered_requests_acknowledged_over_tier3_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853243);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let part_idx = 7;
    let header = (PeerMessage::StateRequestHeader(shard_id, sync_hash), PartOrHeader::Header);
    let part = (
        PeerMessage::StateRequestPart(shard_id, sync_hash, part_idx),
        PartOrHeader::Part { part_idx },
    );
    for (client, body) in [
        (client_answering(StateRequestResponse::Busy), StateRequestAckBody::Busy),
        (client_answering(StateRequestResponse::Error), StateRequestAckBody::Error),
        // The client fails to respond.
        (noop().into_multi_sender(), StateRequestAckBody::Error),
    ] {
        let pm = peer_manager::testonly::start_with_state_request_sender(
            clock.clone(),
            TestDB::new(),
            chain.make_config(rng),
            chain.clone(),
            client,
        )
        .await;
        let (_, mut stream) = connect_requester(&pm, &chain, &clock, rng, encoding).await;

        // The host would send a response right after the ack, so receiving the next request's
        // ack instead shows that it sent none. The header request is sent again at the end to
        // check the part request.
        for (request, part_or_header) in [&header, &part, &header] {
            stream.write(request).await;
            let ack =
                StateRequestAck { shard_id, sync_hash, part_or_header: *part_or_header, body };
            assert_eq!(stream.read().await.unwrap(), PeerMessage::StateRequestAck(ack));
        }
    }
}

// Verify that a node accepts an ack for its state request over a Tier3 connection.
#[tokio::test]
async fn ack_accepted_over_tier3() {
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        ack_accepted_over_tier3_with_encoding(encoding).await;
    }
}

async fn ack_accepted_over_tier3_with_encoding(encoding: Encoding) {
    init_test_logger();
    let mut rng = make_rng(921853244);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let pm = peer_manager::testonly::start(
        clock.clone(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;

    // Simulate having sent a state request to the host, so that its connection is accepted.
    let host_key = data::make_secret_key(rng);
    let host = PeerId::new(host_key.public_key());
    let now = clock.now();
    pm.with_state(move |s| async move {
        s.pending_tier3_requests.insert(host, now);
    })
    .await;
    let mut events = pm.events.from_now();
    let (stream_id, mut stream) =
        connect_tier3(&pm, &chain, &host_key, &clock, encoding, None).await;
    assert_eq!(handshake_result(&mut events, stream_id).await, Ok(()));

    let ack = StateRequestAck {
        shard_id: ShardId::new(0),
        sync_hash: CryptoHash::hash_borsh(rng.r#gen::<u64>()),
        part_or_header: PartOrHeader::Header,
        body: StateRequestAckBody::Busy,
    };
    stream.write(&PeerMessage::StateRequestAck(ack.clone())).await;
    let processed = events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T3, PeerMessage::StateRequestAck(ack)) => Some(ack),
            Event::ConnectionClosed(ev) if ev.stream_id == stream_id => {
                panic!("connection closed: {}", ev.reason)
            }
            _ => None,
        })
        .await;
    assert_eq!(processed, ack);
}

// Verify that a node routes state requests over Tier2 to a snapshot host which doesn't advertise its
// address, and that the host acks them over Tier2 and sends the responses over a Tier3 connection
// which it opens to the requester.
#[tokio::test]
async fn routed_requests_answered_over_tier3() {
    init_test_logger();
    let mut rng = make_rng(921853246);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let host = start_pm(&clock, &chain, rng).await;
    // The requester puts its address in the requests, so that the host can connect to it.
    let mut cfg = chain.make_config(rng);
    cfg.tier3_public_addr = cfg.node_addr.as_ref().map(|addr| **addr);
    let requester =
        peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;
    let host_id = host.cfg.node_id();
    let requester_id = requester.cfg.node_id();
    requester.connect_to(&host.peer_info(), tcp::Tier::T2).await;
    requester.wait_for_routing_table(&[(host_id.clone(), vec![host_id.clone()])]).await;
    host.wait_for_routing_table(&[(requester_id.clone(), vec![requester_id])]).await;

    // Let the requester know that the host has a snapshot of the shard. The host doesn't advertise
    // its address, so the requests are routed to it.
    let shard_id = ShardId::new(0);
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    advertise_snapshot(&requester, &host.cfg, snapshot_hash, shard_id, None).await;

    // The host connects to the requester to answer the first request, and reuses the connection
    // for the second.
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let part_idx = 7;
    for (request, part_or_header, part_idx) in [
        (
            NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx },
            PartOrHeader::Part { part_idx },
            Some(part_idx),
        ),
        (
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, snapshot_hash },
            PartOrHeader::Header,
            None,
        ),
    ] {
        let events = requester.events.from_now();
        assert_eq!(
            send_request(&requester, request).await,
            NetworkResponses::SelectedDestination(host_id.clone())
        );

        let ack = StateRequestAck {
            shard_id,
            sync_hash,
            part_or_header,
            body: StateRequestAckBody::WillRespond,
        };
        let want_ack: TieredMessageBody = T2MessageBody::StateRequestAck(ack).into();
        events
            .clone()
            .recv_until(|ev| match ev {
                Event::MessageProcessed(tcp::Tier::T2, PeerMessage::Routed(msg))
                    if msg.body() == &want_ack =>
                {
                    Some(())
                }
                _ => None,
            })
            .await;
        let want_response = state_response(shard_id, sync_hash, part_idx);
        events
            .clone()
            .recv_until(|ev| match ev {
                Event::MessageProcessed(
                    tcp::Tier::T3,
                    PeerMessage::VersionedStateResponse(response),
                ) if response == want_response => Some(()),
                _ => None,
            })
            .await;
    }
}

/// Sends `msg` from `peer` and waits until the peer manager with the `events` processes it.
/// Panics if any of the connections `open` is closed first.
async fn send_and_wait(
    events: &mut broadcast::Receiver<Event>,
    peer: &PeerHandle,
    msg: PeerMessage,
    open: &[tcp::StreamId],
) {
    peer.send(msg.clone()).await;
    events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(_, processed) if processed == msg => Some(()),
            Event::ConnectionClosed(ev) if open.contains(&ev.stream_id) => {
                panic!("connection closed: {}", ev.reason)
            }
            _ => None,
        })
        .await;
}

/// Waits until the connection `closed` is closed. Panics if any of the connections `open` is
/// closed first.
async fn wait_closed(
    events: &mut broadcast::Receiver<Event>,
    closed: tcp::StreamId,
    open: &[tcp::StreamId],
) {
    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev) if ev.stream_id == closed => Some(()),
            Event::ConnectionClosed(ev) if open.contains(&ev.stream_id) => {
                panic!("connection closed: {}", ev.reason)
            }
            _ => None,
        })
        .await;
}

// Verify that a Tier3 connection is closed once nothing has been sent or received on it for
// TIER3_IDLE_TIMEOUT, regardless of the traffic on a Tier2 connection to the same peer, and that
// the Tier2 connection stays open.
#[tokio::test]
async fn idle_timeout_ignores_tier2() {
    init_test_logger();
    let mut rng = make_rng(921853251);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm = peer_manager::testonly::start(
        clock.clock(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    // Connect to a peer on both Tier2 and Tier3, and to another peer on Tier3 only.
    let cfg = chain.make_config(rng);
    let conn = pm.start_outbound(chain.clone(), cfg.clone(), tcp::Tier::T2).await;
    let t2_stream_id = conn.stream.id();
    let t2_peer = conn.handshake(&clock.clock()).await;
    let conn = pm.start_outbound(chain.clone(), cfg, tcp::Tier::T3).await;
    let t3_stream_id = conn.stream.id();
    let t3_peer = conn.handshake(&clock.clock()).await;
    let conn = pm.start_outbound(chain.clone(), chain.make_config(rng), tcp::Tier::T3).await;
    let idle_stream_id = conn.stream.id();
    let _idle_peer = conn.handshake(&clock.clock()).await;

    let mut events = pm.events.from_now();
    let t2_msg = PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts { hosts: vec![] });
    let t3_msg = PeerMessage::VersionedStateResponse(state_response(
        ShardId::new(0),
        CryptoHash::hash_borsh(rng.r#gen::<u64>()),
        None,
    ));
    let open = [t2_stream_id, t3_stream_id];

    // Let the Tier2 connection idle for longer than TIER3_IDLE_TIMEOUT, but not the Tier3
    // connection to the same peer. Once the Tier3 connection to the other peer is closed for
    // being idle, both connections to the peer are still open.
    clock.advance(TIER3_IDLE_TIMEOUT - time::Duration::seconds(1));
    send_and_wait(&mut events, &t3_peer, t3_msg.clone(), &open).await;
    clock.advance(time::Duration::seconds(2));
    wait_closed(&mut events, idle_stream_id, &open).await;
    send_and_wait(&mut events, &t3_peer, t3_msg, &open).await;
    send_and_wait(&mut events, &t2_peer, t2_msg.clone(), &open).await;

    // Now let the Tier3 connection idle for longer than TIER3_IDLE_TIMEOUT, but not the Tier2
    // connection. The Tier3 connection is closed, and the Tier2 connection stays open.
    let step = TIER3_IDLE_TIMEOUT / 2 + time::Duration::seconds(1);
    clock.advance(step);
    send_and_wait(&mut events, &t2_peer, t2_msg.clone(), &open).await;
    clock.advance(step);
    wait_closed(&mut events, t3_stream_id, &[t2_stream_id]).await;
    send_and_wait(&mut events, &t2_peer, t2_msg, &[t2_stream_id]).await;
}

// Verify that a Tier3 connection on which messages are only sent, as when a host answers requests
// that it received over Tier2, isn't closed while it is in use.
#[tokio::test]
async fn idle_timeout_counts_sent_messages() {
    init_test_logger();
    let mut rng = make_rng(921853252);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));

    let pm = peer_manager::testonly::start(
        clock.clock(),
        TestDB::new(),
        chain.make_config(rng),
        chain.clone(),
    )
    .await;
    // Connect to a peer which the test sends messages to, and to another peer whose connection
    // stays idle.
    let conn = pm.start_outbound(chain.clone(), chain.make_config(rng), tcp::Tier::T3).await;
    let stream_id = conn.stream.id();
    let peer = conn.handshake(&clock.clock()).await;
    let conn = pm.start_outbound(chain.clone(), chain.make_config(rng), tcp::Tier::T3).await;
    let idle_stream_id = conn.stream.id();
    let _idle_peer = conn.handshake(&clock.clock()).await;
    let mut events = pm.events.from_now();

    // Send a message to the peer shortly before the connection times out.
    clock.advance(TIER3_IDLE_TIMEOUT - time::Duration::seconds(1));
    let msg = PeerMessage::VersionedStateResponse(state_response(
        ShardId::new(0),
        CryptoHash::hash_borsh(rng.r#gen::<u64>()),
        None,
    ));
    let mut peer_events = peer.events.from_now();
    let (peer_id, sent_msg) = (peer.cfg.id(), Arc::new(msg.clone()));
    let sent = pm
        .with_state_and_transport(move |_, transport| async move {
            transport.send_message(tcp::Tier::T3, peer_id, sent_msg)
        })
        .await;
    assert!(sent);
    peer_events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(tcp::Tier::T3, received) if received == msg => Some(()),
            _ => None,
        })
        .await;

    // Pass the timeout counting from the last message received on the connection, but not from
    // the message sent. Once the idle connection is closed, the connection to the peer is still
    // open.
    clock.advance(time::Duration::seconds(2));
    wait_closed(&mut events, idle_stream_id, &[stream_id]).await;
    send_and_wait(&mut events, &peer, msg, &[stream_id]).await;
}

// Verify that a node sends state requests to a snapshot host which advertises its address over a
// Tier3 connection to that address, and receives the responses over it. The node needs neither a
// public address of its own nor a route to the host, and concurrent requests share a connection.
#[tokio::test]
async fn requests_sent_to_advertised_addr() {
    init_test_logger();
    let mut rng = make_rng(921853247);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let host = start_pm(&clock, &chain, rng).await;
    let requester = start_pm(&clock, &chain, rng).await;
    assert!(requester.with_state(|s| async move { s.my_public_addr.read().is_none() }).await);

    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    advertise_snapshot(&requester, &host.cfg, snapshot_hash, shard_id, host.peer_info().addr).await;

    let mut events = requester.events.from_now();
    let part_ids: HashSet<_> = (0..10).collect();
    let requests = part_ids.iter().map(|&part_idx| {
        let request =
            NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx };
        send_request(&requester, request)
    });
    for response in join_all(requests).await {
        assert_eq!(response, NetworkResponses::SelectedDestination(host.cfg.node_id()));
    }

    let mut parts = HashSet::new();
    while parts.len() < part_ids.len() {
        if let Event::MessageProcessed(tcp::Tier::T3, PeerMessage::VersionedStateResponse(part)) =
            events.recv().await
        {
            parts.insert(part.part_idx().unwrap());
        }
    }
    assert_eq!(parts, part_ids);
    let host_tier3: Vec<PeerId> = host.tcp.tier3.load().ready.keys().cloned().collect();
    assert_eq!(host_tier3, vec![requester.cfg.node_id()]);
}

// Verify that state requests to a snapshot host which doesn't advertise its address are routed
// over Tier2, which needs the node's own public address.
#[tokio::test]
async fn requests_routed_to_host_without_addr() {
    init_test_logger();
    let mut rng = make_rng(921853248);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let requester = start_pm(&clock, &chain, rng).await;
    let host = chain.make_config(rng);
    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    advertise_snapshot(&requester, &host, snapshot_hash, shard_id, None).await;
    let request =
        NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx: 0 };
    assert_eq!(send_request(&requester, request).await, NetworkResponses::MyPublicAddrNotKnown);
}

// Verify that a node with direct state requests disabled routes state requests over Tier2 even to a
// snapshot host which advertises its address.
#[tokio::test]
async fn requests_routed_if_direct_requests_disabled() {
    init_test_logger();
    let mut rng = make_rng(921853250);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let mut cfg = chain.make_config(rng);
    cfg.direct_state_requests_disabled = true;
    let requester =
        peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;
    let host = chain.make_config(rng);
    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let host_addr = host.node_addr.as_ref().map(|addr| **addr);
    advertise_snapshot(&requester, &host, snapshot_hash, shard_id, host_addr).await;
    let request =
        NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx: 0 };
    assert_eq!(send_request(&requester, request).await, NetworkResponses::MyPublicAddrNotKnown);
}

/// Makes `pm` aware of a snapshot that `host` advertises with `addr` as its address, requests a
/// part of it, and checks that `pm` tells the client that the request wasn't sent.
async fn request_not_sent(
    pm: &ActorHandler,
    host: &NetworkConfig,
    addr: SocketAddr,
    rng: &mut impl Rng,
) {
    let shard_id = ShardId::new(0);
    let sync_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    let snapshot_hash = CryptoHash::hash_borsh(rng.r#gen::<u64>());
    advertise_snapshot(pm, host, snapshot_hash, shard_id, Some(addr)).await;

    let mut state_request_updates = pm.state_request_updates.from_now();
    let request =
        NetworkRequests::StateRequestPart { shard_id, sync_hash, snapshot_hash, part_idx: 0 };
    assert_eq!(
        send_request(pm, request).await,
        NetworkResponses::SelectedDestination(host.node_id())
    );
    let part_or_header = PartOrHeader::Part { part_idx: 0 };
    let update = StateRequestUpdate::NotSent { shard_id, sync_hash, part_or_header };
    assert_eq!(
        state_request_updates.recv().await,
        StateRequestUpdateMessage { peer_id: host.node_id(), update }
    );
}

// Verify that a node tells the client when it can't send a state request because connecting to the
// snapshot host fails fast: nothing listens on the host's address, or a different node listens
// there and rejects the handshake.
#[tokio::test]
async fn unreachable_hosts_reported() {
    init_test_logger();
    let mut rng = make_rng(921853249);
    let rng = &mut rng;
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, rng, 10));
    let clock = clock.clock();

    let mut cfg = chain.make_config(rng);
    cfg.handshake_timeout = time::Duration::hours(1);
    let requester =
        peer_manager::testonly::start(clock.clone(), TestDB::new(), cfg, chain.clone()).await;

    // Nothing listens on this host's address.
    let host = chain.make_config(rng);
    request_not_sent(&requester, &host, **host.node_addr.as_ref().unwrap(), rng).await;

    // A different node listens on this host's address, and rejects the handshake meant for the host.
    let other_cfg = chain.make_config(rng);
    let other =
        peer_manager::testonly::start(clock.clone(), TestDB::new(), other_cfg, chain.clone()).await;
    let host = chain.make_config(rng);
    request_not_sent(&requester, &host, other.peer_info().addr.unwrap(), rng).await;
}

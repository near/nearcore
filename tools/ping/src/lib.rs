use anyhow::Context;
use axum::Router;
use axum::routing::get;
pub use cli::PingCommand;
use near_network::raw::{ConnectError, Connection, Message, RoutedMessage};
use near_network::types::HandshakeFailureReason;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::ProtocolVersion;
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::pin::Pin;
use time::ext::InstantExt as _;

pub mod cli;
mod csv;
mod metrics;

// TODO: also log number of bytes/other messages (like Blocks) received?
#[derive(Debug, Default)]
struct PingStats {
    pings_sent: usize,
    pongs_received: usize,
    // TODO: these latency stats could be separated into time to first byte
    // + time to last byte, etc.
    min_latency: near_time::Duration,
    max_latency: near_time::Duration,
    average_latency: near_time::Duration,
}

impl PingStats {
    fn pong_received(&mut self, latency: near_time::Duration) {
        self.pongs_received += 1;

        if self.min_latency == near_time::Duration::ZERO || self.min_latency > latency {
            self.min_latency = latency;
        }
        if self.max_latency < latency {
            self.max_latency = latency;
        }
        let n = self.pongs_received as u32;
        self.average_latency = ((n - 1) * self.average_latency + latency) / n;
    }
}

type Nonce = u64;

#[derive(Debug, Eq, PartialEq)]
struct PingTarget {
    peer_id: PeerId,
    last_pinged: Option<near_time::Instant>,
}

impl PartialOrd for PingTarget {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTarget {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match &self.last_pinged {
            Some(my_last_pinged) => match &other.last_pinged {
                Some(their_last_pinged) => my_last_pinged
                    .cmp(their_last_pinged)
                    .then_with(|| self.peer_id.cmp(&other.peer_id)),
                None => cmp::Ordering::Greater,
            },
            None => match &other.last_pinged {
                Some(_) => cmp::Ordering::Less,
                None => self.peer_id.cmp(&other.peer_id),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PingTimeout {
    peer_id: PeerId,
    nonce: u64,
    timeout: near_time::Instant,
}

impl PartialOrd for PingTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTimeout {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.timeout.cmp(&other.timeout).then_with(|| {
            self.nonce.cmp(&other.nonce).then_with(|| self.peer_id.cmp(&other.peer_id))
        })
    }
}

const MAX_PINGS_IN_FLIGHT: usize = 10;
const PING_TIMEOUT: near_time::Duration = near_time::Duration::seconds(100);

#[derive(Debug)]
struct PingState {
    stats: PingStats,
    last_pinged: Option<near_time::Instant>,
}

struct PingTimes {
    sent_at: near_time::Instant,
    timeout: near_time::Instant,
}

struct AppInfo {
    stats: HashMap<PeerId, PingState>,
    // we will ping targets in round robin fashion. So this keeps a set of
    // targets ordered by when we last pinged them.
    requests: BTreeMap<PingTarget, HashMap<Nonce, PingTimes>>,
    timeouts: BTreeSet<PingTimeout>,
    chain_id: String,
}

impl AppInfo {
    fn new(chain_id: &str) -> Self {
        Self {
            stats: HashMap::new(),
            requests: BTreeMap::new(),
            timeouts: BTreeSet::new(),
            chain_id: chain_id.to_owned(),
        }
    }

    fn pick_next_target(&self) -> Option<PeerId> {
        for (target, pending_pings) in &self.requests {
            if pending_pings.len() < MAX_PINGS_IN_FLIGHT {
                return Some(target.peer_id.clone());
            }
        }
        None
    }

    fn ping_sent(&mut self, peer_id: &PeerId, nonce: u64, chain_id: &str) {
        let timestamp = near_time::Instant::now();
        let timeout = timestamp + PING_TIMEOUT;

        crate::metrics::PING_SENT.with_label_values(&[chain_id, &peer_id.to_string()]).inc();

        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                let mut pending_pings = self
                    .requests
                    .remove(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                println!("send ping --------------> {}", peer_id);

                state.stats.pings_sent += 1;
                state.last_pinged = Some(timestamp);

                match pending_pings.entry(nonce) {
                    Entry::Occupied(_) => {
                        tracing::warn!(
                            target: "ping", %nonce, ?peer_id, "internal error, sent two pings with same nonce, \
                            latency stats will probably be wrong"
                        );
                    }
                    Entry::Vacant(e) => {
                        e.insert(PingTimes { sent_at: timestamp, timeout });
                    }
                };
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: Some(timestamp) },
                    pending_pings,
                );
                self.timeouts.insert(PingTimeout { peer_id: peer_id.clone(), nonce, timeout })
            }
            Entry::Vacant(_) => {
                panic!("sent ping to {:?}, but not present in stats HashMap", peer_id)
            }
        };
    }

    fn pong_received(
        &mut self,
        peer_id: &PeerId,
        nonce: u64,
        received_at: near_time::Instant,
    ) -> Option<near_time::Duration> {
        match self.stats.get_mut(peer_id) {
            Some(state) => {
                let pending_pings = self
                    .requests
                    .get_mut(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                match pending_pings.remove(&nonce) {
                    Some(times) => {
                        let latency = received_at.signed_duration_since(times.sent_at);
                        state.stats.pong_received(latency);
                        assert!(self.timeouts.remove(&PingTimeout {
                            peer_id: peer_id.clone(),
                            nonce,
                            timeout: times.timeout,
                        }));

                        let l: std::time::Duration = latency.try_into().unwrap();
                        println!("recv pong <-------------- {} latency: {:?}", peer_id, l);
                        Some(latency)
                    }
                    None => {
                        tracing::warn!(
                            target: "ping",
                            %nonce, peer = %peer_id,
                            "received pong after probably treating it as timed out previously"
                        );
                        None
                    }
                }
            }
            None => {
                tracing::warn!(target: "ping", ?peer_id, "received pong but don't know of this peer");
                None
            }
        }
    }

    fn pop_timeout(&mut self, t: &PingTimeout) {
        assert!(self.timeouts.remove(&t));
        let state = self.stats.get(&t.peer_id).unwrap();

        let pending_pings = self
            .requests
            .get_mut(&PingTarget { peer_id: t.peer_id.clone(), last_pinged: state.last_pinged })
            .unwrap();
        assert!(pending_pings.remove(&t.nonce).is_some());
        println!("{} timeout after {} ---------", t.peer_id, PING_TIMEOUT);
    }

    fn add_peer(&mut self, peer_id: PeerId) {
        if let Entry::Vacant(e) = self.stats.entry(peer_id.clone()) {
            e.insert(PingState { last_pinged: None, stats: PingStats::default() });
            self.requests.insert(PingTarget { peer_id, last_pinged: None }, HashMap::new());
        }
    }
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: Message,
    received_at: near_time::Instant,
    latencies_csv: Option<&mut crate::csv::LatenciesCsv>,
) -> anyhow::Result<()> {
    if let Message::Routed(RoutedMessage::Pong { nonce, source }) = msg {
        if let Some(latency) = app_info.pong_received(&source, nonce, received_at) {
            crate::metrics::PONG_RECEIVED
                .with_label_values(&[&app_info.chain_id, &source.to_string()])
                .observe(latency.as_seconds_f64());
            if let Some(csv) = latencies_csv {
                csv.write(&source, latency).context("Failed writing to CSV file")?;
            }
        }
    }
    Ok(())
}

fn collect_stats(app_info: AppInfo, ping_stats: &mut Vec<(PeerId, PingStats)>) {
    for (peer_id, state) in app_info.stats {
        ping_stats.push((peer_id, state.stats));
    }
}

fn prepare_timeout(sleep: Pin<&mut tokio::time::Sleep>, app_info: &AppInfo) -> Option<PingTimeout> {
    if let Some(t) = app_info.timeouts.iter().next() {
        sleep.reset(tokio::time::Instant::from_std(t.timeout.try_into().unwrap()));
        Some(t.clone())
    } else {
        None
    }
}

async fn ping_via_node(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    ttl: u8,
    ping_frequency_millis: u64,
    recv_timeout_seconds: u32,
    mut latencies_csv: Option<crate::csv::LatenciesCsv>,
    ping_stats: &mut Vec<(PeerId, PingStats)>,
    prometheus_addr: &str,
    just_handshake: bool,
) -> anyhow::Result<()> {
    let mut app_info = AppInfo::new(chain_id);

    app_info.add_peer(peer_id.clone());

    let clock = near_time::Clock::real();

    let mut peer = match Connection::connect(
        &clock,
        peer_addr,
        peer_id,
        protocol_version,
        chain_id,
        genesis_hash,
        head_height,
        vec![ShardId::new(0)],
        Some(near_time::Duration::seconds(recv_timeout_seconds.into())),
    )
    .await
    {
        Ok(p) => p,
        Err(ConnectError::HandshakeFailure(reason)) => match reason {
            HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => anyhow::bail!(
                "Received Handshake Failure: {:?}. Try running again with --protocol-version between {} and {}",
                reason,
                oldest_supported_version,
                version
            ),
            HandshakeFailureReason::GenesisMismatch(_) => anyhow::bail!(
                "Received Handshake Failure: {:?}. Try running again with --chain-id and --genesis-hash set to these values.",
                reason,
            ),
            HandshakeFailureReason::InvalidTarget => anyhow::bail!(
                "Received Handshake Failure: {:?}. Is the public key given with --peer correct?",
                reason,
            ),
        },
        Err(e) => {
            anyhow::bail!("Error connecting to {:?}: {}", peer_addr, e);
        }
    };

    // If just_handshake is true, exit after successful connection
    if just_handshake {
        println!("Handshake completed successfully. Exiting as requested.");
        return Ok(());
    }

    let mut result = Ok(());
    let mut nonce = 1;
    let next_ping = tokio::time::sleep(std::time::Duration::ZERO);
    tokio::pin!(next_ping);
    let next_timeout = tokio::time::sleep(std::time::Duration::ZERO);
    tokio::pin!(next_timeout);

    let app = Router::new().route("/metrics", get(near_jsonrpc::prometheus_handler));
    let listener = tokio::net::TcpListener::bind(prometheus_addr).await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    loop {
        let target = app_info.pick_next_target();
        let pending_timeout = prepare_timeout(next_timeout.as_mut(), &app_info);

        tokio::select! {
            _ = &mut next_ping, if target.is_some() => {
                let target = target.unwrap();
                result = peer.send_routed_message(RoutedMessage::Ping{nonce}, target.clone(), ttl)
                            .await.with_context(|| format!("Failed sending ping to {:?}", &target));
                if result.is_err() {
                    break;
                }
                app_info.ping_sent(&target, nonce, &chain_id);
                nonce += 1;
                next_ping.as_mut().reset(tokio::time::Instant::now() + std::time::Duration::from_millis(ping_frequency_millis));
            }
            res = peer.recv() => {
                let (msg, first_byte_time) = match res {
                    Ok(x) => x,
                    Err(e) => {
                        result = Err(e).context("Failed receiving messages");
                        break;
                    }
                };
                result = handle_message(
                            &mut app_info,
                            msg,
                            first_byte_time.try_into().unwrap(),
                            latencies_csv.as_mut()
                        );
                if result.is_err() {
                    break;
                }
            }
            _ = &mut next_timeout, if pending_timeout.is_some() => {
                let t = pending_timeout.unwrap();
                app_info.pop_timeout(&t);
                crate::metrics::PONG_TIMEOUTS.with_label_values(&[chain_id, &t.peer_id.to_string()]).inc();
                if let Some(csv) = latencies_csv.as_mut() {
                    result = csv.write_timeout(&t.peer_id).context("Failed writing to CSV file");
                    if result.is_err() {
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
    collect_stats(app_info, ping_stats);
    result
}

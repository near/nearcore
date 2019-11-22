use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::net::TcpListener;
use std::time::{Duration, Instant};

use actix::{Actor, AsyncContext, Context, System};
use byteorder::{ByteOrder, LittleEndian};
use futures::future;
use futures::future::Future;
use rand::{thread_rng, RngCore};
use tokio::timer::Delay;

use near_crypto::{KeyType, SecretKey};
use near_primitives::hash::hash;
use near_primitives::types::EpochId;

use crate::types::{NetworkConfig, PeerId, PeerInfo};

/// Returns available port.
pub fn open_port() -> u16 {
    // use port 0 to allow the OS to assign an open port
    // TcpListener's Drop impl will unbind the port as soon as
    // listener goes out of scope
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

impl NetworkConfig {
    /// Returns network config with given seed used for peer id.
    pub fn from_seed(seed: &str, port: u16) -> Self {
        let secret_key = SecretKey::from_seed(KeyType::ED25519, seed);
        let public_key = secret_key.public_key();
        NetworkConfig {
            public_key,
            secret_key,
            account_id: Some(seed.to_string()),
            addr: Some(format!("0.0.0.0:{}", port).parse().unwrap()),
            boot_nodes: vec![],
            handshake_timeout: Duration::from_secs(60),
            reconnect_delay: Duration::from_secs(60),
            bootstrap_peers_period: Duration::from_millis(100),
            peer_max_count: 10,
            ban_window: Duration::from_secs(1),
            peer_expiration_duration: Duration::from_secs(60 * 60),
            max_send_peers: 512,
            peer_stats_period: Duration::from_secs(5),
            ttl_account_id_router: Duration::from_secs(60 * 60),
            max_routes_to_store: 1,
            most_weighted_peer_height_horizon: 5,
        }
    }
}

pub fn convert_boot_nodes(boot_nodes: Vec<(&str, u16)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, port) in boot_nodes {
        let id = SecretKey::from_seed(KeyType::ED25519, peer_seed).public_key();
        result.push(PeerInfo::new(id.into(), format!("127.0.0.1:{}", port).parse().unwrap()))
    }
    result
}

impl PeerId {
    pub fn random() -> Self {
        SecretKey::from_random(KeyType::ED25519).public_key().into()
    }
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn random() -> Self {
        PeerInfo { id: PeerId::random(), addr: None, account_id: None }
    }
}

/// Timeouts by stopping system without any condition and raises panic.
/// Useful in tests to prevent them from running forever.
#[allow(unreachable_code)]
pub fn wait_or_panic(max_wait_ms: u64) {
    actix::spawn(Delay::new(Instant::now() + Duration::from_millis(max_wait_ms)).then(|_| {
        System::current().stop();
        panic!("Timeout exceeded.");
        future::result(Ok(()))
    }));
}

/// Waits until condition or timeouts with panic.
/// Use in tests to check for a condition and stop or fail otherwise.
///
/// # Example
///
/// ```
/// use actix::{System, Actor};
/// use near_network::test_utils::WaitOrTimeout;
/// use std::time::{Instant, Duration};
///
/// System::run(|| {
///     let start = Instant::now();
///     WaitOrTimeout::new(Box::new(move |ctx| {
///             if start.elapsed() > Duration::from_millis(10) {
///                 System::current().stop()
///             }
///         }),
///         1000,
///         60000,
///     ).start();
/// }).unwrap();
/// ```
pub struct WaitOrTimeout {
    f: Box<dyn FnMut(&mut Context<WaitOrTimeout>)>,
    check_interval_ms: u64,
    max_wait_ms: u64,
    ms_slept: u64,
}

impl WaitOrTimeout {
    pub fn new(
        f: Box<dyn FnMut(&mut Context<WaitOrTimeout>)>,
        check_interval_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        WaitOrTimeout { f, check_interval_ms, max_wait_ms, ms_slept: 0 }
    }

    fn wait_or_timeout(&mut self, ctx: &mut Context<Self>) {
        (self.f)(ctx);
        ctx.run_later(Duration::from_millis(self.check_interval_ms), move |act, ctx| {
            act.ms_slept += act.check_interval_ms;
            if act.ms_slept > act.max_wait_ms {
                println!("BBBB Slept {}; max_wait_ms {}", act.ms_slept, act.max_wait_ms);
                panic!("Timed out waiting for the condition");
            }
            act.wait_or_timeout(ctx);
        });
    }
}

impl Actor for WaitOrTimeout {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.wait_or_timeout(ctx);
    }
}

pub fn vec_ref_to_str(values: Vec<&str>) -> Vec<String> {
    values.iter().map(|x| x.to_string()).collect()
}

pub fn random_peer_id() -> PeerId {
    let sk = SecretKey::from_random(KeyType::ED25519);
    sk.public_key().into()
}

pub fn random_epoch_id() -> EpochId {
    let mut buffer = [0u8; size_of::<u64>()];
    LittleEndian::write_u64(&mut buffer, thread_rng().next_u64());
    EpochId(hash(buffer.as_ref()))
}

pub fn expected_routing_tables(
    current: HashMap<PeerId, HashSet<PeerId>>,
    expected: Vec<(PeerId, Vec<PeerId>)>,
) -> bool {
    if current.len() != expected.len() {
        return false;
    }

    for (peer, paths) in expected.into_iter() {
        let cur_paths = current.get(&peer);
        if !cur_paths.is_some() {
            return false;
        }
        let cur_paths = cur_paths.unwrap();
        if cur_paths.len() != paths.len() {
            return false;
        }
        for next_hop in paths.into_iter() {
            if !cur_paths.contains(&next_hop) {
                return false;
            }
        }
    }

    true
}

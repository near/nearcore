use std::net::TcpListener;
use std::time::{Duration, Instant};

use actix::{Actor, AsyncContext, Context, System};
use futures::future::Future;
use tokio::timer::Delay;

use near_primitives::crypto::signature::get_key_pair;
use near_primitives::test_utils::get_key_pair_from_seed;

use crate::types::{NetworkConfig, PeerInfo};
use futures::future;

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
        let (public_key, secret_key) = get_key_pair_from_seed(seed);
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
        }
    }
}

pub fn convert_boot_nodes(boot_nodes: Vec<(&str, u16)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, port) in boot_nodes {
        let (id, _) = get_key_pair_from_seed(peer_seed);
        result.push(PeerInfo::new(id.into(), format!("127.0.0.1:{}", port).parse().unwrap()))
    }
    result
}

impl PeerInfo {
    /// Creates random peer info.
    pub fn random() -> Self {
        let (id, _) = get_key_pair();
        PeerInfo { id: id.into(), addr: None, account_id: None }
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

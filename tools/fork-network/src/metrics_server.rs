//! A `/metrics` endpoint for `neard fork-network`.
//!
//! Tools in this repo get metrics served one of two ways. Long-running ones that host a node
//! — `tools/mirror` via `nearcore::start_with_config` — get `/metrics` for free, because
//! starting a node starts `near_jsonrpc::start_http`. Standalone ones that never build a node
//! have to serve it themselves; `tools/ping` is the precedent.
//!
//! `fork-network` is the second kind. It binds no socket at all today, so the metrics its
//! phases already register — memtrie arena memory, RocksDB op latency, flat storage — are
//! mutated for hours and are unreadable. This serves them, reusing the same handler
//! `near_jsonrpc` exposes for `neard run` rather than re-encoding the registry by hand.

use axum::Router;
use axum::routing::get;
use std::net::SocketAddr;
use std::thread::Builder;
use tokio::net::TcpListener;
use tokio::runtime;

/// Starts a `/metrics` server on `addr` and detaches it. An empty `addr` disables the server.
///
/// The server gets its own OS thread with its own runtime, and deliberately does not use the
/// runtime `ForkNetworkCommand::run()` builds. That one is `new_current_thread()` and its only
/// task spends hours inside synchronous state rewriting, so a `tokio::spawn`ed server would
/// never be polled: it would answer in a unit test and time out in production for exactly the
/// hours we need it. This is the one place `tools/ping` should not be copied — it spawns onto
/// the main runtime, but only because it builds a multi-thread one.
///
/// Failures to bind or serve are logged and otherwise ignored. Observability must not become a
/// new way for image creation to fail.
pub(crate) fn spawn(addr: &str) {
    if addr.is_empty() {
        tracing::info!("metrics server disabled");
        return;
    }
    let addr = addr.to_owned();
    let thread = Builder::new().name("fork-network-metrics".to_owned()).spawn(move || serve(&addr));
    if let Err(err) = thread {
        tracing::error!(?err, "failed spawning metrics server thread");
    }
}

fn serve(addr: &str) {
    let socket_addr = match addr.parse::<SocketAddr>() {
        Ok(socket_addr) => socket_addr,
        Err(err) => {
            tracing::error!(%addr, ?err, "not a valid metrics address");
            return;
        }
    };
    let runtime = match runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::error!(?err, "failed building metrics runtime");
            return;
        }
    };
    runtime.block_on(async move {
        let listener = match TcpListener::bind(socket_addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(
                    %socket_addr,
                    ?err,
                    "failed binding metrics address, continuing without /metrics"
                );
                return;
            }
        };
        tracing::info!(%socket_addr, "serving /metrics");
        let app = Router::new().route("/metrics", get(near_jsonrpc::prometheus_handler));
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!(?err, "metrics server stopped");
        }
    });
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    // Imported as a module, not by item: a bare `spawn` would shadow this module's own.
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::runtime;

    fn scrape(addr: SocketAddr) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline, "metrics server never answered on {addr}");
            thread::sleep(Duration::from_millis(50));
            let Ok(mut stream) = TcpStream::connect(addr) else { continue };
            stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let request =
                format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
            if stream.write_all(request.as_bytes()).is_err() {
                continue;
            }
            let mut response = String::new();
            if stream.read_to_string(&mut response).is_ok() && !response.is_empty() {
                return response;
            }
        }
    }

    /// Reproduces the situation every fork-network phase is in: a `new_current_thread` runtime
    /// whose single task is blocked in synchronous work, exactly as `ForkNetworkCommand::run`
    /// builds it. Scraping happens from a second OS thread, since the one running the runtime
    /// is by construction unavailable.
    ///
    /// This is the regression guard for the starvation trap. Change `spawn` to put the server
    /// on the caller's runtime with `tokio::spawn` and the server never gets polled, the scrape
    /// never completes, and this test fails on the deadline rather than passing by luck.
    #[test]
    fn serves_metrics_while_the_callers_runtime_is_blocked() {
        // Take a port from the OS, then release it so the server can bind it.
        let addr = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        crate::metrics::init();

        let scraper = thread::spawn(move || scrape(addr));

        let blocking_runtime = runtime::Builder::new_current_thread().enable_all().build().unwrap();
        blocking_runtime.block_on(async move {
            super::spawn(&addr.to_string());
            // Synchronous work inside `block_on`, never yielding to the runtime — a fork phase
            // does this for hours.
            let until = Instant::now() + Duration::from_secs(2);
            while Instant::now() < until {
                thread::sleep(Duration::from_millis(20));
            }
        });

        let response = scraper.join().expect("scraper thread panicked");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("near_fork_network_phase"), "{response}");
    }
}

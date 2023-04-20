use std::sync::Arc;

struct TestAuthHandler;

impl turn::auth::AuthHandler for TestAuthHandler {
    fn auth_handle(
        &self,
        _username: &str,
        _realm: &str,
        _src_addr: std::net::SocketAddr,
    ) -> Result<Vec<u8>, turn::Error> {
        unimplemented!();
    }
}

/// STUN server. Use new() to spawn a new server, use close() to close it.
/// Remember to call close() explicitly, otherwise the server will keep running
/// in the background.
pub(crate) struct Server {
    inner: turn::server::Server,
    addr: std::net::SocketAddr,
}

impl Server {
    /// Spawns a new STUN server on localhost interface.
    /// In fact a TURN server is spawned, which implements a superset
    /// of STUN functionality.
    pub async fn new() -> Self {
        let server_conn = Arc::new(tokio::net::UdpSocket::bind("[::1]:0").await.unwrap());
        let server_addr = server_conn.local_addr().unwrap();

        Self {
            addr: server_addr,
            inner: turn::server::Server::new(turn::server::config::ServerConfig {
                conn_configs: vec![turn::server::config::ConnConfig {
                    conn: server_conn,
                    relay_addr_generator: Box::new(
                        turn::relay::relay_none::RelayAddressGeneratorNone {
                            address: "[::]".to_owned(),
                            net: Arc::new(webrtc_util::vnet::net::Net::new(None)),
                        },
                    ),
                }],
                realm: String::default(),
                auth_handler: Arc::new(TestAuthHandler),
                channel_bind_timeout: std::time::Duration::from_secs(0),
            })
            .await
            .unwrap(),
        }
    }

    pub fn addr(&self) -> super::ServerAddr {
        self.addr.to_string()
    }

    /// Closes the STUN server. close() is async so it cannot be implemented as Drop.
    pub async fn close(self) {
        self.inner.close().await.unwrap();
    }
}

use near_async::time;
use std::net::SocketAddr;
use std::sync::Arc;
use stun::message::Getter as _;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod testonly;

/// Address of the format "<domain/ip>:<port>" of STUN servers.
pub type ServerAddr = String;

pub(crate) type Error = stun::Error;

/// Convert from ServerAddr to SocketAddr via DNS resolution.
/// Looks for IPv4 or IPv6 according to `want_ipv4`.
pub(crate) async fn lookup_host(addr: &ServerAddr, want_ipv4: bool) -> Option<SocketAddr> {
    for socket_addr in tokio::net::lookup_host(addr).await.ok()? {
        if want_ipv4 == socket_addr.is_ipv4() {
            return Some(socket_addr);
        }
    }
    None
}

const QUERY_TIMEOUT: time::Duration = time::Duration::seconds(5);

/// Sends a STUN BINDING request to `addr`.
/// Returns the result of the query: the IP of this machine as perceived by the STUN server.
/// It should be used to determine the public IP of this machine.
pub(crate) async fn query(
    clock: &time::Clock,
    addr: &SocketAddr,
) -> Result<std::net::IpAddr, Error> {
    let socket = tokio::net::UdpSocket::bind("[::]:0").await?;
    socket.connect(addr).await?;
    let mut client = stun::client::ClientBuilder::new().with_conn(Arc::new(socket)).build()?;
    let mut msg = stun::message::Message::new();
    msg.new_transaction_id()?;
    msg.set_type(stun::message::BINDING_REQUEST);
    msg.build(&[])?;
    let (send, mut recv) = tokio::sync::mpsc::unbounded_channel();
    client.send(&msg, Some(Arc::new(send))).await?;
    // Note that both clock.sleep() and recv.recv() are cancellable,
    // so it is safe to use them in tokio::select!.
    let ip = tokio::select! {
        _ = clock.sleep(QUERY_TIMEOUT) => {
            return Err(Error::ErrTransactionTimeOut);
        }
        e = recv.recv() => match e {
            None => {
                // stun crate doesn't document whether and when it can happen.
                // We treat it as a failed STUN transaction and log an error because
                // it is not an expected behavior.
                tracing::error!("STUN client has closed the output channel before returning a response - this is unexpected");
                return Err(Error::ErrTransactionStopped);
            }
            Some(e) => {
                let mut addr = stun::xoraddr::XorMappedAddress::default();
                addr.get_from(&e.event_body?)?;
                addr.ip
            }
        }
    };
    client.close().await?;
    Ok(ip)
}

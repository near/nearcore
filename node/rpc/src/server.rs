use jsonrpc_core::IoHandler;
use jsonrpc_http_server::{DomainsValidation, Server, ServerBuilder};
use jsonrpc_http_server::cors::AccessControlAllowOrigin;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub fn get_server(io: IoHandler, addr: Option<SocketAddr>) -> Server {
    let addr = addr.unwrap_or_else(|| {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3030)
    });
    ServerBuilder::new(io)
        .cors(DomainsValidation::AllowOnly(vec![AccessControlAllowOrigin::Null]))
        .start_http(&addr)
        .expect("Unable to start RPC server")
}

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use crate::collector::Collector;

mod collector;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let collector = Collector::new();
    tonic::transport::Server::builder()
        .add_service(opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer::new(collector))
        .serve(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4317))).await?;
    Ok(())
}

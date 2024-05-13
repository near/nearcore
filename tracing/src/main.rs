use crate::collector::Collector;
use crate::db::Database;
use clap::Parser;
use queries::run_query_server;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

mod collector;
mod db;
mod primitives;
pub mod profile;
mod queries;

#[derive(clap::Parser, Debug)]
struct CollectorCmd {
    #[clap(long)]
    mongodb_uri: String,

    #[clap(long, default_value = "4317")]
    otlp_port: u16,
}

#[derive(clap::Parser, Debug)]
struct QuerierCmd {
    #[clap(long)]
    mongodb_uri: String,

    #[clap(long, default_value = "8080")]
    query_port: u16,
}

#[derive(clap::Parser, Debug)]
enum Cmd {
    Collector(CollectorCmd),
    Querier(QuerierCmd),
}

impl CollectorCmd {
    async fn run(&self) -> anyhow::Result<()> {
        let db = Database::new(&self.mongodb_uri, true).await;
        let collector = Collector::new(db);
        tonic::transport::Server::builder()
        .add_service(opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer::new(collector))
        .serve(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.otlp_port))).await?;
        Ok(())
    }
}

impl QuerierCmd {
    async fn run(&self) -> anyhow::Result<()> {
        let db = Database::new(&self.mongodb_uri, false).await;
        run_query_server(db, self.query_port).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd = Cmd::parse();
    match cmd {
        Cmd::Collector(collector) => collector.run().await?,
        Cmd::Querier(querier) => querier.run().await?,
    }
    Ok(())
}

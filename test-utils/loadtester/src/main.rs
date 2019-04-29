use crate::transactions_executor::Executor;
use env_logger::Builder;
use std::net::SocketAddr;
use std::str::FromStr;

pub mod remote_node;
pub mod sampler;
pub mod transactions_executor;
pub mod transactions_generator;
use remote_node::RemoteNode;

#[allow(dead_code)]
fn configure_logging(log_level: log::LevelFilter) {
    let internal_targets = vec!["observer"];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });
    builder.default_format_timestamp_nanos(true);
    builder.try_init().unwrap();
}

fn main() {
    configure_logging(log::LevelFilter::Debug);
    let nodes =
        vec![RemoteNode::new(SocketAddr::from_str("127.0.0.1:3030").unwrap(), &vec!["near.0"], 1)];
    // Start the executor.
    let handle = Executor::spawn(nodes, None, 700);
    handle.join().unwrap();
}

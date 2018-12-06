extern crate client;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate network;
extern crate parking_lot;
extern crate primitives;
extern crate node_runtime;
extern crate tokio;

use client::Client;
use env_logger::Builder;
use futures::Future;
use primitives::traits::GenericResult;
use produce_blocks::generate_produce_blocks_task;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime;

mod produce_blocks;
#[cfg(feature = "test-utils")]
pub mod test_utils;

pub fn run_service(
    client: &Arc<Client>,
    network_task: impl Future<Item=(), Error=()>,
    produce_blocks_interval: Duration,
) -> GenericResult {
    let mut builder = Builder::new();
    builder.filter(Some("runtime"), log::LevelFilter::Debug);
    builder.filter(Some("service"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();

    let mut current_thread = runtime::current_thread::Runtime::new().unwrap();
    let produce_blocks_task = generate_produce_blocks_task(
        &client,
        produce_blocks_interval,
    );
    let tasks: Vec<Box<Future<Item=(), Error=()>>> = vec![
        Box::new(network_task),
        Box::new(produce_blocks_task),
    ];
    let task = futures::select_all(tasks)
        .and_then(move |_| {
            info!("Service task failed");
            Ok(())
        }).map_err(|(r, _, _)| r)
        .map_err(|e| {
            debug!(target: "service", "service error: {:?}", e);
        });
    Ok(current_thread.block_on(task).unwrap())
}

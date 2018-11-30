use client::test_utils::generate_test_client;
use run_service;
use std::sync::Arc;
use std::time::Duration;
use network::test_utils::get_noop_network_task;

pub fn run_test_service() {
    let client = Arc::new(generate_test_client());
    let network_task = get_noop_network_task();
    let produce_blocks_interval_duration = Duration::from_secs(2);
    run_service(
        &client,
        network_task,
        produce_blocks_interval_duration,
    ).unwrap();
}

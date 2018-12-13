extern crate node_cli;

pub use node_cli::service::ServiceConfig;

pub fn start_devnet(service_config: Option<ServiceConfig>) {
    let service_config = service_config.unwrap_or_else(|| {
        ServiceConfig::default()
    });
    node_cli::service::start_service(
        service_config,
        node_cli::test_utils::spawn_pasthrough_consensus,
    );
}

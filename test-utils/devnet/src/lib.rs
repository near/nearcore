extern crate node_cli;
extern crate consensus;

pub use node_cli::service::ServiceConfig;
use consensus::passthrough;

pub fn start_devnet(service_config: Option<ServiceConfig>) {
    let service_config = service_config.unwrap_or_else(|| {
        ServiceConfig::default()
    });
    node_cli::service::start_service(
        service_config,
        passthrough::spawn_consensus,
    );
}

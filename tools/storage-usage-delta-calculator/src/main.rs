use near_chain_configs::{Genesis, GenesisValidationMode};
use near_o11y::ColorOutput;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::state_record::StateRecord;
use near_primitives::version::PROTOCOL_VERSION;
use node_runtime::Runtime;
use std::fs::File;
use tracing::debug;

/// Calculates delta between actual storage usage and one saved in state
/// output.json should contain dump of current state,
/// run 'neard --home ~/.near/mainnet/ view_state dump_state'
/// to get it
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let env_filter = near_o11y::EnvFilterBuilder::from_env().verbose(Some("")).finish().unwrap();
    let _subscriber = near_o11y::default_subscriber(env_filter, &ColorOutput::Auto).await.global();
    debug!(target: "storage-calculator", "Start");

    let genesis = Genesis::from_file("output.json", GenesisValidationMode::Full);
    debug!(target: "storage-calculator", "Genesis read");

    let config_store = RuntimeConfigStore::new(None);
    let config = config_store.get_config(PROTOCOL_VERSION);
    let storage_usage = Runtime::new().compute_storage_usage(&genesis.records.0[..], config);
    debug!(target: "storage-calculator", "Storage usage calculated");

    let mut result = Vec::new();
    for record in genesis.records.0 {
        if let StateRecord::Account { account_id, account } = record {
            let actual_storage_usage = storage_usage.get(&account_id).unwrap();
            let saved_storage_usage = account.storage_usage();
            let delta = actual_storage_usage - saved_storage_usage;
            if delta != 0 {
                debug!("{},{}", account_id, delta);
                result.push((account_id.clone(), delta));
            }
        }
    }
    serde_json::to_writer_pretty(&File::create("storage_usage_delta.json")?, &result)?;
    Ok(())
}

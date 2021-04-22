use near_chain_configs::Genesis;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::state_record::StateRecord;
use node_runtime::Runtime;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Error;

fn main() -> Result<(), Error> {
    println!("Start");

    let genesis = Genesis::from_file("output.json");
    let mut writer = OpenOptions::new().append(true).create(true).open("delta.csv").unwrap();

    println!("Genesis read");
    let storage_usage =
        Runtime::compute_storage_usage(&genesis.records.0[..], &RuntimeConfig::default());
    println!("Storage usage calculated");
    for record in genesis.records.0 {
        if let StateRecord::Account { account_id, account } = record {
            let actual_storage_usage = storage_usage.get(account_id.as_str()).unwrap();
            let saved_storage_usage = account.storage_usage();
            let delta = actual_storage_usage - saved_storage_usage;
            if delta != 0 {
                println!("{},{}", account_id, delta);
                writeln!(&mut writer, "{},{}", account_id, delta)?;
            }
        }
    }
    Ok(())
}

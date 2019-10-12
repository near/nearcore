//! Reads keys from a csv file and creates a genesis config.
use csv::Reader;
use near::GenesisConfig;
use node_runtime::StateRecord;
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn keys_to_genesis_config(keys_file: &Path) -> Result<GenesisConfig> {
    unimplemented!()
}

fn keys_to_state_records(keys_file: &Path) -> Result<Vec<StateRecord>> {
    let mut reader = Reader::from_path(keys_file)?;
    assert_eq!(
        reader.headers()?,
        vec!["AccountId", "PublicKey", "Amount"],
        "Expected different csv structure."
    );
    for record in reader.records() {
        let record = record?;
        let account_id = record.get(0).expect("Expected Account Id record");
    }
}

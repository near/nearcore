use near_chain_configs::{Genesis, GenesisValidationMode};

#[test]
fn test_load_genesis() {
    Genesis::from_file("res/mainnet_genesis.json", GenesisValidationMode::Full).unwrap();
}

use near_chain_configs::Genesis;

#[test]
fn test_load_genesis() {
    Genesis::from_file("res/mainnet_genesis.json");
}

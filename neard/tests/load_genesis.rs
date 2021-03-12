use near_chain_configs::Genesis;

#[test]
fn test_load_genesis() {
    #[cfg(feature = "protocol_feature_add_account_versions")]
    Genesis::from_file("res/genesis_for_test.json");
    #[cfg(not(feature = "protocol_feature_add_account_versions"))]
    Genesis::from_file("res/mainnet_genesis.json");
}

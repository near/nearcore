// TODO: implement storage for all protocol versions of epoch configs.

#[cfg(test)]
mod tests {
    use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};
    use near_primitives::version::PROTOCOL_VERSION;
    use std::fs;

    fn run(chain_id: &str) {
        let path = format!("../../core/parameters/res/epoch_configs/{}_genesis.json", chain_id);
        let content = fs::read_to_string(path).unwrap();
        let initial_epoch_config: EpochConfig = serde_json::from_str(&content).unwrap();
        let all_config =
            AllEpochConfig::new_with_test_overrides(true, initial_epoch_config, chain_id, None);
        let config = all_config.for_protocol_version(PROTOCOL_VERSION);

        let expected_path = format!("../../core/parameters/res/epoch_configs/{}.json", chain_id);
        let content = fs::read_to_string(expected_path).unwrap();
        let expected_config: EpochConfig = serde_json::from_str(&content).unwrap();
        assert_eq!(config, expected_config);
    }

    #[test]
    fn mainnet() {
        run("mainnet");
    }

    #[test]
    fn testnet() {
        run("testnet");
    }

    #[test]
    fn mocknet() {
        run("mocknet");
    }
}

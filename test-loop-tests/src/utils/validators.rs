use near_client::Client;

/// Get all validator account names for the latest epoch.
pub(crate) fn get_epoch_all_validators(client: &Client) -> Vec<String> {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let all_validators = client.epoch_manager.get_epoch_all_validators(&epoch_id).unwrap();
    all_validators.into_iter().map(|vs| vs.account_id().to_string()).collect()
}

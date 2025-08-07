use near_chain_configs::test_genesis::ValidatorsSpec;
use near_primitives::types::AccountId;

pub fn create_account_id(s: &str) -> AccountId {
    s.parse().unwrap()
}

pub fn create_account_ids<const N: usize>(s: [&str; N]) -> [AccountId; N] {
    s.map(|s| create_account_id(s))
}

pub fn create_validator_id(idx: usize) -> AccountId {
    create_account_id(&format!("validator{idx}"))
}

pub fn create_validator_ids(count: usize) -> Vec<AccountId> {
    (0..count).map(create_validator_id).collect()
}

pub fn rpc_account_id() -> AccountId {
    "rpc".parse().unwrap()
}

pub fn create_validators_spec(
    num_block_and_chunk_producers: usize,
    num_chunk_validators_only: usize,
) -> ValidatorsSpec {
    let mut validator_ids =
        create_validator_ids(num_block_and_chunk_producers + num_chunk_validators_only);
    let chunk_validators_only = validator_ids.split_off(num_block_and_chunk_producers);
    let block_and_chunk_producers = validator_ids;
    assert_eq!(block_and_chunk_producers.len(), num_block_and_chunk_producers);
    assert_eq!(chunk_validators_only.len(), num_chunk_validators_only);
    ValidatorsSpec::DesiredRoles { block_and_chunk_producers, chunk_validators_only }
}

pub fn validators_spec_clients(spec: &ValidatorsSpec) -> Vec<AccountId> {
    let mut clients = vec![];
    match spec {
        ValidatorsSpec::DesiredRoles { block_and_chunk_producers, chunk_validators_only } => {
            clients.extend_from_slice(block_and_chunk_producers);
            clients.extend_from_slice(chunk_validators_only);
        }
        ValidatorsSpec::Raw { validators, .. } => {
            clients.extend(validators.iter().map(|info| info.account_id.clone()));
        }
    }
    clients
}

pub fn validators_spec_clients_with_rpc(spec: &ValidatorsSpec) -> Vec<AccountId> {
    let mut clients = validators_spec_clients(spec);
    clients.push(rpc_account_id());
    clients
}

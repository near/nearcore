//! Constructs control for Nightshade using the current Client state.
use client::Client;
use mempool::pool_task::MemPoolControl;
use primitives::types::AuthorityId;

pub fn get_control(client: &Client, block_index: u64) -> MemPoolControl {
    // TODO: Get authorities for the correct block index. For now these are the same authorities
    // that built the first block. In other words use `block_index` instead of `mock_block_index`.
    let mock_block_index = 2;
    let (owner_uid, uid_to_authority_map) = client.get_uid_to_authority_map(mock_block_index);
    if owner_uid.is_none() {
        return MemPoolControl::Stop;
    }
    let owner_uid = owner_uid.unwrap();
    let num_authorities = uid_to_authority_map.len();

    let mut public_keys = vec![];
    let mut bls_public_keys = vec![];
    for i in 0..num_authorities {
        info!("Authority #{}: account_id={:?} me={} block_index={}", i, uid_to_authority_map[&i].account_id, i == owner_uid, block_index);
        public_keys.push(uid_to_authority_map[&i].public_key);
        bls_public_keys.push(uid_to_authority_map[&i].bls_public_key.clone());
    }

    MemPoolControl::Reset {
        authority_id: owner_uid as AuthorityId,
        num_authorities,
        block_index,
        public_keys,
        bls_public_keys,
    }
}

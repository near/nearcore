use near_chain::types::Tip;
use near_client::Client;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::ShardId;
use near_store::{ShardUId, Trie};

/// Returns `true` if `client` is tracking the shard having the given `shard_id`.
pub fn client_tracking_shard(client: &Client, shard_id: ShardId, parent_hash: &CryptoHash) -> bool {
    let signer = client.validator_signer.get();
    let account_id = signer.as_ref().map(|s| s.validator_id());
    client.shard_tracker.cares_about_shard(account_id, parent_hash, shard_id, true)
}

// Finds the client who tracks the shard with `shard_id` among the list of `clients`.
pub fn get_client_tracking_shard<'a>(
    clients: &'a [&Client],
    tip: &Tip,
    shard_id: ShardId,
) -> &'a Client {
    for client in clients {
        if client_tracking_shard(client, shard_id, &tip.prev_block_hash) {
            return client;
        }
    }
    panic!(
        "get_client_tracking_shard() could not find client tracking shard {} at {} #{}",
        shard_id, &tip.last_block_hash, tip.height
    );
}

/// Prints the accounts inside all shards and asserts that no shard is empty.
pub fn print_and_assert_shard_accounts(clients: &[&Client], tip: &Tip) {
    let epoch_config = clients[0].epoch_manager.get_epoch_config(&tip.epoch_id).unwrap();
    for shard_uid in epoch_config.shard_layout.shard_uids() {
        let client = get_client_tracking_shard(clients, tip, shard_uid.shard_id());
        let chunk_extra = client.chain.get_chunk_extra(&tip.prev_block_hash, &shard_uid).unwrap();
        let trie = client
            .runtime_adapter
            .get_trie_for_shard(
                shard_uid.shard_id(),
                &tip.prev_block_hash,
                *chunk_extra.state_root(),
                false,
            )
            .unwrap();
        let mut shard_accounts = vec![];
        for item in trie.lock_for_iter().iter().unwrap() {
            let (key, value) = item.unwrap();
            let state_record = StateRecord::from_raw_key_value(&key, value);
            if let Some(StateRecord::Account { account_id, .. }) = state_record {
                shard_accounts.push(account_id.to_string());
            }
        }
        println!("accounts for shard {}: {:?}", shard_uid, shard_accounts);
        assert!(!shard_accounts.is_empty());
    }
}

/// Get the Memtrie of a shard at a certain block hash.
pub fn get_memtrie_for_shard(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) -> Trie {
    let state_root =
        *client.chain.get_chunk_extra(prev_block_hash, shard_uid).unwrap().state_root();

    // Here memtries will be used as long as client has memtries enabled.
    let memtrie = client
        .runtime_adapter
        .get_trie_for_shard(shard_uid.shard_id(), prev_block_hash, state_root, false)
        .unwrap();
    assert!(memtrie.has_memtries());
    memtrie
}

// We want to understand if the most recent block is the first block with the
// new shard layout. This is also the block immediately after the resharding
// block. To do this check if the latest block is an epoch start and compare the
// two epochs' shard layouts.
pub fn this_block_has_new_shard_layout(epoch_manager: &dyn EpochManagerAdapter, tip: &Tip) -> bool {
    if !epoch_manager.is_next_block_epoch_start(&tip.prev_block_hash).unwrap() {
        return false;
    }

    let prev_epoch_id = epoch_manager.get_epoch_id(&tip.prev_block_hash).unwrap();
    let this_epoch_id = epoch_manager.get_epoch_id(&tip.last_block_hash).unwrap();

    let prev_shard_layout = epoch_manager.get_shard_layout(&prev_epoch_id).unwrap();
    let this_shard_layout = epoch_manager.get_shard_layout(&this_epoch_id).unwrap();

    this_shard_layout != prev_shard_layout
}

// We want to understand if the most recent block is a resharding block. To do
// this check if the latest block is an epoch start and compare the two epochs'
// shard layouts.
pub fn next_block_has_new_shard_layout(epoch_manager: &dyn EpochManagerAdapter, tip: &Tip) -> bool {
    if !epoch_manager.is_next_block_epoch_start(&tip.last_block_hash).unwrap() {
        return false;
    }

    next_epoch_has_new_shard_layout(epoch_manager, tip)
}

pub fn next_epoch_has_new_shard_layout(epoch_manager: &dyn EpochManagerAdapter, tip: &Tip) -> bool {
    let this_shard_layout = epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let next_shard_layout = epoch_manager.get_shard_layout(&tip.next_epoch_id).unwrap();

    this_shard_layout != next_shard_layout
}

pub fn shard_was_split(shard_layout: &ShardLayout, shard_id: ShardId) -> bool {
    let Ok(parent) = shard_layout.get_parent_shard_id(shard_id) else {
        return false;
    };
    parent != shard_id
}

pub fn get_tracked_shards_from_prev_block(
    client: &Client,
    prev_block_hash: &CryptoHash,
) -> Vec<ShardUId> {
    let signer = client.validator_signer.get();
    let account_id = signer.as_ref().map(|s| s.validator_id());
    let shard_layout =
        client.epoch_manager.get_shard_layout_from_prev_block(prev_block_hash).unwrap();
    let mut tracked_shards = vec![];
    for shard_uid in shard_layout.shard_uids() {
        if client.shard_tracker.cares_about_shard(
            account_id,
            prev_block_hash,
            shard_uid.shard_id(),
            true,
        ) {
            tracked_shards.push(shard_uid);
        }
    }
    tracked_shards
}

pub fn get_tracked_shards(client: &Client, block_hash: &CryptoHash) -> Vec<ShardUId> {
    let block_header = client.chain.get_block_header(block_hash).unwrap();
    get_tracked_shards_from_prev_block(client, block_header.prev_hash())
}

pub fn get_shards_will_care_about(client: &Client, block_hash: &CryptoHash) -> Vec<ShardUId> {
    let signer = client.validator_signer.get();
    let account_id = signer.as_ref().map(|s| s.validator_id());
    let block_header = client.chain.get_block_header(block_hash).unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&block_header.epoch_id()).unwrap();
    let mut shards_needs_for_next_epoch = vec![];
    for shard_uid in shard_layout.shard_uids() {
        if client.shard_tracker.will_care_about_shard(
            account_id,
            &block_header.prev_hash(),
            shard_uid.shard_id(),
            true,
        ) {
            shards_needs_for_next_epoch.push(shard_uid);
        }
    }
    shards_needs_for_next_epoch
}

use near_chain::{ChainStore, ChainStoreAccess, RuntimeWithEpochManagerAdapter};
use near_primitives::{
    account::Account,
    borsh::{BorshDeserialize, BorshSerialize},
    challenge::PartialState,
    hash::{self, CryptoHash},
    receipt::DelayedReceiptIndices,
    state::ValueRef,
    state_part::PartId,
    syncing::get_num_state_parts,
    trie_key::{trie_key_parsers::parse_account_id_from_raw_key, TrieKey},
    types::{AccountId, StateRoot},
};
use near_store::{
    flat_state::store_helper::{self, get_flat_head},
    trie::{insert_delete::NodesStorage, StorageHandle},
    DBCol, NibbleSlice, NodeStorage, RawTrieNode, RawTrieNodeWithSize, StorageError, Store, Trie,
    TrieStorage,
};
use nearcore::{load_config, NearConfig, NightshadeRuntime};
use std::{collections::HashMap, fs::File, str};
use std::{collections::HashSet, fmt::Write};
use std::{path::Path, str::FromStr, sync::Arc};

pub struct PartOfTrie {
    memory: NodesStorage,
    trie: Trie,
    root_node: StorageHandle,
}

impl PartOfTrie {
    pub fn new() -> Self {
        let mock_storage = MockTrieStorage { entries: HashMap::new() };
        let root_node_hash = StateRoot::new();
        let in_memory_trie = Trie::new(Box::new(mock_storage), root_node_hash, None);
        let mut memory = NodesStorage::new();

        let root_node = in_memory_trie.move_node_to_mutable(&mut memory, &root_node_hash).unwrap();
        PartOfTrie { memory, trie: in_memory_trie, root_node }
    }
    pub fn insert(&mut self, key: &[u8], value: Vec<u8>) {
        let key = NibbleSlice::new(key);
        self.root_node = self.trie.insert(&mut self.memory, self.root_node, key, value).unwrap();
    }

    // This should also verify...
    pub fn add_boundaries() {}

    pub fn serialize_to_part() {}

    pub fn parse_from_part(contents: Vec<u8>) -> Result<Self, std::io::Error> {
        let partial_state = PartialState::try_from_slice(&contents)?;
        println!("Parts: {:?} ", partial_state.0.len());
        let mut parsed = 0;
        let mut failed = 0;

        let mut content_map = HashMap::new();

        for part in partial_state.0.iter() {
            if RawTrieNodeWithSize::decode(part).is_ok() {
                parsed += 1;
            } else {
                failed += 1;
            }
            println!("Hash: {:?}", near_primitives::hash::hash(part));
            content_map.insert(near_primitives::hash::hash(part), part.to_vec());
        }
        println!("Parsed: {:?} failed: {:?}", parsed, failed);

        let mock_storage = MockTrieStorage { entries: content_map };
        let root_node_hash =
            CryptoHash::from_str("4n75a4ZdvE2L4dw99nWdSaAgCianzer4TJuVzEXYiePy").unwrap();
        let in_memory_trie = Trie::new(Box::new(mock_storage), root_node_hash, None);

        // Try finding left & right boundaries in here.
        todo!()
    }

    pub fn validate() {}
}

mod test {
    use std::{fs::File, io::Read};

    use crate::PartOfTrie;

    #[test]
    fn basic_test() {
        let mut part = PartOfTrie::new();
        part.insert("hello".as_bytes(), "How are you".into());
    }

    #[test]
    fn parse_test() {
        let mut file =
            File::open("~/tmp/0126_state_with_flat/node0/state_parts/state_part_000004").unwrap();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();

        let part = PartOfTrie::parse_from_part(contents).unwrap();
    }
}
/// Calculates delta between actual storage usage and one saved in state
/// output.json should contain dump of current state,
/// run 'neard --home ~/.near/mainnet/ view_state dump_state'
/// to get it

// Converts the list of Nibbles to a readable string.
fn nibbles_to_string(prefix: &[u8]) -> String {
    let (chunks, remainder) = near_stdx::as_chunks::<2, _>(prefix);
    let mut result = chunks
        .into_iter()
        .map(|chunk| (chunk[0] * 16) + chunk[1])
        .flat_map(|ch| std::ascii::escape_default(ch).map(char::from))
        .collect::<String>();
    if let Some(final_nibble) = remainder.first() {
        write!(&mut result, "\\x{:x}_", final_nibble).unwrap();
    }
    result
}

fn iter_flat_state_keys(
    store: &Store,
    from: &[u8],
    to: &Option<Vec<u8>>,
) -> Vec<(Box<[u8]>, Box<[u8]>)> {
    let mut result = Vec::new();
    for item in store.iter(DBCol::FlatState) {
        let (key, value) = item.unwrap();
        if key >= from.into() && to.as_ref().map_or(true, |x| *(*x) >= *key) {
            result.push((key, value));
        }
    }
    result
}

pub struct MockTrieStorage {
    entries: HashMap<CryptoHash, Vec<u8>>,
}

impl TrieStorage for MockTrieStorage {
    fn retrieve_raw_bytes(&self, hash: &CryptoHash) -> Result<Arc<[u8]>, near_store::StorageError> {
        self.entries
            .get(hash)
            .and_then(|a| Some(a.as_slice().into()))
            .ok_or(StorageError::StorageInternalError)
    }

    fn get_trie_nodes_count(&self) -> near_primitives::types::TrieNodesCount {
        todo!()
    }
}

fn main() {
    // Experiment with the delayed receipt.

    {
        let payload = DelayedReceiptIndices { first_index: 42, next_available_index: 42 };

        let serialized_payload = payload.try_to_vec().unwrap();

        println!("Hash of the delayed is {:?}", near_primitives::hash::hash(&serialized_payload));

        let leaf_node = RawTrieNode::Leaf(
            vec![32],
            serialized_payload.len().try_into().unwrap(),
            near_primitives::hash::hash(&serialized_payload),
        );

        let mut ooo = Vec::new();
        leaf_node.encode_into(&mut ooo);

        let key_cost = 1 * 2;
        let memory_usage_cost = 8;

        let memory_usage = serialized_payload.len() + 50 + ooo.len() + key_cost + memory_usage_cost;
        println!("Memory usage {:?}", memory_usage);
        /*TRIE_COSTS.node_cost
        + (key.len() as u64) * TRIE_COSTS.byte_of_key
        + Self::memory_usage_value(value, memory)*/
        let leaf_node_with_size =
            RawTrieNodeWithSize { node: leaf_node, memory_usage: memory_usage as u64 };

        let encoded_leaf_with_size = leaf_node_with_size.encode();

        println!("Memory usage of the final leaf: {:?}", encoded_leaf_with_size.len());

        println!("Hash of leaf: {:?}", near_primitives::hash::hash(&encoded_leaf_with_size));

        /*let delayed_receipt_index = TrieKey::DelayedReceiptIndices;

        let index: u64 = 0;
        index.to_le_bytes()*/
    }

    let home_dir = "/home/cyfra/tmp/0126_state_with_flat/node0";
    let home_dir = Path::new(home_dir);
    let config = Default::default();
    let near_config =
        load_config(home_dir, near_chain_configs::GenesisValidationMode::Full).unwrap();
    let opener = NodeStorage::opener(home_dir, &config, None);

    let store = opener.open_in_mode(near_store::Mode::ReadOnly).unwrap();

    let store = store.get_hot_store();

    let res = store.get(DBCol::DbVersion, b"VERSION");

    tracing::error!("Got {:?}", res);
    println!("Hello {:?} ", res);

    let flat_head = get_flat_head(&store, 0);

    println!("Flat head {:?}", flat_head);

    let mut chain_store = ChainStore::new(store.clone(), 0, false);
    let shard_id = 0;

    let sync_prev_hash =
        CryptoHash::from_str("9eZNkmT59phfBdNhFu4n2e874vwKwut2fxVxzkgU6aYG").unwrap();

    let sync_prev_block = chain_store.get_block(&sync_prev_hash).unwrap();

    println!("Got block {:?}", sync_prev_block);

    let state_root = sync_prev_block.chunks()[shard_id as usize].prev_state_root();
    println!("State root is {:?}", state_root);

    let runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));

    let state_root_node =
        runtime_adapter.get_state_root_node(shard_id, &sync_prev_hash, &state_root).unwrap();
    let num_parts = get_num_state_parts(state_root_node.memory_usage);

    println!("Parts: {:?} State root node: {:?}", num_parts, state_root_node);

    let trie = runtime_adapter.get_view_trie_for_shard(0, &sync_prev_hash, state_root).unwrap();

    let existing_delayed_receipt =
        trie.get(&TrieKey::DelayedReceiptIndices.to_vec()).unwrap().unwrap();

    let mut tmp1 = &existing_delayed_receipt[..];

    println!("!! Existing: {:?} ", DelayedReceiptIndices::deserialize(&mut tmp1));

    let mut previous_key = Vec::new();

    let mut all_nodes = Vec::new();

    for part_id in 0..num_parts + 1 {
        let path = trie.find_path_for_part_boundary(part_id, num_parts).unwrap();
        let path_as_str = str::from_utf8(&path);
        let other_convert = nibbles_to_string(&path);
        println!("******* Path for {}: {:?}, {:?}", part_id, path, other_convert);

        if part_id == 0 {
            continue;
        }

        let key = if part_id == num_parts {
            None
        } else {
            let mut key = trie.path_to_key(&path);
            let node = trie.lookup_trie_node_by_path(&path).unwrap();
            println!("Node is {:?}", node);

            match node {
                near_store::RawTrieNode::Leaf(existing_key, _, _) => {
                    let nibbles = NibbleSlice::from_encoded(&existing_key).0;
                    assert_eq!(nibbles.len() % 2, 0);

                    let mut tmp = existing_key.clone();
                    key.append(&mut tmp.split_off(1));
                }
                near_store::RawTrieNode::Branch(_, _) => {
                    // We're on a branch with a value - nothing to do.
                }
                near_store::RawTrieNode::Extension(_, _) => {
                    //panic!("We should not end up on extension");
                }
            }
            println!(
                "And our key to fetch from flat storage is: {:?} {:?}",
                key,
                str::from_utf8(&key)
            );

            let trie_result = trie.get_ref(&key, near_store::KeyLookupMode::Trie).unwrap();
            println!("Trie: {:?}", trie_result);
            let flat_result = trie.get_ref(&key, near_store::KeyLookupMode::FlatStorage).unwrap();

            println!("Comparison {:?} {:?}", trie_result, flat_result);
            Some(key)
        };

        //println!("Reading data between {:?} and {:?}", previous_key, key);

        let flat_items = iter_flat_state_keys(&store, &previous_key, &key);

        println!("Got {} flat_items", flat_items.len());

        let mut mm = HashSet::new();
        let mut flat_based_nodes = Vec::new();

        let mut flat_values = 0;

        for entry in flat_items.iter() {
            let account = parse_account_id_from_raw_key(&entry.0.clone()).unwrap();
            mm.insert(account.clone());

            if let Some(account) = account {
                if account == AccountId::from_str("shard0").unwrap() {
                    //flat_values.push(entry.1);
                    //if let Ok(trie_node) = RawTrieNode::decode(&entry.1) {
                    //    flat_based_nodes.push(trie_node);
                    //} else {
                    //    flat_values += 1;
                    // }

                    let value_ref = ValueRef::decode(((*(entry.1)).try_into()).unwrap());

                    flat_based_nodes.push((entry.0.clone(), value_ref.clone()));

                    let raw_bytes = trie.get_raw_bytes(&value_ref).unwrap().unwrap();

                    // TODO: This is wrong - we should read from the valueref pointer instead...
                    all_nodes.push((entry.0.clone(), raw_bytes));
                }
            }
        }
        println!("Set: {:?} flat based value refs in shard0: {},", mm, flat_based_nodes.len(),);

        previous_key = key.clone().unwrap_or(vec![]).clone();
    }

    let mock_storage = MockTrieStorage { entries: HashMap::new() };
    let in_memory_trie = Trie::new(Box::new(mock_storage), StateRoot::new(), None);

    let trie_updates = in_memory_trie
        .update(all_nodes.iter().map(|(a, b)| (a.to_vec(), Some(b.clone()))))
        .unwrap();

    println!(
        "!!! Trie updates: old root: {:?}  new root: {:?} added nodes: {:?} removed nodes: {:?}",
        trie_updates.old_root,
        trie_updates.new_root,
        trie_updates.insertions().len(),
        trie_updates.deletions.len(),
    );

    for node in trie_updates.insertions() {
        let decoded_node = RawTrieNodeWithSize::decode(node.payload());

        println!("Node: {:?} is node: {:?}", node.hash(), decoded_node.is_ok());
        if let Ok(decoded_node) = decoded_node {
            println!("Memory: {}", decoded_node.memory_usage);
        }
    }

    for part_id in 0..num_parts {
        let part_id_s = PartId::new(part_id, num_parts);
        let original_state_part =
            runtime_adapter.obtain_state_part(0, &sync_prev_hash, &state_root, part_id_s).unwrap();
        let original_partial_state = trie.get_trie_nodes_for_part(part_id_s).unwrap();

        println!(
            "Part {:?} - original size: {} nodes: {}",
            part_id_s,
            original_state_part.len(),
            original_partial_state.0.len()
        );
    }

    let mut old_trie = File::create("/tmp/old_trie").unwrap();

    trie.print_recursive(&mut old_trie, &state_root, 20);

    println!("***************************************************************************");

    let other_mock_storage = MockTrieStorage {
        entries: trie_updates
            .insertions()
            .iter()
            .map(|inser| (inser.hash().clone(), inser.clone().trie_node_or_value))
            .collect(),
    };

    let other_memory_trie = Trie::new(Box::new(other_mock_storage), trie_updates.new_root, None);

    let mut new_trie = File::create("/tmp/new_trie").unwrap();
    other_memory_trie.print_recursive(&mut new_trie, &trie_updates.new_root, 20);

    //store.get_ser(DB, key)

    /*
    let env_filter = near_o11y::EnvFilterBuilder::from_env().verbose(Some("")).finish().unwrap();
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();
    debug!(target: "storage-calculator", "Start");

    let genesis = Genesis::from_file("output.json", GenesisValidationMode::Full);
    debug!(target: "storage-calculator", "Genesis read");

    let config_store = RuntimeConfigStore::new(None);
    let config = config_store.get_config(PROTOCOL_VERSION);
    let storage_usage = Runtime::new().compute_genesis_storage_usage(&genesis, config);
    debug!(target: "storage-calculator", "Storage usage calculated");

    let mut result = Vec::new();
    genesis.for_each_record(|record| {
        if let StateRecord::Account { account_id, account } = record {
            let actual_storage_usage = storage_usage.get(account_id).unwrap();
            let saved_storage_usage = account.storage_usage();
            let delta = actual_storage_usage - saved_storage_usage;
            if delta != 0 {
                debug!("{},{}", account_id, delta);
                result.push((account_id.clone(), delta));
            }
        }
    });
    serde_json::to_writer_pretty(&File::create("storage_usage_delta.json")?, &result)?;*/
}

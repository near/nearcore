#[cfg(test)]
mod tests {
    use near_primitives_core::types::AccountId;

    use crate::shard_layout::{self, account_id_to_shard_id, ShardLayout, ShardLayoutV1};
    use crate::trie_key::TrieKey;

    #[test]
    fn test_assignment() {}

    fn generate_account_ids() -> Vec<AccountId> {
        const MAX_LEN: usize = 5;
        let chars = ['a', 'b', '-' , '_' , '.'];
        let mut all = Vec::new();
        let mut state = vec!["".to_owned()];
        for len in 1..=MAX_LEN {
            let mut next_state = Vec::new();
            for s in &state {
                for ch in chars {
                    let mut s = s.clone();
                    s.push(ch);
                    next_state.push(s);
                }
            }
            state = next_state;
            for s in state {
            }
        }
        all
    }

    fn generate_trie_keys(account_id: AccountId) -> Vec<TrieKey> {
        vec![]
    }

    fn generate_shard_layouts(account_ids: &[AccountId]) -> Vec<ShardLayoutV1> {
        vec![]
    }

    fn check_shard_assignment(trie_keys: Vec<TrieKey>, shard_layout_v1: ShardLayoutV1) {
        let shard_layout = ShardLayout::V1(shard_layout_v1.clone());
        for shard_id in 0..shard_layout.num_shards() {
            let shard_ranges = shard_layout_v1
                .account_ranges(shard_id)
                .iter()
                .flat_map(|rng| TrieKey::calc_trie_key_ranges(rng))
                .collect::<Vec<_>>();
            for trie_key in &trie_keys {
                let trie_key_bytes = trie_key.to_vec();
                let ranges_contain = shard_ranges.iter().any(|rng| rng.contains(&trie_key_bytes));
                let actual_shard_id =
                    account_id_to_shard_id(trie_key.account_id().unwrap(), &shard_layout);
                let shard_contains = actual_shard_id == shard_id;
                assert_eq!(ranges_contain, shard_contains);
            }
        }
    }
}

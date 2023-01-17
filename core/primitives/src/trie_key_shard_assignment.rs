#[cfg(test)]
mod tests {
    use near_primitives_core::types::AccountId;

    use crate::shard_layout::{account_id_to_shard_id, ShardLayout};
    use crate::trie_key::TrieKey;

    #[test]
    fn test_trie_key_shard_assignment() {
        let trie_keys: Vec<_> =
            generate_account_ids().iter().flat_map(|acc| generate_trie_keys(acc)).collect();
        let shard_layout = create_shard_layout(&["ab"], &["aa", "bb"]);
        check_shard_assignment(trie_keys, shard_layout);
    }

    fn create_shard_layout(fixed_shards: &[&str], boundary_accounts: &[&str]) -> ShardLayout {
        ShardLayout::v1(
            fixed_shards.iter().map(|s| s.parse().unwrap()).collect(),
            boundary_accounts.iter().map(|s| s.parse().unwrap()).collect(),
            None,
            0,
        )
    }

    fn generate_account_ids() -> Vec<AccountId> {
        const MAX_LEN: usize = 5;
        generate_superset(&[], &['a', 'b', '-', '_', '.'].map(|ch| ch as u8), MAX_LEN)
            .iter()
            .flat_map(|bytes| std::str::from_utf8(bytes).unwrap().parse::<AccountId>())
            .collect()
    }

    fn generate_trie_keys(account_id: &AccountId) -> Vec<TrieKey> {
        let mut all = vec![];
        all.push(TrieKey::Account { account_id: account_id.clone() });
        all
    }

    fn check_shard_assignment(trie_keys: Vec<TrieKey>, shard_layout: ShardLayout) {
        for shard_id in 0..shard_layout.num_shards() {
            let shard_ranges = shard_layout
                .account_ranges(shard_id)
                .unwrap()
                .iter()
                .flat_map(|rng| TrieKey::calc_trie_key_ranges(rng))
                .collect::<Vec<_>>();
            for trie_key in &trie_keys {
                let trie_key_bytes = trie_key.to_vec();
                let ranges_contain = shard_ranges.iter().any(|rng| rng.contains(&trie_key_bytes));
                let actual_shard_id =
                    account_id_to_shard_id(trie_key.account_id().unwrap(), &shard_layout);
                let shard_contains = actual_shard_id == shard_id;
                assert_eq!(
                    ranges_contain, shard_contains,
                    "key={:?} belongs to shard {}: {}, but expected {}",
                    trie_key, shard_id, ranges_contain, shard_contains
                );
            }
        }
    }

    fn generate_superset(start: &[u8], elements: &[u8], c: usize) -> Vec<Vec<u8>> {
        let mut all = vec![start.to_vec()];
        let mut cur = vec![start.to_vec()];
        for _ in 0..c {
            let mut nxt = Vec::new();
            for v in cur {
                for &el in elements {
                    nxt.push(v.iter().cloned().chain([el]).collect());
                }
            }
            cur = nxt;
            all.extend(cur.iter().cloned());
        }
        all
    }
}

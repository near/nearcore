#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use near_primitives_core::types::AccountId;

    use crate::shard_layout::{account_id_to_shard_id, ShardLayout};
    use crate::trie_key::TrieKey;
    use rand::prelude::*;

    const ACCOUNT_ID_CHARS: [char; 5] = ['a', 'b', '-', '_', '.'];
    const MAX_ACCOUNT_LEN: usize = 5;

    #[test]
    fn test_trie_key_shard_assignment() {
        let trie_keys: Vec<_> =
            generate_account_ids().iter().flat_map(generate_trie_keys).collect();
        check_shard_assignment(&trie_keys, create_shard_layout(&["ab"], &["aa", "bb"]));
        check_shard_assignment(&trie_keys, create_shard_layout(&[], &["aa", "bb"]));
        check_shard_assignment(
            &trie_keys,
            create_shard_layout(
                &["a.b", "aa", "aaaaa", "bbbbbb", "a_a.a", "a_b.a"],
                &["a.a", "aa.a.b", "aa.b", "b_ab", "bbbb"],
            ),
        );
    }

    #[test]
    fn rand_test_trie_key_shard_assignment() {
        let account_ids = generate_account_ids();
        let trie_keys: Vec<_> = account_ids.iter().flat_map(generate_trie_keys).collect();
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..300 {
            let shard_layout = generate_rand_shard_layout(&mut rng, &account_ids);
            check_shard_assignment(&trie_keys, shard_layout);
        }
    }

    fn create_shard_layout(fixed_shards: &[&str], boundary_accounts: &[&str]) -> ShardLayout {
        let fixed_shards = fixed_shards.iter().map(|s| s.parse().unwrap()).collect();
        let mut boundary_accounts: Vec<_> =
            boundary_accounts.iter().map(|s| s.parse().unwrap()).collect();
        boundary_accounts.sort();
        ShardLayout::v1(fixed_shards, boundary_accounts, None, 0)
    }

    fn generate_rand_shard_layout(rng: &mut StdRng, account_ids: &[AccountId]) -> ShardLayout {
        let fixed_shards = pick_rand_accounts(rng, account_ids, 3);
        let mut boundary_accounts = pick_rand_accounts(rng, account_ids, 3);
        boundary_accounts.sort();
        ShardLayout::v1(fixed_shards, boundary_accounts, None, 0)
    }

    fn pick_rand_accounts(
        rng: &mut StdRng,
        account_ids: &[AccountId],
        max_len: usize,
    ) -> Vec<AccountId> {
        let len = rng.gen_range(0..=max_len);
        (0..len)
            .map(|_| rng.gen_range(0..len))
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|i| account_ids[i].clone())
            .collect()
    }

    fn generate_account_ids() -> Vec<AccountId> {
        generate_superset(&[], &ACCOUNT_ID_CHARS.map(|ch| ch as u8), MAX_ACCOUNT_LEN)
            .iter()
            .flat_map(|bytes| std::str::from_utf8(bytes).unwrap().parse::<AccountId>())
            .collect()
    }

    fn generate_trie_keys(account_id: &AccountId) -> Vec<TrieKey> {
        let mut all = vec![];
        all.push(TrieKey::Account { account_id: account_id.clone() });
        all
    }

    fn check_shard_assignment(trie_keys: &[TrieKey], shard_layout: ShardLayout) {
        for shard_id in 0..shard_layout.num_shards() {
            let shard_ranges = shard_layout
                .account_ranges(shard_id)
                .unwrap()
                .iter()
                .flat_map(|rng| TrieKey::calc_trie_key_ranges(rng))
                .collect::<Vec<_>>();
            for trie_key in trie_keys {
                let trie_key_bytes = trie_key.to_vec();
                let ranges_contain = shard_ranges.iter().any(|rng| rng.contains(&trie_key_bytes));
                let actual_shard_id =
                    account_id_to_shard_id(trie_key.account_id().unwrap(), &shard_layout);
                let shard_contains = actual_shard_id == shard_id;
                assert_eq!(
                    ranges_contain, shard_contains,
                    "key={trie_key:?} belongs to shard {shard_id}: \
                    got {ranges_contain}, but expected {shard_contains} (layout={shard_layout:?})"
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

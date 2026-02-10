use crate::epoch_manager::EpochConfigStore;
use crate::shard_layout::{ShardLayout, ShardUId};
use itertools::Itertools;
use near_primitives_core::types::ProtocolVersion;
use near_primitives_core::types::{AccountId, ShardId};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap};

use super::{ShardsSplitMapV1, ShardsSplitMapV2};

fn new_shard_ids_vec(shard_ids: Vec<u64>) -> Vec<ShardId> {
    shard_ids.into_iter().map(Into::into).collect()
}

fn new_shards_split_map_v1(shards_split_map: Vec<Vec<u64>>) -> ShardsSplitMapV1 {
    shards_split_map.into_iter().map(new_shard_ids_vec).collect()
}

fn new_shards_split_map_v2(shards_split_map: BTreeMap<u64, Vec<u64>>) -> ShardsSplitMapV2 {
    shards_split_map.into_iter().map(|(k, v)| (k.into(), new_shard_ids_vec(v))).collect()
}

impl ShardLayout {
    /// Constructor for tests that need a shard layout for a specific protocol version.
    pub fn for_protocol_version(protocol_version: ProtocolVersion) -> Self {
        let config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
        config_store.get_config(protocol_version).static_shard_layout()
    }
}

#[test]
fn v0() {
    let num_shards = 4;
    #[allow(deprecated)]
    let shard_layout = ShardLayout::v0(num_shards, 0);
    let mut shard_id_distribution: HashMap<ShardId, _> =
        shard_layout.shard_ids().map(|shard_id| (shard_id.into(), 0)).collect();
    let mut rng = StdRng::from_seed([0; 32]);
    for _i in 0..1000 {
        let s: Vec<u8> = (&mut rng).sample_iter(&Alphanumeric).take(10).collect();
        let s = String::from_utf8(s).unwrap();
        let account_id = s.to_lowercase().parse().unwrap();
        let shard_id = shard_layout.account_id_to_shard_id(&account_id);
        *shard_id_distribution.get_mut(&shard_id).unwrap() += 1;

        let shard_id: u64 = shard_id.into();
        assert!(shard_id < num_shards);
    }
    let expected_distribution: HashMap<ShardId, _> = [
        (ShardId::new(0), 247),
        (ShardId::new(1), 268),
        (ShardId::new(2), 233),
        (ShardId::new(3), 252),
    ]
    .into_iter()
    .collect();
    assert_eq!(shard_id_distribution, expected_distribution);
}

#[test]
fn v1() {
    let aid = |s: &str| s.parse().unwrap();
    let sid = |s: u64| ShardId::new(s);

    let boundary_accounts =
        ["aurora", "bar", "foo", "foo.baz", "paz"].iter().map(|a| a.parse().unwrap()).collect();
    #[allow(deprecated)]
    let shard_layout = ShardLayout::v1(
        boundary_accounts,
        Some(new_shards_split_map_v1(vec![vec![0, 1, 2], vec![3, 4, 5]])),
        1,
    );
    assert_eq!(
        shard_layout.get_children_shards_uids(ShardId::new(0)).unwrap(),
        (0..3).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
    );
    assert_eq!(
        shard_layout.get_children_shards_uids(ShardId::new(1)).unwrap(),
        (3..6).map(|x| ShardUId { version: 1, shard_id: x }).collect::<Vec<_>>()
    );
    for x in 0..3 {
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(x)).unwrap(), sid(0));
        assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(x + 3)).unwrap(), sid(1));
    }

    assert_eq!(shard_layout.account_id_to_shard_id(&aid("aurora")), sid(1));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.aurora")), sid(3));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar.foo.aurora")), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar")), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("bar.bar")), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo")), sid(3));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("baz.foo")), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.baz")), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("a.foo.baz")), sid(0));

    assert_eq!(shard_layout.account_id_to_shard_id(&aid("aaa")), sid(0));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("abc")), sid(0));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("bbb")), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("foo.goo")), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("goo")), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&aid("zoo")), sid(5));
}

#[test]
fn v2() {
    let sid = |s: u64| ShardId::new(s);
    let shard_layout = get_test_shard_layout_v2();

    // check accounts mapping in the middle of each range
    assert_eq!(shard_layout.account_id_to_shard_id(&"aaa".parse().unwrap()), sid(3));
    assert_eq!(shard_layout.account_id_to_shard_id(&"ddd".parse().unwrap()), sid(8));
    assert_eq!(shard_layout.account_id_to_shard_id(&"mmm".parse().unwrap()), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&"rrr".parse().unwrap()), sid(7));

    // check accounts mapping for the boundary accounts
    assert_eq!(shard_layout.account_id_to_shard_id(&"ccc".parse().unwrap()), sid(8));
    assert_eq!(shard_layout.account_id_to_shard_id(&"kkk".parse().unwrap()), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&"ppp".parse().unwrap()), sid(7));

    // check shard ids
    assert_eq!(shard_layout.shard_ids().collect_vec(), new_shard_ids_vec(vec![3, 8, 4, 7]));

    // check shard uids
    let version = 3;
    let u = |shard_id| ShardUId { shard_id, version };
    assert_eq!(shard_layout.shard_uids().collect_vec(), vec![u(3), u(8), u(4), u(7)]);

    // check parent
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(3)).unwrap(), sid(3));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(8)).unwrap(), sid(1));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(4)).unwrap(), sid(4));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(7)).unwrap(), sid(1));

    // check child
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(1)).unwrap(),
        new_shard_ids_vec(vec![7, 8])
    );
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(3)).unwrap(),
        new_shard_ids_vec(vec![3])
    );
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(4)).unwrap(),
        new_shard_ids_vec(vec![4])
    );
}

fn get_test_shard_layout_v2() -> ShardLayout {
    let b0 = "ccc".parse().unwrap();
    let b1 = "kkk".parse().unwrap();
    let b2 = "ppp".parse().unwrap();

    let boundary_accounts = vec![b0, b1, b2];
    let shard_ids = vec![3, 8, 4, 7];
    let shard_ids = new_shard_ids_vec(shard_ids);

    // the mapping from parent to the child
    // shard 1 is split into shards 7 & 8 while other shards stay the same
    let shards_split_map = BTreeMap::from([(1, vec![7, 8]), (3, vec![3]), (4, vec![4])]);
    let shards_split_map = new_shards_split_map_v2(shards_split_map);
    let shards_split_map = Some(shards_split_map);

    ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
}

#[test]
fn v3() {
    let sid = |s: u64| ShardId::new(s);
    let shard_layout = get_test_shard_layout_v3();

    // check accounts mapping in the middle of each range
    assert_eq!(shard_layout.account_id_to_shard_id(&"aaa".parse().unwrap()), sid(4));
    assert_eq!(shard_layout.account_id_to_shard_id(&"ddd".parse().unwrap()), sid(5));
    assert_eq!(shard_layout.account_id_to_shard_id(&"mmm".parse().unwrap()), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&"rrr".parse().unwrap()), sid(3));

    // check accounts mapping for the boundary accounts
    assert_eq!(shard_layout.account_id_to_shard_id(&"ccc".parse().unwrap()), sid(5));
    assert_eq!(shard_layout.account_id_to_shard_id(&"kkk".parse().unwrap()), sid(2));
    assert_eq!(shard_layout.account_id_to_shard_id(&"ppp".parse().unwrap()), sid(3));

    // check shard ids
    assert_eq!(shard_layout.shard_ids().collect_vec(), new_shard_ids_vec(vec![4, 5, 2, 3]));

    // check shard uids
    let version = 3;
    let u = |shard_id| ShardUId { shard_id, version };
    assert_eq!(shard_layout.shard_uids().collect_vec(), vec![u(4), u(5), u(2), u(3)]);

    // check parent
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(4)).unwrap(), sid(1));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(5)).unwrap(), sid(1));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(2)).unwrap(), sid(2));
    assert_eq!(shard_layout.get_parent_shard_id(ShardId::new(3)).unwrap(), sid(3));
    assert!(shard_layout.get_parent_shard_id(ShardId::new(1234)).is_err());

    // check child
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(1)).unwrap(),
        new_shard_ids_vec(vec![4, 5])
    );
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(2)).unwrap(),
        new_shard_ids_vec(vec![2])
    );
    assert_eq!(
        shard_layout.get_children_shards_ids(ShardId::new(3)).unwrap(),
        new_shard_ids_vec(vec![3])
    );

    // check (de)serialization
    let json = serde_json::to_string(&shard_layout).unwrap();
    let deserialized: ShardLayout = serde_json::from_str(&json).unwrap();
    assert_eq!(shard_layout, deserialized);
}

fn get_test_shard_layout_v3() -> ShardLayout {
    let b0 = "ccc".parse().unwrap();
    let b1 = "kkk".parse().unwrap();
    let b2 = "ppp".parse().unwrap();

    let boundary_accounts = vec![b0, b1, b2];
    let shard_ids = new_shard_ids_vec(vec![4, 5, 2, 3]);

    let last_split = ShardId::new(1);
    let shards_split_map = to_shards_split_map([(1, vec![4, 5])]);

    ShardLayout::v3(boundary_accounts, shard_ids, shards_split_map, last_split)
}

fn to_boundary_accounts<const N: usize>(accounts: [&str; N]) -> Vec<AccountId> {
    accounts.into_iter().map(|a| a.parse().unwrap()).collect()
}

fn to_shard_ids<const N: usize>(ids: [u32; N]) -> Vec<ShardId> {
    ids.into_iter().map(|id| ShardId::new(id as u64)).collect()
}

fn to_shard_split((parent, children): (u32, Vec<u32>)) -> (ShardId, Vec<ShardId>) {
    (ShardId::new(parent as u64), children.into_iter().map(|id| ShardId::new(id as u64)).collect())
}

fn to_shards_split_map<const N: usize>(
    xs: [(u32, Vec<u32>); N],
) -> BTreeMap<ShardId, Vec<ShardId>> {
    xs.into_iter().map(to_shard_split).collect()
}

fn to_shard_uids<const N: usize>(ids: [u32; N]) -> Vec<ShardUId> {
    ids.into_iter().map(|id| ShardUId::new(3, ShardId::new(id as u64))).collect()
}

#[test]
fn derive_layout() {
    // [] -> ["test1"]
    // [(0, [1,2])]
    // [0] -> [1,2]
    let base_layout = ShardLayout::v2(vec![], vec![ShardId::new(0)], None);
    let boundary: AccountId = "test1.near".parse().unwrap();
    let derived_layout = ShardLayout::derive_shard_layout(&base_layout, boundary);
    assert_eq!(
        derived_layout,
        ShardLayout::v2(
            to_boundary_accounts(["test1.near"]),
            to_shard_ids([1, 2]),
            Some(to_shards_split_map([(0, vec![1, 2])])),
        ),
    );

    // ["test1"] -> ["test1", "test3"]
    // [(1, [1]), (2, [3, 4])]
    // [1, 2] -> [1, 3, 4]
    let base_layout = derived_layout;
    let boundary: AccountId = "test3.near".parse().unwrap();
    let derived_layout = ShardLayout::derive_shard_layout(&base_layout, boundary);
    assert_eq!(
        derived_layout,
        ShardLayout::v2(
            to_boundary_accounts(["test1.near", "test3.near"]),
            to_shard_ids([1, 3, 4]),
            Some(to_shards_split_map([(1, vec![1]), (2, vec![3, 4])])),
        ),
    );

    // ["test1", "test3"] -> ["test0", "test1", "test3"]
    // [(1, [5, 6]), (3, [3]), (4, [4])]
    // [1, 3, 4] -> [5, 6, 3, 4]
    let base_layout = derived_layout;
    let boundary: AccountId = "test0.near".parse().unwrap();
    let derived_layout = ShardLayout::derive_shard_layout(&base_layout, boundary);
    assert_eq!(
        derived_layout,
        ShardLayout::v2(
            to_boundary_accounts(["test0.near", "test1.near", "test3.near"]),
            to_shard_ids([5, 6, 3, 4]),
            Some(to_shards_split_map([(1, vec![5, 6]), (3, vec![3]), (4, vec![4]),])),
        ),
    );

    // ["test0", "test1", "test3"] -> ["test0", "test1", "test2", "test3"]
    // [(5, [5]), (6, [6]), (3, [7, 8]), (4, [4])]
    // [5, 6, 3, 4] -> [5, 6, 7, 8, 4]
    let base_layout = derived_layout;
    let boundary: AccountId = "test2.near".parse().unwrap();
    let derived_layout = ShardLayout::derive_shard_layout(&base_layout, boundary);
    assert_eq!(
        derived_layout,
        ShardLayout::v2(
            to_boundary_accounts(["test0.near", "test1.near", "test2.near", "test3.near"]),
            to_shard_ids([5, 6, 7, 8, 4]),
            Some(to_shards_split_map([(5, vec![5]), (6, vec![6]), (3, vec![7, 8]), (4, vec![4]),])),
        )
    );

    // In case we are changing the shard layout version from hardcoded 3,
    // make sure that we correctly return the shard_uid of the parent shards in
    // get_split_parent_shard_uids function.
    assert_eq!(base_layout.version(), 3);
    assert_eq!(base_layout.version(), derived_layout.version());
}

#[test]
fn derive_v3() {
    // base layout: shards 1 & 2 split from 0 on account "test1"
    let base_layout = ShardLayout::v3(
        to_boundary_accounts(["test1.near"]),
        to_shard_ids([1, 2]),
        to_shards_split_map([(0, vec![1, 2])]),
        ShardId::new(0),
    );

    // derive layout: split shard 2 into 3 & 4 on account "test3"
    let boundary: AccountId = "test3.near".parse().unwrap();
    let derived_layout = base_layout.derive_v3(boundary, || unreachable!());

    assert_eq!(derived_layout.shard_ids().collect_vec(), to_shard_ids([1, 3, 4]));
    assert_eq!(
        derived_layout.boundary_accounts(),
        &to_boundary_accounts(["test1.near", "test3.near"])
    );

    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(0)), None);
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(1)), Some(to_shard_ids([1])));
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(2)), Some(to_shard_ids([3, 4])));

    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(1)).unwrap(),
        Some(ShardId::new(1))
    );
    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(3)).unwrap(),
        Some(ShardId::new(2))
    );
    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(4)).unwrap(),
        Some(ShardId::new(2))
    );

    assert_eq!(
        derived_layout.get_split_parent_shard_ids(),
        to_shard_ids([2]).into_iter().collect()
    );

    assert_eq!(derived_layout.ancestor_uids(ShardId::new(1)), Some(to_shard_uids([0])));
    assert_eq!(derived_layout.ancestor_uids(ShardId::new(3)), Some(to_shard_uids([2, 0])));
    assert_eq!(derived_layout.ancestor_uids(ShardId::new(4)), Some(to_shard_uids([2, 0])));

    // derive layout: split shard 3 into 5 & 6 on account "test2"
    let base_layout = derived_layout;
    let boundary: AccountId = "test2.near".parse().unwrap();
    let derived_layout = base_layout.derive_v3(boundary, || unreachable!());

    assert_eq!(derived_layout.shard_ids().collect_vec(), to_shard_ids([1, 5, 6, 4]));
    assert_eq!(
        derived_layout.boundary_accounts(),
        &to_boundary_accounts(["test1.near", "test2.near", "test3.near"])
    );

    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(0)), None);
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(1)), Some(to_shard_ids([1])));
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(2)), None);
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(3)), Some(to_shard_ids([5, 6])));
    assert_eq!(derived_layout.get_children_shards_ids(ShardId::new(4)), Some(to_shard_ids([4])));

    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(1)).unwrap(),
        Some(ShardId::new(1))
    );
    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(4)).unwrap(),
        Some(ShardId::new(4))
    );
    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(5)).unwrap(),
        Some(ShardId::new(3))
    );
    assert_eq!(
        derived_layout.try_get_parent_shard_id(ShardId::new(6)).unwrap(),
        Some(ShardId::new(3))
    );

    assert_eq!(
        derived_layout.get_split_parent_shard_ids(),
        to_shard_ids([3]).into_iter().collect()
    );

    assert_eq!(derived_layout.ancestor_uids(ShardId::new(1)), Some(to_shard_uids([0])));
    assert_eq!(derived_layout.ancestor_uids(ShardId::new(4)), Some(to_shard_uids([2, 0])));
    assert_eq!(derived_layout.ancestor_uids(ShardId::new(5)), Some(to_shard_uids([3, 2, 0])));
    assert_eq!(derived_layout.ancestor_uids(ShardId::new(6)), Some(to_shard_uids([3, 2, 0])));
}

#[test]
fn derive_v3_from_history() {
    let layout0 = ShardLayout::v2(vec![], vec![ShardId::new(0)], None);
    // split shard 0 into 1 & 2 on account "bbb"
    let layout1 = ShardLayout::derive_shard_layout(&layout0, "bbb".parse().unwrap());
    // split shard 2 into 3 & 4 on account "ccc"
    let layout2 = ShardLayout::derive_shard_layout(&layout1, "ccc".parse().unwrap());

    let boundary = "aaa".parse().unwrap();
    let history = vec![layout2.clone(), layout1, layout0];
    let layout3 = layout2.derive_v3(boundary, || history);

    assert_eq!(layout3.shard_ids().collect_vec(), to_shard_ids([5, 6, 3, 4]));
    assert_eq!(layout3.boundary_accounts(), &to_boundary_accounts(["aaa", "bbb", "ccc"]));

    assert_eq!(layout3.get_children_shards_ids(ShardId::new(1)), Some(to_shard_ids([5, 6])));
    assert_eq!(layout3.get_children_shards_ids(ShardId::new(3)), Some(to_shard_ids([3])));
    assert_eq!(layout3.get_children_shards_ids(ShardId::new(4)), Some(to_shard_ids([4])));

    assert_eq!(layout3.try_get_parent_shard_id(ShardId::new(5)).unwrap(), Some(ShardId::new(1)));
    assert_eq!(layout3.try_get_parent_shard_id(ShardId::new(6)).unwrap(), Some(ShardId::new(1)));
    assert_eq!(layout3.try_get_parent_shard_id(ShardId::new(3)).unwrap(), Some(ShardId::new(3)));
    assert_eq!(layout3.try_get_parent_shard_id(ShardId::new(4)).unwrap(), Some(ShardId::new(4)));

    assert_eq!(layout3.get_split_parent_shard_ids(), to_shard_ids([1]).into_iter().collect());

    assert_eq!(layout3.ancestor_uids(ShardId::new(5)), Some(to_shard_uids([1, 0])));
    assert_eq!(layout3.ancestor_uids(ShardId::new(6)), Some(to_shard_uids([1, 0])));
    assert_eq!(layout3.ancestor_uids(ShardId::new(3)), Some(to_shard_uids([2, 0])));
    assert_eq!(layout3.ancestor_uids(ShardId::new(4)), Some(to_shard_uids([2, 0])));
}

// Check that the ShardLayout::multi_shard method returns interesting shard
// layouts. A shard layout is interesting if it has non-contiguous shard
// ids.
#[test]
fn multi_shard_non_contiguous() {
    for n in 2..10 {
        let shard_layout = ShardLayout::multi_shard(n, 0);
        assert!(!shard_layout.shard_ids().is_sorted());
    }
}

#[test]
fn build_shard_split_map_v3() {
    use crate::shard_layout::v3::build_shard_split_map;

    #[allow(deprecated)]
    let layout0 = ShardLayout::v1(vec![], None, 2);
    let layout1 = ShardLayout::v2(
        to_boundary_accounts(["test1.near"]),
        to_shard_ids([1, 2]),
        Some(to_shards_split_map([(0, vec![1, 2])])),
    );
    let layout2 = ShardLayout::v2(
        to_boundary_accounts(["test1.near", "test2.near"]),
        to_shard_ids([1, 3, 4]),
        Some(to_shards_split_map([(1, vec![1]), (2, vec![3, 4])])),
    );

    let layouts = vec![layout2, layout1, layout0]; // sorted from newest to oldest

    assert!(build_shard_split_map(&[]).is_empty());
    assert!(build_shard_split_map(&layouts[1..]).is_empty());
    assert!(build_shard_split_map(&layouts[2..]).is_empty());
    // layout0 has version 2 so split 0 -> (1, 2) is omitted from history
    assert_eq!(build_shard_split_map(&layouts), to_shards_split_map([(2, vec![3, 4])]));
}

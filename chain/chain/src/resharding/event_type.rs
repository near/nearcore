//! Collection of all resharding V3 event types.

use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_store::ShardUId;
use tracing::error;

/// Struct used to destructure a new shard layout definition into the resulting resharding event.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum ReshardingEventType {
    /// Split of a shard.
    SplitShard(ReshardingSplitShardParams),
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ReshardingSplitShardParams {
    // Shard being split.
    pub parent_shard: ShardUId,
    // Child to the left of the account boundary.
    pub left_child_shard: ShardUId,
    // Child to the right of the account boundary.
    pub right_child_shard: ShardUId,
    /// The account at the boundary between the two children.
    pub boundary_account: AccountId,
    /// Hash of the last block having the old shard layout.
    pub block_hash: CryptoHash,
    /// The block before `block_hash`.
    pub prev_block_hash: CryptoHash,
}

impl ReshardingEventType {
    /// Takes as input a [ShardLayout] definition and deduces which kind of resharding operation
    /// must be performed.
    ///
    /// # Args:
    /// * `next_shard_layout`: the new shard layout
    /// * `block_hash`: hash of the last block with the shard layout before `next_shard_layout`
    /// * `prev_block_hash`: hash of the block preceding `block_hash`
    ///
    /// Returns a [ReshardingEventType] if exactly one resharding change is contained in
    /// `next_shard_layout`, otherwise returns `None`.
    pub fn from_shard_layout(
        next_shard_layout: &ShardLayout,
        block_hash: CryptoHash,
        prev_block_hash: CryptoHash,
    ) -> Result<Option<ReshardingEventType>, Error> {
        let log_and_error = |err_msg: &str| {
            error!(target: "resharding", ?next_shard_layout, err_msg);
            Err(Error::ReshardingError(err_msg.to_owned()))
        };

        // Resharding V3 supports shard layout V2 onwards.
        let (shards_split_map, boundary_accounts) = match next_shard_layout {
            ShardLayout::V0(_) | ShardLayout::V1(_) => {
                return log_and_error("unsupported shard layout!");
            }
            ShardLayout::V2(layout) => {
                let Some(shards_split_map) = layout.shards_split_map() else {
                    return log_and_error("ShardLayoutV2 must have a shards_split_map!");
                };
                (shards_split_map, layout.boundary_accounts())
            }
        };

        let mut event = None;

        // Look for a shard having exactly two children, to detect a split.
        for (parent_id, children_ids) in shards_split_map {
            match children_ids.len() {
                1 => {}
                2 => {
                    if event.is_some() {
                        return log_and_error("can't perform two reshardings at the same time!");
                    }
                    // Parent shard is no longer part of this shard layout.
                    let parent_shard = ShardUId {
                        version: next_shard_layout.version(),
                        shard_id: *parent_id as u32,
                    };
                    let left_child_shard =
                        ShardUId::from_shard_id_and_layout(children_ids[0], next_shard_layout);
                    let right_child_shard =
                        ShardUId::from_shard_id_and_layout(children_ids[1], next_shard_layout);
                    // Find the boundary account between the two children.
                    let Some(boundary_account_index) = next_shard_layout
                        .shard_ids()
                        .position(|id| id == left_child_shard.shard_id())
                    else {
                        return log_and_error(&format!(
                            "shard {left_child_shard} not found in shard layout"
                        ));
                    };
                    let boundary_account = boundary_accounts[boundary_account_index].clone();
                    event = Some(ReshardingEventType::SplitShard(ReshardingSplitShardParams {
                        parent_shard,
                        left_child_shard,
                        right_child_shard,
                        boundary_account,
                        block_hash,
                        prev_block_hash,
                    }));
                }
                _ => {
                    return log_and_error(&format!(
                        "invalid number of children for shard {parent_id}"
                    ));
                }
            }
        }

        // We may have found at least one resharding event by now.
        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::AccountId;
    use near_store::ShardUId;
    use std::collections::BTreeMap;

    /// Shorthand to create account ID.
    macro_rules! account {
        ($str:expr) => {
            $str.parse::<AccountId>().unwrap()
        };
    }

    /// Verify that the correct type of resharding is deduced from a new shard layout.
    #[test]
    fn parse_event_type_from_shard_layout() {
        let block = CryptoHash::hash_bytes(&[1]);
        let prev_block = CryptoHash::hash_bytes(&[2]);

        // Shard layouts V0 and V1 are rejected.
        assert!(ReshardingEventType::from_shard_layout(
            &ShardLayout::v0_single_shard(),
            block,
            prev_block
        )
        .is_err());
        assert!(ReshardingEventType::from_shard_layout(&ShardLayout::v1_test(), block, prev_block)
            .is_err());

        // No resharding is ok.
        let shards_split_map = BTreeMap::from([(0, vec![0])]);
        let layout = ShardLayout::v2(vec![], vec![0], Some(shards_split_map));
        assert!(ReshardingEventType::from_shard_layout(&layout, block, prev_block)
            .is_ok_and(|event| event.is_none()));

        // Single split shard is ok.
        let shards_split_map = BTreeMap::from([(0, vec![0]), (1, vec![2, 3])]);
        let layout = ShardLayout::v2(
            vec![account!("ff"), account!("pp")],
            vec![0, 2, 3],
            Some(shards_split_map),
        );

        let event_type =
            ReshardingEventType::from_shard_layout(&layout, block, prev_block).unwrap();
        assert_eq!(
            event_type,
            Some(ReshardingEventType::SplitShard(ReshardingSplitShardParams {
                parent_shard: ShardUId { version: 3, shard_id: 1 },
                left_child_shard: ShardUId { version: 3, shard_id: 2 },
                right_child_shard: ShardUId { version: 3, shard_id: 3 },
                block_hash: block,
                prev_block_hash: prev_block,
                boundary_account: account!("pp")
            }))
        );

        // Double split shard is not ok.
        let shards_split_map = BTreeMap::from([(0, vec![2, 3]), (1, vec![4, 5])]);
        let layout = ShardLayout::v2(
            vec![account!("ff"), account!("pp"), account!("ss")],
            vec![2, 3, 4, 5],
            Some(shards_split_map),
        );
        assert!(ReshardingEventType::from_shard_layout(&layout, block, prev_block).is_err());
    }
}

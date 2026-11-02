use super::network_rules::{Action, NetworkRules};
use super::state::NodeExecutionData;
use crate::utils::network::{
    blocks_at_heights_matcher, chunk_endorsement_dropper_by_hash,
    designated_spice_endorsement_matcher, endorsements_from_matcher, spice_data_request_matcher,
};
use near_async::messaging::{CanSend, LateBoundSender};
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::sender::TestLoopSender;
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::block_header::BlockHeader;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{AccountId, BlockHeight, ShardId, ShardIndex};
use near_primitives::version::ProtocolFeature;
use near_vm_runner::logic::ProtocolVersion;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

pub enum DropCondition {
    /// Whether test loop should drop all chunks validated by the given account.
    /// Works if number of nodes is significant enough (at least three?).
    ChunksValidatedBy(AccountId),
    /// Whether test loop should drop all endorsements from the given account.
    EndorsementsFrom(AccountId),
    /// Whether test loop should drop all chunks in the given range of heights
    /// relative to first block height where protocol version changes.
    ProtocolUpgradeChunkRange(ProtocolVersion, HashMap<ShardIndex, Range<i64>>),
    /// Specifies the chunks that should be produced by their appearance in the
    /// chain with respect to the start of an epoch. That is, a given chunk at height
    /// `height_created` for shard `shard_id` will be produced if
    /// self.0[`shard_id`][`height_created` - `epoch_start`] is true, or if
    /// `height_created` - `epoch_start` > self.0[`shard_id`].len()
    ChunksProducedByHeight(HashMap<ShardId, Vec<bool>>),
    /// Drops the broadcast of every block at a height in the set, through a network rule
    /// on every node. The rules swallow the requests other nodes make for a block it
    /// dropped, so the skip holds even when the dropped block's producer builds the next
    /// block on it: the child would otherwise make its receivers fetch the parent. What was
    /// dropped and swallowed is reported by `NetworkRules::kept_blocks`.
    BlocksByHeight(HashSet<BlockHeight>),
    /// Drops the pre-spice endorsements of the given shards' chunks of whichever block
    /// turns out to be the last pre-spice block, decided per chunk from its parent before
    /// the block exists, see [`ends_pre_spice_epoch`]. Keeps hitting the boundary when
    /// something moves it away from the estimate.
    LastPreSpiceChunks { shards: HashSet<ShardId> },
    /// Drop every chunk of the given shards produced in any of the listed epoch
    /// heights. Unlike `ChunksProducedByHeight` (keyed on height-within-epoch),
    /// this is keyed on the absolute epoch height, so it silences a shard for
    /// whole specific epochs, including the first block of each such epoch.
    ChunksForShardsInEpochs { shards: HashSet<ShardId>, epoch_heights: HashSet<u64> },
    /// Drops every SPICE chunk endorsement whose sender is designated for the endorsed chunk,
    /// disabling the designated certification path so chunks can certify only via the all-stake
    /// fallback. A network rule on every node.
    DesignatedSpiceEndorsements,
    /// Drops every request for spice data, so a node can only get data that was pushed to
    /// it. A network rule on every node.
    SpiceDataRequests,
}

/// The tags of the network rules the conditions install, by which a node's account shows
/// whether it has them.
mod rule_tags {
    pub const ENDORSEMENTS_FROM: &str = "drop_condition:endorsements_from";
    pub const BLOCKS_BY_HEIGHT: &str = "drop_condition:blocks_by_height";
    pub const DESIGNATED_SPICE_ENDORSEMENTS: &str = "drop_condition:designated_spice_endorsements";
    pub const SPICE_DATA_REQUESTS: &str = "drop_condition:spice_data_requests";
}

/// Whether a condition that is a network rule is installed for a node's account
/// unconditionally, as when the condition is added, or only if the account has no rule of
/// the condition's kind yet, as when a node is set up: a restarted node's account already
/// has the rules of every condition added before, which are keyed by account.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuleInstall {
    Always,
    IfMissing,
}

/// Whether `header` is a last pre-spice block, decided from the header alone before the
/// block is processed, as when its producer broadcasts it: the block starts no epoch
/// itself, its last final block, which the header names, plus three reaches the epoch's
/// estimated end, and the next epoch is spice. The epoch manager has processed the parent
/// and the last final block, since the producer built on them.
pub fn is_last_pre_spice_header(
    epoch_manager: &dyn EpochManagerAdapter,
    header: &BlockHeader,
) -> bool {
    let Ok(parent) = epoch_manager.get_block_info(header.prev_hash()) else {
        return false;
    };
    if epoch_manager.is_next_block_epoch_start(header.prev_hash()).unwrap_or(true) {
        return false;
    }
    let Ok(last_final) = epoch_manager.get_block_info(header.last_final_block()) else {
        return false;
    };
    ends_pre_spice_epoch_from(epoch_manager, &parent, last_final.height())
}

/// Whether the block built on `parent_hash` will end a pre-spice epoch into a spice one,
/// so it will be the last pre-spice block, decided before it exists. The next block starts
/// a new epoch when the current block's last final height plus three reaches the epoch's
/// estimated end, and a block's last final block is its grandparent when the last three
/// heights are consecutive, the parent's last final block otherwise; the block is taken to
/// sit one above its parent, which is where a chunk built on the parent is created.
pub fn ends_pre_spice_epoch(
    epoch_manager: &dyn EpochManagerAdapter,
    parent_hash: &CryptoHash,
) -> bool {
    let Ok(parent) = epoch_manager.get_block_info(parent_hash) else {
        return false;
    };
    let Ok(grandparent) = epoch_manager.get_block_info(parent.prev_hash()) else {
        return false;
    };
    if epoch_manager.is_next_block_epoch_start(parent_hash).unwrap_or(true) {
        return false;
    }
    let last_final_height = if parent.height() == grandparent.height() + 1 {
        grandparent.height()
    } else {
        match epoch_manager.get_block_info(parent.last_final_block_hash()) {
            Ok(last_final) => last_final.height(),
            Err(_) => return false,
        }
    };
    ends_pre_spice_epoch_from(epoch_manager, &parent, last_final_height)
}

/// Whether a block built on `parent` whose last final height is `last_final_height` ends a
/// pre-spice epoch into a spice one.
fn ends_pre_spice_epoch_from(
    epoch_manager: &dyn EpochManagerAdapter,
    parent: &BlockInfo,
    last_final_height: BlockHeight,
) -> bool {
    let Ok(estimated_next_epoch_start) = epoch_manager.get_estimated_next_epoch_start(parent)
    else {
        return false;
    };
    if last_final_height + 3 < estimated_next_epoch_start {
        return false;
    }
    let Ok(version) = epoch_manager.get_epoch_protocol_version(parent.epoch_id()) else {
        return false;
    };
    let Ok(next_version) =
        epoch_manager.get_next_epoch_protocol_version_from_prev_block(parent.hash())
    else {
        return false;
    };
    !ProtocolFeature::Spice.enabled(version) && ProtocolFeature::Spice.enabled(next_version)
}

/// Stores all chunks ever observed on chain. Determines if a chunk can be
/// dropped within a test loop.
///
/// Needed to intercept network messages storing chunk hash only, while
/// interception requires more detailed information like shard id.
#[derive(Default)]
pub struct TestLoopChunksStorage {
    /// Mapping from chunk hashes to headers.
    storage: HashMap<ChunkHash, ShardChunkHeader>,
    /// Minimal chunk height ever observed.
    min_chunk_height: Option<BlockHeight>,
}

impl TestLoopChunksStorage {
    pub fn insert(&mut self, chunk_header: ShardChunkHeader) {
        let chunk_height = chunk_header.height_created();
        self.min_chunk_height = Some(
            self.min_chunk_height
                .map_or(chunk_height, |current_height| current_height.min(chunk_height)),
        );
        self.storage.insert(chunk_header.chunk_hash().clone(), chunk_header);
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&ShardChunkHeader> {
        self.storage.get(chunk_hash)
    }

    /// The headers of every chunk produced so far that `selects`, in no particular order.
    pub fn find(&self, selects: impl Fn(&ShardChunkHeader) -> bool) -> Vec<ShardChunkHeader> {
        self.storage.values().filter(|header| selects(header)).cloned().collect()
    }

    /// If chunk height is too low, don't drop chunk, allow the chain to warm
    /// up.
    pub fn can_drop_chunk(&self, chunk_header: &ShardChunkHeader) -> bool {
        self.min_chunk_height
            .is_some_and(|min_height| chunk_header.height_created() >= min_height + 3)
    }
}

/// Custom implementation of `Sender` for messages from `Client` to
/// `ShardsManagerActor` that allows to intercept all messages indicating
/// any chunk production and storing all chunks.
pub struct ClientToShardsManagerSender {
    pub sender: Arc<LateBoundSender<TestLoopSender<ShardsManagerActor>>>,
    /// Storage of chunks shared between all test loop nodes.
    pub chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
}

impl CanSend<ShardsManagerRequestFromClient> for ClientToShardsManagerSender {
    fn send(&self, message: ShardsManagerRequestFromClient) {
        // `DistributeEncodedChunk` indicates that a certain chunk was produced.
        if let ShardsManagerRequestFromClient::DistributeEncodedChunk { partial_chunk, .. } =
            &message
        {
            let mut chunks_storage = self.chunks_storage.lock();
            chunks_storage.insert(partial_chunk.cloned_header());
        }
        // After maybe storing the chunk, send the message as usual.
        self.sender.send(message);
    }
}

impl NodeExecutionData {
    /// Installs `drop_condition` for this node: as a handler on its peer manager, or as a
    /// network rule on its account for the conditions that are rules, which `install` says
    /// whether to add when the account has one of the kind already.
    pub fn register_drop_condition(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        network_rules: &NetworkRules,
        drop_condition: &DropCondition,
        install: RuleInstall,
    ) {
        let rule =
            |tag: &'static str, matches: Box<dyn Fn(&NetworkRequests) -> bool + Send + Sync>| {
                if install == RuleInstall::IfMissing
                    && !network_rules.tagged(&self.account_id, tag).is_empty()
                {
                    return;
                }
                network_rules.add_tagged(self.account_id.clone(), tag, matches, Action::Drop);
            };
        match drop_condition {
            DropCondition::ChunksValidatedBy(account_id) => {
                self.register_drop_chunks_validated_by(test_loop_data, chunks_storage, account_id)
            }
            DropCondition::EndorsementsFrom(account_id) => {
                rule(
                    rule_tags::ENDORSEMENTS_FROM,
                    Box::new(endorsements_from_matcher(account_id.clone())),
                );
            }
            DropCondition::ProtocolUpgradeChunkRange(protocol_version, chunk_ranges) => {
                self.register_drop_protocol_upgrade_chunks(
                    test_loop_data,
                    chunks_storage,
                    *protocol_version,
                    chunk_ranges.clone(),
                );
            }
            DropCondition::ChunksProducedByHeight(chunks_produced) => {
                self.register_drop_chunks_by_height(
                    test_loop_data,
                    chunks_storage,
                    chunks_produced.clone(),
                );
            }
            DropCondition::BlocksByHeight(heights) => {
                rule(
                    rule_tags::BLOCKS_BY_HEIGHT,
                    Box::new(blocks_at_heights_matcher(heights.clone())),
                );
            }
            DropCondition::LastPreSpiceChunks { shards } => {
                self.register_drop_last_pre_spice_chunks(
                    test_loop_data,
                    chunks_storage,
                    shards.clone(),
                );
            }
            DropCondition::ChunksForShardsInEpochs { shards, epoch_heights } => {
                self.register_drop_chunks_for_shards_in_epochs(
                    test_loop_data,
                    chunks_storage,
                    shards.clone(),
                    epoch_heights.clone(),
                );
            }
            DropCondition::DesignatedSpiceEndorsements => {
                let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
                let epoch_manager = client_actor.client.chain.epoch_manager.clone();
                rule(
                    rule_tags::DESIGNATED_SPICE_ENDORSEMENTS,
                    Box::new(designated_spice_endorsement_matcher(epoch_manager)),
                );
            }
            DropCondition::SpiceDataRequests => {
                rule(rule_tags::SPICE_DATA_REQUESTS, Box::new(spice_data_request_matcher()));
            }
        }
    }

    fn register_drop_chunks_for_shards_in_epochs(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        shards: HashSet<ShardId>,
        epoch_heights: HashSet<u64>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            if !shards.contains(&chunk.shard_id()) {
                return false;
            }
            let epoch_height = inner_epoch_manager
                .get_epoch_height_from_prev_block(chunk.prev_block_hash())
                .unwrap();
            epoch_heights.contains(&epoch_height)
        });

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }

    fn register_drop_chunks_validated_by(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        account_id: &AccountId,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let account_id = account_id.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            is_chunk_validated_by(inner_epoch_manager.as_ref(), chunk, account_id.clone())
        });

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }

    fn register_drop_protocol_upgrade_chunks(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        protocol_version: ProtocolVersion,
        chunk_ranges: HashMap<ShardIndex, Range<i64>>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            should_drop_chunk_for_protocol_upgrade(
                inner_epoch_manager.as_ref(),
                chunk,
                protocol_version,
                chunk_ranges.clone(),
            )
        });

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }

    fn register_drop_chunks_by_height(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        chunks_produced: HashMap<ShardId, Vec<bool>>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();

        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            should_drop_chunk_by_height(
                inner_epoch_manager.as_ref(),
                chunk,
                chunks_produced.clone(),
            )
        });

        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(
                chunks_storage,
                epoch_manager.clone(),
                drop_chunks_condition.clone(),
            ),
        );
    }

    fn register_drop_last_pre_spice_chunks(
        &self,
        test_loop_data: &mut TestLoopData,
        chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
        shards: HashSet<ShardId>,
    ) {
        let client_actor = test_loop_data.get(&self.client_sender.actor_handle());
        let epoch_manager = client_actor.client.chain.epoch_manager.clone();
        let inner_epoch_manager = epoch_manager.clone();
        let drop_chunks_condition = Box::new(move |chunk: ShardChunkHeader| -> bool {
            shards.contains(&chunk.shard_id())
                && ends_pre_spice_epoch(inner_epoch_manager.as_ref(), chunk.prev_block_hash())
        });
        self.register_override_handler(
            test_loop_data,
            chunk_endorsement_dropper_by_hash(chunks_storage, epoch_manager, drop_chunks_condition),
        );
    }
}

/// Checks whether chunk is validated by the given account.
fn is_chunk_validated_by(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    account_id: AccountId,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();
    let epoch_id = epoch_manager_adapter.get_epoch_id_from_prev_block(prev_block_hash).unwrap();

    let chunk_validators = epoch_manager_adapter
        .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)
        .unwrap();
    return chunk_validators.contains(&account_id);
}

/// returns !chunks_produced[shard_id][height_created - epoch_start].
fn should_drop_chunk_by_height(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    chunks_produced: HashMap<ShardId, Vec<bool>>,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();

    let height_in_epoch =
        if epoch_manager_adapter.is_next_block_epoch_start(prev_block_hash).unwrap() {
            0
        } else {
            let epoch_start =
                epoch_manager_adapter.get_epoch_start_height(prev_block_hash).unwrap();
            height_created - epoch_start
        };
    let Some(chunks_produced) = chunks_produced.get(&shard_id) else {
        return false;
    };
    let Some(should_produce) = chunks_produced.get(height_in_epoch as usize) else {
        return false;
    };
    !*should_produce
}

/// Returns true if the chunk should be dropped based on the
/// `DropCondition::ProtocolUpgradeChunkRange`.
fn should_drop_chunk_for_protocol_upgrade(
    epoch_manager_adapter: &dyn EpochManagerAdapter,
    chunk: ShardChunkHeader,
    version_of_protocol_upgrade: ProtocolVersion,
    chunk_ranges: HashMap<ShardIndex, Range<i64>>,
) -> bool {
    let prev_block_hash = chunk.prev_block_hash();
    let shard_id = chunk.shard_id();
    let height_created = chunk.height_created();
    let epoch_id = epoch_manager_adapter.get_epoch_id_from_prev_block(prev_block_hash).unwrap();
    let shard_layout = epoch_manager_adapter.get_shard_layout(&epoch_id).unwrap();
    let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
    // If there is no condition for the shard, all chunks
    // pass through.
    let Some(range) = chunk_ranges.get(&shard_index) else {
        return false;
    };

    let epoch_protocol_version =
        epoch_manager_adapter.get_epoch_protocol_version(&epoch_id).unwrap();
    // Drop condition for the first epoch with new protocol version.
    if epoch_protocol_version >= version_of_protocol_upgrade {
        let prev_epoch_id =
            epoch_manager_adapter.get_prev_epoch_id_from_prev_block(prev_block_hash).unwrap();
        let prev_epoch_protocol_version =
            epoch_manager_adapter.get_epoch_protocol_version(&prev_epoch_id).unwrap();
        // If this is not the first epoch with new protocol version,
        // all chunks go through.
        if prev_epoch_protocol_version >= version_of_protocol_upgrade {
            return false;
        }

        // Check the first block height in the epoch separately,
        // because the block itself is not created yet.
        // Its relative height is 0.
        if epoch_manager_adapter.is_next_block_epoch_start(prev_block_hash).unwrap() {
            return range.contains(&0);
        }

        // Otherwise we can get start height of the epoch by
        // the previous hash.
        let epoch_start_height =
            epoch_manager_adapter.get_epoch_start_height(&prev_block_hash).unwrap();
        range.contains(&(height_created as i64 - epoch_start_height as i64))
    } else if epoch_protocol_version < version_of_protocol_upgrade {
        // Drop condition for the last epoch with old protocol version.
        let maybe_upgrade_height = epoch_manager_adapter
            .get_estimated_protocol_upgrade_block_height(*prev_block_hash)
            .unwrap();

        // The protocol upgrade height is known if and only if
        // protocol upgrade happens in the next epoch.
        let Some(upgrade_height) = maybe_upgrade_height else {
            return false;
        };
        let next_epoch_id =
            epoch_manager_adapter.get_next_epoch_id_from_prev_block(prev_block_hash).unwrap();
        let next_epoch_protocol_version =
            epoch_manager_adapter.get_epoch_protocol_version(&next_epoch_id).unwrap();
        assert!(epoch_protocol_version < next_epoch_protocol_version);
        range.contains(&(height_created as i64 - upgrade_height as i64))
    } else {
        false
    }
}

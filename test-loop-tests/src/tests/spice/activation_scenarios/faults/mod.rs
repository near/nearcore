//! The faults of the catalog, one file per tag. Each implements [`Fault`] and renders its
//! string form in `Debug`: the tag, then every parameter that differs from its default as
//! `:key=value`.

mod archival;
mod bad_boundary_endorsement;
mod bad_boundary_witness;
mod bad_first_spice_block;
mod data_before_block;
mod drop_contract_accesses;
mod endorsement_delay;
mod fallback;
mod fork_at_boundary;
mod gc_tight;
mod height_skip;
mod late_chunk;
mod late_tracker;
mod missing_chunk;
mod node_crash;
mod partial_data_fault;
mod shard_rotation;
mod shard_skip;
mod slow_node;
mod track_all;
mod validator_churn;

pub use archival::Archival;
pub use bad_boundary_endorsement::BadBoundaryEndorsement;
pub use bad_boundary_witness::BadBoundaryWitness;
pub use bad_first_spice_block::{BadFirstSpiceBlock, Mode as BadBlockMode};
pub use data_before_block::{DataBeforeBlock, What};
pub use drop_contract_accesses::DropContractAccesses;
pub use endorsement_delay::{EndorsementDelay, WindowEnd, WindowStart};
pub use fallback::Fallback;
pub use fork_at_boundary::{ForkAtBoundary, Kind as ForkKind};
pub use gc_tight::GcTight;
pub use height_skip::HeightSkip;
pub use late_chunk::{LateChunk, Outcome};
pub use late_tracker::LateTracker;
pub use missing_chunk::{How, MissingChunk, Shards};
pub use node_crash::{At, NodeCrash};
pub use partial_data_fault::{Kind, Only, PartialDataFault, Producers};
pub use shard_skip::{Epochs, ShardSkip};
pub use slow_node::SlowNode;
pub use track_all::TrackAll;
pub use validator_churn::{Kind as ChurnKind, ValidatorChurn};
// The variant it carries is rejected until its milestone lands, so no preset names it yet.
#[allow(unused_imports)]
pub use node_crash::Retrack;
// Cannot fire on profile A, see the fault; no preset names it.
#[allow(unused_imports)]
pub use shard_rotation::ShardRotation;

use super::scenario::{Narrowing, OracleId, Relaxation};
use crate::setup::drop_condition::ends_pre_spice_epoch;
use crate::setup::env::TestLoopEnv;
use crate::utils::gaps::producer_outage;
use crate::utils::node::TestLoopNode;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use std::fmt::{self, Formatter};
use std::sync::Arc;

/// Writes the string form of a fault: `tag`, then `:key=value` for every parameter whose
/// value is `Some`, which is how a fault reports the parameters that differ from their
/// defaults.
fn render(f: &mut Formatter<'_>, tag: &str, params: &[(&str, Option<String>)]) -> fmt::Result {
    f.write_str(tag)?;
    for (key, value) in params {
        if let Some(value) = value {
            write!(f, ":{key}={value}")?;
        }
    }
    Ok(())
}

/// The shard layout of the epoch the driver node's head is in.
fn shard_layout_at_head(env: &TestLoopEnv) -> ShardLayout {
    let node = env.rpc_node();
    node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap()
}

/// Whether a chunk header is `shard_id`'s chunk at `offset` against the boundary, for the
/// chunk-part rules, decided on `epoch_manager`, the one of the node whose rule asks.
fn chunk_at_offset_selector(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    estimate: BlockHeight,
    shard_id: ShardId,
    offset: i64,
) -> impl Fn(&ShardChunkHeader) -> bool + Send + Sync {
    move |chunk: &ShardChunkHeader| {
        if chunk.shard_id() != shard_id {
            return false;
        }
        let at_offset = match offset {
            -1 => ends_pre_spice_epoch(epoch_manager.as_ref(), chunk.prev_block_hash()),
            0 => is_last_pre_spice_block(epoch_manager.as_ref(), chunk.prev_block_hash())
                .unwrap_or(false),
            _ => chunk.height_created() as i64 == estimate as i64 + offset,
        };
        at_offset
            && !produced_by_block_producer(
                epoch_manager.as_ref(),
                chunk.prev_block_hash(),
                chunk.shard_id(),
                chunk.height_created(),
            )
    }
}

/// Whether `shard_id`'s chunk created for `height_created` on the block `prev_hash` is
/// produced by the block producer of `height_created`, who then has it without the
/// network. Both lookups are the ones the producers themselves make, so a node that has
/// the parent answers as they do.
pub(super) fn produced_by_block_producer(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_hash: &CryptoHash,
    shard_id: ShardId,
    height_created: BlockHeight,
) -> bool {
    let Ok(chunk_producer) =
        epoch_manager.get_chunk_producer_info_from_prev_block(prev_hash, shard_id)
    else {
        return false;
    };
    let Ok(epoch_id) = epoch_manager.get_epoch_id_from_prev_block(prev_hash) else {
        return false;
    };
    epoch_manager
        .get_block_producer(&epoch_id, height_created)
        .is_ok_and(|block_producer| block_producer == *chunk_producer.account_id())
}

/// Whether `shard_id`'s chunk that the block at `height` on `node`'s chain carries or
/// lacks, the one created on the block's parent, is its block producer's own, see
/// [`produced_by_block_producer`]. False when no block is at `height`.
fn chunk_produced_by_block_producer(
    node: &TestLoopNode,
    height: BlockHeight,
    shard_id: ShardId,
) -> bool {
    let Ok(block) = node.client().chain.get_block_by_height(height) else {
        return false;
    };
    let prev_hash = block.header().prev_hash();
    let height_created = block.header().prev_height().unwrap_or(0) + 1;
    produced_by_block_producer(
        node.client().epoch_manager.as_ref(),
        prev_hash,
        shard_id,
        height_created,
    )
}

/// The relaxations an outage of `account_id`'s production from height `from` earns, as
/// the framework's `producer_outage` reads it off the driver node's chain: *height
/// contiguity* at its skipped slots and *chunk completeness* at its missing chunks.
fn producer_outage_relaxations(
    env: &TestLoopEnv,
    account_id: &AccountId,
    from: BlockHeight,
    resumable_after: Option<BlockHeight>,
) -> Vec<Relaxation> {
    let outage = producer_outage(&env.rpc_node(), account_id, from, resumable_after);
    vec![
        Relaxation {
            oracle: OracleId::HeightContiguity,
            narrowing: Narrowing::Heights(outage.skipped_heights),
        },
        Relaxation {
            oracle: OracleId::ChunkCompleteness,
            narrowing: Narrowing::ShardHeights(outage.missing_chunks),
        },
    ]
}

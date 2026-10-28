//! The faults of the catalog, one file per tag. Each implements [`Fault`] and renders its
//! string form in `Debug`: the tag, then every parameter that differs from its default as
//! `:key=value`.

mod archival;
mod drop_contract_accesses;
mod endorsement_delay;
mod missing_chunk;
mod partial_data_fault;
mod shard_rotation;
mod shard_skip;
mod track_all;

pub use archival::Archival;
pub use drop_contract_accesses::DropContractAccesses;
pub use endorsement_delay::{EndorsementDelay, WindowEnd, WindowStart};
pub use missing_chunk::{MissingChunk, Shards};
pub use partial_data_fault::{Kind, Only, PartialDataFault, Producers};
pub use shard_skip::ShardSkip;
pub use track_all::TrackAll;
// The variants these carry are rejected until their milestones land, so no preset names
// them yet.
#[allow(unused_imports)]
pub use missing_chunk::How;
#[allow(unused_imports)]
pub use shard_skip::Epochs;
// Cannot fire on profile A, see the fault; no preset names it.
#[allow(unused_imports)]
pub use shard_rotation::ShardRotation;

use crate::setup::env::TestLoopEnv;
use near_primitives::shard_layout::ShardLayout;
use std::fmt::{self, Formatter};

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

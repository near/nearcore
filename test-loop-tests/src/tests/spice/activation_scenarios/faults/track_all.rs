//! `track_all`: every validator tracks every shard.

use super::super::scenario::{Fault, Narrowing, OracleId, Relaxation};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use std::fmt::{self, Debug, Formatter};

/// Every validator applies every chunk itself, so nobody endorses from a witness: the
/// stateless-endorser half of the boundary check and the code-request oracle are off.
#[derive(Default)]
pub struct TrackAll;

impl TrackAll {
    pub fn new() -> Self {
        Self
    }
}

impl Debug for TrackAll {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(f, self.tag(), &[])
    }
}

impl Fault for TrackAll {
    fn tag(&self) -> &'static str {
        "track_all"
    }

    fn setup(&self, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder.track_all_shards()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        vec![
            Relaxation { oracle: OracleId::StatelessEndorser, narrowing: Narrowing::Whole },
            Relaxation { oracle: OracleId::CodeRequested, narrowing: Narrowing::Whole },
        ]
    }
}

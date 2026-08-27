//! Fetch-engine state for SPICE data distribution.

// Nothing is wired up to the actors yet; the cutover PRs consume this.
#![allow(dead_code, unused_imports)]

mod item;
mod scheduler;

pub(crate) use item::{
    Assembly, AssemblyError, FetchItem, FetchState, InFlightRequest, PartInsertResult, PullTiming,
    SpiceData, VerifiedCodedPart,
};
pub(crate) use scheduler::{Backoff, DeadlineScheduler, TimingConfig};
use std::cmp::Ordering;

/// Scheduling class of an item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    /// Consensus-critical: this node is an assigned validator or producer for it.
    Priority,
    /// RPC, state-sync or catch-up traffic; never starves `Priority`.
    Background,
}

impl Lane {
    /// Urgency rank; greater is more urgent, so `max` escalates.
    fn rank(self) -> u8 {
        match self {
            Lane::Background => 0,
            Lane::Priority => 1,
        }
    }
}

impl Ord for Lane {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl PartialOrd for Lane {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests;

//! Core state for SPICE data distribution.

// Nothing is wired up to the actors yet; the cutover PRs consume this.
#![allow(dead_code, unused_imports)]

mod item;

pub(crate) use item::{
    Assembly, AssemblyError, FetchItem, FetchState, PartInsertResult, SpiceData, VerifiedCodedPart,
};

#[cfg(test)]
mod tests;

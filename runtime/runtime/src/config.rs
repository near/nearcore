//! Settings of the parameters of the runtime.
//!
//! The fee computation lives in [`near_primitives::fees`] so that crates below
//! the runtime, such as genesis state initialization, can use it. It is
//! re-exported here for backwards compatibility.

pub use near_primitives::fees::*;
pub use near_primitives::num_rational::Rational32;

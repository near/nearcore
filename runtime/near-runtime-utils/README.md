# NEAR Runtime utilities

This crate contains utility functions for NEAR runtime.

The crate doesn't include dependencies on `near-primitives` and `near-crypto` to keep the dependencies limited.
Historically, `near-primitives` contained a bunch of dependencies that prevented this crate from being compiled to Wasm.

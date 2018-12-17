//! TxFlow implementation is independent on whether it runs on the shard chain or the beacon chain,
//! and it is independent on the content of the payload. This module provides some adapters for
//! converting specific structs like transaction to non-specific structs, like payload.
pub mod transaction_to_payload;

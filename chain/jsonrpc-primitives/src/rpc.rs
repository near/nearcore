//! This module defines RPC types to nearcore public APIs.
//!
//! NOTE: This module should be only used in RPC server and RPC client implementations, and
//! should not leak these types anywhere else.
use near_primitives::types::MaybeBlockId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct RpcValidatorsOrderedRequest {
    pub block_id: MaybeBlockId,
}

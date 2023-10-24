use borsh::{BorshDeserialize, BorshSerialize};

use crate::challenge::PartialState;
use crate::sharding::ReceiptProof;
use crate::types::StateRoot;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct ChunkStateWitness {
    pub prev_state_root: StateRoot,
    pub state: PartialState,
    pub incoming_receipts: Vec<ReceiptProof>,
}

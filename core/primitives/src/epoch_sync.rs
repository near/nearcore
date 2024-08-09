use crate::block_header::BlockHeader;
use crate::epoch_block_info::BlockInfo;
use crate::epoch_info::EpochInfo;
use crate::merkle::PartialMerkleTree;
use crate::views::LightClientBlockView;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, Clone)]
pub struct EpochSyncFinalizationResponse {
    pub cur_epoch_header: BlockHeader,
    pub prev_epoch_headers: Vec<BlockHeader>,
    pub header_sync_init_header: BlockHeader,
    pub header_sync_init_header_tree: PartialMerkleTree,
    // This Block Info is required by Epoch Manager when it checks if it's a good time to start a new Epoch.
    // Epoch Manager asks for height difference by obtaining first Block Info of the Epoch.
    pub prev_epoch_first_block_info: BlockInfo,
    // This Block Info is required in State Sync that is started right after Epoch Sync is finished.
    // It is used by `verify_chunk_signature_with_header_parts` in `save_block` as it calls `get_epoch_id_from_prev_block`.
    pub prev_epoch_prev_last_block_info: BlockInfo,
    // This Block Info is connected with the first actual Block received in State Sync.
    // It is also used in Epoch Manager.
    pub prev_epoch_last_block_info: BlockInfo,
    pub prev_epoch_info: EpochInfo,
    pub cur_epoch_info: EpochInfo,
    // Next Epoch Info is required by Block Sync when Blocks of current Epoch will come.
    // It asks in `process_block_single`, returns `Epoch Out Of Bounds` error otherwise.
    pub next_epoch_info: EpochInfo,
}

#[derive(BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, Clone)]
pub enum EpochSyncResponse {
    UpToDate,
    Advance { light_client_block_view: Box<LightClientBlockView> },
}

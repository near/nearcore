use anyhow::Error;
use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::test_loop::{data::TestLoopDataHandle, TestLoopV2};
use near_client::{GetBlock, GetChunk, GetValidatorInfo, ViewClientActorInner};
use near_primitives::views::{BlockView, ChunkView, EpochValidatorInfo};

use crate::test_loop::env::TestData;

/// Utility for making calls to [`ViewClientActor`] to retrieve some information.
pub(crate) struct ViewRequestSender {
    /// List of data handles to the view client senders for sending the requests.
    /// Used to locate the right view client to send a request (by index).
    handles: Vec<TestLoopDataHandle<ViewClientActorInner>>,
}

impl ViewRequestSender {
    pub(crate) fn new(test_data: &Vec<TestData>) -> Self {
        Self {
            // Save the handles for the view client senders.
            handles: test_data
                .iter()
                .map(|data| data.view_client_sender.actor_handle())
                .collect_vec(),
        }
    }

    /// Gets a block using a [`GetBlock`] request from the view client at index `idx`.
    pub(crate) fn get_block(
        &self,
        request: GetBlock,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<BlockView, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        view_client.handle(request).map_err(|e| e.into())
    }

    /// Gets a chunk using a [`GetChunk`] request from the view client at index `idx`.
    pub(crate) fn get_chunk(
        &self,
        request: GetChunk,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<ChunkView, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        view_client.handle(request).map_err(|e| e.into())
    }

    /// Gets validator information using a [`GetValidatorInfo`] request from the view client at index `idx`.
    /// The validator info is extracted from epoch info, so if the request contains a block identifier, it
    /// should be the last block of the epoch.
    pub(crate) fn get_validator_info(
        &self,
        request: GetValidatorInfo,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<EpochValidatorInfo, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        view_client.handle(request).map_err(|e| e.into())
    }
}

use anyhow::Error;
use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::test_loop::{data::TestLoopDataHandle, TestLoopV2};
use near_client::{GetBlock, GetChunk, ViewClientActorInner};
use near_primitives::views::{BlockView, ChunkView};

use crate::test_loop::env::TestData;

pub(crate) struct ViewRequestSender {
    /// List of data handles to the view client senders for sending the requests.
    handles: Vec<TestLoopDataHandle<ViewClientActorInner>>,
}

impl ViewRequestSender {
    pub(crate) fn new(test_data: &Vec<TestData>) -> Self {
        Self {
            handles: test_data
                .iter()
                .map(|data| data.view_client_sender.actor_handle())
                .collect_vec(),
        }
    }

    pub(crate) fn get_block(
        &self,
        request: GetBlock,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<BlockView, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        match view_client.handle(request) {
            Ok(block) => Ok(block),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn get_chunk(
        &self,
        request: GetChunk,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<ChunkView, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        match view_client.handle(request) {
            Ok(chunk) => Ok(chunk),
            Err(e) => Err(e.into()),
        }
    }
}

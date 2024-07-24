use crate::test_loop::env::TestData;
use anyhow::Error;
use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::test_loop::{data::TestLoopDataHandle, TestLoopV2};
use near_client::ViewClientActorInner;
use near_client_primitives::types::GetBlock;
use near_primitives::{
    types::{BlockId, BlockReference},
    views::BlockView,
};

struct RpcSender {
    /// List of data handles to the view client senders for sending the requests.
    handles: Vec<TestLoopDataHandle<ViewClientActorInner>>,
}

impl RpcSender {
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
        height: u64,
        test_loop: &mut TestLoopV2,
        idx: usize,
    ) -> Result<BlockView, Error> {
        let view_client = test_loop.data.get_mut(&self.handles[idx]);
        match view_client.handle(GetBlock(BlockReference::BlockId(BlockId::Height(height)))) {
            Ok(block) => Ok(block),
            Err(e) => Err(e.into()),
        }
    }
}

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

use near_async::messaging::CanSend;
use near_client::DistributeStateWitnessRequest;

#[derive(Clone, Default)]
pub struct MockPartialWitnessAdapter {
    distribution_request: Arc<RwLock<VecDeque<DistributeStateWitnessRequest>>>,
}

impl CanSend<DistributeStateWitnessRequest> for MockPartialWitnessAdapter {
    fn send(&self, msg: DistributeStateWitnessRequest) {
        self.distribution_request.write().push_back(msg);
    }
}

impl MockPartialWitnessAdapter {
    pub fn pop_distribution_request(&self) -> Option<DistributeStateWitnessRequest> {
        self.distribution_request.write().pop_front()
    }
}

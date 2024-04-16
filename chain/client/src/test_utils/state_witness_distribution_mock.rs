use near_async::messaging::CanSend;
use near_network::types::PeerManagerAdapter;

use crate::stateless_validation::state_witness_distribution_actions::StateWitnessDistributionActions;
use crate::stateless_validation::state_witness_distribution_actor::DistributeChunkStateWitnessRequest;

#[derive(Clone)]
pub struct SynchronousStateWitnessDistributionAdapter {
    actions: StateWitnessDistributionActions,
}

impl SynchronousStateWitnessDistributionAdapter {
    pub fn new(network_adapter: PeerManagerAdapter) -> Self {
        Self { actions: StateWitnessDistributionActions::new(network_adapter) }
    }
}

impl CanSend<DistributeChunkStateWitnessRequest> for SynchronousStateWitnessDistributionAdapter {
    fn send(&self, msg: DistributeChunkStateWitnessRequest) {
        self.actions.handle_distribute_chunk_state_witness_request(msg).unwrap();
    }
}

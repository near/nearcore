use near_async::messaging::CanSend;
use near_chain::Error;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};

use super::state_witness_distribution_actor::DistributeChunkStateWitnessRequest;

#[derive(Clone)]
pub struct StateWitnessDistributionActions {
    network_adapter: PeerManagerAdapter,
}

impl StateWitnessDistributionActions {
    pub fn new(network_adapter: PeerManagerAdapter) -> Self {
        Self { network_adapter }
    }

    pub fn handle_distribute_chunk_state_witness_request(
        &self,
        msg: DistributeChunkStateWitnessRequest,
    ) -> Result<(), Error> {
        let DistributeChunkStateWitnessRequest { chunk_validators, signed_witness } = msg;

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators, signed_witness),
        ));

        Ok(())
    }
}

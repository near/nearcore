use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{Actor, Handler, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
};
use near_network::types::PeerManagerAdapter;
use near_performance_metrics_macros::perf;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::ShardId;
use std::sync::Arc;

use crate::client_actor::ClientSenderForPartialWitness;

use super::partial_witness_actor_v2::{
    PartialWitnessMsg, PartialWitnessSender, PartialWitnessService,
};

pub struct PartialWitnessActor {
    tx: PartialWitnessSender,
}

impl Actor for PartialWitnessActor {}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub state_witness: ChunkStateWitness,
    pub contract_updates: ContractUpdates,
    pub main_transition_shard_id: ShardId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PartialWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
}

impl Handler<DistributeStateWitnessRequest> for PartialWitnessActor {
    #[perf]
    fn handle(&mut self, msg: DistributeStateWitnessRequest) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::DistributeStateWitnessRequest(Box::new(msg))).await.unwrap();
        });
    }
}

impl Handler<ChunkStateWitnessAckMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkStateWitnessAckMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::ChunkStateWitnessAckMessage(msg)).await.unwrap();
        });
    }
}

impl Handler<PartialEncodedStateWitnessMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::PartialEncodedStateWitnessMessage(msg)).await.unwrap();
        });
    }
}

impl Handler<PartialEncodedStateWitnessForwardMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedStateWitnessForwardMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::PartialEncodedStateWitnessForwardMessage(msg))
                .await
                .unwrap();
        });
    }
}

impl Handler<ChunkContractAccessesMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ChunkContractAccessesMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::ChunkContractAccessesMessage(msg)).await.unwrap();
        });
    }
}

impl Handler<PartialEncodedContractDeploysMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: PartialEncodedContractDeploysMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::PartialEncodedContractDeploysMessage(msg)).await.unwrap();
        });
    }
}

impl Handler<ContractCodeRequestMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeRequestMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::ContractCodeRequestMessage(msg)).await.unwrap();
        });
    }
}

impl Handler<ContractCodeResponseMessage> for PartialWitnessActor {
    fn handle(&mut self, msg: ContractCodeResponseMessage) {
        let tx = self.tx.clone();
        actix::spawn(async move {
            tx.send(PartialWitnessMsg::ContractCodeResponseMessage(msg)).await.unwrap();
        });
    }
}

impl PartialWitnessActor {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        client_sender: ClientSenderForPartialWitness,
        my_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        compile_contracts_spawner: Arc<dyn AsyncComputationSpawner>,
        partial_witness_spawner: Arc<dyn AsyncComputationSpawner>,
    ) -> Self {
        let tx = PartialWitnessService::new(
            clock,
            network_adapter,
            client_sender,
            my_signer,
            epoch_manager,
            runtime,
            compile_contracts_spawner,
            partial_witness_spawner,
        );
        Self { tx }
    }
}

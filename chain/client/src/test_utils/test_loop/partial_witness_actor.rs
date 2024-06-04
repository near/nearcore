use crate::stateless_validation::partial_witness::partial_witness_actor::PartialWitnessSenderForClient;
use crate::{PartialWitnessActor, PartialWitnessSenderForClientMessage};
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::v2::{self, LoopData, LoopStream};
use near_network::state_witness::{
    PartialWitnessSenderForNetwork, PartialWitnessSenderForNetworkMessage,
};

pub fn forward_messages_from_network_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActor, PartialWitnessSenderForNetworkMessage> {
    LoopEventHandler::new_simple(|msg, partial_witness_actor: &mut PartialWitnessActor| match msg {
        PartialWitnessSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
            partial_witness_actor.handle(msg);
        }
        PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness(msg) => {
            partial_witness_actor.handle(msg);
        }
        PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness_forward(msg) => {
            partial_witness_actor.handle(msg);
        }
    })
}

pub fn forward_messages_from_client_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActor, PartialWitnessSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, partial_witness_actor: &mut PartialWitnessActor| match msg {
        PartialWitnessSenderForClientMessage::_distribute_chunk_state_witness(msg) => {
            partial_witness_actor.handle(msg);
        }
    })
}

#[derive(Clone)]
#[must_use = "Builder should be used to build; otherwise events would not be handled"]
pub struct LoopPartialWitnessActorBuilder {
    pub from_client_stream: LoopStream<PartialWitnessSenderForClientMessage>,
    pub from_network_stream: LoopStream<PartialWitnessSenderForNetworkMessage>,
    pub from_client: PartialWitnessSenderForClient,
    pub from_network: PartialWitnessSenderForNetwork,
}

#[derive(Clone)]
pub struct LoopPartialWitnessActor {
    pub actor: LoopData<PartialWitnessActor>,
    pub from_client: PartialWitnessSenderForClient,
    pub from_network: PartialWitnessSenderForNetwork,
}

pub fn loop_partial_witness_actor_builder(
    test: &mut v2::TestLoop,
) -> LoopPartialWitnessActorBuilder {
    let from_client_stream = test.new_stream();
    let from_client = from_client_stream.wrapped_multi_sender();
    let from_network_stream = test.new_stream();
    let from_network = from_network_stream.wrapped_multi_sender();
    LoopPartialWitnessActorBuilder {
        from_client_stream,
        from_network_stream,
        from_client,
        from_network,
    }
}

impl LoopPartialWitnessActorBuilder {
    pub fn build(
        self,
        test: &mut v2::TestLoop,
        actor: PartialWitnessActor,
    ) -> LoopPartialWitnessActor {
        let actor = test.add_data(actor);
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_messages_from_client_to_partial_witness_actor(),
        );
        self.from_network_stream.handle1_legacy(
            test,
            actor,
            forward_messages_from_network_to_partial_witness_actor(),
        );
        LoopPartialWitnessActor {
            actor,
            from_client: self.from_client,
            from_network: self.from_network,
        }
    }
}

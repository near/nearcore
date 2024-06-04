use super::forward_messages_from_partial_witness_actor_to_client;
use crate::client_actor::{
    ClientActorInner, ClientSenderForClient, ClientSenderForClientMessage,
    ClientSenderForPartialWitness, ClientSenderForPartialWitnessMessage,
};
use crate::sync_jobs_actor::{ClientSenderForSyncJobs, ClientSenderForSyncJobsMessage};
use crate::SyncMessage;
use near_async::messaging::{Handler, Sender};
use near_async::test_loop::event_handler::LoopEventHandler;
use near_async::test_loop::futures::TestLoopDelayedActionRunner;
use near_async::v2::{self, LoopData, LoopStream};
use near_chunks::client::ShardsManagerResponse;
use near_network::client::{ClientSenderForNetwork, ClientSenderForNetworkMessage};

pub fn forward_client_messages_from_network_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForNetworkMessage> {
    LoopEventHandler::new(|msg, client_actor: &mut ClientActorInner| {
        match msg {
            ClientSenderForNetworkMessage::_state_response(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_approval(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_transaction(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_block_headers(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_challenge(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_network_info(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            ClientSenderForNetworkMessage::_chunk_endorsement(msg) => {
                (msg.callback)(Ok(client_actor.handle(msg.message)));
            }
            _ => {
                return Err(msg);
            }
        }
        Ok(())
    })
}

pub fn forward_client_messages_from_client_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| match msg {
        ClientSenderForClientMessage::_apply_chunks_done(msg) => client_actor.handle(msg),
    })
}

pub fn forward_client_messages_from_sync_jobs_to_client_actor(
) -> LoopEventHandler<ClientActorInner, ClientSenderForSyncJobsMessage> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| match msg {
        ClientSenderForSyncJobsMessage::_apply_state_parts_response(msg) => {
            client_actor.handle(msg)
        }
        ClientSenderForSyncJobsMessage::_block_catch_up_response(msg) => client_actor.handle(msg),
        ClientSenderForSyncJobsMessage::_resharding_response(msg) => client_actor.handle(msg),
        ClientSenderForSyncJobsMessage::_load_memtrie_response(msg) => client_actor.handle(msg),
    })
}

pub fn forward_client_messages_from_shards_manager(
) -> LoopEventHandler<ClientActorInner, ShardsManagerResponse> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| {
        client_actor.handle(msg);
    })
}

pub fn forward_client_messages_from_sync_adapter() -> LoopEventHandler<ClientActorInner, SyncMessage>
{
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| {
        client_actor.handle(msg);
    })
}

#[derive(Clone)]
#[must_use = "Builder should be used to build; otherwise events would not be handled"]
pub struct LoopClientActorBuilder {
    pub from_client_stream: LoopStream<ClientSenderForClientMessage>,
    pub from_client: ClientSenderForClient,
    pub from_network_stream: LoopStream<ClientSenderForNetworkMessage>,
    pub from_network: ClientSenderForNetwork,
    pub from_sync_jobs_stream: LoopStream<ClientSenderForSyncJobsMessage>,
    pub from_sync_jobs: ClientSenderForSyncJobs,
    pub from_partial_witness_stream: LoopStream<ClientSenderForPartialWitnessMessage>,
    pub from_partial_witness: ClientSenderForPartialWitness,
    pub from_shards_manager_stream: LoopStream<ShardsManagerResponse>,
    pub from_shards_manager: Sender<ShardsManagerResponse>,
    pub from_sync_adapter_stream: LoopStream<SyncMessage>,
    pub from_sync_adapter: Sender<SyncMessage>,
}

pub fn loop_client_actor_builder(test: &mut v2::TestLoop) -> LoopClientActorBuilder {
    let from_client_stream = test.new_stream();
    let from_client = from_client_stream.wrapped_multi_sender();
    let from_network_stream = test.new_stream();
    let from_network = from_network_stream.wrapped_multi_sender();
    let from_sync_jobs_stream = test.new_stream();
    let from_sync_jobs = from_sync_jobs_stream.wrapped_multi_sender();
    let from_partial_witness_stream = test.new_stream();
    let from_partial_witness = from_partial_witness_stream.wrapped_multi_sender();
    let from_shards_manager_stream = test.new_stream();
    let from_shards_manager = from_shards_manager_stream.sender();
    let from_sync_adapter_stream = test.new_stream();
    let from_sync_adapter = from_sync_adapter_stream.sender();
    LoopClientActorBuilder {
        from_client_stream,
        from_client,
        from_network_stream,
        from_network,
        from_sync_jobs_stream,
        from_sync_jobs,
        from_partial_witness_stream,
        from_partial_witness,
        from_shards_manager_stream,
        from_shards_manager,
        from_sync_adapter_stream,
        from_sync_adapter,
    }
}

#[derive(Clone)]
pub struct LoopClientActor {
    pub actor: LoopData<ClientActorInner>,
    pub from_client: ClientSenderForClient,
    pub from_network: ClientSenderForNetwork,
    pub from_sync_jobs: ClientSenderForSyncJobs,
    pub from_partial_witness: ClientSenderForPartialWitness,
    pub from_shards_manager: Sender<ShardsManagerResponse>,
    pub from_sync_adapter: Sender<SyncMessage>,
    pub delayed_action_runner: TestLoopDelayedActionRunner<ClientActorInner>,
}

impl LoopClientActorBuilder {
    pub fn build(self, test: &mut v2::TestLoop, actor: ClientActorInner) -> LoopClientActor {
        let actor = test.add_data(actor);
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_client_messages_from_client_to_client_actor(),
        );
        self.from_network_stream.handle1_legacy(
            test,
            actor,
            forward_client_messages_from_network_to_client_actor(),
        );
        self.from_sync_jobs_stream.handle1_legacy(
            test,
            actor,
            forward_client_messages_from_sync_jobs_to_client_actor(),
        );
        self.from_partial_witness_stream.handle1_legacy(
            test,
            actor,
            forward_messages_from_partial_witness_actor_to_client(),
        );
        self.from_shards_manager_stream.handle1_legacy(
            test,
            actor,
            forward_client_messages_from_shards_manager(),
        );
        self.from_sync_adapter_stream.handle1_legacy(
            test,
            actor,
            forward_client_messages_from_sync_adapter(),
        );
        let delayed_action_runner = test.new_delayed_actions_runner(actor);
        LoopClientActor {
            actor,
            from_client: self.from_client,
            from_network: self.from_network,
            from_sync_jobs: self.from_sync_jobs,
            from_partial_witness: self.from_partial_witness,
            from_shards_manager: self.from_shards_manager,
            from_sync_adapter: self.from_sync_adapter,
            delayed_action_runner,
        }
    }
}

use crate::client_actor::ClientActor;
use crate::view_client::ViewClientActor;
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::IntoSender;
use near_network::client::{BlockRequest, BlockResponse, ClientSenderForNetwork};
use near_network::types::{
    PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg,
};
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::PartialEncodedChunk;

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct RecvPartialEncodedChunkForward(pub PartialEncodedChunkForwardMsg);

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct RecvPartialEncodedChunk(pub PartialEncodedChunk);

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct RecvPartialEncodedChunkResponse(
    pub PartialEncodedChunkResponseMsg,
    pub std::time::Instant,
);

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct RecvPartialEncodedChunkRequest(pub PartialEncodedChunkRequestMsg, pub CryptoHash);

pub fn client_sender_for_network(
    client_addr: actix::Addr<ClientActor>,
    view_client_addr: actix::Addr<ViewClientActor>,
) -> ClientSenderForNetwork {
    let client_addr = client_addr.with_auto_span_context();
    let view_client_addr = view_client_addr.with_auto_span_context();
    ClientSenderForNetwork {
        block: client_addr.clone().into_sender(),
        block_headers: client_addr.clone().into_sender(),
        block_approval: client_addr.clone().into_sender(),
        block_headers_request: view_client_addr.clone().into_sender(),
        block_request: view_client_addr.clone().into_sender(),
        challenge: client_addr.clone().into_sender(),
        network_info: client_addr.clone().into_sender(),
        state_request_header: view_client_addr.clone().into_sender(),
        state_request_part: view_client_addr.clone().into_sender(),
        state_response: client_addr.clone().into_sender(),
        transaction: client_addr.clone().into_sender(),
        tx_status_request: view_client_addr.clone().into_sender(),
        tx_status_response: view_client_addr.clone().into_sender(),
    }

    async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        match self.view_client_addr.send(AnnounceAccountRequest(accounts).with_span_context()).await
        {
            Ok(res) => res,
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(vec![])
            }
        }
    }

    async fn chunk_state_witness(&self, witness: ChunkStateWitness, peer_id: PeerId) {
        match self
            .client_addr
            .send(
                ChunkStateWitnessMessage { witness, peer_id, attempts_remaining: 5 }
                    .with_span_context(),
            )
            .await
        {
            Ok(()) => {}
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }

    async fn chunk_endorsement(&self, endorsement: ChunkEndorsement) {
        match self.client_addr.send(ChunkEndorsementMessage(endorsement).with_span_context()).await
        {
            Ok(()) => {}
            Err(err) => tracing::error!("mailbox error: {err}"),
        }
    }
}

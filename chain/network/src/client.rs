use crate::types::{NetworkInfo, ReasonForBan};
use near_primitives::block::{Approval, Block, BlockHeader};
use near_primitives::challenge::Challenge;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::state_sync::{
    StateRequestHeader, StateRequestPart, StateResponseHeader, StateResponsePart,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, EpochId};
use near_primitives::views::FinalExecutionOutcomeView;

/// A strongly typed asynchronous API for the Client logic.
/// It abstracts away the fact that client is implemented using actix
/// actors.
#[async_trait::async_trait]
pub trait Client: Send + Sync + 'static {
    async fn tx_status_request(
        &self,
        account_id: AccountId,
        tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>>;

    async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView);

    async fn state_request_header(
        &self,
        request: StateRequestHeader,
    ) -> Result<Option<StateResponseHeader>, ReasonForBan>;

    async fn state_request_part(
        &self,
        request: StateRequestPart,
    ) -> Result<Option<StateResponsePart>, ReasonForBan>;

    async fn state_response_header(&self, header: StateResponseHeader);

    async fn state_response_part(&self, header: StateResponsePart);

    async fn block_approval(&self, approval: Approval, peer_id: PeerId);

    async fn transaction(&self, transaction: SignedTransaction, is_forwarded: bool);

    async fn block_request(&self, hash: CryptoHash) -> Option<Box<Block>>;

    async fn block_headers_request(&self, hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>>;

    async fn block(&self, block: Block, peer_id: PeerId, was_requested: bool);

    async fn block_headers(
        &self,
        headers: Vec<BlockHeader>,
        peer_id: PeerId,
    ) -> Result<(), ReasonForBan>;

    async fn challenge(&self, challenge: Challenge);

    async fn network_info(&self, info: NetworkInfo);

    async fn announce_account(
        &self,
        accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan>;
}

/// Implementation of Client which doesn't do anything and never returns errors.
pub struct Noop;

#[async_trait::async_trait]
impl Client for Noop {
    async fn tx_status_request(
        &self,
        _account_id: AccountId,
        _tx_hash: CryptoHash,
    ) -> Option<Box<FinalExecutionOutcomeView>> {
        None
    }

    async fn tx_status_response(&self, _tx_result: FinalExecutionOutcomeView) {}

    async fn state_request_header(
        &self,
        _request: StateRequestHeader,
    ) -> Result<Option<StateResponseHeader>, ReasonForBan> {
        Ok(None)
    }

    async fn state_request_part(
        &self,
        _request: StateRequestPart,
    ) -> Result<Option<StateResponsePart>, ReasonForBan> {
        Ok(None)
    }

    async fn state_response_header(&self, _header: StateResponseHeader) {}
    async fn state_response_part(&self, _part: StateResponsePart) {}

    async fn block_approval(&self, _approval: Approval, _peer_id: PeerId) {}

    async fn transaction(&self, _transaction: SignedTransaction, _is_forwarded: bool) {}

    async fn block_request(&self, _hash: CryptoHash) -> Option<Box<Block>> {
        None
    }

    async fn block_headers_request(&self, _hashes: Vec<CryptoHash>) -> Option<Vec<BlockHeader>> {
        None
    }

    async fn block(&self, _block: Block, _peer_id: PeerId, _was_requested: bool) {}

    async fn block_headers(
        &self,
        _headers: Vec<BlockHeader>,
        _peer_id: PeerId,
    ) -> Result<(), ReasonForBan> {
        Ok(())
    }

    async fn challenge(&self, _challenge: Challenge) {}

    async fn network_info(&self, _info: NetworkInfo) {}

    async fn announce_account(
        &self,
        _accounts: Vec<(AnnounceAccount, Option<EpochId>)>,
    ) -> Result<Vec<AnnounceAccount>, ReasonForBan> {
        Ok(vec![])
    }
}

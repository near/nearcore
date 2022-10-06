
pub(crate) Client {
    /// Address of the client actor.
    pub client_addr: Recipient<NetworkClientMessages>,
    /// Address of the view client actor.
    pub view_client_addr: Recipient<NetworkViewClientMessages>,
}

pub(crate) Result<T> = Result<T,ReasonForBan>;

impl Client {
    pub async fn tx_status_request(&self, account_id: AccountId, tx_hash: CryptoHash) -> Result<Option<FinalExecutionOutcomeView>> {
        match self.view_client_addr.send(NetworkViewClientMessages::TxStatus {
            tx_hash: tx_hash,
            signer_account_id: account_id,
        }).await {
            Ok(NetworkViewClientResponses::TxStatus(tx_result)) => Ok(Some(tx_result)),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None),
            },
        }
    }

    pub async fn tx_status_response(&self, tx_result: FinalExecutionOutcomeView) -> Result<()> {
        match self.view_client_addr.send(NetworkViewClientMessages::TxStatusResponse(Box::new(tx_result.clone())).await {
            Ok(NetworkViewClientResponses::NoResponse) => Ok(()),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
    
    pub async fn state_request_header(&self, shard_id: ShardId, sync_hash: CryptoHash) -> Result<Option<StateResponseInfo>> {
        match self.view_client_addr.send(NetworkViewClientMessages::StateRequestHeader {
            shard_id: shard_id,
            sync_hash: sync_hash,
        }).await {
            Ok(NetworkViewClientResponses::StateResponse(resp)) => Ok(Some(resp)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            },
        }
    }
    
    pub async fn state_request_part(&self, shard_id: ShardId, sync_hash: CryptoHash, part_id: u64) -> Result<Option<StateResponseInfo>> {
        match self.view_client_addr.send(NetworkViewClientMessages::StateRequestPart {
            shard_id: shard_id,
            sync_hash: sync_hash,
            part_id: part_id,
        }).await {
            Ok(NetworkViewClientResponses::StateResponse(resp)) => Ok(Some(resp)),
            Ok(NetworkViewClientResponses::NoResponse => Ok(None),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            },
        }
    }

    pub async fn state_response(&self, info: StateResponseInfo) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::StateResponse(info)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
    
    pub async fn block_approval(&self, approval: Approval, peer_id: PeerId) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::BlockApproval(approval, peer_id)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
    
    pub async fn transaction(&self, transaction: Transaction, is_forwarded: bool, check_only: bool) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::Transaction{transaction, is_forwarded, check_only}).await {
            Ok(NetworkClientResponses::ValidTx) => Ok(())
            Ok(NetworkClientResponses::InvalidTx(err)) => {
                warn!(target: "network", "Received invalid tx from peer {}: {}", act.peer_info, err);
                // TODO: count as malicious behavior?
                Ok(())
            }
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }

    pub async fn partial_encoded_chunk_request(req: PartialEncodedChunkRequestMsg, msg_hash: CryptoHash) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::PartialEncodedChunkRequest(req, msg_hash)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }

    pub async fn partial_encoded_chunk_response(resp: PartialEncodedChunkResponseMsg, timestamp: time::Instant) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::PartialEncodedChunkResponse(resp, timestamp)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
    
    pub async fn partial_encoded_chunk(chunk: PartialEncodedChunk) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::PartialEncodedChunk(chunk)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }

    pub async fn partial_encoded_chunk_forward(msg: PartialEncodedChunkForwardMsg) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::PartialEncodedChunkForward(msg)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
            
    pub async fn block_request(hash: CryptoHash) -> Result<Option<Block>> {
        match self.view_client_addr.send(NetworkViewClientMessages::BlockRequest(hash)).await {
            Ok(NetworkViewClientResponses::Block(block) => Ok(Some(block)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            },
        }
    }

    pub async fn block_headers_request(hashes: Vec<CryptoHash>) -> Result<Option<Vec<BlockHeader>>> {
        match self.view_client_addr.send(NetworkViewClientMessages::BlockHeadersRequest(hashes)).await {
            Ok(NetworkViewClientResponses::BlockHeaders(block_headers) => Ok(Some(block_headers)),
            Ok(NetworkViewClientResponses::NoResponse) => Ok(None),
            Ok(NetworkViewClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ViewClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(None)
            },
        }
    }
  
    pub async fn block(block: Block, peer_id: PeerId) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::Block(block,peer_id/*,???*/)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }
   
    pub async fn block_headers(block: Block, peer_id: PeerId) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::BlockHeaders(block, peer_id)).await {
            Ok(NetworkClientResponses::NoResponse) => Ok(()),
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                Ok(())
            },
        }
    }

    pub async fn challenge(challenge: Challenge) -> Result<()> {
        match self.client_addr.send(NetworkClientMessages::Challenge(challenge)).await {
            Ok(NetworkClientResponses::NoResponse) => None,
            Ok(NetworkClientResponses::Ban(reason)) => Err(reason),
            Ok(resp) => panic!("unexpected ClientResponse: {resp:?}"),
            Err(err) => {
                tracing::error!("mailbox error: {err}");
                None
            },
        }
    }
}

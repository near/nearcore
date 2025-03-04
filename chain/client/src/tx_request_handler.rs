use near_async::actix_wrapper::SyncActixWrapper;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::time::Clock;
use near_chain::Chain;
use near_chain::ChainGenesis;
use near_chain::types::RuntimeAdapter;
use near_chain::types::Tip;
use near_chain_configs::ClientConfig;
use near_chain_configs::MutableValidatorSigner;
use near_chunks::client::ShardedTransactionPool;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::account_id_to_shard_id;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::ProcessTxRequest;
use near_network::client::ProcessTxResponse;
use near_network::types::NetworkRequests;
use near_network::types::PeerManagerAdapter;
use near_network::types::PeerManagerMessageRequest;
use near_pool::InsertTransactionResult;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::EpochId;
use near_primitives::types::ShardId;
use near_primitives::unwrap_or_return;
use near_primitives::validator_signer::ValidatorSigner;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use crate::metrics;

pub type TxRequestHandlerActor = SyncActixWrapper<TxRequestHandler>;

impl Handler<ProcessTxRequest> for TxRequestHandler {
    fn handle(&mut self, msg: ProcessTxRequest) -> ProcessTxResponse {
        let ProcessTxRequest { transaction, is_forwarded, check_only } = msg;
        self.process_tx(transaction, is_forwarded, check_only)
    }
}

pub fn spawn_tx_request_handler_actor(
    clock: Clock,
    config: ClientConfig,
    tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    validator_signer: MutableValidatorSigner,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_genesis: ChainGenesis,
    network_adapter: PeerManagerAdapter,
) -> actix::Addr<TxRequestHandlerActor> {
    actix::SyncArbiter::start(config.transaction_request_handler_threads, move || {
        let view_client_actor = TxRequestHandler::new(
            clock.clone(),
            config.clone(),
            tx_pool.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            validator_signer.clone(),
            runtime.clone(),
            chain_genesis.clone(),
            network_adapter.clone(),
        )
        .unwrap();
        SyncActixWrapper::new(view_client_actor)
    })
}

#[derive(Clone)]
struct TxRequestHandlerConfig {
    tx_routing_height_horizon: u64,
    epoch_length: u64,
}

/// Accepts `process_tx` requests. Pushes the incoming transactions to the pool.
pub struct TxRequestHandler {
    config: TxRequestHandlerConfig,
    tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    chain: Chain,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    validator_signer: MutableValidatorSigner,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
}

impl TxRequestHandler {
    pub fn new(
        clock: Clock,
        config: ClientConfig,
        tx_pool: Arc<Mutex<ShardedTransactionPool>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        validator_signer: MutableValidatorSigner,
        runtime: Arc<dyn RuntimeAdapter>,
        chain_genesis: ChainGenesis,
        network_adapter: PeerManagerAdapter,
    ) -> Result<Self, near_client_primitives::types::Error> {
        let my_config = TxRequestHandlerConfig {
            tx_routing_height_horizon: config.tx_routing_height_horizon,
            epoch_length: config.epoch_length,
        };

        let chain = Chain::new_for_view_client(
            clock.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            runtime.clone(),
            &chain_genesis,
            near_chain::DoomslugThresholdMode::TwoThirds,
            config.save_trie_changes,
        )?;

        Ok(Self {
            config: my_config,
            tx_pool,
            validator_signer,
            chain,
            epoch_manager,
            runtime,
            shard_tracker,
            network_adapter,
        })
    }

    /// Submits the transaction for future inclusion into the chain.
    ///
    /// If accepted, it will be added to the transaction pool and possibly forwarded to another validator.
    #[must_use]
    pub fn process_tx(
        &mut self,
        tx: SignedTransaction,
        is_forwarded: bool,
        check_only: bool,
    ) -> ProcessTxResponse {
        let signer = self.validator_signer.get();
        unwrap_or_return!(self.process_tx_internal(&tx, is_forwarded, check_only, &signer), {
            let me = signer.as_ref().map(|signer| signer.validator_id());
            tracing::warn!(target: "client", ?me, ?tx, "Dropping tx");
            ProcessTxResponse::NoResponse
        })
    }

    /// Process transaction and either add it to the mempool or return to redirect to another validator.
    fn process_tx_internal(
        &mut self,
        tx: &SignedTransaction,
        is_forwarded: bool,
        check_only: bool,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<ProcessTxResponse, near_client_primitives::types::Error> {
        let head = self.chain.head()?;
        let me = signer.as_ref().map(|vs| vs.validator_id());
        let cur_block = self.chain.get_head_block()?;
        let cur_block_header = cur_block.header();
        // here it is fine to use `cur_block_header` as it is a best effort estimate. If the transaction
        // were to be included, the block that the chunk points to will have height >= height of
        // `cur_block_header`.
        if let Err(e) = self
            .chain
            .chain_store()
            .check_transaction_validity_period(&cur_block_header, tx.transaction.block_hash())
        {
            tracing::debug!(target: "client", ?tx, "Invalid tx: expired or from a different fork");
            return Ok(ProcessTxResponse::InvalidTx(e));
        }
        let gas_price = cur_block_header.next_gas_price();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        let shard_layout = self.runtime.get_shard_layout(&epoch_id)?;
        let receiver_shard = account_id_to_shard_id(
            self.epoch_manager.as_ref(),
            tx.transaction.receiver_id(),
            &epoch_id,
        )?;
        let receiver_congestion_info =
            cur_block.block_congestion_info().get(&receiver_shard).copied();
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;

        if let Err(err) = self.runtime.validate_tx(
            &shard_layout,
            tx,
            protocol_version,
            receiver_congestion_info,
        ) {
            tracing::debug!(target: "client", tx_hash = ?tx.get_hash(), ?err, "Invalid tx during basic validation");
            return Ok(ProcessTxResponse::InvalidTx(err));
        }

        let shard_id = account_id_to_shard_id(
            self.epoch_manager.as_ref(),
            tx.transaction.signer_id(),
            &epoch_id,
        )?;
        let cares_about_shard =
            self.shard_tracker.cares_about_shard(me, &head.last_block_hash, shard_id, true);
        let will_care_about_shard =
            self.shard_tracker.will_care_about_shard(me, &head.last_block_hash, shard_id, true);

        if cares_about_shard || will_care_about_shard {
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
            let state_root = match self.chain.get_chunk_extra(&head.last_block_hash, &shard_uid) {
                Ok(chunk_extra) => *chunk_extra.state_root(),
                Err(_) => {
                    // Not being able to fetch a state root most likely implies that we haven't
                    //     caught up with the next epoch yet.
                    if is_forwarded {
                        return Err(near_client_primitives::types::Error::Other("Node has not caught up yet".to_string()));
                    } else {
                        self.forward_tx(&epoch_id, tx, signer)?;
                        return Ok(ProcessTxResponse::RequestRouted);
                    }
                }
            };
            if let Err(err) = self.runtime.can_verify_and_charge_tx(
                &shard_layout,
                gas_price,
                state_root,
                tx,
                protocol_version,
            ) {
                tracing::debug!(target: "client", ?err, "Invalid tx");
                return Ok(ProcessTxResponse::InvalidTx(err));
            }
            if check_only {
                return Ok(ProcessTxResponse::ValidTx);
            }
            // Transactions only need to be recorded if the node is a validator.
            if me.is_some() {
                let mut pool = self.tx_pool.lock().unwrap();
                match pool.insert_transaction(shard_uid, tx.clone())
                {
                    InsertTransactionResult::Success => {
                        tracing::trace!(target: "client", ?shard_uid, tx_hash = ?tx.get_hash(), "Recorded a transaction.");
                    }
                    InsertTransactionResult::Duplicate => {
                        tracing::trace!(target: "client", ?shard_uid, tx_hash = ?tx.get_hash(), "Duplicate transaction, not forwarding it.");
                        return Ok(ProcessTxResponse::ValidTx);
                    }
                    InsertTransactionResult::NoSpaceLeft => {
                        if is_forwarded {
                            tracing::trace!(target: "client", ?shard_uid, tx_hash = ?tx.get_hash(), "Transaction pool is full, dropping the transaction.");
                        } else {
                            tracing::trace!(target: "client", ?shard_uid, tx_hash = ?tx.get_hash(), "Transaction pool is full, trying to forward the transaction.");
                        }
                    }
                }
            }

            // Active validator:
            //   possibly forward to next epoch validators
            // Not active validator:
            //   forward to current epoch validators,
            //   possibly forward to next epoch validators
            if self.active_validator(shard_id, signer)? {
                tracing::trace!(target: "client", account = ?me, ?shard_id, tx_hash = ?tx.get_hash(), is_forwarded, "Recording a transaction.");
                metrics::TRANSACTION_RECEIVED_VALIDATOR.inc();

                if !is_forwarded {
                    self.possibly_forward_tx_to_next_epoch(tx, signer)?;
                }
                return Ok(ProcessTxResponse::ValidTx);
            }
            if !is_forwarded {
                tracing::trace!(target: "client", ?shard_id, tx_hash = ?tx.get_hash(), "Forwarding a transaction.");
                metrics::TRANSACTION_RECEIVED_NON_VALIDATOR.inc();
                self.forward_tx(&epoch_id, tx, signer)?;
                return Ok(ProcessTxResponse::RequestRouted);
            }
            tracing::trace!(target: "client", ?shard_id, tx_hash = ?tx.get_hash(), "Non-validator received a forwarded transaction, dropping it.");
            metrics::TRANSACTION_RECEIVED_NON_VALIDATOR_FORWARDED.inc();
            return Ok(ProcessTxResponse::NoResponse);
        }

        if check_only {
            return Ok(ProcessTxResponse::DoesNotTrackShard);
        }
        if is_forwarded {
            // Received forwarded transaction but we are not tracking the shard
            tracing::debug!(target: "client", ?me, ?shard_id, tx_hash = ?tx.get_hash(), "Received forwarded transaction but no tracking shard");
            return Ok(ProcessTxResponse::NoResponse);
        }
        // We are not tracking this shard, so there is no way to validate this tx. Just rerouting.
        self.forward_tx(&epoch_id, tx, signer).map(|()| ProcessTxResponse::RequestRouted)
    }

    /// Forwards given transaction to upcoming validators.
    fn forward_tx(
        &self,
        epoch_id: &EpochId,
        tx: &SignedTransaction,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), near_client_primitives::types::Error> {
        let shard_id = account_id_to_shard_id(
            self.epoch_manager.as_ref(),
            tx.transaction.signer_id(),
            epoch_id,
        )?;
        // Use the header head to make sure the list of validators is as
        // up-to-date as possible.
        let head = self.chain.header_head()?;
        let maybe_next_epoch_id = self.get_next_epoch_id_if_at_boundary(&head)?;

        let mut validators = HashSet::new();
        for horizon in (2..=self.config.tx_routing_height_horizon)
            .chain(vec![self.config.tx_routing_height_horizon * 2].into_iter())
        {
            let target_height = head.height + horizon - 1;
            let validator = self
                .epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id: *epoch_id,
                    height_created: target_height,
                    shard_id,
                })?
                .take_account_id();
            validators.insert(validator);
            if let Some(next_epoch_id) = &maybe_next_epoch_id {
                let next_shard_id = account_id_to_shard_id(
                    self.epoch_manager.as_ref(),
                    tx.transaction.signer_id(),
                    next_epoch_id,
                )?;
                let validator = self
                    .epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        epoch_id: *next_epoch_id,
                        height_created: target_height,
                        shard_id: next_shard_id,
                    })?
                    .take_account_id();
                validators.insert(validator);
            }
        }

        if let Some(account_id) = signer.as_ref().map(|bp| bp.validator_id()) {
            validators.remove(account_id);
        }
        for validator in validators {
            let tx_hash = tx.get_hash();
            tracing::trace!(target: "client", me = ?signer.as_ref().map(|bp| bp.validator_id()), ?tx_hash, ?validator, ?shard_id, "Routing a transaction");

            // Send message to network to actually forward transaction.
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::ForwardTx(validator, tx.clone()),
            ));
        }

        Ok(())
    }

    /// Determine if I am a validator in next few blocks for specified shard, assuming epoch doesn't change.
    fn active_validator(
        &self,
        shard_id: ShardId,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<bool, near_client_primitives::types::Error> {
        let head = self.chain.head()?;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash)?;

        let account_id = if let Some(vs) = signer.as_ref() {
            vs.validator_id()
        } else {
            return Ok(false);
        };

        for i in 1..=self.config.tx_routing_height_horizon {
            let chunk_producer = self
                .epoch_manager
                .get_chunk_producer_info(&ChunkProductionKey {
                    epoch_id,
                    height_created: head.height + i,
                    shard_id,
                })?
                .take_account_id();
            if &chunk_producer == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// If we're a validator in one of the next few chunks, but epoch switch could happen soon,
    /// we forward to a validator from next epoch.
    fn possibly_forward_tx_to_next_epoch(
        &mut self,
        tx: &SignedTransaction,
        signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), near_client_primitives::types::Error> {
        let head = self.chain.head()?;
        if let Some(next_epoch_id) = self.get_next_epoch_id_if_at_boundary(&head)? {
            self.forward_tx(&next_epoch_id, tx, signer)?;
        } else {
            self.forward_tx(&head.epoch_id, tx, signer)?;
        }
        Ok(())
    }

    /// If we are close to epoch boundary, return next epoch id, otherwise return None.
    fn get_next_epoch_id_if_at_boundary(
        &self,
        head: &Tip,
    ) -> Result<Option<EpochId>, near_client_primitives::types::Error> {
        let next_epoch_started =
            self.epoch_manager.is_next_block_epoch_start(&head.last_block_hash)?;
        if next_epoch_started {
            return Ok(None);
        }
        let next_epoch_estimated_height =
            self.epoch_manager.get_epoch_start_height(&head.last_block_hash)?
                + self.config.epoch_length;

        let epoch_boundary_possible =
            head.height + self.config.tx_routing_height_horizon >= next_epoch_estimated_height;
        if epoch_boundary_possible {
            Ok(Some(self.epoch_manager.get_next_epoch_id_from_prev_block(&head.last_block_hash)?))
        } else {
            Ok(None)
        }
    }
}

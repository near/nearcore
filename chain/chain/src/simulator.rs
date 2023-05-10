use std::{
    collections::VecDeque,
    sync::{mpsc, Arc, RwLock},
    thread::JoinHandle,
};

use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    block::{Block, Tip},
    block_header::BlockHeader,
    hash::CryptoHash,
    receipt::{Receipt, ReceiptEnum},
    runtime::migration_data::MigrationFlags,
    sharding::ShardChunk,
    transaction::{ExecutionOutcome, SignedTransaction},
    types::{EpochInfoProvider, ShardId},
};
use near_store::{
    get_received_data, DBCol, Store, StoreCompiledContractCache, TrieUpdate, FINAL_HEAD_KEY,
    TAIL_KEY,
};
use node_runtime::{ApplyState, ApplyStats, Runtime};
use tokio::sync::oneshot;

use crate::types::RuntimeAdapter;

pub struct TransactionSimulator {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_info_provider: Arc<dyn EpochInfoProvider>,
    runtime: Runtime,
}

impl TransactionSimulator {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
    ) -> Self {
        Self { epoch_manager, runtime_adapter, epoch_info_provider, runtime: Runtime::new() }
    }

    pub fn prepare_simulation(
        &self,
        prev_block_hash: &CryptoHash,
        state_roots: &[CryptoHash],
        transaction: &SignedTransaction,
    ) -> Result<SimulationState, crate::Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let mut simulation_state = SimulationState::new(
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            self.epoch_info_provider.clone(),
            prev_block_hash,
            state_roots,
        )?;
        let tx_shard_id = self
            .epoch_manager
            .account_id_to_shard_id(&transaction.transaction.signer_id, &epoch_id)?;
        let mut stats = ApplyStats::default();
        let (receipt, outcome) = self
            .runtime
            .process_transaction(
                &mut simulation_state.trie_updates[tx_shard_id as usize],
                &simulation_state.apply_state,
                transaction,
                &mut stats,
            )
            .map_err(|err| crate::Error::Other(err.to_string()))?;
        simulation_state.remaining_receipts.push_back((
            if receipt.receiver_id == receipt.predecessor_id { 0 } else { 1 },
            receipt.clone(),
        ));
        simulation_state.tx_outcome = Some(outcome.outcome);
        Ok(simulation_state)
    }

    pub fn simulate(
        &self,
        prev_block_hash: &CryptoHash,
        state_roots: &[CryptoHash],
        transaction: &SignedTransaction,
    ) -> SimulationResult {
        // println!(
        //     "[SIM] Simulating transaction {:?} to {:?}",
        //     transaction.get_hash(),
        //     transaction.transaction.receiver_id
        // );
        let mut state = match self.prepare_simulation(prev_block_hash, state_roots, transaction) {
            Ok(state) => state,
            Err(err) => {
                return SimulationResult {
                    tx_outcome: None,
                    outcomes: Vec::new(),
                    error: Some(err.to_string()),
                }
            }
        };
        let error = if let Err(err) = state.simulate_all_receipts() { Some(err) } else { None };
        let total_gas_by_shard_and_depth = state
            .outcomes
            .iter()
            .map(|outcome| ((outcome.shard_id, outcome.execution_depth), outcome.outcome.gas_burnt))
            .group_by(|(key, _)| key.clone())
            .into_iter()
            .map(|((shard_id, depth), group)| {
                ((shard_id, depth), group.map(|(_, gas)| gas).reduce(|a, b| a + b).unwrap())
            })
            .collect::<Vec<_>>();
        // println!("[SIM]   Total gas per shard and depth:");
        // for ((shard_id, depth), gas) in total_gas_by_shard_and_depth {
        //     println!("[SIM]    Shard {} depth {}: {}", shard_id, depth, gas);
        // }
        SimulationResult {
            tx_outcome: state.tx_outcome,
            outcomes: state.outcomes,
            error: error.map(|err| err.to_string()),
        }
    }
}

pub struct SimulationState {
    trie_updates: Vec<TrieUpdate>,
    remaining_receipts: VecDeque<(u32, Receipt)>,
    tx_outcome: Option<ExecutionOutcome>,
    outcomes: Vec<ReceiptExecutionOutcome>,
    apply_state: ApplyState,

    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_info_provider: Arc<dyn EpochInfoProvider>,
    runtime: Runtime,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct SimulationResult {
    pub tx_outcome: Option<ExecutionOutcome>,
    pub outcomes: Vec<ReceiptExecutionOutcome>,
    pub error: Option<String>,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct ReceiptExecutionOutcome {
    pub receipt: Receipt,
    pub outcome: ExecutionOutcome,
    pub shard_id: ShardId,
    pub execution_depth: u32,
}

impl SimulationState {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
        prev_block_hash: &CryptoHash,
        state_roots: &[CryptoHash],
    ) -> Result<Self, crate::Error> {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let num_shards = shard_layout.num_shards();
        let mut trie_updates = Vec::with_capacity(num_shards as usize);
        for shard_id in 0..num_shards {
            let trie = runtime_adapter.get_trie_for_shard(
                shard_id,
                prev_block_hash,
                state_roots[shard_id as usize],
                false,
            )?;
            let trie_update = TrieUpdate::new(trie);
            trie_updates.push(trie_update);
        }

        let prev_block = runtime_adapter
            .store()
            .get_ser::<BlockHeader>(DBCol::BlockHeader, prev_block_hash.as_ref())?
            .ok_or(crate::Error::DBNotFoundErr(format!(
                "No such prev block {:?}",
                prev_block_hash
            )))?;

        let apply_state = ApplyState {
            block_hash: prev_block_hash.clone(), // arbitrary
            block_height: prev_block.height() + 1,
            block_timestamp: prev_block.raw_timestamp() + 1,
            prev_block_hash: prev_block_hash.clone(),
            epoch_id: epoch_id.clone(),
            epoch_height: epoch_manager.get_epoch_height_from_prev_block(prev_block_hash)?,
            gas_price: prev_block.gas_price(),
            gas_limit: None, // should we add some?
            random_seed: *prev_block.random_value(),
            config: runtime_adapter.get_protocol_config(&epoch_id)?.runtime_config.into(),
            current_protocol_version: epoch_manager.get_epoch_protocol_version(&epoch_id)?,
            is_new_chunk: true,
            cache: Some(Box::new(StoreCompiledContractCache::new(runtime_adapter.store()))),
            migration_data: runtime_adapter.get_migration_data(),
            migration_flags: MigrationFlags::default(),
        };

        Ok(Self {
            trie_updates,
            remaining_receipts: VecDeque::new(),
            tx_outcome: None,
            outcomes: Vec::new(),
            apply_state,
            epoch_manager,
            runtime_adapter,
            epoch_info_provider,
            runtime: Runtime::new(),
        })
    }

    pub fn take_next_runnable_receipt(&mut self) -> Result<Option<(u32, Receipt)>, crate::Error> {
        let mut attempts_remaining = self.remaining_receipts.len();
        'outer: while attempts_remaining > 0 {
            attempts_remaining -= 1;
            let (depth, receipt) = self.remaining_receipts.pop_front().unwrap();
            match &receipt.receipt {
                ReceiptEnum::Action(action) => {
                    let recipient_account_id = receipt.receiver_id.clone();
                    let recipient_shard_id = self.epoch_manager.account_id_to_shard_id(
                        &recipient_account_id,
                        &self.apply_state.epoch_id,
                    )?;
                    for input_data_id in &action.input_data_ids {
                        if get_received_data(
                            &self.trie_updates[recipient_shard_id as usize],
                            &recipient_account_id,
                            *input_data_id,
                        )?
                        .is_none()
                        {
                            // TODO: These depths are incorrect.
                            self.remaining_receipts.push_back((depth, receipt));
                            continue 'outer;
                        }
                    }

                    return Ok(Some((depth, receipt)));
                }
                ReceiptEnum::Data(_) => return Ok(Some((depth, receipt))),
            }
        }
        Ok(None)
    }

    pub fn simulate_receipt(&mut self, receipt: Receipt, depth: u32) -> Result<(), crate::Error> {
        let shard_id = self
            .epoch_manager
            .account_id_to_shard_id(&receipt.receiver_id, &self.apply_state.epoch_id)?;
        let mut outgoing_receipts = Vec::new();
        let mut validator_proposals = Vec::new();
        let mut stats = ApplyStats::default();
        let result = self
            .runtime
            .process_receipt(
                &mut self.trie_updates[shard_id as usize],
                &self.apply_state,
                &receipt,
                &mut outgoing_receipts,
                &mut validator_proposals,
                &mut stats,
                self.epoch_info_provider.as_ref(),
            )
            .map_err(|err| crate::Error::Other(err.to_string()))?;
        if let Some(result) = result {
            self.outcomes.push(ReceiptExecutionOutcome {
                receipt,
                outcome: result.outcome,
                shard_id,
                execution_depth: depth,
            });
        }
        self.remaining_receipts
            .extend(outgoing_receipts.into_iter().map(|receipt| (depth + 1, receipt)));
        Ok(())
    }

    pub fn simulate_all_receipts(&mut self) -> Result<(), crate::Error> {
        while let Some((depth, receipt)) = self.take_next_runnable_receipt()? {
            self.simulate_receipt(receipt, depth)?;
        }
        Ok(())
    }
}

pub struct SimulationRunner {}

impl SimulationRunner {
    fn simulate_history_once(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        store: Store,
        simulation_sender: crossbeam_channel::Sender<SimulationRequest>,
        batch_size: u64,
    ) -> Result<bool, crate::Error> {
        let last_simulated = store.get_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"")?;
        let last_simulated = match last_simulated {
            Some(last_simulated) => last_simulated,
            None => {
                let head = store
                    .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
                    .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No final head")))?;
                let start =
                    epoch_manager.get_prev_epoch_id_from_prev_block(&head.prev_block_hash)?;
                let start_block_hash = start.0;
                let start_block_header = store
                    .get_ser::<BlockHeader>(DBCol::BlockHeader, start_block_hash.as_ref())?
                    .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No start block header")))?;
                let start_ordinal = start_block_header.block_ordinal();
                let mut update = store.store_update();
                update.set_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"", &start_ordinal)?;
                update.commit()?;
                start_ordinal
            }
        };

        let final_head = store
            .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
            .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No final head")))?;
        let final_head_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, final_head.last_block_hash.as_ref())?
            .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No final head block")))?;
        let final_head_ordinal = final_head_header.block_ordinal();
        if last_simulated + batch_size > final_head_ordinal {
            return Ok(false);
        }

        let mut requests = Vec::<(CryptoHash, oneshot::Receiver<SimulationResult>)>::new();

        for block_ordinal in last_simulated + 1..=last_simulated + batch_size {
            let next_block_hash = store
                .get_ser::<CryptoHash>(DBCol::BlockOrdinal, &block_ordinal.try_to_vec().unwrap())?;
            let next_block = match next_block_hash {
                None => None,
                Some(next_block_hash) => {
                    store.get_ser::<Block>(DBCol::Block, next_block_hash.as_ref())?
                }
            };
            let next_block = match next_block {
                None => {
                    println!(
                    "Block ordinal {} is missing, resetting last simulated ordinal; sleeping for 10 seconds to try again",
                    block_ordinal
                );
                    let mut update = store.store_update();
                    update.delete(DBCol::LastSimulatedBlockOrdinal, b"");
                    update.commit()?;
                    return Ok(false);
                }
                Some(next_block) => next_block,
            };
            let state_roots =
                next_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect::<Vec<_>>();

            for chunk in next_block.chunks().iter() {
                let chunk_body = store
                    .get_ser::<ShardChunk>(DBCol::Chunks, chunk.chunk_hash().as_ref())?
                    .ok_or_else(|| {
                        crate::Error::DBNotFoundErr(format!(
                            "No chunk for hash {:?}",
                            chunk.chunk_hash()
                        ))
                    })?;
                for transaction in chunk_body.transactions() {
                    let (sender, receiver) = oneshot::channel();
                    simulation_sender
                        .send(SimulationRequest {
                            transaction: transaction.clone(),
                            state_roots: state_roots.clone(),
                            prev_block_hash: next_block.header().prev_hash().clone(),
                            done: sender,
                        })
                        .unwrap();
                    requests.push((transaction.get_hash(), receiver));
                }
            }
        }

        let mut txns_simulated = 0;
        let mut txns_simulated_with_error = 0;
        let mut update = store.store_update();
        for (hash, receiver) in requests {
            let result = receiver.blocking_recv().unwrap();
            if let Some(err) = &result.error {
                println!("[SIM]   Error simulating transaction {:?}: {:?}", hash, err);
                txns_simulated_with_error += 1;
            }
            update
                .set_ser(
                    DBCol::TransactionSimulationResult,
                    hash.as_bytes(),
                    &result.try_to_vec().unwrap(),
                )
                .unwrap();
            txns_simulated += 1;
        }

        let last_simulated = last_simulated + batch_size;
        update.set_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"", &last_simulated)?;
        update.commit()?;
        println!(
            "Simulated {} transactions, {} with error, from {} to {}",
            txns_simulated,
            txns_simulated_with_error,
            last_simulated - 9,
            last_simulated
        );

        return Ok(true);
    }

    pub fn continuously_simulate_history(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
        store: Store,
        num_threads: usize,
        batch_size: u64,
    ) -> JoinHandle<()> {
        let (sender, receiver) = crossbeam_channel::unbounded::<SimulationRequest>();
        for _ in 0..num_threads {
            let receiver = receiver.clone();
            let epoch_manager = epoch_manager.clone();
            let runtime_adapter = runtime_adapter.clone();
            let epoch_info_provider = epoch_info_provider.clone();
            std::thread::spawn(move || {
                let simulator = Arc::new(TransactionSimulator::new(
                    epoch_manager.clone(),
                    runtime_adapter,
                    epoch_info_provider,
                ));
                for request in receiver {
                    let SimulationRequest { transaction, state_roots, prev_block_hash, done } =
                        request;
                    let result = simulator.simulate(&prev_block_hash, &state_roots, &transaction);
                    done.send(result).unwrap();
                }
            });
        }

        println!("Starting simulation thread");

        std::thread::spawn(move || loop {
            match Self::simulate_history_once(
                epoch_manager.clone(),
                store.clone(),
                sender.clone(),
                batch_size,
            ) {
                Ok(true) => {}
                Ok(false) => {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
                Err(err) => {
                    println!(
                        "Error simulating history, sleeping for 10 seconds to try again: {:?}",
                        err
                    );
                    std::thread::sleep(std::time::Duration::from_secs(10));
                }
            }
        })
    }
}

pub struct SimulationRequest {
    pub transaction: SignedTransaction,
    pub state_roots: Vec<CryptoHash>,
    pub prev_block_hash: CryptoHash,
    pub done: oneshot::Sender<SimulationResult>,
}

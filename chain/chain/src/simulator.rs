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

pub struct SimulationRunner {
    thread: std::thread::JoinHandle<()>,
    sender: mpsc::SyncSender<SimulationRequest>,
}

impl SimulationRunner {
    pub fn maybe_send(request: SimulationRequest) -> bool {
        let runner = SIMULATION_RUNNER.simulator.read().unwrap().as_ref().cloned();
        if let Some(runner) = runner {
            runner.sender.send(request).is_ok()
        } else {
            false
        }
    }

    pub fn init(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
    ) {
        let simulator = Arc::new(TransactionSimulator::new(
            epoch_manager,
            runtime_adapter.clone(),
            epoch_info_provider,
        ));
        let (sender, receiver) = mpsc::sync_channel(1000);
        let thread = std::thread::spawn(move || {
            for request in receiver {
                let SimulationRequest { transaction, state_roots, prev_block_hash, done } = request;
                let result = simulator.simulate(&prev_block_hash, &state_roots, &transaction);
                // if let Some(err) = result.error {
                //     let hash = transaction.get_hash();
                //     println!("[SIM]   Error simulating transaction {:?}: {:?}", hash, err);
                // }
                let mut update = runtime_adapter.store().store_update();
                update
                    .set_ser(
                        DBCol::TransactionSimulationResult,
                        transaction.get_hash().as_bytes(),
                        &result.try_to_vec().unwrap(),
                    )
                    .unwrap();
                update.commit().unwrap();
                done.send(()).unwrap();
            }
        });

        *SIMULATION_RUNNER.simulator.write().unwrap() = Some(Arc::new(Self { thread, sender }));
    }

    fn simulate_history_once(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
        store: Store,
    ) -> Result<bool, crate::Error> {
        let simulator = Arc::new(TransactionSimulator::new(
            epoch_manager.clone(),
            runtime_adapter,
            epoch_info_provider,
        ));
        let last_simulated = store.get_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"")?;
        let last_simulated = match last_simulated {
            Some(last_simulated) => last_simulated,
            None => {
                let mut tail_height = store
                    .get_ser::<u64>(DBCol::BlockMisc, TAIL_KEY)?
                    .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No tail key")))?;
                let tail_block = loop {
                    let tail_block = store.get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        tail_height.to_be_bytes().as_ref(),
                    )?;
                    match tail_block {
                        Some(tail_block) => break tail_block,
                        None => {
                            println!(
                                "Could not find tail block at height {}, trying again at +1",
                                tail_height
                            );
                            tail_height += 1;
                        }
                    }
                };
                let tail_ordinal = tail_block.block_ordinal();
                let mut update = store.store_update();
                update.set_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"", &tail_ordinal)?;
                update.commit()?;
                tail_ordinal
            }
        };
        let next_block_ordinal = last_simulated + 1;
        let final_head = store
            .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
            .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No final head")))?;
        let final_head_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, final_head.last_block_hash.as_ref())?
            .ok_or_else(|| crate::Error::DBNotFoundErr(format!("No final head block")))?;
        let final_head_ordinal = final_head_header.block_ordinal();
        if next_block_ordinal > final_head_ordinal {
            return Ok(false);
        }
        let next_block_hash = store
            .get_ser::<CryptoHash>(DBCol::BlockOrdinal, &next_block_ordinal.try_to_vec().unwrap())?
            .ok_or_else(|| {
                crate::Error::DBNotFoundErr(format!(
                    "No block hash for ordinal {}",
                    next_block_ordinal
                ))
            })?;
        let next_block =
            store.get_ser::<Block>(DBCol::Block, next_block_hash.as_ref())?.ok_or_else(|| {
                crate::Error::DBNotFoundErr(format!("No block for hash {:?}", next_block_hash))
            })?;
        let state_roots =
            next_block.chunks().iter().map(|chunk| chunk.prev_state_root()).collect::<Vec<_>>();

        let mut txns_simulated = 0;
        let mut txns_simulated_with_error = 0;
        let mut update = store.store_update();
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
                let result = simulator.simulate(
                    &next_block.header().prev_hash(),
                    &state_roots,
                    &transaction,
                );
                update.set_ser::<SimulationResult>(
                    DBCol::TransactionSimulationResult,
                    transaction.get_hash().as_bytes(),
                    &result,
                )?;
                txns_simulated += 1;
                if result.error.is_some() {
                    txns_simulated_with_error += 1;
                }
            }
        }
        update.set_ser::<u64>(DBCol::LastSimulatedBlockOrdinal, b"", &next_block_ordinal)?;
        update.commit()?;
        println!(
            "Simulated {} transactions, {} with error, at block {}",
            txns_simulated, txns_simulated_with_error, next_block_ordinal
        );

        return Ok(true);
    }

    pub fn continuously_simulate_history(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_info_provider: Arc<dyn EpochInfoProvider>,
        store: Store,
    ) -> JoinHandle<()> {
        println!("Starting simulation thread");
        std::thread::spawn(move || loop {
            match Self::simulate_history_once(
                epoch_manager.clone(),
                runtime_adapter.clone(),
                epoch_info_provider.clone(),
                store.clone(),
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
    pub done: oneshot::Sender<()>,
}

pub struct SimulationRunnerStatic {
    simulator: RwLock<Option<Arc<SimulationRunner>>>,
}

static SIMULATION_RUNNER: SimulationRunnerStatic =
    SimulationRunnerStatic { simulator: RwLock::new(None) };

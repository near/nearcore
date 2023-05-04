use std::{
    collections::VecDeque,
    sync::{mpsc, Arc, Mutex, RwLock},
};

use itertools::Itertools;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::{
    block_header::BlockHeader,
    hash::CryptoHash,
    receipt::{Receipt, ReceiptEnum},
    runtime::migration_data::MigrationFlags,
    transaction::{ExecutionOutcome, ExecutionOutcomeWithId, SignedTransaction},
    types::{EpochInfoProvider, ShardId},
};
use near_store::{get_received_data, DBCol, StoreCompiledContractCache, TrieUpdate};
use node_runtime::{ApplyState, ApplyStats, Runtime};

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
        state_root: &CryptoHash,
        transaction: &SignedTransaction,
    ) -> Result<SimulationState, crate::Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let mut simulation_state = SimulationState::new(
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            self.epoch_info_provider.clone(),
            prev_block_hash,
            state_root,
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
        state_root: &CryptoHash,
        transaction: &SignedTransaction,
    ) -> SimulationResult {
        println!(
            "[SIM] Simulating transaction {:?} to {:?}",
            transaction.get_hash(),
            transaction.transaction.receiver_id
        );
        let mut state = match self.prepare_simulation(prev_block_hash, state_root, transaction) {
            Ok(state) => state,
            Err(err) => {
                return SimulationResult {
                    tx_outcome: None,
                    outcomes: Vec::new(),
                    error: Some(err),
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
        println!("[SIM] Total gas per shard and depth:");
        for ((shard_id, depth), gas) in total_gas_by_shard_and_depth {
            println!("[SIM]   Shard {} depth {}: {}", shard_id, depth, gas);
        }
        SimulationResult { tx_outcome: state.tx_outcome, outcomes: state.outcomes, error }
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

pub struct SimulationResult {
    pub tx_outcome: Option<ExecutionOutcome>,
    pub outcomes: Vec<ReceiptExecutionOutcome>,
    pub error: Option<crate::Error>,
}

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
        state_root: &CryptoHash,
    ) -> Result<Self, crate::Error> {
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
        let num_shards = shard_layout.num_shards();
        let mut trie_updates = Vec::with_capacity(num_shards as usize);
        for shard_id in 0..num_shards {
            let trie = runtime_adapter.get_trie_for_shard(
                shard_id,
                prev_block_hash,
                *state_root,
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
            runtime_adapter,
            epoch_info_provider,
        ));
        let (sender, receiver) = mpsc::sync_channel(1000);
        let thread = std::thread::spawn(move || {
            for request in receiver {
                let SimulationRequest { transaction, global_state_root, prev_block_hash } = request;
                let result = simulator.simulate(&global_state_root, &prev_block_hash, &transaction);
                if let Some(err) = result.error {
                    let hash = transaction.get_hash();
                    println!("Error simulating transaction {:?}: {:?}", hash, err);
                }
            }
        });

        *SIMULATION_RUNNER.simulator.write().unwrap() = Some(Arc::new(Self { thread, sender }));
    }
}

pub struct SimulationRequest {
    pub transaction: SignedTransaction,
    pub global_state_root: CryptoHash,
    pub prev_block_hash: CryptoHash,
}

pub struct SimulationRunnerStatic {
    simulator: RwLock<Option<Arc<SimulationRunner>>>,
}

static SIMULATION_RUNNER: SimulationRunnerStatic =
    SimulationRunnerStatic { simulator: RwLock::new(None) };

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use near_chain::spice_core::get_uncertified_chunks;
use near_chain::types::{PendingTxQueueValidationResult, RuntimeAdapter};
use near_chain_primitives::Error;
use near_crypto::PublicKey;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{SignedTransaction, TransactionKey};
use near_primitives::types::{AccountId, Balance, EpochId, ShardId};
use near_store::ShardUId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use node_runtime::config::tx_cost;

pub struct ShardedPendingTransactionQueue {
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    queues: HashMap<ShardUId, PendingTransactionQueue>,
}

impl ShardedPendingTransactionQueue {
    pub fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self { runtime_adapter, epoch_manager, queues: HashMap::new() }
    }

    pub fn get_queue_mut(&mut self, shard_uid: ShardUId) -> &mut PendingTransactionQueue {
        self.queues.entry(shard_uid).or_insert_with(|| {
            PendingTransactionQueue::new(self.runtime_adapter.clone(), self.epoch_manager.clone())
        })
    }

    pub fn reset_from_uncertified_chunks(
        &mut self,
        head: &CryptoHash,
        store: &ChainStoreAdapter,
        shard_tracker: &ShardTracker,
    ) -> Result<(), Error> {
        self.queues.clear();
        let chunk_infos = get_uncertified_chunks(store, head)?;
        let mut shard_ids_by_block_hash: HashMap<CryptoHash, HashSet<ShardId>> = HashMap::new();
        for chunk_info in chunk_infos {
            let block_hash = chunk_info.chunk_id.block_hash;
            let shard_id = chunk_info.chunk_id.shard_id;
            shard_ids_by_block_hash.entry(block_hash).or_default().insert(shard_id);
        }
        for (block_hash, shard_ids) in shard_ids_by_block_hash {
            let block = store.get_block(&block_hash)?;
            let prev_block_header = store.get_block_header(&block.header().prev_hash())?;
            // Spice does not have missing chunks so we can use iter_new here.
            for chunk_header in block.chunks().iter_new() {
                let shard_id = chunk_header.shard_id();
                // TODO(spice-resharding): Is this correct?
                if !shard_tracker.cares_about_shard(block.header().prev_hash(), shard_id) {
                    continue;
                }
                if !shard_ids.contains(&chunk_header.shard_id()) {
                    continue;
                }
                let epoch_id = block.header().epoch_id();
                let chunk = store.chunk_store().get_chunk(&chunk_header.chunk_hash())?;
                // TODO(spice): Gas price should be calculated properly.
                let gas_price = prev_block_header.next_gas_price();
                self.get_queue_mut(shard_id_to_uid(&*self.epoch_manager, shard_id, epoch_id)?)
                    .add_transactions(
                        block_hash,
                        block.header().epoch_id(),
                        gas_price,
                        chunk.into_transactions(),
                    );
            }
        }
        Ok(())
    }
}

/// A queue that holds summarized, per payer information about transactions that
/// are included in blocks, but not yet executed for a given shard.
pub struct PendingTransactionQueue {
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pending_txs: VecDeque<(CryptoHash, PendingTransactionSummary)>,
    summary: PendingTransactionSummary,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PayerKey {
    Account { account_id: AccountId },
    GasKey { account_id: AccountId, public_key: PublicKey },
}

impl PayerKey {
    pub fn account_id(&self) -> &AccountId {
        match self {
            PayerKey::Account { account_id } => account_id,
            PayerKey::GasKey { account_id, .. } => account_id,
        }
    }
}

struct PendingTransactionSummary {
    count_and_cost: HashMap<PayerKey, (usize, Balance)>,
    num_contract_deploys: HashMap<AccountId, usize>,
}

impl PendingTransactionSummary {
    fn new() -> Self {
        Self { count_and_cost: HashMap::new(), num_contract_deploys: HashMap::new() }
    }

    fn add(&mut self, key: PayerKey, cost: Balance, num_contract_deploys: usize) {
        let account_id = key.account_id().clone();
        let entry_deploys = self.num_contract_deploys.entry(account_id).or_default();
        *entry_deploys += num_contract_deploys;

        let (entry_count, entry_cost) = self.count_and_cost.entry(key).or_default();
        *entry_count += 1;
        *entry_cost = entry_cost.checked_add(cost).unwrap();
    }

    fn add_summary(&mut self, other: &PendingTransactionSummary) {
        for (key, (count, cost)) in &other.count_and_cost {
            let (entry_count, entry_cost) = self.count_and_cost.entry(key.clone()).or_default();
            *entry_count += count;
            *entry_cost = entry_cost.checked_add(*cost).unwrap();
        }
        for (account_id, num_contract_deploys) in &other.num_contract_deploys {
            let entry_deploys = self.num_contract_deploys.entry(account_id.clone()).or_default();
            *entry_deploys += num_contract_deploys;
        }
    }

    fn subtract_summary(&mut self, other: &PendingTransactionSummary) {
        for (key, (count, cost)) in &other.count_and_cost {
            if let Some((entry_count, entry_cost)) = self.count_and_cost.get_mut(key) {
                *entry_count -= count;
                *entry_cost = entry_cost.checked_sub(*cost).unwrap();
                if *entry_count == 0 {
                    self.count_and_cost.remove(key);
                }
            }
        }

        for (account_id, num_contract_deploys) in &other.num_contract_deploys {
            if let Some(entry_deploys) = self.num_contract_deploys.get_mut(account_id) {
                *entry_deploys -= num_contract_deploys;
                if *entry_deploys == 0 {
                    self.num_contract_deploys.remove(account_id);
                }
            }
        }
    }
}

impl PendingTransactionQueue {
    fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self {
            runtime_adapter,
            epoch_manager,
            pending_txs: VecDeque::new(),
            summary: PendingTransactionSummary::new(),
        }
    }

    fn calculate_total_cost(
        &self,
        tx: &SignedTransaction,
        gas_price: Balance,
        epoch_id: &EpochId,
    ) -> Balance {
        // TODO(spice): Handle error properly.
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id).unwrap();
        let runtime_config = self.runtime_adapter.get_runtime_config(protocol_version);
        // TODO(spice): Handle error (overflow) properly.
        tx_cost(runtime_config, &tx.transaction, gas_price).unwrap().total_cost
    }

    fn num_contract_deploys(&self, tx: &SignedTransaction) -> usize {
        tx.transaction
            .actions()
            .iter()
            .filter(|action| {
                matches!(action, near_primitives::transaction::Action::DeployContract(_))
            })
            .count()
    }

    pub fn add_transactions(
        &mut self,
        block_hash: CryptoHash,
        epoch_id: &EpochId,
        gas_price: Balance,
        txs: Vec<SignedTransaction>,
    ) {
        let mut chunk_summary = PendingTransactionSummary::new();
        for tx in txs {
            let account_id = tx.transaction.signer_id().clone();
            let key = match tx.transaction.key() {
                TransactionKey::AccessKey { .. } => PayerKey::Account { account_id },
                TransactionKey::GasKey { public_key, .. } => {
                    PayerKey::GasKey { account_id, public_key }
                }
            };
            let cost = self.calculate_total_cost(&tx, gas_price, epoch_id);
            let num_contract_deploys = self.num_contract_deploys(&tx);
            chunk_summary.add(key, cost, num_contract_deploys);
        }
        self.summary.add_summary(&chunk_summary);
        self.pending_txs.push_back((block_hash, chunk_summary));
    }

    pub fn remove_transactions(&mut self, block_hash: &CryptoHash) {
        // TODO(spice): Enforce that removed element is at the front of the queue.
        if let Some(pos) = self.pending_txs.iter().position(|(hash, _)| hash == block_hash) {
            let (_, chunk_summary) = self.pending_txs.remove(pos).unwrap();
            self.summary.subtract_summary(&chunk_summary);
        }
    }

    pub fn get_count(&self, key: &PayerKey) -> Option<&usize> {
        self.summary.count_and_cost.get(key).map(|(count, _)| count)
    }

    pub fn get_cost(&self, key: &PayerKey) -> Option<&Balance> {
        self.summary.count_and_cost.get(key).map(|(_, cost)| cost)
    }

    pub fn get_num_contract_deploys(&self, account_id: &AccountId) -> usize {
        *self.summary.num_contract_deploys.get(account_id).unwrap_or(&0)
    }

    pub fn validate(&self, _tx: &SignedTransaction) -> PendingTxQueueValidationResult {
        // Here, some additional restrictions apply because of the pending transaction queue:
        // 1. If there is a contract deployed to the account:
        //    - We need to get the number of pending transactions for this account from the pending queue.
        //      if it is more than 4, the transaction is skipped. (i.e., it will be put back to the pool and retried later).
        // (If there is no contract deployed to the account, we allow unlimited number of pending transactions)
        // 2. If there is a contract being deployed to the account, and the tx is issued
        //    from an access key, we skip the transaction (it will be put back to the pool and retried later).
        todo!("implement validation logic")
    }
}

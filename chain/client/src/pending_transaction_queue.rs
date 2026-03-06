use std::collections::HashMap;
use std::sync::Arc;

use near_chain::types::{HasContract, PendingConstraints, PendingTxCheckResult};
use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex};
use node_runtime::config::tx_cost;
use parking_lot::Mutex;

/// Checked subtraction that falls back to `Default::default()` on underflow,
/// logging an error and firing a debug_assert so tests catch the invariant violation.
macro_rules! checked_sub_or_default {
    ($a:expr, $b:expr, $msg:expr) => {
        match ($a).checked_sub($b) {
            Some(v) => v,
            None => {
                debug_assert!(false, $msg);
                tracing::error!(target: "client", $msg);
                Default::default()
            }
        }
    };
}

/// Maximum number of access key transactions allowed for accounts with contracts.
/// Per NEP-611, accounts with deployed contracts cannot have more than P_MAX
/// pending access key transactions across all uncertified blocks.
pub const P_MAX: usize = 4;

/// Maps ShardUId -> per-shard PendingTransactionQueue.
pub struct ShardedPendingTransactionQueue {
    queues: HashMap<ShardUId, PendingTransactionQueue>,
}

impl ShardedPendingTransactionQueue {
    pub fn new() -> Self {
        Self { queues: HashMap::new() }
    }

    pub fn get_or_create(&mut self, shard_uid: ShardUId) -> &mut PendingTransactionQueue {
        self.queues.entry(shard_uid).or_insert_with(PendingTransactionQueue::new)
    }

    pub fn get(&self, shard_uid: &ShardUId) -> Option<&PendingTransactionQueue> {
        self.queues.get(shard_uid)
    }

    pub fn get_mut(&mut self, shard_uid: &ShardUId) -> Option<&mut PendingTransactionQueue> {
        self.queues.get_mut(shard_uid)
    }

    pub fn remove_certified_block(&mut self, block_hash: &CryptoHash) {
        for queue in self.queues.values_mut() {
            queue.remove_certified_chunk_by_block_hash(block_hash);
        }
    }

    pub fn clear(&mut self) {
        self.queues.clear();
    }
}

/// Per-shard pending transaction queue. Tracks transactions that have been
/// included in blocks but not yet certified (executed).
pub struct PendingTransactionQueue {
    /// Per-chunk aggregate data, keyed by block hash (since SpiceChunkId
    /// is (block_hash, shard_id) and the shard is implicit).
    chunks: HashMap<CryptoHash, PendingChunkData>,
    /// Per-account aggregates (P_MAX counts, balance commitments).
    pending_accounts: HashMap<AccountId, PendingAccount>,
    /// Per-nonce-index nonce tracking.
    pending_nonces: HashMap<(AccountId, PublicKey, Option<NonceIndex>), PendingNonce>,
    /// Per-gas-key cost. Includes gas_key_cost from gas key txs + WithdrawFromGasKey amounts.
    pending_gas_key_costs: HashMap<(AccountId, PublicKey), Balance>,
}

/// Nonce tracking for a single (account, key, nonce_index).
struct PendingNonce {
    max_nonce: Nonce,
    /// Number of chunks contributing to this entry. When this drops to zero
    /// on chunk removal, the entry is removed.
    chunk_count: usize,
}

/// Per-chunk aggregate. No individual transaction records stored.
struct PendingChunkData {
    /// Per-account aggregates for this chunk.
    accounts: HashMap<AccountId, PendingAccount>,
    /// Per-nonce-index max nonce for this chunk.
    nonces: HashMap<(AccountId, PublicKey, Option<NonceIndex>), Nonce>,
    /// Per-gas-key costs for this chunk (gas_key_cost + WithdrawFromGasKey).
    gas_key_costs: HashMap<(AccountId, PublicKey), Balance>,
}

/// Aggregate for a set of transactions, per account.
/// Used both per-chunk and as pending transaction queue totals. Supports add/subtract.
#[derive(Clone, Default)]
struct PendingAccount {
    access_key_tx_count: usize,
    deploy_tx_count: usize,
    /// Access key total_cost + gas key deposit_cost.
    paid_from_balance: Balance,
}

impl PendingAccount {
    fn add(&mut self, other: &PendingAccount) {
        self.access_key_tx_count += other.access_key_tx_count;
        self.deploy_tx_count += other.deploy_tx_count;
        self.paid_from_balance = self.paid_from_balance.saturating_add(other.paid_from_balance);
    }

    fn subtract(&mut self, other: &PendingAccount) {
        self.access_key_tx_count = checked_sub_or_default!(
            self.access_key_tx_count,
            other.access_key_tx_count,
            "access_key_tx_count underflow in pending transaction queue subtract"
        );
        self.deploy_tx_count = checked_sub_or_default!(
            self.deploy_tx_count,
            other.deploy_tx_count,
            "deploy_tx_count underflow in pending transaction queue subtract"
        );
        self.paid_from_balance = checked_sub_or_default!(
            self.paid_from_balance,
            other.paid_from_balance,
            "paid_from_balance underflow in pending transaction queue subtract"
        );
    }

    fn is_zero(&self) -> bool {
        self.access_key_tx_count == 0
            && self.deploy_tx_count == 0
            && self.paid_from_balance.is_zero()
    }
}

/// Returns true if the action is a deploy-like action per NEP-611:
/// DeployContract, UseGlobalContract, DeterministicStateInit, or
/// Delegate wrapping a deploy-like action.
fn is_deploy_like_action(action: &Action) -> bool {
    match action {
        Action::DeployContract(_)
        | Action::DeployGlobalContract(_)
        | Action::UseGlobalContract(_)
        | Action::DeterministicStateInit(_) => true,
        Action::Delegate(signed_delegate) => {
            signed_delegate.delegate_action.get_actions().iter().any(is_deploy_like_action)
        }
        Action::CreateAccount(_)
        | Action::FunctionCall(_)
        | Action::Transfer(_)
        | Action::Stake(_)
        | Action::AddKey(_)
        | Action::DeleteKey(_)
        | Action::DeleteAccount(_)
        | Action::TransferToGasKey(_)
        | Action::WithdrawFromGasKey(_) => false,
    }
}

/// Returns true if any action in the list is deploy-like.
fn has_deploy_action(actions: &[Action]) -> bool {
    actions.iter().any(is_deploy_like_action)
}

impl PendingTransactionQueue {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
            pending_accounts: HashMap::new(),
            pending_nonces: HashMap::new(),
            pending_gas_key_costs: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
            && self.pending_accounts.is_empty()
            && self.pending_nonces.is_empty()
            && self.pending_gas_key_costs.is_empty()
    }

    /// Add transactions from a newly accepted block's chunk.
    pub fn add_chunk_transactions(
        &mut self,
        block_hash: CryptoHash,
        transactions: &[SignedTransaction],
        config: &RuntimeConfig,
        gas_price: Balance,
    ) {
        let mut chunk_data = PendingChunkData {
            accounts: HashMap::new(),
            nonces: HashMap::new(),
            gas_key_costs: HashMap::new(),
        };

        for signed_tx in transactions {
            let tx = &signed_tx.transaction;
            let signer_id = tx.signer_id().clone();
            let public_key = tx.public_key().clone();
            let nonce_index = tx.nonce().nonce_index();
            let nonce = tx.nonce().nonce();
            let is_gas_key_tx = nonce_index.is_some();

            let cost = match tx_cost(config, tx, gas_price) {
                Ok(cost) => cost,
                Err(e) => {
                    tracing::warn!(
                        target: "client",
                        ?e,
                        "tx_cost failed for block transaction in pending transaction queue"
                    );
                    continue;
                }
            };

            // Update per-account aggregates.
            let chunk_account = chunk_data.accounts.entry(signer_id.clone()).or_default();
            if is_gas_key_tx {
                // Gas key tx: only deposit_cost is paid from account balance.
                chunk_account.paid_from_balance =
                    chunk_account.paid_from_balance.saturating_add(cost.deposit_cost);
            } else {
                // Access key tx: total_cost is paid from account balance.
                chunk_account.access_key_tx_count += 1;
                chunk_account.paid_from_balance =
                    chunk_account.paid_from_balance.saturating_add(cost.total_cost);
            }
            if has_deploy_action(tx.actions()) {
                chunk_account.deploy_tx_count += 1;
            }

            // Track gas key costs (gas_key_cost for gas key txs).
            if is_gas_key_tx {
                let gas_key_entry = chunk_data
                    .gas_key_costs
                    .entry((signer_id.clone(), public_key.clone()))
                    .or_insert(Balance::ZERO);
                *gas_key_entry = gas_key_entry.saturating_add(cost.gas_cost);
            }

            // Scan actions for WithdrawFromGasKey (affects gas key balance).
            for action in tx.actions() {
                if let Action::WithdrawFromGasKey(withdraw) = action {
                    let gas_key_entry = chunk_data
                        .gas_key_costs
                        .entry((signer_id.clone(), withdraw.public_key.clone()))
                        .or_insert(Balance::ZERO);
                    *gas_key_entry = gas_key_entry.saturating_add(withdraw.amount);
                }
            }

            // Track nonces. Done last to consume signer_id and public_key.
            let max_nonce =
                chunk_data.nonces.entry((signer_id, public_key, nonce_index)).or_insert(0);
            *max_nonce = std::cmp::max(*max_nonce, nonce);
        }

        // Merge chunk data into pending transaction queue totals.
        for (account_id, chunk_account) in &chunk_data.accounts {
            let total_account = self.pending_accounts.entry(account_id.clone()).or_default();
            total_account.add(chunk_account);
        }
        for (nonce_key, &chunk_nonce) in &chunk_data.nonces {
            let entry = self
                .pending_nonces
                .entry(nonce_key.clone())
                .or_insert(PendingNonce { max_nonce: 0, chunk_count: 0 });
            entry.max_nonce = std::cmp::max(entry.max_nonce, chunk_nonce);
            entry.chunk_count += 1;
        }
        for (gas_key, &chunk_gas_key_cost) in &chunk_data.gas_key_costs {
            let entry = self.pending_gas_key_costs.entry(gas_key.clone()).or_insert(Balance::ZERO);
            *entry = entry.saturating_add(chunk_gas_key_cost);
        }

        debug_assert!(
            !self.chunks.contains_key(&block_hash),
            "duplicate block_hash in pending transaction queue"
        );
        self.chunks.insert(block_hash, chunk_data);
    }

    /// Remove a certified chunk's transactions from the pending transaction queue.
    pub fn remove_certified_chunk_by_block_hash(&mut self, block_hash: &CryptoHash) {
        let Some(chunk_data) = self.chunks.remove(block_hash) else {
            tracing::debug!(
                target: "client",
                ?block_hash,
                "chunk not found in pending transaction queue during removal"
            );
            return;
        };

        // Reverse per-account aggregates.
        for (account_id, chunk_account) in &chunk_data.accounts {
            if let Some(total_account) = self.pending_accounts.get_mut(account_id) {
                total_account.subtract(chunk_account);
                if total_account.is_zero() {
                    self.pending_accounts.remove(account_id);
                }
            }
        }

        // Reverse nonce tracking. Note: max_nonce is NOT recomputed on removal -- this is not an
        // issue as chunks should become certified in order.
        for (nonce_key, _) in &chunk_data.nonces {
            if let Some(entry) = self.pending_nonces.get_mut(nonce_key) {
                entry.chunk_count = checked_sub_or_default!(
                    entry.chunk_count,
                    1,
                    "chunk_count underflow in remove_certified_chunk"
                );
                if entry.chunk_count == 0 {
                    self.pending_nonces.remove(nonce_key);
                }
            }
        }

        // Reverse gas key costs.
        for (gas_key, &chunk_gas_key_cost) in &chunk_data.gas_key_costs {
            if let Some(entry) = self.pending_gas_key_costs.get_mut(gas_key) {
                *entry = checked_sub_or_default!(
                    *entry,
                    chunk_gas_key_cost,
                    "gas key cost underflow in remove_certified_chunk"
                );
                if entry.is_zero() {
                    self.pending_gas_key_costs.remove(gas_key);
                }
            }
        }
    }

    /// Clear all pending transaction data (used for reorg re-initialization).
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.pending_accounts.clear();
        self.pending_nonces.clear();
        self.pending_gas_key_costs.clear();
    }

    /// Extract constraints for a given transaction without Skip/Admit logic.
    /// Used by the RPC handler for balance/nonce verification against certified state.
    pub fn get_pending_constraints(&self, tx: &SignedTransaction) -> PendingConstraints {
        let signer_id = tx.transaction.signer_id();
        let public_key = tx.transaction.public_key();
        let nonce_index = tx.transaction.nonce().nonce_index();

        let paid_from_balance = self
            .pending_accounts
            .get(signer_id)
            .map(|a| a.paid_from_balance)
            .unwrap_or(Balance::ZERO);

        let gas_key = (signer_id.clone(), public_key.clone());
        let paid_from_gas_key =
            self.pending_gas_key_costs.get(&gas_key).copied().unwrap_or(Balance::ZERO);

        let nonce_key = (gas_key.0, gas_key.1, nonce_index);
        let max_nonce = self.pending_nonces.get(&nonce_key).map(|n| n.max_nonce).unwrap_or(0);

        PendingConstraints { paid_from_balance, paid_from_gas_key, max_nonce }
    }
}

/// Per-chunk-production session that combines pending transaction queue state
/// with session-local tracking for P_MAX / deploy exclusivity constraints.
///
/// This is a no-op stub that always admits transactions.
/// The real implementation will enforce P_MAX and deploy exclusivity per NEP-611.
pub struct PendingTxSession {
    _pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
    _shard_uid: ShardUId,
}

impl PendingTxSession {
    pub fn new(
        pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
        shard_uid: ShardUId,
    ) -> Self {
        Self { _pending_transaction_queue: pending_transaction_queue, _shard_uid: shard_uid }
    }

    pub fn check_pending(
        &mut self,
        _tx: &SignedTransaction,
        _has_contract: HasContract,
    ) -> PendingTxCheckResult {
        PendingTxCheckResult::Admit(PendingConstraints::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::transaction::SignedTransaction;

    const TEST_SHARD_UID: ShardUId = ShardUId { version: 0, shard_id: 0 };
    const TEST_GAS_PRICE: Balance = Balance::from_yoctonear(100_000_000);
    const TEST_DEPOSIT: Balance = Balance::from_yoctonear(1);

    fn test_signer() -> Signer {
        InMemorySigner::from_seed("alice.near".parse().unwrap(), KeyType::ED25519, "seed")
    }

    fn make_transfer_tx(
        signer: &Signer,
        receiver: &str,
        nonce: Nonce,
        deposit: Balance,
    ) -> SignedTransaction {
        SignedTransaction::send_money(
            nonce,
            signer.get_account_id(),
            receiver.parse().unwrap(),
            signer,
            deposit,
            CryptoHash::default(),
        )
    }

    fn with_shard_ptq<R>(
        sharded: &Mutex<ShardedPendingTransactionQueue>,
        f: impl FnOnce(&mut PendingTransactionQueue) -> R,
    ) -> R {
        f(sharded.lock().get_or_create(TEST_SHARD_UID))
    }

    fn add_chunk_txs(
        sharded: &Mutex<ShardedPendingTransactionQueue>,
        block_hash: CryptoHash,
        txs: &[SignedTransaction],
        config: &RuntimeConfig,
        gas_price: Balance,
    ) {
        with_shard_ptq(sharded, |ptq| {
            ptq.add_chunk_transactions(block_hash, txs, config, gas_price)
        });
    }

    fn make_sharded_ptq() -> Arc<Mutex<ShardedPendingTransactionQueue>> {
        Arc::new(Mutex::new(ShardedPendingTransactionQueue::new()))
    }

    #[test]
    fn test_add_and_remove_chunk() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let tx1 = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        let tx2 = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        let block_hash = CryptoHash::hash_bytes(&[1]);
        add_chunk_txs(&sharded, block_hash, &[tx1, tx2], &config, TEST_GAS_PRICE);

        with_shard_ptq(&sharded, |ptq| {
            let account = ptq.pending_accounts.get(&signer.get_account_id()).unwrap();
            assert_eq!(account.access_key_tx_count, 2);
            assert!(!account.paid_from_balance.is_zero());
        });
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&block_hash));
        with_shard_ptq(&sharded, |ptq| assert!(ptq.is_empty()));
    }

    #[test]
    fn test_incremental_chunk_removal() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let hash1 = CryptoHash::hash_bytes(&[1]);
        let hash2 = CryptoHash::hash_bytes(&[2]);
        let tx1 = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        let tx2 = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        add_chunk_txs(&sharded, hash1, &[tx1], &config, TEST_GAS_PRICE);
        add_chunk_txs(&sharded, hash2, &[tx2], &config, TEST_GAS_PRICE);
        let nonce_key = (signer.get_account_id(), signer.public_key(), None);

        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.pending_accounts.get(&signer.get_account_id()).unwrap().access_key_tx_count,
                2
            );
            assert_eq!(ptq.pending_nonces.get(&nonce_key).unwrap().chunk_count, 2);
            assert_eq!(ptq.pending_nonces.get(&nonce_key).unwrap().max_nonce, 2);
        });
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&hash1));
        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.pending_accounts.get(&signer.get_account_id()).unwrap().access_key_tx_count,
                1
            );
            assert_eq!(ptq.pending_nonces.get(&nonce_key).unwrap().chunk_count, 1);
        });
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&hash2));
        with_shard_ptq(&sharded, |ptq| assert!(ptq.is_empty()));
    }

    #[test]
    fn test_clear() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let tx = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[tx], &config, TEST_GAS_PRICE);

        with_shard_ptq(&sharded, |ptq| ptq.clear());
        with_shard_ptq(&sharded, |ptq| assert!(ptq.is_empty()));
    }

    #[test]
    fn test_get_pending_constraints() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let tx1 = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        let expected_cost = tx_cost(&config, &tx1.transaction, TEST_GAS_PRICE).unwrap().total_cost;

        // Before adding anything, constraints should be all zero/default.
        assert!(sharded.lock().get(&TEST_SHARD_UID).is_none());

        // Add a chunk with two transactions.
        let tx2 = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        let expected_cost2 = tx_cost(&config, &tx2.transaction, TEST_GAS_PRICE).unwrap().total_cost;
        let block_hash = CryptoHash::hash_bytes(&[1]);
        add_chunk_txs(&sharded, block_hash, &[tx1.clone(), tx2], &config, TEST_GAS_PRICE);
        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.get_pending_constraints(&tx1),
                PendingConstraints {
                    paid_from_balance: expected_cost.saturating_add(expected_cost2),
                    paid_from_gas_key: Balance::ZERO,
                    max_nonce: 2,
                },
            );
        });

        // After removing the chunk, constraints go back to zero.
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&block_hash));
        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(ptq.get_pending_constraints(&tx1), PendingConstraints::default());
        });
    }
}

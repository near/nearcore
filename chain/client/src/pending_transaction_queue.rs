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

    /// Query pending state for a single transaction. Extracts the counts and
    /// constraints needed by `PendingTxSession::check_pending`. This is called
    /// under the lock and should be fast.
    fn query_pending_state(
        &self,
        signer_id: &AccountId,
        public_key: &PublicKey,
        nonce_index: Option<NonceIndex>,
    ) -> PendingStateSnapshot {
        let pending_account = self.pending_accounts.get(signer_id);
        let access_key_tx_count = pending_account.map(|a| a.access_key_tx_count).unwrap_or(0);
        let deploy_tx_count = pending_account.map(|a| a.deploy_tx_count).unwrap_or(0);
        let paid_from_balance =
            pending_account.map(|a| a.paid_from_balance).unwrap_or(Balance::ZERO);

        let gas_key = (signer_id.clone(), public_key.clone());
        let pending_gas_key_cost =
            self.pending_gas_key_costs.get(&gas_key).copied().unwrap_or(Balance::ZERO);

        let nonce_key = (gas_key.0, gas_key.1, nonce_index);
        let max_nonce = self.pending_nonces.get(&nonce_key).map(|n| n.max_nonce).unwrap_or(0);

        PendingStateSnapshot {
            access_key_tx_count,
            deploy_tx_count,
            paid_from_balance,
            max_nonce,
            pending_gas_key_cost,
        }
    }
}

/// Snapshot of pending state for a single transaction's signer, extracted
/// under the lock and used outside it.
#[derive(Default)]
struct PendingStateSnapshot {
    access_key_tx_count: usize,
    deploy_tx_count: usize,
    paid_from_balance: Balance,
    max_nonce: Nonce,
    pending_gas_key_cost: Balance,
}

/// Per-chunk-production session. Combines pending transaction queue state with session-local tracking
/// for constraints NOT handled by the ephemeral TrieUpdate.
///
/// The ephemeral TrieUpdate handles within-chunk accumulation for balance
/// (deducts cost), gas key balance (deducts gas_key_cost), and nonces
/// (advances after each accepted tx). The session only tracks what the
/// ephemeral state does NOT cover:
/// - P_MAX / deploy exclusivity counts (per account)
/// - WithdrawFromGasKey amounts (action effects not applied by ephemeral state)
///
/// The session holds an `Arc<Mutex<ShardedPendingTransactionQueue>>` and acquires the lock briefly
/// per transaction rather than holding it for the entire chunk production duration. This avoids
/// blocking block processing and RPC handlers.
pub struct PendingTxSession {
    pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
    shard_uid: ShardUId,
    session_access_key_tx_counts: HashMap<AccountId, usize>,
    session_deploy_tx_counts: HashMap<AccountId, usize>,
    session_gas_key_withdrawals: HashMap<(AccountId, PublicKey), Balance>,
}

impl PendingTxSession {
    pub fn new(
        pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
        shard_uid: ShardUId,
    ) -> Self {
        Self {
            pending_transaction_queue,
            shard_uid,
            session_access_key_tx_counts: HashMap::new(),
            session_deploy_tx_counts: HashMap::new(),
            session_gas_key_withdrawals: HashMap::new(),
        }
    }

    /// Check if a transaction can be admitted given pending constraints.
    /// If admitted, updates session state and returns constraints
    /// for the runtime's balance/nonce validation.
    ///
    /// Acquires the PTQ lock briefly to read pending state, then releases it.
    pub fn check_pending(
        &mut self,
        tx: &SignedTransaction,
        has_contract: HasContract,
    ) -> PendingTxCheckResult {
        let signer_id = tx.transaction.signer_id();
        let public_key = tx.transaction.public_key();
        let nonce_index = tx.transaction.nonce().nonce_index();
        let is_gas_key_tx = nonce_index.is_some();

        let snapshot = {
            let guard = self.pending_transaction_queue.lock();
            match guard.get(&self.shard_uid) {
                Some(ptq) => ptq.query_pending_state(signer_id, public_key, nonce_index),
                None => PendingStateSnapshot::default(),
            }
        };

        let session_access_key_count =
            self.session_access_key_tx_counts.get(signer_id).copied().unwrap_or(0);
        let session_deploy_count =
            self.session_deploy_tx_counts.get(signer_id).copied().unwrap_or(0);
        let total_access_key_count = snapshot.access_key_tx_count + session_access_key_count;
        let total_deploy_count = snapshot.deploy_tx_count + session_deploy_count;
        let tx_has_deploy = has_deploy_action(tx.transaction.actions());

        // Deploy exclusivity: a deploy cannot coexist with any other access
        // key tx (including another deploy) in the pending window.
        if !is_gas_key_tx {
            if total_deploy_count > 0 {
                return PendingTxCheckResult::Skip;
            }
            if tx_has_deploy && total_access_key_count > 0 {
                return PendingTxCheckResult::Skip;
            }
        }

        // P_MAX for contract accounts.
        if has_contract == HasContract::Yes && !is_gas_key_tx && total_access_key_count >= P_MAX {
            return PendingTxCheckResult::Skip;
        }

        // Build constraints for runtime validation.
        let gas_key = (signer_id.clone(), public_key.clone());
        let session_gas_key_withdrawal =
            self.session_gas_key_withdrawals.get(&gas_key).copied().unwrap_or(Balance::ZERO);
        let paid_from_gas_key =
            snapshot.pending_gas_key_cost.saturating_add(session_gas_key_withdrawal);

        // Update session state optimistically (assumes tx will be accepted).
        // If the runtime subsequently rejects the tx (e.g. insufficient
        // balance), these counts are not rolled back. This means a rejected tx
        // may consume a P_MAX or deploy exclusivity slot for the remainder of
        // this chunk production session. Affected transactions are reintroduced
        // to the pool and retried in a future chunk, so this only impacts
        // throughput under high contention, not correctness. The risk is
        // mitigated by the fact that check_pending is called after signature
        // verification and basic validation, so only transactions with valid
        // signatures can reach this point -- an adversary cannot cheaply spam
        // rejected txs to exhaust slots.
        if !is_gas_key_tx {
            *self.session_access_key_tx_counts.entry(signer_id.clone()).or_insert(0) += 1;
        }
        if tx_has_deploy {
            *self.session_deploy_tx_counts.entry(signer_id.clone()).or_insert(0) += 1;
        }
        // Track WithdrawFromGasKey amounts from this tx's actions.
        for action in tx.transaction.actions() {
            if let Action::WithdrawFromGasKey(withdraw) = action {
                let entry = self
                    .session_gas_key_withdrawals
                    .entry((signer_id.clone(), withdraw.public_key.clone()))
                    .or_insert(Balance::ZERO);
                *entry = entry.saturating_add(withdraw.amount);
            }
        }

        PendingTxCheckResult::Admit(PendingConstraints {
            paid_from_balance: snapshot.paid_from_balance,
            paid_from_gas_key,
            max_nonce: snapshot.max_nonce,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::action::{DeployContractAction, TransferAction};
    use near_primitives::transaction::{SignedTransaction, TransactionNonce};

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

    fn make_deploy_tx(signer: &Signer, nonce: Nonce) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            signer.get_account_id(),
            signer.get_account_id(),
            signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![0u8; 10] })],
            CryptoHash::default(),
        )
    }

    fn make_gas_key_deploy_tx(
        signer: &Signer,
        nonce: Nonce,
        nonce_index: NonceIndex,
    ) -> SignedTransaction {
        SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(nonce, nonce_index),
            signer.get_account_id(),
            signer.get_account_id(),
            signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![0u8; 10] })],
            CryptoHash::default(),
        )
    }

    /// Wrap a sharded PTQ in Arc<Mutex<...>>.
    fn make_sharded_ptq() -> Arc<Mutex<ShardedPendingTransactionQueue>> {
        Arc::new(Mutex::new(ShardedPendingTransactionQueue::new()))
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

    fn with_shard_ptq<R>(
        sharded: &Mutex<ShardedPendingTransactionQueue>,
        f: impl FnOnce(&mut PendingTransactionQueue) -> R,
    ) -> R {
        f(sharded.lock().get_or_create(TEST_SHARD_UID))
    }

    fn make_session(sharded: &Arc<Mutex<ShardedPendingTransactionQueue>>) -> PendingTxSession {
        PendingTxSession::new(Arc::clone(sharded), TEST_SHARD_UID)
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
    fn test_session_p_max_enforcement() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let txs: Vec<_> = (1..=P_MAX)
            .map(|i| make_transfer_tx(&signer, "bob.near", i as Nonce, TEST_DEPOSIT))
            .collect();
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &txs, &config, TEST_GAS_PRICE);
        let next_tx = make_transfer_tx(&signer, "bob.near", (P_MAX + 1) as Nonce, TEST_DEPOSIT);

        // The next access key tx from a contract account should be skipped.
        let mut session = make_session(&sharded);
        assert_eq!(session.check_pending(&next_tx, HasContract::Yes), PendingTxCheckResult::Skip);

        // But from a non-contract account it should be admitted.
        let mut session2 = make_session(&sharded);
        assert!(matches!(session2.check_pending(&next_tx, HasContract::No), PendingTxCheckResult::Admit(_)));
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
    fn test_deploy_exclusivity_pending_deploy_blocks_new_access_key_tx() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        add_chunk_txs(
            &sharded,
            CryptoHash::hash_bytes(&[1]),
            &[make_deploy_tx(&signer, 1)],
            &config,
            TEST_GAS_PRICE,
        );

        // A non-deploy access key tx should be skipped (deploy is pending).
        let mut session = make_session(&sharded);
        let transfer_tx = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        assert_eq!(session.check_pending(&transfer_tx, HasContract::No), PendingTxCheckResult::Skip);
    }

    #[test]
    fn test_deploy_exclusivity_pending_access_key_tx_blocks_deploy() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let transfer_tx = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        add_chunk_txs(
            &sharded,
            CryptoHash::hash_bytes(&[1]),
            &[transfer_tx],
            &config,
            TEST_GAS_PRICE,
        );

        // A deploy tx should be skipped (non-deploy access key tx is pending).
        let mut session = make_session(&sharded);
        let deploy_tx = make_deploy_tx(&signer, 2);
        assert_eq!(session.check_pending(&deploy_tx, HasContract::No), PendingTxCheckResult::Skip);
    }

    #[test]
    fn test_deploy_exclusivity_gas_key_deploy_blocks_access_key_tx() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        // A gas key tx with a deploy action is pending.
        add_chunk_txs(
            &sharded,
            CryptoHash::hash_bytes(&[1]),
            &[make_gas_key_deploy_tx(&signer, 1, 0)],
            &config,
            TEST_GAS_PRICE,
        );

        // An access key tx should be skipped (gas key deploy is pending).
        let mut session = make_session(&sharded);
        let transfer_tx = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        assert_eq!(session.check_pending(&transfer_tx, HasContract::No), PendingTxCheckResult::Skip);

        // But another gas key tx should still be admitted.
        let gas_key_transfer = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(3, 0),
            signer.get_account_id(),
            "bob.near".parse().unwrap(),
            &signer,
            vec![Action::Transfer(TransferAction { deposit: TEST_DEPOSIT })],
            CryptoHash::default(),
        );
        assert!(matches!(
            session.check_pending(&gas_key_transfer, HasContract::No),
            PendingTxCheckResult::Admit(_)
        ));
    }

    #[test]
    fn test_session_accumulates_across_calls() {
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let mut session = make_session(&sharded);

        // Admit P_MAX access key txs from a contract account within a single session.
        for i in 1..=P_MAX {
            let tx = make_transfer_tx(&signer, "bob.near", i as Nonce, TEST_DEPOSIT);
            assert!(
                matches!(session.check_pending(&tx, HasContract::Yes), PendingTxCheckResult::Admit(_)),
                "tx {} should be admitted",
                i
            );
        }
        // The (P_MAX + 1)th should be skipped.
        let tx = make_transfer_tx(&signer, "bob.near", (P_MAX + 1) as Nonce, TEST_DEPOSIT);
        assert_eq!(session.check_pending(&tx, HasContract::Yes), PendingTxCheckResult::Skip);
    }

    #[test]
    fn test_constraints_include_pending_balance() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let tx = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        let expected_cost = tx_cost(&config, &tx.transaction, TEST_GAS_PRICE).unwrap().total_cost;
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[tx], &config, TEST_GAS_PRICE);
        let mut session = make_session(&sharded);
        let next_tx = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        assert_eq!(
            session.check_pending(&next_tx, HasContract::No),
            PendingTxCheckResult::Admit(PendingConstraints {
                paid_from_balance: expected_cost,
                paid_from_gas_key: Balance::ZERO,
                max_nonce: 1,
            }),
        );
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

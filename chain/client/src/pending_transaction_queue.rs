use near_chain::types::{PendingConstraints, PendingTxCheckResult};
use near_crypto::PublicKeyHandle;
use near_parameters::RuntimeConfig;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::{SignedTransaction, Transaction};
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex};
use node_runtime::config::tx_cost;
use parking_lot::Mutex;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

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

/// Maximum number of pending access key transactions per account across all
/// uncertified blocks.
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

/// What a pending nonce is scoped to: the account, the key carrying the nonce,
/// and a `NonceIndex` selecting one of a gas key's nonces. The key is absent for
/// a self-signed universal state init, which has no access key yet.
///
/// Keys are stored as handles rather than full public keys: a handle is what the
/// trie holds, and for ML-DSA-65 it is a 32-byte hash in place of a 1952-byte key.
type NonceScope = (AccountId, Option<PublicKeyHandle>, Option<NonceIndex>);

/// A gas key, which unlike a nonce scope always names a key of its own.
type GasKey = (AccountId, PublicKeyHandle);

/// The scope of a nonce carried by an access or gas key.
fn key_nonce_scope(tx: &Transaction, key_handle: &PublicKeyHandle) -> NonceScope {
    (tx.signer_id().clone(), Some(key_handle.clone()), tx.nonce().nonce_index())
}

/// The nonce scope of a bootstrap-shaped transaction (i.e. self-signed state init).
/// It is scoped by account ID only, because the bootstrap nonce lives in the account
/// and is shared by all potential access keys for that account.
fn bootstrap_nonce_scope(account_id: &AccountId) -> NonceScope {
    (account_id.clone(), None, None)
}

/// Per-shard pending transaction queue. Tracks transactions that have been
/// included in blocks but not yet certified (executed).
pub struct PendingTransactionQueue {
    /// Per-chunk aggregate data, keyed by block hash (since SpiceChunkId
    /// is (block_hash, shard_id) and the shard is implicit).
    chunks: HashMap<CryptoHash, PendingChunkData>,
    /// Per-account aggregates (P_MAX counts, balance commitments).
    pending_accounts: HashMap<AccountId, PendingAccount>,
    /// Nonce tracking, per [`NonceScope`].
    pending_nonces: HashMap<NonceScope, PendingNonce>,
    /// Per-gas-key cost. Includes gas_key_cost from gas key txs + WithdrawFromGasKey amounts.
    pending_gas_key_costs: HashMap<GasKey, Balance>,
}

/// Nonce tracking for a single [`NonceScope`]. Stores each
/// contributing chunk's max nonce as a sorted multiset, so `max_nonce()`
/// reflects only currently-uncertified chunks. In case a chunk contains an
/// invalid transaction, this limits its impact to only that chunk.
#[derive(Default)]
struct PendingNonce {
    /// nonce -> number of chunks contributing this value.
    chunk_nonces: BTreeMap<Nonce, usize>,
}

impl PendingNonce {
    fn add(&mut self, nonce: Nonce) {
        *self.chunk_nonces.entry(nonce).or_insert(0) += 1;
    }

    fn remove(&mut self, nonce: Nonce) {
        if let Some(count) = self.chunk_nonces.get_mut(&nonce) {
            *count = checked_sub_or_default!(*count, 1, "chunk count underflow in PendingNonce");
            if *count == 0 {
                self.chunk_nonces.remove(&nonce);
            }
        }
    }

    fn max_nonce(&self) -> Nonce {
        self.chunk_nonces.last_key_value().map(|(&k, _)| k).unwrap_or(0)
    }

    #[cfg(test)]
    fn chunk_count(&self) -> usize {
        self.chunk_nonces.values().sum()
    }

    fn is_empty(&self) -> bool {
        self.chunk_nonces.is_empty()
    }
}

/// Per-chunk aggregate. No individual transaction records stored.
struct PendingChunkData {
    /// Per-account aggregates for this chunk.
    accounts: HashMap<AccountId, PendingAccount>,
    /// Max nonce for this chunk, per [`NonceScope`].
    nonces: HashMap<NonceScope, Nonce>,
    /// Per-gas-key costs for this chunk (gas_key_cost + WithdrawFromGasKey).
    gas_key_costs: HashMap<GasKey, Balance>,
}

/// Aggregate for a set of transactions, per account.
/// Used both per-chunk and as pending transaction queue totals. Supports add/subtract.
#[derive(Clone, Default)]
struct PendingAccount {
    access_key_tx_count: usize,
    /// Access key total_cost + gas key deposit_cost.
    paid_from_balance: Balance,
}

impl PendingAccount {
    fn add(&mut self, other: &PendingAccount) {
        self.access_key_tx_count += other.access_key_tx_count;
        self.paid_from_balance = self.paid_from_balance.saturating_add(other.paid_from_balance);
    }

    fn subtract(&mut self, other: &PendingAccount) {
        self.access_key_tx_count = checked_sub_or_default!(
            self.access_key_tx_count,
            other.access_key_tx_count,
            "access_key_tx_count underflow in pending transaction queue subtract"
        );
        self.paid_from_balance = checked_sub_or_default!(
            self.paid_from_balance,
            other.paid_from_balance,
            "paid_from_balance underflow in pending transaction queue subtract"
        );
    }

    fn is_zero(&self) -> bool {
        self.access_key_tx_count == 0 && self.paid_from_balance.is_zero()
    }
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
            let signer_id = tx.signer_id();
            let nonce_index = tx.nonce().nonce_index();
            let nonce = tx.nonce().nonce();
            let is_gas_key_tx = nonce_index.is_some();
            let key_handle = PublicKeyHandle::from(tx.public_key());

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

            // Track gas key costs (gas_key_cost for gas key txs).
            if is_gas_key_tx {
                let gas_key_entry = chunk_data
                    .gas_key_costs
                    .entry((signer_id.clone(), key_handle.clone()))
                    .or_insert(Balance::ZERO);
                *gas_key_entry = gas_key_entry.saturating_add(cost.gas_cost);
            }

            // Scan actions for WithdrawFromGasKey (affects gas key balance).
            for action in tx.actions() {
                if let Action::WithdrawFromGasKey(withdraw) = action {
                    let gas_key_entry = chunk_data
                        .gas_key_costs
                        .entry((signer_id.clone(), (&withdraw.public_key).into()))
                        .or_insert(Balance::ZERO);
                    *gas_key_entry = gas_key_entry.saturating_add(withdraw.amount);
                }
            }

            let mut record_nonce = |scope| {
                let max_nonce = chunk_data.nonces.entry(scope).or_insert(0);
                *max_nonce = max(*max_nonce, nonce);
            };
            record_nonce(key_nonce_scope(tx, &key_handle));
            if tx.is_state_init_bootstrap() {
                record_nonce(bootstrap_nonce_scope(signer_id));
            }
        }

        // Merge chunk data into pending transaction queue totals.
        for (account_id, chunk_account) in &chunk_data.accounts {
            let total_account = self.pending_accounts.entry(account_id.clone()).or_default();
            total_account.add(chunk_account);
        }
        for (scope, &chunk_nonce) in &chunk_data.nonces {
            self.pending_nonces.entry(scope.clone()).or_default().add(chunk_nonce);
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

        for (scope, &chunk_nonce) in &chunk_data.nonces {
            if let Some(entry) = self.pending_nonces.get_mut(scope) {
                entry.remove(chunk_nonce);
                if entry.is_empty() {
                    self.pending_nonces.remove(scope);
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
        let key_handle = PublicKeyHandle::from(tx.transaction.public_key());
        let snapshot = self.query_pending_state(&tx.transaction, &key_handle);
        PendingConstraints {
            paid_from_balance: snapshot.paid_from_balance,
            paid_from_gas_key: snapshot.pending_gas_key_cost,
            max_nonce: snapshot.max_nonce,
            max_bootstrap_nonce: snapshot.max_bootstrap_nonce,
        }
    }

    /// Highest nonce any uncertified chunk holds for `scope`, 0 if none does.
    fn max_pending_nonce(&self, scope: &NonceScope) -> Nonce {
        self.pending_nonces.get(scope).map(|n| n.max_nonce()).unwrap_or(0)
    }

    /// Query pending state for a single transaction. Extracts the counts and
    /// constraints needed by `PendingTxSession::check_pending`. This is called
    /// under the lock and should be fast.
    fn query_pending_state(
        &self,
        tx: &Transaction,
        key_handle: &PublicKeyHandle,
    ) -> PendingStateSnapshot {
        let signer_id = tx.signer_id();
        let pending_account = self.pending_accounts.get(signer_id);
        let access_key_tx_count = pending_account.map(|a| a.access_key_tx_count).unwrap_or(0);
        let paid_from_balance =
            pending_account.map(|a| a.paid_from_balance).unwrap_or(Balance::ZERO);

        let gas_key = (signer_id.clone(), key_handle.clone());
        let pending_gas_key_cost =
            self.pending_gas_key_costs.get(&gas_key).copied().unwrap_or(Balance::ZERO);

        let max_nonce = self.max_pending_nonce(&key_nonce_scope(tx, key_handle));
        // Kept apart from `max_nonce` so it reaches only the reader it belongs to,
        // which is a still uninitialized account (see `bootstrap_nonce_scope`).
        let max_bootstrap_nonce = self.max_pending_nonce(&bootstrap_nonce_scope(signer_id));

        PendingStateSnapshot {
            access_key_tx_count,
            paid_from_balance,
            max_nonce,
            max_bootstrap_nonce,
            pending_gas_key_cost,
        }
    }
}

/// Snapshot of pending state for a single transaction's signer, extracted
/// under the lock and used outside it.
#[derive(Default)]
struct PendingStateSnapshot {
    access_key_tx_count: usize,
    paid_from_balance: Balance,
    max_nonce: Nonce,
    max_bootstrap_nonce: Nonce,
    pending_gas_key_cost: Balance,
}

/// Per-chunk-production session. Combines pending transaction queue state with session-local tracking
/// for constraints NOT handled by the ephemeral TrieUpdate.
///
/// The ephemeral TrieUpdate handles within-chunk accumulation for balance
/// (deducts cost), gas key balance (deducts gas_key_cost), and nonces
/// (advances after each accepted tx). The session only tracks what the
/// ephemeral state does NOT cover:
/// - P_MAX counts (per account)
/// - WithdrawFromGasKey amounts (action effects not applied by ephemeral state)
///
/// The session holds an `Arc<Mutex<ShardedPendingTransactionQueue>>` and acquires the lock briefly
/// per transaction rather than holding it for the entire chunk production duration. This avoids
/// blocking block processing and RPC handlers.
pub struct PendingTxSession {
    pending_transaction_queue: Arc<Mutex<ShardedPendingTransactionQueue>>,
    shard_uid: ShardUId,
    session_access_key_tx_counts: HashMap<AccountId, usize>,
    session_gas_key_withdrawals: HashMap<GasKey, Balance>,
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
            session_gas_key_withdrawals: HashMap::new(),
        }
    }

    /// Check if a transaction can be admitted given pending constraints.
    /// If admitted, updates session state and returns constraints
    /// for the runtime's balance/nonce validation.
    ///
    /// Acquires the pending transaction queue lock briefly to read pending state, then releases it.
    pub fn check_pending(&mut self, tx: &SignedTransaction) -> PendingTxCheckResult {
        let signer_id = tx.transaction.signer_id();
        let nonce_index = tx.transaction.nonce().nonce_index();
        let is_gas_key_tx = nonce_index.is_some();
        // Derived before taking the lock: for ML-DSA-65 this hashes a 1952-byte key.
        let key_handle = PublicKeyHandle::from(tx.transaction.public_key());

        let snapshot = {
            let guard = self.pending_transaction_queue.lock();
            match guard.get(&self.shard_uid) {
                Some(ptq) => ptq.query_pending_state(&tx.transaction, &key_handle),
                None => PendingStateSnapshot::default(),
            }
        };

        let session_access_key_count =
            self.session_access_key_tx_counts.get(signer_id).copied().unwrap_or(0);
        let total_access_key_count = snapshot.access_key_tx_count + session_access_key_count;

        if !is_gas_key_tx && total_access_key_count >= P_MAX {
            return PendingTxCheckResult::Skip;
        }

        // Build constraints for runtime validation.
        let gas_key = (signer_id.clone(), key_handle);
        let session_gas_key_withdrawal =
            self.session_gas_key_withdrawals.get(&gas_key).copied().unwrap_or(Balance::ZERO);
        let paid_from_gas_key =
            snapshot.pending_gas_key_cost.saturating_add(session_gas_key_withdrawal);

        // Update session state optimistically (assumes tx will be accepted).
        // If the runtime subsequently rejects the tx (e.g. insufficient
        // balance), these counts are not rolled back and the tx is discarded
        // (not reintroduced to the pool). This means a rejected tx may consume
        // a P_MAX slot for the remainder of this chunk production session,
        // reducing throughput under high contention. The risk is mitigated by
        // the fact that check_pending is called after signature verification
        // and basic validation, so only transactions with valid signatures
        // can reach this point -- an adversary cannot cheaply spam rejected
        // txs to exhaust slots.
        if !is_gas_key_tx {
            *self.session_access_key_tx_counts.entry(signer_id.clone()).or_insert(0) += 1;
        }
        // Track WithdrawFromGasKey amounts from this tx's actions.
        for action in tx.transaction.actions() {
            if let Action::WithdrawFromGasKey(withdraw) = action {
                let entry = self
                    .session_gas_key_withdrawals
                    .entry((signer_id.clone(), (&withdraw.public_key).into()))
                    .or_insert(Balance::ZERO);
                *entry = entry.saturating_add(withdraw.amount);
            }
        }

        PendingTxCheckResult::Admit(PendingConstraints {
            paid_from_balance: snapshot.paid_from_balance,
            paid_from_gas_key,
            max_nonce: snapshot.max_nonce,
            max_bootstrap_nonce: snapshot.max_bootstrap_nonce,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::action::UniversalStateInitAction;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
    use near_primitives::utils::derive_universal_account_id;
    use std::collections::BTreeSet;
    use std::slice;

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

    /// Wrap a sharded pending transaction queue in Arc<Mutex<...>>.
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

    /// `query_pending_state` with the key handle its callers derive for it.
    fn query_pending(
        ptq: &PendingTransactionQueue,
        tx: &SignedTransaction,
    ) -> PendingStateSnapshot {
        let key_handle = PublicKeyHandle::from(tx.transaction.public_key());
        ptq.query_pending_state(&tx.transaction, &key_handle)
    }

    /// Two signers and one `0u` id: the state init commits to both keys, so
    /// either is authorized to bootstrap the account its hash derives to.
    /// Returns the derived id, both signers under that id, and one self-signed
    /// init per key, all at `nonce`.
    fn make_self_signed_init_txs(nonce: Nonce) -> (AccountId, [Signer; 2], [SignedTransaction; 2]) {
        // The id is a hash of the keys, so the keys come first, under a
        // placeholder. Only the seed decides the key, so re-deriving each signer
        // under the id it just produced leaves the keys alone.
        let seeded = |account_id: AccountId, seed| {
            InMemorySigner::from_seed(account_id, KeyType::ED25519, seed)
        };
        let placeholder: AccountId = "unused.near".parse().unwrap();
        let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: Default::default(),
            access_keys: BTreeSet::from([
                PublicKeyHandle::from(seeded(placeholder.clone(), "first").public_key()),
                PublicKeyHandle::from(seeded(placeholder, "second").public_key()),
            ]),
        });
        let raw_state_init = state_init.to_raw();
        let account_id = derive_universal_account_id(&raw_state_init);
        let signers = [seeded(account_id.clone(), "first"), seeded(account_id.clone(), "second")];
        let init_tx = |signer: &Signer| {
            SignedTransaction::from_actions(
                nonce,
                account_id.clone(),
                account_id.clone(),
                signer,
                vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                    state_init: raw_state_init.clone(),
                    deposit: Balance::ZERO,
                }))],
                CryptoHash::default(),
            )
        };
        let txs = [init_tx(&signers[0]), init_tx(&signers[1])];
        (account_id, signers, txs)
    }

    /// A self-signed state init's nonce lives on the account, not on the key it
    /// is signed with, so two of them signed by two committed keys have to share
    /// one pending floor. Keyed per key, the second would see a floor of zero and
    /// be admitted at a nonce that is already spoken for.
    #[test]
    fn test_pending_nonce_for_self_signed_init_is_account_scoped() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let (account_id, _, [first, second]) = make_self_signed_init_txs(7);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[first], &config, TEST_GAS_PRICE);

        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.pending_nonces
                    .get(&bootstrap_nonce_scope(&account_id))
                    .map(|pending| pending.max_nonce()),
                Some(7),
                "the nonce belongs to the account, not only to the key that signed"
            );
        });

        let snapshot = with_shard_ptq(&sharded, |ptq| query_pending(ptq, &second));
        assert_eq!(
            snapshot.max_bootstrap_nonce, 7,
            "the sibling committed key must see the same floor"
        );
    }

    /// Same, within one chunk: the two inits merge into a single account entry
    /// rather than one per key, so neither can be counted twice.
    #[test]
    fn test_two_self_signed_inits_in_one_chunk_share_one_floor() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let (account_id, _, txs) = make_self_signed_init_txs(7);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &txs, &config, TEST_GAS_PRICE);

        with_shard_ptq(&sharded, |ptq| {
            let pending = ptq.pending_nonces.get(&bootstrap_nonce_scope(&account_id)).unwrap();
            assert_eq!(pending.chunk_count(), 1, "one entry per account, not one per key");
            assert_eq!(pending.max_nonce(), 7);
        });
    }

    /// The account scope must not be the only record of the nonce. A state-init
    /// shaped transaction against an initialized account succeeds by its access
    /// key, spending that key's nonce, so an ordinary transaction from the same
    /// key has to see the floor the init left behind.
    #[test]
    fn test_state_init_floor_is_visible_to_its_signing_key() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let (_, [signer, _], [init, _]) = make_self_signed_init_txs(7);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[init], &config, TEST_GAS_PRICE);

        let ordinary = make_transfer_tx(&signer, "bob.near", 7, TEST_DEPOSIT);
        let snapshot = with_shard_ptq(&sharded, |ptq| query_pending(ptq, &ordinary));
        assert_eq!(snapshot.max_nonce, 7, "the signing key must see the nonce it spent");
    }

    /// The other side of that: the floor an init leaves on the account is reported
    /// on its own, so it cannot reach the keys that did not sign it. Only a still
    /// uninitialized account reads it, and there is nothing else to sign with there.
    #[test]
    fn test_state_init_does_not_bound_a_sibling_key() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let (_, [_, sibling], [init, _]) = make_self_signed_init_txs(7);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[init], &config, TEST_GAS_PRICE);

        let ordinary = make_transfer_tx(&sibling, "bob.near", 3, TEST_DEPOSIT);
        let snapshot = with_shard_ptq(&sharded, |ptq| query_pending(ptq, &ordinary));
        assert_eq!(snapshot.max_nonce, 0, "a key that spent no nonce keeps its own floor");
        assert_eq!(snapshot.max_bootstrap_nonce, 7, "while the account's floor still stands");
    }

    /// The nonce scope holds a key *handle*, and for ML-DSA-65 that handle is a
    /// hash of the key rather than the key itself, so the derivation has to agree
    /// between recording a nonce and looking it up. Also the case the handle
    /// exists for: a 32-byte handle in place of a 1952-byte public key.
    #[test]
    fn test_pending_nonce_round_trips_an_ml_dsa_handle() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer =
            InMemorySigner::from_seed("alice.near".parse().unwrap(), KeyType::MLDSA65, "pq-seed");
        let tx = make_transfer_tx(&signer, "bob.near", 9, TEST_DEPOSIT);
        add_chunk_txs(
            &sharded,
            CryptoHash::hash_bytes(&[1]),
            slice::from_ref(&tx),
            &config,
            TEST_GAS_PRICE,
        );

        let snapshot = with_shard_ptq(&sharded, |ptq| query_pending(ptq, &tx));
        assert_eq!(snapshot.max_nonce, 9, "an ML-DSA-65 nonce must be found by its handle");

        let key_handle = PublicKeyHandle::from(signer.public_key());
        with_shard_ptq(&sharded, |ptq| {
            assert!(
                ptq.pending_nonces.contains_key(&key_nonce_scope(&tx.transaction, &key_handle)),
                "the entry must be keyed by the handle, not the full key"
            );
        });
    }

    /// An ordinary transaction stays scoped to the key that carries its nonce,
    /// so a second signer on the same account gets its own floor.
    #[test]
    fn test_pending_nonce_for_ordinary_tx_is_key_scoped() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let other =
            InMemorySigner::from_seed(signer.get_account_id(), KeyType::ED25519, "other-key");
        let tx = make_transfer_tx(&signer, "bob.near", 5, TEST_DEPOSIT);
        add_chunk_txs(&sharded, CryptoHash::hash_bytes(&[1]), &[tx], &config, TEST_GAS_PRICE);

        let other_tx = make_transfer_tx(&other, "bob.near", 5, TEST_DEPOSIT);
        let snapshot = with_shard_ptq(&sharded, |ptq| query_pending(ptq, &other_tx));
        assert_eq!(snapshot.max_nonce, 0, "a different key must not inherit the floor");
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
        let key_scope = (signer.get_account_id(), Some(signer.public_key().into()), None);

        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.pending_accounts.get(&signer.get_account_id()).unwrap().access_key_tx_count,
                2
            );
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().chunk_count(), 2);
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().max_nonce(), 2);
        });
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&hash1));
        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(
                ptq.pending_accounts.get(&signer.get_account_id()).unwrap().access_key_tx_count,
                1
            );
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().chunk_count(), 1);
        });
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&hash2));
        with_shard_ptq(&sharded, |ptq| assert!(ptq.is_empty()));
    }

    #[test]
    fn test_max_nonce_recomputed_after_partial_removal() {
        let config = RuntimeConfig::test();
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let hash1 = CryptoHash::hash_bytes(&[1]);
        let hash2 = CryptoHash::hash_bytes(&[2]);
        let tx1 = make_transfer_tx(&signer, "bob.near", 1, TEST_DEPOSIT);
        let tx2 = make_transfer_tx(&signer, "bob.near", 2, TEST_DEPOSIT);
        add_chunk_txs(&sharded, hash1, &[tx1], &config, TEST_GAS_PRICE);
        add_chunk_txs(&sharded, hash2, &[tx2], &config, TEST_GAS_PRICE);
        let key_scope = (signer.get_account_id(), Some(signer.public_key().into()), None);

        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().max_nonce(), 2);
        });
        // Remove the chunk with the higher nonce first.
        with_shard_ptq(&sharded, |ptq| ptq.remove_certified_chunk_by_block_hash(&hash2));
        with_shard_ptq(&sharded, |ptq| {
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().chunk_count(), 1);
            assert_eq!(ptq.pending_nonces.get(&key_scope).unwrap().max_nonce(), 1);
        });
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

        let mut session = make_session(&sharded);
        assert_eq!(session.check_pending(&next_tx), PendingTxCheckResult::Skip);
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
    fn test_session_accumulates_across_calls() {
        let sharded = make_sharded_ptq();
        let signer = test_signer();
        let mut session = make_session(&sharded);

        // Admit P_MAX access key txs within a single session.
        for i in 1..=P_MAX {
            let tx = make_transfer_tx(&signer, "bob.near", i as Nonce, TEST_DEPOSIT);
            assert!(
                matches!(session.check_pending(&tx), PendingTxCheckResult::Admit(_)),
                "tx {} should be admitted",
                i
            );
        }
        // The (P_MAX + 1)th should be skipped.
        let tx = make_transfer_tx(&signer, "bob.near", (P_MAX + 1) as Nonce, TEST_DEPOSIT);
        assert_eq!(session.check_pending(&tx), PendingTxCheckResult::Skip);
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
            session.check_pending(&next_tx),
            PendingTxCheckResult::Admit(PendingConstraints {
                paid_from_balance: expected_cost,
                paid_from_gas_key: Balance::ZERO,
                max_nonce: 1,
                max_bootstrap_nonce: 0,
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
                    max_bootstrap_nonce: 0,
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

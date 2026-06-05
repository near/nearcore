use crate::{
    DBCol, MappedTx, MappedTxProvenance, NonceLookupKey, SignedTransaction, TxAwaitingNonce,
};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::SecretKey;
use near_primitives::transaction::Transaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives_core::types::Nonce;
use rocksdb::DB;
use std::collections::HashMap;
use std::collections::HashSet;

const DEFERRED_TX_TTL: BlockHeight = 50;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub(crate) struct DeferredTx {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    provenance: MappedTxProvenance,
    target_secret_key: String,
    pub(crate) target_tx: Transaction,
    nonce_updates: HashSet<NonceLookupKey>,
    deferred_at: BlockHeight,
}

impl DeferredTx {
    pub(crate) fn new(tx: TxAwaitingNonce, deferred_at: BlockHeight) -> Self {
        Self {
            source_signer_id: tx.source_signer_id,
            source_receiver_id: tx.source_receiver_id,
            provenance: tx.provenance,
            target_secret_key: tx.target_secret_key.to_string(),
            target_tx: tx.target_tx,
            nonce_updates: tx.nonce_updates,
            deferred_at,
        }
    }

    pub(crate) fn nonce_key(&self) -> NonceLookupKey {
        NonceLookupKey::from_tx(&self.target_tx)
    }

    pub(crate) fn into_ready(self) -> MappedTx {
        let target_secret_key: SecretKey =
            self.target_secret_key.parse().expect("serialized our own target secret key");
        let target_tx = SignedTransaction::new(
            target_secret_key.sign(&self.target_tx.get_hash_and_size().0.as_ref()),
            self.target_tx,
        );
        MappedTx {
            source_signer_id: self.source_signer_id,
            source_receiver_id: self.source_receiver_id,
            provenance: self.provenance,
            target_tx,
            nonce_updates: self.nonce_updates,
            sent_successfully: false,
        }
    }
}

pub(crate) struct DeferredTxTracker {
    txs: HashMap<NonceLookupKey, Vec<DeferredTx>>,
}

impl DeferredTxTracker {
    pub(crate) fn load(db: &DB) -> anyhow::Result<Self> {
        let mut txs = HashMap::new();
        let handle = db.cf_handle(DBCol::DeferredTxs.name()).unwrap();
        for item in db.iterator_cf(handle, rocksdb::IteratorMode::Start) {
            let (key, value) = item?;
            txs.insert(
                NonceLookupKey::try_from_slice(&key).unwrap(),
                Vec::<DeferredTx>::try_from_slice(&value).unwrap(),
            );
        }
        Ok(Self { txs })
    }

    pub(crate) fn keys(&self) -> Vec<NonceLookupKey> {
        self.txs.keys().cloned().collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub(crate) fn push(
        &mut self,
        db: &DB,
        tx: TxAwaitingNonce,
        target_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let deferred = DeferredTx::new(tx, target_height);
        let nonce_key = deferred.nonce_key();
        let entry = self.txs.entry(nonce_key.clone()).or_default();
        entry.push(deferred);
        Self::db_put(db, &nonce_key, entry)?;
        tracing::debug!(target: "mirror", ?nonce_key, "deferred tx until its access key appears on the target chain");
        Ok(())
    }

    pub(crate) fn resolve(
        &mut self,
        nonce_key: &NonceLookupKey,
        nonce: &mut Option<Nonce>,
    ) -> Vec<DeferredTx> {
        let Some(next_nonce) = nonce.as_mut() else {
            return Vec::new();
        };
        let Some(mut txs) = self.txs.remove(nonce_key) else {
            return Vec::new();
        };
        for tx in &mut txs {
            *next_nonce += 1;
            *tx.target_tx.nonce_mut() = *next_nonce;
        }
        tracing::debug!(target: "mirror", ?nonce_key, count = txs.len(), "resending deferred txs");
        txs
    }

    pub(crate) fn prune(&mut self, db: &DB, target_height: BlockHeight) -> anyhow::Result<()> {
        let mut emptied_keys = Vec::new();
        for (nonce_key, txs) in &mut self.txs {
            let before = txs.len();
            txs.retain(|tx| target_height.saturating_sub(tx.deferred_at) <= DEFERRED_TX_TTL);
            let dropped = before - txs.len();
            if dropped > 0 {
                tracing::warn!(target: "mirror", ?nonce_key, dropped, "dropping deferred txs whose access key never appeared on the target chain");
                Self::db_put(db, nonce_key, txs)?;
            }
            if txs.is_empty() {
                emptied_keys.push(nonce_key.clone());
            }
        }
        for nonce_key in emptied_keys {
            self.txs.remove(&nonce_key);
        }
        Ok(())
    }

    // Clear the DeferredTxs column for keys whose txs were resent and confirmed
    // sent. The stored target nonce is already advanced by on_tx_sent().
    pub(crate) fn commit_sent(db: &DB, keys: &[NonceLookupKey]) -> anyhow::Result<()> {
        for nonce_key in keys {
            Self::db_put(db, nonce_key, &[])?;
        }
        Ok(())
    }

    fn db_put(db: &DB, key: &NonceLookupKey, txs: &[DeferredTx]) -> anyhow::Result<()> {
        let handle = db.cf_handle(DBCol::DeferredTxs.name()).unwrap();
        if txs.is_empty() {
            db.delete_cf(handle, &key.db_key())?;
        } else {
            db.put_cf(handle, &key.db_key(), &borsh::to_vec(txs).unwrap())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{TargetNonce, open_db};
    use near_crypto::KeyType;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::ShardId;

    fn awaiting_nonce(signer: &str, nonce: Nonce) -> TxAwaitingNonce {
        let target_secret_key = SecretKey::from_seed(KeyType::ED25519, signer);
        let signer_id: AccountId = signer.parse().unwrap();
        let receiver_id: AccountId = "receiver.near".parse().unwrap();
        let target_tx = Transaction::new_v0(
            signer_id.clone(),
            target_secret_key.public_key(),
            receiver_id.clone(),
            nonce,
            CryptoHash::default(),
        );
        TxAwaitingNonce {
            source_signer_id: signer_id,
            source_receiver_id: receiver_id,
            provenance: MappedTxProvenance::MappedSourceTx(1, ShardId::new(0), 0),
            target_secret_key,
            target_tx,
            nonce_updates: HashSet::new(),
            target_nonce: TargetNonce::default(),
        }
    }

    fn assigned_nonce(tx: &DeferredTx) -> Nonce {
        tx.target_tx.nonce().nonce()
    }

    #[test]
    fn push_persists_across_reload() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();
        let tx = awaiting_nonce("alice.near", 0);
        let key = NonceLookupKey::from_tx(&tx.target_tx);

        DeferredTxTracker::load(&db).unwrap().push(&db, tx, 5).unwrap();

        assert_eq!(DeferredTxTracker::load(&db).unwrap().keys(), vec![key]);
    }

    #[test]
    fn resolve_assigns_increasing_nonces() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();
        let mut tracker = DeferredTxTracker::load(&db).unwrap();
        let key = NonceLookupKey::from_tx(&awaiting_nonce("alice.near", 0).target_tx);
        tracker.push(&db, awaiting_nonce("alice.near", 0), 5).unwrap();
        tracker.push(&db, awaiting_nonce("alice.near", 0), 5).unwrap();

        let mut nonce = Some(10);
        let resolved = tracker.resolve(&key, &mut nonce);

        assert_eq!(resolved.iter().map(assigned_nonce).collect::<Vec<_>>(), vec![11, 12]);
        assert_eq!(nonce, Some(12));
        assert!(tracker.keys().is_empty());
    }

    #[test]
    fn resolve_without_known_nonce_keeps_txs() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();
        let mut tracker = DeferredTxTracker::load(&db).unwrap();
        let key = NonceLookupKey::from_tx(&awaiting_nonce("alice.near", 0).target_tx);
        tracker.push(&db, awaiting_nonce("alice.near", 0), 5).unwrap();

        let mut nonce = None;
        assert!(tracker.resolve(&key, &mut nonce).is_empty());
        assert_eq!(tracker.keys(), vec![key]);
    }

    // resolve() removes txs from memory but intentionally leaves the DB rows so a crash
    // between resolving and sending can't lose them; commit_sent() clears them afterwards.
    #[test]
    fn resolve_leaves_db_for_commit_sent() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();
        let mut tracker = DeferredTxTracker::load(&db).unwrap();
        let key = NonceLookupKey::from_tx(&awaiting_nonce("alice.near", 0).target_tx);
        tracker.push(&db, awaiting_nonce("alice.near", 0), 5).unwrap();

        let mut nonce = Some(10);
        tracker.resolve(&key, &mut nonce);
        assert_eq!(DeferredTxTracker::load(&db).unwrap().keys(), vec![key.clone()]);

        DeferredTxTracker::commit_sent(&db, &[key]).unwrap();
        assert!(DeferredTxTracker::load(&db).unwrap().keys().is_empty());
    }

    #[test]
    fn prune_drops_after_ttl() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();
        let mut tracker = DeferredTxTracker::load(&db).unwrap();
        tracker.push(&db, awaiting_nonce("alice.near", 0), 5).unwrap();

        tracker.prune(&db, 5 + DEFERRED_TX_TTL).unwrap();
        assert_eq!(tracker.keys().len(), 1);

        tracker.prune(&db, 5 + DEFERRED_TX_TTL + 1).unwrap();
        assert!(tracker.keys().is_empty());
        assert!(DeferredTxTracker::load(&db).unwrap().keys().is_empty());
    }
}

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

const TTL: BlockHeight = 50;

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
        if self.txs.get(nonce_key).is_none_or(|txs| txs.is_empty()) {
            return Vec::new();
        }
        let Some(next_nonce) = nonce.as_mut() else {
            return Vec::new();
        };
        let mut txs = self.txs.remove(nonce_key).unwrap();
        for tx in &mut txs {
            *next_nonce += 1;
            *tx.target_tx.nonce_mut() = *next_nonce;
        }
        tracing::debug!(target: "mirror", ?nonce_key, count = txs.len(), "resending deferred txs");
        txs
    }

    pub(crate) fn prune(&mut self, db: &DB, target_height: BlockHeight) -> anyhow::Result<()> {
        let mut emptied_keys = Vec::new();
        for (nonce_key, txs) in self.txs.iter_mut() {
            let before = txs.len();
            txs.retain(|tx| target_height.saturating_sub(tx.deferred_at) <= TTL);
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

    pub(crate) fn commit_sent(db: &DB, sent: &[(NonceLookupKey, Nonce)]) -> anyhow::Result<()> {
        let mut by_key: HashMap<&NonceLookupKey, Nonce> = HashMap::new();
        for (nonce_key, nonce) in sent {
            by_key
                .entry(nonce_key)
                .and_modify(|n| *n = std::cmp::max(*n, *nonce))
                .or_insert(*nonce);
        }
        for (nonce_key, max_nonce) in &by_key {
            Self::db_put(db, nonce_key, &[])?;
            let mut stored = crate::read_target_nonce(db, nonce_key)?.unwrap();
            stored.nonce = std::cmp::max(stored.nonce, Some(*max_nonce));
            crate::put_target_nonce(db, nonce_key, &stored)?;
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

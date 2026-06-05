use crate::{MappedTx, MappedTxProvenance, NonceLookupKey, SignedTransaction, TxAwaitingNonce};
use near_crypto::SecretKey;
use near_primitives::transaction::Transaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives_core::types::Nonce;
use std::collections::HashMap;
use std::collections::HashSet;

const DEFERRED_TX_TTL: BlockHeight = 50;

#[derive(Clone, Debug)]
pub(crate) struct DeferredTx {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    provenance: MappedTxProvenance,
    target_secret_key: SecretKey,
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
            target_secret_key: tx.target_secret_key,
            target_tx: tx.target_tx,
            nonce_updates: tx.nonce_updates,
            deferred_at,
        }
    }

    pub(crate) fn nonce_key(&self) -> NonceLookupKey {
        NonceLookupKey::from_tx(&self.target_tx)
    }

    pub(crate) fn into_ready(self) -> MappedTx {
        let target_tx = SignedTransaction::new(
            self.target_secret_key.sign(&self.target_tx.get_hash_and_size().0.as_ref()),
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
    pub(crate) fn new() -> Self {
        Self { txs: HashMap::new() }
    }

    pub(crate) fn keys(&self) -> Vec<NonceLookupKey> {
        self.txs.keys().cloned().collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub(crate) fn push(&mut self, tx: TxAwaitingNonce, target_height: BlockHeight) {
        let deferred = DeferredTx::new(tx, target_height);
        let nonce_key = deferred.nonce_key();
        self.txs.entry(nonce_key.clone()).or_default().push(deferred);
        tracing::debug!(target: "mirror", ?nonce_key, "deferred tx until its access key appears on the target chain");
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

    pub(crate) fn prune(&mut self, target_height: BlockHeight) {
        self.txs.retain(|nonce_key, txs| {
            let before = txs.len();
            txs.retain(|tx| target_height.saturating_sub(tx.deferred_at) <= DEFERRED_TX_TTL);
            let dropped = before - txs.len();
            if dropped > 0 {
                tracing::warn!(target: "mirror", ?nonce_key, dropped, "dropping deferred txs whose access key never appeared on the target chain");
            }
            !txs.is_empty()
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::TargetNonce;
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
    fn resolve_assigns_increasing_nonces() {
        let mut tracker = DeferredTxTracker::new();
        let key = NonceLookupKey::from_tx(&awaiting_nonce("alice.near", 0).target_tx);
        tracker.push(awaiting_nonce("alice.near", 0), 5);
        tracker.push(awaiting_nonce("alice.near", 0), 5);

        let mut nonce = Some(10);
        let resolved = tracker.resolve(&key, &mut nonce);

        assert_eq!(resolved.iter().map(assigned_nonce).collect::<Vec<_>>(), vec![11, 12]);
        assert_eq!(nonce, Some(12));
        assert!(tracker.keys().is_empty());
    }

    #[test]
    fn resolve_without_known_nonce_keeps_txs() {
        let mut tracker = DeferredTxTracker::new();
        let key = NonceLookupKey::from_tx(&awaiting_nonce("alice.near", 0).target_tx);
        tracker.push(awaiting_nonce("alice.near", 0), 5);

        let mut nonce = None;
        assert!(tracker.resolve(&key, &mut nonce).is_empty());
        assert_eq!(tracker.keys(), vec![key]);
    }

    #[test]
    fn prune_drops_after_ttl() {
        let mut tracker = DeferredTxTracker::new();
        tracker.push(awaiting_nonce("alice.near", 0), 5);

        tracker.prune(5 + DEFERRED_TX_TTL);
        assert_eq!(tracker.keys().len(), 1);

        tracker.prune(5 + DEFERRED_TX_TTL + 1);
        assert!(tracker.keys().is_empty());
    }
}

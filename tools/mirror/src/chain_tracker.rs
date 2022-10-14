use crate::MappedBlock;
use near_crypto::PublicKey;
use near_indexer::StreamerMessage;
use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives_core::types::{Nonce, ShardId};
use std::cmp::Ordering;
use std::collections::hash_map;
use std::collections::HashMap;
use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::time::{Duration, Instant};

struct TxSendInfo {
    sent_at: Instant,
    source_height: BlockHeight,
    target_height: BlockHeight,
}

#[derive(PartialEq, Eq, Debug)]
struct TxId {
    hash: CryptoHash,
    nonce: Nonce,
}

impl PartialOrd for TxId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TxId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.nonce.cmp(&other.nonce).then_with(|| self.hash.cmp(&other.hash))
    }
}

// we want a reference to transactions in .queued_blocks that need to have nonces
// set later. To avoid having the struct be self referential we keep this struct
// with enough info to look it up later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TxRef {
    height: BlockHeight,
    shard_id: ShardId,
    tx_idx: usize,
}

struct TxAwaitingNonceCursor<'a> {
    txs: &'a [TxRef],
    idx: usize,
}

impl<'a> TxAwaitingNonceCursor<'a> {
    fn new(txs: &'a [TxRef]) -> Self {
        Self { txs, idx: 0 }
    }
}

pub(crate) struct TxAwaitingNonceIter<'a> {
    queued_blocks: &'a VecDeque<MappedBlock>,
    iter: hash_map::Iter<'a, BlockHeight, Vec<TxRef>>,
    cursor: Option<TxAwaitingNonceCursor<'a>>,
}

impl<'a> TxAwaitingNonceIter<'a> {
    fn new(
        queued_blocks: &'a VecDeque<MappedBlock>,
        txs_awaiting_nonce: &'a HashMap<BlockHeight, Vec<TxRef>>,
    ) -> Self {
        let mut iter = txs_awaiting_nonce.iter();
        let cursor = iter.next().map(|(_height, txs)| TxAwaitingNonceCursor::new(txs));
        Self { queued_blocks, iter, cursor }
    }
}

impl<'a> Iterator for TxAwaitingNonceIter<'a> {
    type Item = (&'a TxRef, &'a crate::TxAwaitingNonce);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.cursor {
            Some(c) => {
                let tx_ref = &c.txs[c.idx];
                c.idx += 1;
                if c.idx == c.txs.len() {
                    self.cursor =
                        self.iter.next().map(|(_height, txs)| TxAwaitingNonceCursor::new(txs));
                }
                let block_idx = self
                    .queued_blocks
                    .binary_search_by(|b| b.source_height.cmp(&tx_ref.height))
                    .unwrap();
                let block = &self.queued_blocks[block_idx];
                let chunk = block.chunks.iter().find(|c| c.shard_id == tx_ref.shard_id).unwrap();
                match &chunk.txs[tx_ref.tx_idx] {
                    crate::TargetChainTx::AwaitingNonce(tx) => Some((tx_ref, tx)),
                    crate::TargetChainTx::Ready(_) => unreachable!(),
                }
            }
            None => None,
        }
    }
}

// Keeps the queue of upcoming transactions and provides them in regular intervals via next_batch()
// Also keeps track of txs we've sent so far and looks for them on chain, for metrics/logging purposes.
#[derive(Default)]
pub(crate) struct TxTracker {
    sent_txs: HashMap<CryptoHash, TxSendInfo>,
    txs_by_signer: HashMap<(AccountId, PublicKey), BTreeSet<TxId>>,
    queued_blocks: VecDeque<MappedBlock>,
    txs_awaiting_nonce: HashMap<BlockHeight, Vec<TxRef>>,
    pending_access_keys: HashMap<(AccountId, PublicKey), usize>,
    height_queued: Option<BlockHeight>,
    send_time: Option<Pin<Box<tokio::time::Sleep>>>,
    // Config value in the target chain, used to judge how long to wait before sending a new batch of txs
    min_block_production_delay: Duration,
    // timestamps in the target chain, used to judge how long to wait before sending a new batch of txs
    recent_block_timestamps: VecDeque<u64>,
}

impl TxTracker {
    pub(crate) fn new(min_block_production_delay: Duration) -> Self {
        Self { min_block_production_delay, ..Default::default() }
    }

    pub(crate) fn height_queued(&self) -> Option<BlockHeight> {
        self.height_queued
    }

    pub(crate) fn num_blocks_queued(&self) -> usize {
        self.queued_blocks.len()
    }

    pub(crate) fn pending_access_keys_iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a (AccountId, PublicKey)> {
        self.pending_access_keys.iter().map(|(x, _)| x)
    }

    pub(crate) fn tx_awaiting_nonce_iter<'a>(&'a self) -> TxAwaitingNonceIter<'a> {
        TxAwaitingNonceIter::new(&self.queued_blocks, &self.txs_awaiting_nonce)
    }

    fn pending_access_keys_deref(
        &mut self,
        source_signer_id: AccountId,
        source_public_key: PublicKey,
    ) {
        match self.pending_access_keys.entry((source_signer_id, source_public_key)) {
            hash_map::Entry::Occupied(mut e) => {
                let ref_count = e.get_mut();
                if *ref_count == 1 {
                    e.remove();
                } else {
                    *ref_count -= 1;
                }
            }
            hash_map::Entry::Vacant(_) => unreachable!(),
        }
    }

    // We now know of a valid nonce for the transaction referenced by tx_ref.
    // Set the nonce and mark the tx as ready to be sent later.
    pub(crate) fn set_tx_nonce(&mut self, tx_ref: &TxRef, nonce: Nonce) {
        let block_idx =
            self.queued_blocks.binary_search_by(|b| b.source_height.cmp(&tx_ref.height)).unwrap();
        let block = &mut self.queued_blocks[block_idx];
        let chunk = block.chunks.iter_mut().find(|c| c.shard_id == tx_ref.shard_id).unwrap();
        let tx = &mut chunk.txs[tx_ref.tx_idx];

        match self.txs_awaiting_nonce.entry(tx_ref.height) {
            hash_map::Entry::Occupied(mut e) => {
                let txs = e.get_mut();
                if txs.len() == 1 {
                    assert!(&txs[0] == tx_ref);
                    e.remove();
                } else {
                    let idx = txs.iter().position(|t| t == tx_ref).unwrap();
                    txs.swap_remove(idx);
                }
            }
            hash_map::Entry::Vacant(_) => unreachable!(),
        }
        let (source_signer_id, source_public_key) = match &tx {
            crate::TargetChainTx::AwaitingNonce(tx) => {
                (tx.source_signer_id.clone(), tx.source_public.clone())
            }
            crate::TargetChainTx::Ready(_) => unreachable!(),
        };

        tx.set_nonce(nonce);
        self.pending_access_keys_deref(source_signer_id, source_public_key);
    }

    pub(crate) fn queue_block(&mut self, block: MappedBlock) {
        self.height_queued = Some(block.source_height);
        let mut txs_awaiting_nonce = Vec::new();
        for c in block.chunks.iter() {
            for (tx_idx, tx) in c.txs.iter().enumerate() {
                if let crate::TargetChainTx::AwaitingNonce(tx) = tx {
                    txs_awaiting_nonce.push(TxRef {
                        height: block.source_height,
                        shard_id: c.shard_id,
                        tx_idx,
                    });
                    *self
                        .pending_access_keys
                        .entry((tx.source_signer_id.clone(), tx.source_public.clone()))
                        .or_default() += 1;
                }
            }
        }
        if !txs_awaiting_nonce.is_empty() {
            self.txs_awaiting_nonce.insert(block.source_height, txs_awaiting_nonce);
        }
        self.queued_blocks.push_back(block);
    }

    pub(crate) fn next_batch_time(&self) -> Instant {
        match &self.send_time {
            Some(t) => t.as_ref().deadline().into_std(),
            None => Instant::now(),
        }
    }

    pub(crate) async fn next_batch(&mut self) -> Option<MappedBlock> {
        if let Some(sleep) = &mut self.send_time {
            sleep.await;
        }
        let block = self.queued_blocks.pop_front();
        if let Some(block) = &block {
            self.txs_awaiting_nonce.remove(&block.source_height);
            for chunk in block.chunks.iter() {
                for tx in chunk.txs.iter() {
                    match &tx {
                        crate::TargetChainTx::AwaitingNonce(tx) => self.pending_access_keys_deref(
                            tx.source_signer_id.clone(),
                            tx.source_public.clone(),
                        ),
                        crate::TargetChainTx::Ready(_) => {}
                    }
                }
            }
        }
        block
    }

    fn remove_tx(&mut self, tx: &IndexerTransactionWithOutcome) {
        let k = (tx.transaction.signer_id.clone(), tx.transaction.public_key.clone());
        match self.txs_by_signer.entry(k.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                let txs = e.get_mut();
                if !txs.remove(&TxId { hash: tx.transaction.hash, nonce: tx.transaction.nonce }) {
                    tracing::warn!(target: "mirror", "tried to remove nonexistent tx {} from txs_by_signer", tx.transaction.hash);
                }
                // split off from hash: default() since that's the smallest hash, which will leave us with every tx with nonce
                // greater than this one in txs_left.
                let txs_left = txs.split_off(&TxId {
                    hash: CryptoHash::default(),
                    nonce: tx.transaction.nonce + 1,
                });
                if !txs.is_empty() {
                    tracing::warn!(
                        target: "mirror", "{} Transactions for {:?} skipped by inclusion of tx with nonce {}: {:?}. These will never make it on chain.",
                        txs.len(), &k, tx.transaction.nonce, &txs
                    );
                }
                *txs = txs_left;
                if txs.is_empty() {
                    self.txs_by_signer.remove(&k);
                }
            }
            hash_map::Entry::Vacant(_) => {
                tracing::warn!(
                    target: "mirror", "recently removed tx {}, but ({:?}, {:?}) not in txs_by_signer",
                    tx.transaction.hash, tx.transaction.signer_id, tx.transaction.public_key
                );
                return;
            }
        };
    }

    fn record_block_timestamp(&mut self, msg: &StreamerMessage) {
        self.recent_block_timestamps.push_back(msg.block.header.timestamp_nanosec);
        if self.recent_block_timestamps.len() > 10 {
            self.recent_block_timestamps.pop_front();
        }
    }

    pub(crate) fn on_target_block(&mut self, msg: &StreamerMessage) {
        self.record_block_timestamp(msg);
        for s in msg.shards.iter() {
            if let Some(c) = &s.chunk {
                for tx in c.transactions.iter() {
                    if let Some(send_info) = self.sent_txs.remove(&tx.transaction.hash) {
                        let latency = Instant::now() - send_info.sent_at;
                        tracing::debug!(
                            target: "mirror", "found my tx {} from source #{} in target #{} {:?} after sending @ target #{}",
                            tx.transaction.hash, send_info.source_height, msg.block.header.height, latency, send_info.target_height
                        );
                        crate::metrics::TRANSACTIONS_INCLUDED.inc();

                        self.remove_tx(tx);
                    }
                }
            }
        }
    }

    fn on_tx_sent(
        &mut self,
        tx: &SignedTransaction,
        source_height: BlockHeight,
        target_height: BlockHeight,
    ) {
        let hash = tx.get_hash();
        if self.sent_txs.contains_key(&hash) {
            tracing::warn!(target: "mirror", "transaction sent twice: {}", &hash);
            return;
        }

        // TODO: don't keep adding txs if we're not ever finding them on chain, since we'll OOM eventually
        // if that happens.
        self.sent_txs
            .insert(hash, TxSendInfo { sent_at: Instant::now(), source_height, target_height });
        let txs = self
            .txs_by_signer
            .entry((tx.transaction.signer_id.clone(), tx.transaction.public_key.clone()))
            .or_default();

        if let Some(highest_nonce) = txs.iter().next_back() {
            if highest_nonce.nonce > tx.transaction.nonce {
                tracing::warn!(
                    target: "mirror", "transaction sent with out of order nonce: {}: {}. Sent so far: {:?}",
                    &hash, tx.transaction.nonce, txs
                );
            }
        }
        if !txs.insert(TxId { hash, nonce: tx.transaction.nonce }) {
            tracing::warn!(target: "mirror", "inserted tx {} twice into txs_by_signer", &hash);
        }
    }

    // among the last 10 blocks, what's the second longest time between their timestamps?
    // probably there's a better heuristic to use than that but this will do for now.
    fn second_longest_recent_block_delay(&self) -> Option<Duration> {
        if self.recent_block_timestamps.len() < 5 {
            return None;
        }
        let mut last = *self.recent_block_timestamps.front().unwrap();
        let mut longest = None;
        let mut second_longest = None;

        for timestamp in self.recent_block_timestamps.iter().skip(1) {
            let delay = timestamp - last;

            match longest {
                Some(l) => match second_longest {
                    Some(s) => {
                        if delay > l {
                            second_longest = longest;
                            longest = Some(delay);
                        } else if delay > s {
                            second_longest = Some(delay);
                        }
                    }
                    None => {
                        if delay > l {
                            second_longest = longest;
                            longest = Some(delay);
                        } else {
                            second_longest = Some(delay);
                        }
                    }
                },
                None => {
                    longest = Some(delay);
                }
            }
            last = *timestamp;
        }
        let delay = Duration::from_nanos(second_longest.unwrap());
        if delay > 2 * self.min_block_production_delay {
            tracing::warn!(
                "Target chain blocks are taking longer than expected to be produced. Observing delays \
                of {:?} and {:?} vs min_block_production_delay of {:?} ",
                delay,
                Duration::from_nanos(longest.unwrap()),
                self.min_block_production_delay,
            )
        }
        Some(delay)
    }

    // We just successfully sent some transactions. Remember them so we can see if they really show up on chain.
    pub(crate) fn on_txs_sent(
        &mut self,
        txs: &[SignedTransaction],
        source_height: BlockHeight,
        target_height: BlockHeight,
    ) {
        tracing::info!(
            target: "mirror", "Sent {} transactions from source #{} with target HEAD @ #{}",
            txs.len(), source_height, target_height
        );
        for tx in txs.iter() {
            self.on_tx_sent(tx, source_height, target_height);
        }

        let block_delay = self
            .second_longest_recent_block_delay()
            .unwrap_or(self.min_block_production_delay + Duration::from_millis(100));
        match &mut self.send_time {
            Some(t) => t.as_mut().reset(tokio::time::Instant::now() + block_delay),
            None => {
                self.send_time = Some(Box::pin(tokio::time::sleep(block_delay)));
            }
        }
    }
}

use crate::MappedBlock;
use near_crypto::PublicKey;
use near_indexer::StreamerMessage;
use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives_core::types::Nonce;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
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

// Keeps the queue of upcoming transactions and provides them in regular intervals via next_batch()
// Also keeps track of txs we've sent so far and looks for them on chain, for metrics/logging purposes.
#[derive(Default)]
pub(crate) struct TxTracker {
    sent_txs: HashMap<CryptoHash, TxSendInfo>,
    txs_by_signer: HashMap<(AccountId, PublicKey), BTreeSet<TxId>>,
    queued_blocks: VecDeque<MappedBlock>,
    num_txs_awaiting_nonce: usize,
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

    pub(crate) fn num_txs_awaiting_nonce(&self) -> usize {
        self.num_txs_awaiting_nonce
    }

    pub(crate) fn num_blocks_queued(&self) -> usize {
        self.queued_blocks.len()
    }

    pub(crate) fn queued_blocks_iter<'a>(
        &'a self,
    ) -> std::collections::vec_deque::Iter<'a, MappedBlock> {
        self.queued_blocks.iter()
    }

    // We now know of a valid nonce for the transaction with hash tx_hash that is
    // in the txs_awaiting_nonce set for the given block/chunk. Set the nonce and add
    // the tx to the set to be sent later.
    // panics if any of block_idx, shard_id, tx_hash is invalid
    pub(crate) fn set_tx_nonce(
        &mut self,
        block_idx: usize,
        shard_id: usize,
        tx_hash: &CryptoHash,
        nonce: Nonce,
    ) {
        let chunk = &mut self.queued_blocks[block_idx].chunks[shard_id];
        let mut t = chunk.txs_awaiting_nonce.remove(tx_hash).unwrap();
        self.num_txs_awaiting_nonce -= 1;
        t.tx.nonce = nonce;
        let tx = SignedTransaction::new(
            t.target_private.sign(&t.tx.get_hash_and_size().0.as_ref()),
            t.tx,
        );
        tracing::debug!(
            target: "mirror", "prepared a transaction for ({:?}, {:?}) that was previously waiting for the access key to appear on chain",
            &tx.transaction.signer_id, &tx.transaction.public_key
        );
        chunk.txs.push(tx);
    }

    pub(crate) fn queue_block(&mut self, block: MappedBlock) {
        self.height_queued = Some(block.source_height);
        for c in block.chunks.iter() {
            self.num_txs_awaiting_nonce += c.txs_awaiting_nonce.len();
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
            for c in block.chunks.iter() {
                self.num_txs_awaiting_nonce -= c.txs_awaiting_nonce.len();
            }
        }
        block
    }

    fn remove_tx(&mut self, tx: &IndexerTransactionWithOutcome) {
        let k = (tx.transaction.signer_id.clone(), tx.transaction.public_key.clone());
        match self.txs_by_signer.entry(k.clone()) {
            Entry::Occupied(mut e) => {
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
            Entry::Vacant(_) => {
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

        let block_delay =
            self.second_longest_recent_block_delay().unwrap_or(self.min_block_production_delay);
        match &mut self.send_time {
            Some(t) => t.as_mut().reset(tokio::time::Instant::now() + block_delay),
            None => {
                self.send_time = Some(Box::pin(tokio::time::sleep(block_delay)));
            }
        }
    }
}

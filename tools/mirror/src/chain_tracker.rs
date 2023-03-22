use crate::{
    ChainAccess, ChainError, LatestTargetNonce, MappedBlock, MappedTx, MappedTxProvenance,
    NonceUpdater, TargetChainTx, TargetNonce, TxRef,
};
use actix::Addr;
use anyhow::Context;
use near_client::ViewClientActor;
use near_crypto::{PublicKey, SecretKey};
use near_indexer::StreamerMessage;
use near_indexer_primitives::{IndexerExecutionOutcomeWithReceipt, IndexerTransactionWithOutcome};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::Transaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::{ActionView, ExecutionStatusView, ReceiptEnumView};
use near_primitives_core::types::{Gas, Nonce};
use rocksdb::DB;
use std::cmp::Ordering;
use std::collections::hash_map;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fmt::Write;
use std::pin::Pin;
use std::time::{Duration, Instant};

// Information related to a single transaction that we sent in the past.
// We could just forget it and not save any of this, but keeping this info
// makes it easy to print out human-friendly info later on when we find this
// transaction on chain.
struct TxSendInfo {
    sent_at: Instant,
    source_height: Option<BlockHeight>,
    provenance: MappedTxProvenance,
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    target_signer_id: Option<AccountId>,
    target_receiver_id: Option<AccountId>,
    actions: Vec<String>,
    sent_at_target_height: BlockHeight,
}

impl TxSendInfo {
    fn new(
        tx: &MappedTx,
        source_height: Option<BlockHeight>,
        target_height: BlockHeight,
        now: Instant,
    ) -> Self {
        let target_signer_id = if &tx.source_signer_id != &tx.target_tx.transaction.signer_id {
            Some(tx.target_tx.transaction.signer_id.clone())
        } else {
            None
        };
        let target_receiver_id = if &tx.source_receiver_id != &tx.target_tx.transaction.receiver_id
        {
            Some(tx.target_tx.transaction.receiver_id.clone())
        } else {
            None
        };
        Self {
            source_height,
            provenance: tx.provenance,
            source_signer_id: tx.source_signer_id.clone(),
            source_receiver_id: tx.source_receiver_id.clone(),
            target_signer_id,
            target_receiver_id,
            sent_at: now,
            sent_at_target_height: target_height,
            actions: tx
                .target_tx
                .transaction
                .actions
                .iter()
                .map(|a| a.as_ref().to_string())
                .collect::<Vec<_>>(),
        }
    }
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

fn gas_pretty(gas: Gas) -> String {
    if gas < 1000 {
        format!("{} gas", gas)
    } else if gas < 1_000_000 {
        format!("{} Kgas", gas / 1000)
    } else if gas < 1_000_000_000 {
        format!("{} Mgas", gas / 1_000_000)
    } else if gas < 1_000_000_000_000 {
        format!("{} Ggas", gas / 1_000_000_000)
    } else {
        format!("{} Tgas", gas / 1_000_000_000_000)
    }
}

#[derive(Clone, Debug)]
struct NonceInfo {
    target_nonce: TargetNonce,
    // the last height we have queued that references this access key. After
    // we send the txs at that height, we'll delete this from memory so that
    // the amount of memory we're using for these doesn't keep growing as we run for a while
    last_height: Option<BlockHeight>,
    txs_awaiting_nonce: BTreeSet<TxRef>,
    queued_txs: BTreeSet<TxRef>,
}

pub(crate) enum SentBatch {
    MappedBlock(MappedBlock),
    ExtraTxs(Vec<TargetChainTx>),
}

// Keeps the queue of upcoming transactions and provides them in regular intervals via next_batch()
// Also keeps track of txs we've sent so far and looks for them on chain, for metrics/logging purposes.

// TODO: the separation between what's in here and what's in the main file with struct TxMirror is not
// that clear and doesn't make that much sense. Should refactor
#[derive(Default)]
pub(crate) struct TxTracker {
    sent_txs: HashMap<CryptoHash, TxSendInfo>,
    txs_by_signer: HashMap<(AccountId, PublicKey), BTreeSet<TxId>>,
    queued_blocks: VecDeque<MappedBlock>,
    // for each updater (a tx or receipt hash, or a queued transaction we haven't sent yet), keeps
    // a set of access keys who might be updated by it
    updater_to_keys: HashMap<NonceUpdater, HashSet<(AccountId, PublicKey)>>,
    nonces: HashMap<(AccountId, PublicKey), NonceInfo>,
    next_heights: VecDeque<BlockHeight>,
    height_queued: Option<BlockHeight>,
    // the reason we have these (nonempty_height_queued, height_seen, etc) is so that we can
    // exit after we receive the target block containing the txs we sent for the last source block.
    // It's a minor thing, but otherwise if we just exit after sending the last source block's txs,
    // we won't get to see the resulting txs on chain in the debug logs from log_target_block()
    nonempty_height_queued: Option<BlockHeight>,
    height_popped: Option<BlockHeight>,
    height_seen: Option<BlockHeight>,
    send_time: Option<Pin<Box<tokio::time::Sleep>>>,
    // Config value in the target chain, used to judge how long to wait before sending a new batch of txs
    min_block_production_delay: Duration,
    // timestamps in the target chain, used to judge how long to wait before sending a new batch of txs
    recent_block_timestamps: VecDeque<u64>,
    // last source block we'll be sending transactions for
    stop_height: Option<BlockHeight>,
}

impl TxTracker {
    // `next_heights` should show the next several valid heights in the chain, starting from
    // the first block we want to send txs for. Right now we are assuming this arg is not empty when
    // we unwrap() self.height_queued() in Self::next_heights()
    pub(crate) fn new<'a, I>(
        min_block_production_delay: Duration,
        next_heights: I,
        stop_height: Option<BlockHeight>,
    ) -> Self
    where
        I: IntoIterator<Item = &'a BlockHeight>,
    {
        let next_heights = next_heights.into_iter().map(Clone::clone).collect();
        Self { min_block_production_delay, next_heights, stop_height, ..Default::default() }
    }

    pub(crate) async fn next_heights<T: ChainAccess>(
        &mut self,
        source_chain: &T,
    ) -> anyhow::Result<(Option<BlockHeight>, Option<BlockHeight>)> {
        while self.next_heights.len() <= crate::CREATE_ACCOUNT_DELTA {
            // we unwrap() the height_queued because Self::new() should have been called with
            // nonempty next_heights.
            let h = self
                .next_heights
                .iter()
                .next_back()
                .cloned()
                .unwrap_or_else(|| self.height_queued.unwrap());
            match source_chain.get_next_block_height(h).await {
                Ok(h) => self.next_heights.push_back(h),
                Err(ChainError::Unknown) => break,
                Err(ChainError::Other(e)) => {
                    return Err(e)
                        .with_context(|| format!("failed fetching next height after {}", h))
                }
            };
        }
        let next_height = self.next_heights.get(0).cloned();
        let create_account_height = self.next_heights.get(crate::CREATE_ACCOUNT_DELTA).cloned();
        Ok((next_height, create_account_height))
    }

    pub(crate) fn has_stop_height(&self) -> bool {
        self.stop_height.is_some()
    }

    pub(crate) fn finished(&self) -> bool {
        match self.stop_height {
            Some(_) => {
                self.height_popped >= self.stop_height
                    && self.height_seen >= self.nonempty_height_queued
            }
            None => false,
        }
    }

    pub(crate) fn num_blocks_queued(&self) -> usize {
        self.queued_blocks.len()
    }

    async fn initialize_target_nonce<'a>(
        &'a mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        access_key: &(AccountId, PublicKey),
        source_height: Option<BlockHeight>,
    ) -> anyhow::Result<()> {
        let info = match crate::read_target_nonce(db, &access_key.0, &access_key.1)? {
            Some(t) => NonceInfo {
                target_nonce: TargetNonce {
                    nonce: t.nonce,
                    pending_outcomes: t
                        .pending_outcomes
                        .into_iter()
                        .map(NonceUpdater::ChainObjectId)
                        .collect(),
                },
                last_height: source_height,
                txs_awaiting_nonce: BTreeSet::new(),
                queued_txs: BTreeSet::new(),
            },
            None => {
                let nonce =
                    crate::fetch_access_key_nonce(target_view_client, &access_key.0, &access_key.1)
                        .await?;
                let t = LatestTargetNonce { nonce, pending_outcomes: HashSet::new() };
                crate::put_target_nonce(db, &access_key.0, &access_key.1, &t)?;
                NonceInfo {
                    target_nonce: TargetNonce { nonce: t.nonce, pending_outcomes: HashSet::new() },
                    last_height: source_height,
                    txs_awaiting_nonce: BTreeSet::new(),
                    queued_txs: BTreeSet::new(),
                }
            }
        };
        self.nonces.insert(access_key.clone(), info);
        Ok(())
    }

    async fn read_target_nonce<'a>(
        &'a mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        access_key: &(AccountId, PublicKey),
        source_height: Option<BlockHeight>,
    ) -> anyhow::Result<&'a mut NonceInfo> {
        if !self.nonces.contains_key(access_key) {
            self.initialize_target_nonce(target_view_client, db, access_key, source_height).await?;
        }
        Ok(self.nonces.get_mut(access_key).unwrap())
    }

    pub(crate) async fn next_nonce<'a>(
        &'a mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        signer_id: &AccountId,
        public_key: &PublicKey,
        source_height: BlockHeight,
    ) -> anyhow::Result<&'a TargetNonce> {
        let source_height = Some(source_height);
        let info = self
            .read_target_nonce(
                target_view_client,
                db,
                &(signer_id.clone(), public_key.clone()),
                source_height,
            )
            .await?;
        if source_height > info.last_height {
            info.last_height = source_height;
        }
        if let Some(nonce) = &mut info.target_nonce.nonce {
            *nonce += 1;
        }
        Ok(&info.target_nonce)
    }

    // normally when we're adding txs, we're adding a tx that
    // wants to be sent after all the previous ones for that signer that
    // we've already prepared. So next_nonce() returns the biggest nonce
    // we've used so far + 1. But if we want to add a tx at the beginning,
    // we need to shift all the bigger nonces by one.
    pub(crate) async fn insert_nonce(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        signer_id: &AccountId,
        public_key: &PublicKey,
        secret_key: &SecretKey,
    ) -> anyhow::Result<TargetNonce> {
        let access_key = (signer_id.clone(), public_key.clone());
        if !self.nonces.contains_key(&access_key) {
            self.initialize_target_nonce(target_view_client, db, &access_key, None).await?;
            let info = self.nonces.get_mut(&access_key).unwrap();
            if let Some(nonce) = &mut info.target_nonce.nonce {
                *nonce += 1;
            }
            return Ok(info.target_nonce.clone());
        }
        let mut first_nonce = None;
        let txs = self.nonces.get(&access_key).unwrap().queued_txs.clone();
        for tx_ref in txs {
            let tx = self.get_tx(&tx_ref);
            if first_nonce.is_none() {
                first_nonce = Some(tx.target_nonce());
            }
            tx.inc_target_nonce(secret_key)
        }
        match first_nonce {
            Some(n) => {
                if let Some(nonce) =
                    &mut self.nonces.get_mut(&access_key).unwrap().target_nonce.nonce
                {
                    *nonce += 1;
                }
                Ok(n)
            }
            None => {
                tracing::warn!(target: "mirror", "info for access key {:?} was cached but there are no upcoming transactions queued for it", &access_key);
                Ok(TargetNonce::default())
            }
        }
    }

    fn get_tx(&mut self, tx_ref: &TxRef) -> &mut TargetChainTx {
        let block_idx = self
            .queued_blocks
            .binary_search_by(|b| b.source_height.cmp(&tx_ref.source_height))
            .unwrap();
        let block = &mut self.queued_blocks[block_idx];
        let chunk = block.chunks.iter_mut().find(|c| c.shard_id == tx_ref.shard_id).unwrap();
        &mut chunk.txs[tx_ref.tx_idx]
    }

    async fn insert_access_key_updates(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        tx_ref: &TxRef,
        nonce_updates: &HashSet<(AccountId, PublicKey)>,
        source_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let source_height = Some(source_height);
        for access_key in nonce_updates.iter() {
            let info =
                self.read_target_nonce(target_view_client, db, access_key, source_height).await?;

            if info.last_height < source_height {
                info.last_height = source_height;
            }
            info.target_nonce.pending_outcomes.insert(NonceUpdater::TxRef(tx_ref.clone()));
        }
        if !nonce_updates.is_empty() {
            assert!(self
                .updater_to_keys
                .insert(NonceUpdater::TxRef(tx_ref.clone()), nonce_updates.clone())
                .is_none());
        }
        Ok(())
    }

    pub(crate) async fn queue_block(
        &mut self,
        block: MappedBlock,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
    ) -> anyhow::Result<()> {
        self.height_queued = Some(block.source_height);
        self.next_heights.pop_front().unwrap();

        for c in block.chunks.iter() {
            if !c.txs.is_empty() {
                self.nonempty_height_queued = Some(block.source_height);
            }
            for (tx_idx, tx) in c.txs.iter().enumerate() {
                let tx_ref =
                    TxRef { source_height: block.source_height, shard_id: c.shard_id, tx_idx };
                match tx {
                    crate::TargetChainTx::Ready(tx) => {
                        let info = self
                            .nonces
                            .get_mut(&(
                                tx.target_tx.transaction.signer_id.clone(),
                                tx.target_tx.transaction.public_key.clone(),
                            ))
                            .unwrap();
                        info.queued_txs.insert(tx_ref.clone());
                        self.insert_access_key_updates(
                            target_view_client,
                            db,
                            &tx_ref,
                            &tx.nonce_updates,
                            block.source_height,
                        )
                        .await?;
                    }
                    crate::TargetChainTx::AwaitingNonce(tx) => {
                        let info = self
                            .nonces
                            .get_mut(&(
                                tx.target_tx.signer_id.clone(),
                                tx.target_tx.public_key.clone(),
                            ))
                            .unwrap();
                        info.txs_awaiting_nonce.insert(tx_ref.clone());
                        info.queued_txs.insert(tx_ref.clone());
                        self.insert_access_key_updates(
                            target_view_client,
                            db,
                            &tx_ref,
                            &tx.nonce_updates,
                            block.source_height,
                        )
                        .await?;
                    }
                };
            }
        }
        self.queued_blocks.push_back(block);
        Ok(())
    }

    pub(crate) fn next_batch_time(&self) -> Instant {
        match &self.send_time {
            Some(t) => t.as_ref().deadline().into_std(),
            None => Instant::now(),
        }
    }

    pub(crate) async fn next_batch(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
    ) -> anyhow::Result<MappedBlock> {
        // sleep until 20 milliseconds before we want to send transactions before we check for nonces
        // in the target chain. In the second or so between now and then, we might process another block
        // that will set the nonces.
        if let Some(s) = &self.send_time {
            tokio::time::sleep_until(s.as_ref().deadline() - Duration::from_millis(20)).await;
        }
        let mut needed_access_keys = HashSet::new();
        for c in self.queued_blocks[0].chunks.iter_mut() {
            for tx in c.txs.iter_mut() {
                if let TargetChainTx::AwaitingNonce(t) = tx {
                    needed_access_keys
                        .insert((t.target_tx.signer_id.clone(), t.target_tx.public_key.clone()));
                }
            }
        }
        for access_key in needed_access_keys.iter() {
            self.try_set_nonces(target_view_client, db, access_key, None).await?;
        }
        let block = &mut self.queued_blocks[0];
        self.height_popped = Some(block.source_height);
        for c in block.chunks.iter_mut() {
            for (tx_idx, tx) in c.txs.iter_mut().enumerate() {
                match tx {
                    TargetChainTx::AwaitingNonce(_) => {
                        let tx_ref = TxRef {
                            source_height: block.source_height,
                            shard_id: c.shard_id,
                            tx_idx,
                        };
                        tx.try_set_nonce(None);
                        match tx {
                            TargetChainTx::Ready(t) => {
                                tracing::debug!(
                                    target: "mirror", "Prepared {} for ({}, {:?}) with nonce {} even though there are still pending outcomes that may affect the access key",
                                    &t.provenance, &t.target_tx.transaction.signer_id, &t.target_tx.transaction.public_key, t.target_tx.transaction.nonce
                                );
                                self.nonces
                                    .get_mut(&(
                                        t.target_tx.transaction.signer_id.clone(),
                                        t.target_tx.transaction.public_key.clone(),
                                    ))
                                    .unwrap()
                                    .txs_awaiting_nonce
                                    .remove(&tx_ref);
                            }
                            TargetChainTx::AwaitingNonce(t) => {
                                tracing::warn!(
                                    target: "mirror", "Could not prepare {} for ({}, {:?}). Nonce unknown",
                                    &t.provenance, &t.target_tx.signer_id, &t.target_tx.public_key,
                                );
                                self.nonces
                                    .get_mut(&(
                                        t.target_tx.signer_id.clone(),
                                        t.target_tx.public_key.clone(),
                                    ))
                                    .unwrap()
                                    .txs_awaiting_nonce
                                    .remove(&tx_ref);
                            }
                        };
                    }
                    TargetChainTx::Ready(_) => {}
                };
            }
        }
        if let Some(sleep) = &mut self.send_time {
            sleep.await;
        }
        Ok(self.queued_blocks.pop_front().unwrap())
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
                    for t in txs.iter() {
                        if self.sent_txs.remove(&t.hash).is_none() {
                            tracing::warn!(
                                target: "mirror", "tx with hash {} that we thought was skipped is not in the set of sent txs",
                                &t.hash,
                            );
                        }
                    }
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

    fn log_target_block(&self, msg: &StreamerMessage) {
        // don't do any work here if we're definitely not gonna log it
        if tracing::level_filters::LevelFilter::current()
            > tracing::level_filters::LevelFilter::DEBUG
        {
            return;
        }

        // right now we're just logging this, but it would be nice to collect/index this
        // and have some HTTP debug page where you can see how close the target chain is
        // to the source chain
        let mut log_message = String::new();
        let now = Instant::now();

        for s in msg.shards.iter() {
            let mut other_txs = 0;
            if let Some(c) = &s.chunk {
                if c.header.height_included == msg.block.header.height {
                    write!(
                        log_message,
                        "-------- shard {} gas used: {} ---------\n",
                        s.shard_id,
                        gas_pretty(c.header.gas_used)
                    )
                    .unwrap();
                    for tx in c.transactions.iter() {
                        if let Some(info) = self.sent_txs.get(&tx.transaction.hash) {
                            write!(
                            log_message,
                            "{} signer: \"{}\"{} receiver: \"{}\"{} actions: <{}> sent {:?} ago @ target #{}\n",
                            info.provenance,
                            info.source_signer_id,
                            info.target_signer_id.as_ref().map_or(String::new(), |s| format!(" (mapped to \"{}\")", s)),
                            info.source_receiver_id,
                            info.target_receiver_id.as_ref().map_or(String::new(), |s| format!(" (mapped to \"{}\")", s)),
                            info.actions.join(", "),
                            now - info.sent_at,
                            info.sent_at_target_height,
                        ).unwrap();
                        } else {
                            other_txs += 1;
                        }
                    }
                } else {
                    write!(
                        log_message,
                        "-------- shard {} old chunk (#{}) ---------\n",
                        s.shard_id, c.header.height_included
                    )
                    .unwrap();
                }
            } else {
                write!(log_message, "-------- shard {} chunk missing ---------\n", s.shard_id)
                    .unwrap();
            }
            if other_txs > 0 {
                write!(log_message, "    ...    \n").unwrap();
                write!(
                    log_message,
                    "{} other txs (not ours, or sent before a restart)\n",
                    other_txs
                )
                .unwrap();
                write!(log_message, "    ...    \n").unwrap();
            }
        }
        tracing::debug!(target: "mirror", "received target block #{}:\n{}", msg.block.header.height, log_message);
    }

    fn tx_to_receipt(
        &mut self,
        db: &DB,
        tx_hash: &CryptoHash,
        receipt_id: &CryptoHash,
        access_keys: HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        crate::delete_pending_outcome(db, tx_hash)?;

        let updater = NonceUpdater::ChainObjectId(*tx_hash);
        if let Some(keys) = self.updater_to_keys.remove(&updater) {
            assert!(access_keys == keys);
        }

        let new_updater = NonceUpdater::ChainObjectId(*receipt_id);

        for access_key in access_keys.iter() {
            let mut n = crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
            assert!(n.pending_outcomes.remove(tx_hash));
            n.pending_outcomes.insert(*receipt_id);
            crate::put_target_nonce(db, &access_key.0, &access_key.1, &n)?;

            if let Some(info) = self.nonces.get(access_key) {
                let txs_awaiting_nonce = info.txs_awaiting_nonce.clone();

                for r in txs_awaiting_nonce.iter() {
                    let tx = self.get_tx(r);

                    match tx {
                        TargetChainTx::AwaitingNonce(t) => {
                            assert!(t.target_nonce.pending_outcomes.remove(&updater));
                            t.target_nonce.pending_outcomes.insert(new_updater.clone());
                        }
                        TargetChainTx::Ready(_) => unreachable!(),
                    };
                }

                let info = self.nonces.get_mut(access_key).unwrap();
                assert!(info.target_nonce.pending_outcomes.remove(&updater));
                info.target_nonce.pending_outcomes.insert(new_updater.clone());
            }
        }
        crate::put_pending_outcome(db, *receipt_id, access_keys.clone())?;
        if !access_keys.is_empty() {
            self.updater_to_keys.insert(new_updater, access_keys);
        }
        Ok(())
    }

    async fn try_set_nonces(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        access_key: &(AccountId, PublicKey),
        id: Option<&CryptoHash>,
    ) -> anyhow::Result<()> {
        let mut n = crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
        if let Some(id) = id {
            n.pending_outcomes.remove(id);
        }
        let mut nonce =
            crate::fetch_access_key_nonce(target_view_client, &access_key.0, &access_key.1).await?;
        n.nonce = std::cmp::max(n.nonce, nonce);

        crate::put_target_nonce(db, &access_key.0, &access_key.1, &n)?;

        let updater = id.map(|id| NonceUpdater::ChainObjectId(*id));
        if let Some(info) = self.nonces.get_mut(access_key) {
            if let Some(updater) = &updater {
                info.target_nonce.pending_outcomes.remove(updater);
            }
            let txs_awaiting_nonce = info.txs_awaiting_nonce.clone();
            let mut to_remove = Vec::new();

            for r in txs_awaiting_nonce.iter() {
                let tx = self.get_tx(r);

                match tx {
                    TargetChainTx::AwaitingNonce(t) => {
                        if let Some(updater) = &updater {
                            t.target_nonce.pending_outcomes.remove(updater);
                        }
                        if let Some(nonce) = &mut nonce {
                            *nonce += 1;
                        }

                        if t.target_nonce.pending_outcomes.is_empty() {
                            to_remove.push(r.clone());
                            tx.try_set_nonce(nonce);
                            match tx {
                                TargetChainTx::Ready(t) => {
                                    tracing::debug!(target: "mirror", "set nonce for {:?}'s {} to {}", access_key, r, t.target_tx.transaction.nonce);
                                }
                                _ => {
                                    tracing::warn!(target: "mirror", "Couldn't set nonce for {:?}'s {}", access_key, r);
                                }
                            }
                        } else {
                            t.target_nonce.nonce = std::cmp::max(t.target_nonce.nonce, nonce);
                        }
                    }
                    TargetChainTx::Ready(_) => unreachable!(),
                };
            }

            let info = self.nonces.get_mut(access_key).unwrap();
            for r in to_remove.iter() {
                info.txs_awaiting_nonce.remove(r);
            }
            info.target_nonce.nonce = std::cmp::max(info.target_nonce.nonce, nonce);
        }
        Ok(())
    }

    async fn on_outcome_finished(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        id: &CryptoHash,
        access_keys: HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        let updater = NonceUpdater::ChainObjectId(*id);
        if let Some(keys) = self.updater_to_keys.remove(&updater) {
            assert!(access_keys == keys);
        }

        for access_key in access_keys.iter() {
            self.try_set_nonces(target_view_client, db, &access_key, Some(id)).await?;
        }
        crate::delete_pending_outcome(db, id)
    }

    async fn on_target_block_tx(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        tx: IndexerTransactionWithOutcome,
    ) -> anyhow::Result<()> {
        if let Some(info) = self.sent_txs.remove(&tx.transaction.hash) {
            crate::metrics::TRANSACTIONS_INCLUDED.inc();
            self.remove_tx(&tx);
            if info.source_height > self.height_seen {
                self.height_seen = info.source_height;
            }
        }
        if let Some(access_keys) = crate::read_pending_outcome(db, &tx.transaction.hash)? {
            match tx.outcome.execution_outcome.outcome.status {
                ExecutionStatusView::SuccessReceiptId(receipt_id) => {
                    self.tx_to_receipt(db, &tx.transaction.hash, &receipt_id, access_keys)?
                }
                ExecutionStatusView::SuccessValue(_) | ExecutionStatusView::Unknown => {
                    unreachable!()
                }
                ExecutionStatusView::Failure(_) => {
                    self.on_outcome_finished(
                        target_view_client,
                        db,
                        &tx.transaction.hash,
                        access_keys,
                    )
                    .await?
                }
            }
        }
        Ok(())
    }

    async fn on_target_block_applied_receipt(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        outcome: IndexerExecutionOutcomeWithReceipt,
        staked_accounts: &mut HashMap<(AccountId, PublicKey), AccountId>,
    ) -> anyhow::Result<()> {
        let access_keys = match crate::read_pending_outcome(db, &outcome.execution_outcome.id)? {
            Some(a) => a,
            None => return Ok(()),
        };

        self.on_outcome_finished(
            target_view_client,
            db,
            &outcome.execution_outcome.id,
            access_keys,
        )
        .await?;
        for receipt_id in outcome.execution_outcome.outcome.receipt_ids {
            // we don't carry over the access keys here, because we set pending access keys when we send a tx with
            // an add key action, which should be applied after one receipt. Setting empty access keys here allows us
            // to keep track of which receipts are descendants of our transactions, so that we can reverse any stake actions
            crate::put_pending_outcome(db, receipt_id, HashSet::default())?;
        }
        if !crate::execution_status_good(&outcome.execution_outcome.outcome.status) {
            return Ok(());
        }
        match outcome.receipt.receipt {
            ReceiptEnumView::Action { actions, .. } => {
                // since this receipt was recorded in the DB, it means that this stake action
                // resulted from one of our txs (not an external/manual stake action by the test operator),
                // so we want to reverse it.
                for a in actions {
                    if let ActionView::Stake { public_key, stake } = a {
                        if stake > 0 {
                            staked_accounts.insert(
                                (outcome.receipt.receiver_id.clone(), public_key),
                                outcome.receipt.predecessor_id.clone(),
                            );
                        }
                    }
                }
            }
            _ => {}
        };
        Ok(())
    }

    // return value maps (receiver_id, staked public key) to the predecessor_id in the
    // receipt for any receipts that contain stake actions (w/ nonzero stake) that were
    // generated by our transactions. Then the caller will send extra stake transactions
    // to reverse those.
    pub(crate) async fn on_target_block(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        msg: StreamerMessage,
    ) -> anyhow::Result<HashMap<(AccountId, PublicKey), AccountId>> {
        self.record_block_timestamp(&msg);
        self.log_target_block(&msg);

        let mut staked_accounts = HashMap::new();
        for s in msg.shards {
            if let Some(c) = s.chunk {
                for tx in c.transactions {
                    self.on_target_block_tx(target_view_client, db, tx).await?;
                }
                for outcome in s.receipt_execution_outcomes {
                    self.on_target_block_applied_receipt(
                        target_view_client,
                        db,
                        outcome,
                        &mut staked_accounts,
                    )
                    .await?;
                }
            }
        }
        Ok(staked_accounts)
    }

    async fn on_tx_sent(
        &mut self,
        db: &DB,
        tx_ref: Option<TxRef>,
        tx: MappedTx,
        target_height: BlockHeight,
        now: Instant,
        access_keys_to_remove: &mut HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        let hash = tx.target_tx.get_hash();
        if self.sent_txs.contains_key(&hash) {
            tracing::warn!(target: "mirror", "transaction sent twice: {}", &hash);
            return Ok(());
        }

        if tx.provenance.is_add_key()
            || tx.provenance.is_create_account()
            || tx.provenance.is_unstake()
        {
            tracing::debug!(
                target: "mirror", "Successfully sent transaction {} for {}: {:?}",
                &hash, &tx.provenance, &tx.target_tx.transaction.actions,
            );
        }
        let access_key = (
            tx.target_tx.transaction.signer_id.clone(),
            tx.target_tx.transaction.public_key.clone(),
        );
        let source_height = tx_ref.as_ref().map(|t| t.source_height);
        // TODO: don't keep adding txs if we're not ever finding them on chain, since we'll OOM eventually
        // if that happens.
        self.sent_txs.insert(hash, TxSendInfo::new(&tx, source_height, target_height, now));
        let txs = self.txs_by_signer.entry(access_key.clone()).or_default();

        if let Some(highest_nonce) = txs.iter().next_back() {
            if highest_nonce.nonce > tx.target_tx.transaction.nonce {
                tracing::warn!(
                    target: "mirror", "transaction sent with out of order nonce: {}: {}. Sent so far: {:?}",
                    &hash, tx.target_tx.transaction.nonce, txs
                );
            }
        }
        if !txs.insert(TxId { hash, nonce: tx.target_tx.transaction.nonce }) {
            tracing::warn!(target: "mirror", "inserted tx {} twice into txs_by_signer", &hash);
        }

        match &tx_ref {
            Some(tx_ref) => {
                let updater = NonceUpdater::TxRef(tx_ref.clone());
                let new_updater = NonceUpdater::ChainObjectId(hash);
                assert!(
                    &tx.nonce_updates == &self.updater_to_keys.remove(&updater).unwrap_or_default()
                );
                for access_key in tx.nonce_updates.iter() {
                    let mut t =
                        crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
                    t.pending_outcomes.insert(hash);
                    crate::put_target_nonce(db, &access_key.0, &access_key.1, &t)?;

                    let info = self.nonces.get_mut(access_key).unwrap();
                    assert!(info.target_nonce.pending_outcomes.remove(&updater));
                    info.target_nonce.pending_outcomes.insert(new_updater.clone());
                    let txs_awaiting_nonce = info.txs_awaiting_nonce.clone();
                    if info.last_height <= source_height {
                        access_keys_to_remove.insert(access_key.clone());
                    }

                    for r in txs_awaiting_nonce.iter() {
                        let t = self.get_tx(r);

                        match t {
                            TargetChainTx::AwaitingNonce(t) => {
                                assert!(t.target_nonce.pending_outcomes.remove(&updater));
                                t.target_nonce.pending_outcomes.insert(new_updater.clone());
                            }
                            TargetChainTx::Ready(_) => unreachable!(),
                        };
                    }
                }
            }
            None => {
                // if tx_ref is None, it was an extra tx we sent to unstake an unwanted validator,
                // and there should be no access key updates
                assert!(tx.nonce_updates.is_empty());
            }
        }

        crate::put_pending_outcome(db, hash, tx.nonce_updates)?;

        let mut t = crate::read_target_nonce(
            db,
            &tx.target_tx.transaction.signer_id,
            &tx.target_tx.transaction.public_key,
        )?
        .unwrap();
        t.nonce = std::cmp::max(t.nonce, Some(tx.target_tx.transaction.nonce));
        crate::put_target_nonce(
            db,
            &tx.target_tx.transaction.signer_id,
            &tx.target_tx.transaction.public_key,
            &t,
        )?;
        let info = self.nonces.get_mut(&access_key).unwrap();
        if info.last_height <= source_height {
            access_keys_to_remove.insert(access_key);
        }
        if let Some(tx_ref) = tx_ref {
            assert!(info.queued_txs.remove(&tx_ref));
        }
        Ok(())
    }

    // among the last 10 blocks, what's the second longest time between their timestamps?
    // probably there's a better heuristic to use than that but this will do for now.
    // TODO: it's possible these tiimestamps are just increasing by one nanosecond each time
    // if block producers' clocks are off. should handle that case
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

    fn on_tx_skipped(
        &mut self,
        tx_ref: &Option<TxRef>,
        tx: &Transaction,
        nonce_updates: &HashSet<(AccountId, PublicKey)>,
        access_keys_to_remove: &mut HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        let tx_ref = match tx_ref {
            Some(t) => t,
            None => return Ok(()),
        };
        let updater = NonceUpdater::TxRef(tx_ref.clone());
        if let Some(keys) = self.updater_to_keys.remove(&updater) {
            assert!(&keys == nonce_updates);

            for access_key in keys {
                if let Some(info) = self.nonces.get(&access_key) {
                    let txs_awaiting_nonce = info.txs_awaiting_nonce.clone();
                    let mut to_remove = Vec::new();
                    for r in txs_awaiting_nonce.iter() {
                        let target_tx = self.get_tx(r);
                        match target_tx {
                            TargetChainTx::AwaitingNonce(tx) => {
                                assert!(tx.target_nonce.pending_outcomes.remove(&updater));
                                if tx.target_nonce.pending_outcomes.is_empty() {
                                    target_tx.try_set_nonce(None);
                                    match target_tx {
                                        TargetChainTx::Ready(t) => {
                                            tracing::debug!(target: "mirror", "After skipping {} setting nonce for {:?}'s {} to {}", tx_ref, &access_key, r, t.target_tx.transaction.nonce);
                                        }
                                        _ => {
                                            tracing::warn!(target: "mirror", "After skipping {} could not set nonce for {:?}'s {}", tx_ref, &access_key, r);
                                        }
                                    }
                                    to_remove.push(r.clone());
                                }
                            }
                            TargetChainTx::Ready(_) => unreachable!(),
                        }
                    }

                    let info = self.nonces.get_mut(&access_key).unwrap();
                    for r in to_remove.iter() {
                        info.txs_awaiting_nonce.remove(r);
                    }

                    if info.last_height <= Some(tx_ref.source_height) {
                        access_keys_to_remove.insert(access_key);
                    }
                }
            }
        }
        let access_key = (tx.signer_id.clone(), tx.public_key.clone());
        let info = self.nonces.get_mut(&access_key).unwrap();
        if info.last_height <= Some(tx_ref.source_height) {
            access_keys_to_remove.insert(access_key);
        }
        assert!(info.queued_txs.remove(tx_ref));
        Ok(())
    }

    // We just successfully sent some transactions. Remember them so we can see if they really show up on chain.
    pub(crate) async fn on_txs_sent(
        &mut self,
        db: &DB,
        sent_batch: SentBatch,
        target_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let mut total_sent = 0;
        let now = Instant::now();
        let mut access_keys_to_remove = HashSet::new();

        let (txs_sent, provenance) = match sent_batch {
            SentBatch::MappedBlock(b) => {
                let block_delay = self
                    .second_longest_recent_block_delay()
                    .unwrap_or(self.min_block_production_delay + Duration::from_millis(100));
                match &mut self.send_time {
                    Some(t) => t.as_mut().reset(tokio::time::Instant::now() + block_delay),
                    None => {
                        self.send_time = Some(Box::pin(tokio::time::sleep(block_delay)));
                    }
                }
                crate::set_last_source_height(db, b.source_height)?;
                let txs = b
                    .chunks
                    .into_iter()
                    .flat_map(|c| {
                        c.txs.into_iter().enumerate().map(move |(tx_idx, tx)| {
                            (
                                Some(TxRef {
                                    source_height: b.source_height,
                                    shard_id: c.shard_id,
                                    tx_idx,
                                }),
                                tx,
                            )
                        })
                    })
                    .collect::<Vec<_>>();
                (txs, format!("source #{}", b.source_height))
            }
            SentBatch::ExtraTxs(txs) => (
                txs.into_iter().map(|tx| (None, tx)).collect::<Vec<_>>(),
                String::from("extra unstake transactions"),
            ),
        };
        for (tx_ref, tx) in txs_sent {
            match tx {
                crate::TargetChainTx::Ready(t) => {
                    if t.sent_successfully {
                        self.on_tx_sent(
                            db,
                            tx_ref,
                            t,
                            target_height,
                            now,
                            &mut access_keys_to_remove,
                        )
                        .await?;
                        total_sent += 1;
                    } else {
                        self.on_tx_skipped(
                            &tx_ref,
                            &t.target_tx.transaction,
                            &t.nonce_updates,
                            &mut access_keys_to_remove,
                        )?;
                    }
                }
                crate::TargetChainTx::AwaitingNonce(t) => {
                    self.on_tx_skipped(
                        &tx_ref,
                        &t.target_tx,
                        &t.nonce_updates,
                        &mut access_keys_to_remove,
                    )?;
                }
            }
        }

        for access_key in access_keys_to_remove {
            assert!(self.nonces.remove(&access_key).is_some());
        }
        tracing::info!(
            target: "mirror", "Sent {} transactions from {} with target HEAD @ #{}",
            total_sent, provenance, target_height
        );

        Ok(())
    }
}

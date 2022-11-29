use crate::{
    ChainObjectId, LatestTargetNonce, MappedBlock, MappedTx, MappedTxProvenance, NonceUpdater,
    ReceiptInfo, TargetChainTx, TargetNonce, TxInfo, TxOutcome, TxRef,
};
use actix::Addr;
use near_client::ViewClientActor;
use near_crypto::PublicKey;
use near_indexer::StreamerMessage;
use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::Transaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives_core::types::{Gas, Nonce, ShardId};
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
    source_height: BlockHeight,
    provenance: MappedTxProvenance,
    source_shard_id: ShardId,
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
        source_shard_id: ShardId,
        source_height: BlockHeight,
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
            source_shard_id: source_shard_id,
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
    last_height: BlockHeight,
    txs_awaiting_nonce: BTreeSet<TxRef>,
}

// Keeps the queue of upcoming transactions and provides them in regular intervals via next_batch()
// Also keeps track of txs we've sent so far and looks for them on chain, for metrics/logging purposes.
#[derive(Default)]
pub(crate) struct TxTracker {
    sent_txs: HashMap<CryptoHash, TxSendInfo>,
    txs_by_signer: HashMap<(AccountId, PublicKey), BTreeSet<TxId>>,
    queued_blocks: VecDeque<MappedBlock>,
    // for each updater (a tx or receipt hash, or a queued transaction we haven't sent yet), keeps
    // a set of access keys who might be updated by it
    updater_to_keys: HashMap<NonceUpdater, HashSet<(AccountId, PublicKey)>>,
    nonces: HashMap<(AccountId, PublicKey), NonceInfo>,
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
    pub(crate) fn new(
        min_block_production_delay: Duration,
        stop_height: Option<BlockHeight>,
    ) -> Self {
        Self { min_block_production_delay, stop_height, ..Default::default() }
    }

    pub(crate) fn height_queued(&self) -> Option<BlockHeight> {
        self.height_queued
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

    async fn read_target_nonce<'a>(
        &'a mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        access_key: &(AccountId, PublicKey),
        source_height: BlockHeight,
    ) -> anyhow::Result<&'a mut NonceInfo> {
        if !self.nonces.contains_key(access_key) {
            let nonce =
                crate::fetch_access_key_nonce(target_view_client, &access_key.0, &access_key.1)
                    .await?;

            let info = match crate::read_target_nonce(db, &access_key.0, &access_key.1)? {
                Some(mut t) => {
                    // lazily update it here. things might have changed since we last restarted
                    for id in t.pending_outcomes.iter() {
                        let access_keys = crate::read_access_key_outcome(db, id)?.unwrap();
                        match crate::fetch_outcome(target_view_client, id).await? {
                            TxOutcome::TxPending { .. } => {}
                            TxOutcome::ReceiptPending(receipt_id) => {
                                if matches!(id, ChainObjectId::Tx(_)) {
                                    self.tx_to_receipt(db, id, &receipt_id, access_keys)?;
                                }
                            }
                            TxOutcome::Failure | TxOutcome::Success => {
                                self.on_outcome_finished(target_view_client, db, id, access_keys)
                                    .await?
                            }
                            TxOutcome::Unknown => {
                                tracing::warn!(target: "mirror", "can't find {:?} on chain even though it's stored on disk", id)
                            }
                        };
                    }
                    // TODO: this ugliness is because we are not keeping track of what we have
                    // modified on disk and what has been modified in memory. need to fix w/ a struct StoreUpdate
                    t = crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
                    t.nonce = std::cmp::max(t.nonce, nonce);
                    crate::put_target_nonce(db, &access_key.0, &access_key.1, &t)?;

                    NonceInfo {
                        target_nonce: TargetNonce {
                            nonce: t.nonce.clone(),
                            pending_outcomes: t
                                .pending_outcomes
                                .into_iter()
                                .map(NonceUpdater::ChainObjectId)
                                .collect(),
                        },
                        last_height: source_height,
                        txs_awaiting_nonce: BTreeSet::new(),
                    }
                }
                None => {
                    let t = LatestTargetNonce { nonce, pending_outcomes: HashSet::new() };
                    crate::put_target_nonce(db, &access_key.0, &access_key.1, &t)?;
                    NonceInfo {
                        target_nonce: TargetNonce {
                            nonce: t.nonce,
                            pending_outcomes: HashSet::new(),
                        },
                        last_height: source_height,
                        txs_awaiting_nonce: BTreeSet::new(),
                    }
                }
            };
            self.nonces.insert(access_key.clone(), info);
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
        for c in block.chunks.iter() {
            if !c.txs.is_empty() {
                self.nonempty_height_queued = Some(block.source_height);
            }
            for (tx_idx, tx) in c.txs.iter().enumerate() {
                let tx_ref =
                    TxRef { source_height: block.source_height, shard_id: c.shard_id, tx_idx };
                match tx {
                    crate::TargetChainTx::Ready(tx) => {
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
    ) -> anyhow::Result<&mut MappedBlock> {
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
        Ok(block)
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
                            "source #{}{} {} signer: \"{}\"{} receiver: \"{}\"{} actions: <{}> sent {:?} ago @ target #{}\n",
                            info.source_height,
                            if s.shard_id == info.source_shard_id {
                                String::new()
                            } else {
                                format!(" (source shard {})", info.source_shard_id)
                            },
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
        id: &ChainObjectId,
        receipt_id: &ReceiptInfo,
        access_keys: HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        crate::delete_access_key_outcome(db, id)?;

        let updater = NonceUpdater::ChainObjectId(id.clone());
        if let Some(keys) = self.updater_to_keys.remove(&updater) {
            assert!(access_keys == keys);
        }

        let receipt_id = ChainObjectId::Receipt(receipt_id.clone());
        let new_updater = NonceUpdater::ChainObjectId(receipt_id.clone());

        for access_key in access_keys.iter() {
            let mut n = crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
            assert!(n.pending_outcomes.remove(id));
            n.pending_outcomes.insert(receipt_id.clone());
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
        crate::put_access_key_outcome(db, receipt_id.clone(), access_keys.clone())?;
        self.updater_to_keys.insert(new_updater, access_keys);
        Ok(())
    }

    async fn try_set_nonces(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        access_key: &(AccountId, PublicKey),
        id: Option<&ChainObjectId>,
    ) -> anyhow::Result<()> {
        let mut n = crate::read_target_nonce(db, &access_key.0, &access_key.1)?.unwrap();
        if let Some(id) = id {
            n.pending_outcomes.remove(id);
        }
        let mut nonce =
            crate::fetch_access_key_nonce(target_view_client, &access_key.0, &access_key.1).await?;
        n.nonce = std::cmp::max(n.nonce, nonce);

        crate::put_target_nonce(db, &access_key.0, &access_key.1, &n)?;

        let updater = id.map(|id| NonceUpdater::ChainObjectId(id.clone()));
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
        id: &ChainObjectId,
        access_keys: HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
        let updater = NonceUpdater::ChainObjectId(id.clone());
        if let Some(keys) = self.updater_to_keys.remove(&updater) {
            assert!(access_keys == keys);
        }

        for access_key in access_keys.iter() {
            self.try_set_nonces(target_view_client, db, &access_key, Some(id)).await?;
        }
        crate::delete_access_key_outcome(db, id)
    }

    pub(crate) async fn on_target_block(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        msg: StreamerMessage,
    ) -> anyhow::Result<()> {
        self.record_block_timestamp(&msg);
        self.log_target_block(&msg);

        for s in msg.shards {
            if let Some(c) = s.chunk {
                for tx in c.transactions {
                    if let Some(info) = self.sent_txs.remove(&tx.transaction.hash) {
                        crate::metrics::TRANSACTIONS_INCLUDED.inc();
                        self.remove_tx(&tx);
                        if Some(info.source_height) > self.height_seen {
                            self.height_seen = Some(info.source_height);
                        }
                    }
                    let id = ChainObjectId::Tx(TxInfo {
                        hash: tx.transaction.hash,
                        signer_id: tx.transaction.signer_id,
                        receiver_id: tx.transaction.receiver_id,
                    });
                    if let Some(access_keys) = crate::read_access_key_outcome(db, &id)? {
                        match crate::fetch_outcome(target_view_client, &id).await? {
                            TxOutcome::TxPending{..} | TxOutcome::Unknown => anyhow::bail!("can't find outcome for target chain tx {} even though HEAD block includes it", &tx.transaction.hash),
                            TxOutcome::ReceiptPending(receipt_id) => self.tx_to_receipt(db, &id, &receipt_id, access_keys)?,
                            TxOutcome::Failure | TxOutcome::Success => self.on_outcome_finished(target_view_client, db, &id, access_keys).await?,
                        };
                    }
                }
                for r in c.receipts {
                    let id = ChainObjectId::Receipt(ReceiptInfo {
                        id: r.receipt_id,
                        receiver_id: r.receiver_id.clone(),
                    });
                    if let Some(access_keys) = crate::read_access_key_outcome(db, &id)? {
                        match crate::fetch_outcome(target_view_client, &id).await? {
                            TxOutcome::TxPending{..} => unreachable!(),
                            TxOutcome::ReceiptPending(_) | TxOutcome::Unknown => anyhow::bail!("can't find outcome for target chain receipt {} even though HEAD block includes it", &r.receipt_id),
                            TxOutcome::Failure | TxOutcome::Success => self.on_outcome_finished(target_view_client, db, &id, access_keys).await?,
                        };
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_tx_sent(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        tx_ref: TxRef,
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

        if matches!(
            &tx.provenance,
            MappedTxProvenance::ReceiptAddKey(_) | MappedTxProvenance::TxAddKey(_)
        ) {
            tracing::debug!(
                target: "mirror", "Successfully sent transaction {} for {} @ source #{} to add extra keys: {:?}",
                &hash, &tx.target_tx.transaction.receiver_id, tx_ref.source_height, &tx.target_tx.transaction.actions,
            );
        }
        let access_key = (
            tx.target_tx.transaction.signer_id.clone(),
            tx.target_tx.transaction.public_key.clone(),
        );
        // TODO: don't keep adding txs if we're not ever finding them on chain, since we'll OOM eventually
        // if that happens.
        self.sent_txs.insert(
            hash,
            TxSendInfo::new(&tx, tx_ref.shard_id, tx_ref.source_height, target_height, now),
        );
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

        let source_height = tx_ref.source_height;
        let updater = NonceUpdater::TxRef(tx_ref);

        let new_updater = NonceUpdater::ChainObjectId(ChainObjectId::Tx(TxInfo {
            hash: tx.target_tx.get_hash(),
            signer_id: tx.target_tx.transaction.signer_id.clone(),
            receiver_id: tx.target_tx.transaction.receiver_id.clone(),
        }));
        if !tx.nonce_updates.is_empty() {
            assert!(&tx.nonce_updates == &self.updater_to_keys.remove(&updater).unwrap());
            for access_key in tx.nonce_updates.iter() {
                let mut t = match crate::read_target_nonce(db, &access_key.0, &access_key.1)? {
                    Some(t) => t,
                    None => {
                        let nonce = crate::fetch_access_key_nonce(
                            target_view_client,
                            &access_key.0,
                            &access_key.1,
                        )
                        .await?;
                        LatestTargetNonce { nonce, pending_outcomes: HashSet::new() }
                    }
                };
                t.pending_outcomes.insert(ChainObjectId::Tx(TxInfo {
                    hash,
                    signer_id: tx.target_tx.transaction.signer_id.clone(),
                    receiver_id: tx.target_tx.transaction.receiver_id.clone(),
                }));
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

            crate::put_access_key_outcome(
                db,
                ChainObjectId::Tx(TxInfo {
                    hash,
                    signer_id: tx.target_tx.transaction.signer_id.clone(),
                    receiver_id: tx.target_tx.transaction.receiver_id.clone(),
                }),
                tx.nonce_updates,
            )?;
        }
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
        if self.nonces.get(&access_key).unwrap().last_height <= source_height {
            access_keys_to_remove.insert(access_key);
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
        tx_ref: &TxRef,
        tx: &Transaction,
        nonce_updates: &HashSet<(AccountId, PublicKey)>,
        source_height: BlockHeight,
        access_keys_to_remove: &mut HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<()> {
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

                    if info.last_height <= source_height {
                        access_keys_to_remove.insert(access_key);
                    }
                }
            }
        }
        let access_key = (tx.signer_id.clone(), tx.public_key.clone());
        if self.nonces.get(&access_key).unwrap().last_height <= source_height {
            access_keys_to_remove.insert(access_key);
        }
        Ok(())
    }

    fn pop_block(&mut self) -> MappedBlock {
        let block = self.queued_blocks.pop_front().unwrap();
        for chunk in block.chunks.iter() {
            for (tx_idx, tx) in chunk.txs.iter().enumerate() {
                let access_key = match tx {
                    crate::TargetChainTx::Ready(tx) => (
                        tx.target_tx.transaction.signer_id.clone(),
                        tx.target_tx.transaction.public_key.clone(),
                    ),
                    crate::TargetChainTx::AwaitingNonce(tx) => {
                        (tx.target_tx.signer_id.clone(), tx.target_tx.public_key.clone())
                    }
                };
                let tx_ref =
                    TxRef { source_height: block.source_height, shard_id: chunk.shard_id, tx_idx };
                self.nonces.get_mut(&access_key).unwrap().txs_awaiting_nonce.remove(&tx_ref);
            }
        }
        block
    }

    // We just successfully sent some transactions. Remember them so we can see if they really show up on chain.
    pub(crate) async fn on_txs_sent(
        &mut self,
        target_view_client: &Addr<ViewClientActor>,
        db: &DB,
        target_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let block = self.pop_block();
        let source_height = block.source_height;
        let mut total_sent = 0;
        let now = Instant::now();
        let mut access_keys_to_remove = HashSet::new();

        for chunk in block.chunks.into_iter() {
            for (tx_idx, tx) in chunk.txs.into_iter().enumerate() {
                let tx_ref =
                    TxRef { source_height: block.source_height, shard_id: chunk.shard_id, tx_idx };
                match tx {
                    crate::TargetChainTx::Ready(t) => {
                        if t.sent_successfully {
                            self.on_tx_sent(
                                target_view_client,
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
                                block.source_height,
                                &mut access_keys_to_remove,
                            )?;
                        }
                    }
                    crate::TargetChainTx::AwaitingNonce(t) => {
                        self.on_tx_skipped(
                            &tx_ref,
                            &t.target_tx,
                            &t.nonce_updates,
                            block.source_height,
                            &mut access_keys_to_remove,
                        )?;
                    }
                }
            }
        }
        crate::set_next_source_height(db, block.source_height + 1)?;

        for access_key in access_keys_to_remove {
            assert!(self.nonces.remove(&access_key).is_some());
        }
        tracing::info!(
            target: "mirror", "Sent {} transactions from source #{} with target HEAD @ #{}",
            total_sent, source_height, target_height
        );

        let block_delay = self
            .second_longest_recent_block_delay()
            .unwrap_or(self.min_block_production_delay + Duration::from_millis(100));
        match &mut self.send_time {
            Some(t) => t.as_mut().reset(tokio::time::Instant::now() + block_delay),
            None => {
                self.send_time = Some(Box::pin(tokio::time::sleep(block_delay)));
            }
        }
        Ok(())
    }
}

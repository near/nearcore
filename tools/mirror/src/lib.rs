use actix::Addr;
use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client_primitives::types::{GetBlockError, GetBlockHash, GetChunk, GetChunkError, Query};
use near_crypto::{PublicKey, SecretKey};
use near_indexer::{Indexer, StreamerMessage};
use near_network::types::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteKeyAction, SignedTransaction, Transaction,
};
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, Finality};
use near_primitives::views::{ActionView, QueryRequest, QueryResponseKind};
use near_primitives_core::types::{Nonce, ShardId};
use nearcore::config::NearConfig;
use rocksdb::DB;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use strum::IntoEnumIterator;
use tokio::sync::mpsc;

mod chain_tracker;
pub mod genesis;
mod key_mapping;
mod metrics;
mod secret;

#[derive(strum::EnumIter)]
enum DBCol {
    Misc,
    // This tracks nonces for Access Keys added by AddKey transactions
    // (not present in the genesis state).  For a given key, if
    // there's no entry in the DB, then either the key was present in
    // the genesis state or it was added by an AddKey transaction and
    // then removed by a DeleteKey transaction. Otherwise, we map tx
    // nonces according to the values in this column.
    Nonces,
}

impl DBCol {
    fn name(&self) -> &'static str {
        match self {
            Self::Misc => "miscellaneous",
            Self::Nonces => "nonces",
        }
    }
}

// For a given AddKey Action, records the starting nonces of the
// resulting Access Keys.  We need this because when an AddKey receipt
// is processed, the nonce field of the AddKey action is actually
// ignored, and it's set to block_height*1000000, so to generate
// transactions with valid nonces, we need to map valid source chain
// nonces to valid target chain nonces.
#[derive(BorshDeserialize, BorshSerialize, Debug, Default)]
struct NonceDiff {
    source_start: Option<Nonce>,
    target_start: Option<Nonce>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum MapNonceError {
    #[error("Source chain access key not yet on chain")]
    SourceKeyNotOnChain,
    #[error("Target chain access key not yet on chain")]
    TargetKeyNotOnChain,
    #[error("Nonce arithmetic overflow: {0} + {1}")]
    AddOverflow(Nonce, Nonce),
    #[error("Nonce arithmetic overflow: {0} - {1}")]
    SubOverflow(Nonce, Nonce),
}

impl NonceDiff {
    fn map(&self, nonce: Nonce) -> Result<Nonce, MapNonceError> {
        let source_start = self.source_start.ok_or(MapNonceError::SourceKeyNotOnChain)?;
        let target_start = self.target_start.ok_or(MapNonceError::TargetKeyNotOnChain)?;
        if target_start > source_start {
            let diff = target_start - source_start;
            nonce.checked_add(diff).ok_or_else(|| MapNonceError::AddOverflow(nonce, diff))
        } else {
            let diff = source_start - target_start;
            nonce.checked_sub(diff).ok_or_else(|| MapNonceError::SubOverflow(nonce, diff))
        }
    }

    fn known(&self) -> bool {
        self.source_start.is_some() && self.target_start.is_some()
    }
}

pub struct TxMirror {
    target_stream: mpsc::Receiver<StreamerMessage>,
    source_view_client: Addr<ViewClientActor>,
    source_client: Addr<ClientActor>,
    target_view_client: Addr<ViewClientActor>,
    target_client: Addr<ClientActor>,
    db: DB,
    target_genesis_height: BlockHeight,
    tracked_shards: Vec<ShardId>,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
    next_source_height: Option<BlockHeight>,
}

fn open_db<P: AsRef<Path>>(home: &P, config: &NearConfig) -> anyhow::Result<DB> {
    let db_path =
        near_store::Store::opener(home.as_ref(), &config.config.store).get_path().join("mirror");
    let mut options = rocksdb::Options::default();
    options.create_missing_column_families(true);
    options.create_if_missing(true);
    let cf_descriptors = DBCol::iter()
        .map(|col| rocksdb::ColumnFamilyDescriptor::new(col.name(), options.clone()))
        .collect::<Vec<_>>();
    Ok(DB::open_cf_descriptors(&options, db_path, cf_descriptors)?)
}

// a transaction that's almost prepared, except that we don't yet know
// what nonce to use because the public key was added in an AddKey
// action that we haven't seen on chain yet. The tx field is complete
// except for the nonce field.
#[derive(Debug)]
struct TxAwaitingNonce {
    source_public: PublicKey,
    target_private: SecretKey,
    tx: Transaction,
}

#[derive(Debug)]
struct MappedChunk {
    txs: Vec<SignedTransaction>,
    txs_awaiting_nonce: HashMap<CryptoHash, TxAwaitingNonce>,
    shard_id: ShardId,
}

#[derive(Debug)]
struct MappedBlock {
    source_height: BlockHeight,
    source_block_hash: CryptoHash,
    chunks: Vec<MappedChunk>,
}

async fn fetch_access_key_nonce(
    view_client: &Addr<ViewClientActor>,
    account_id: &AccountId,
    public_key: &PublicKey,
    block_hash: Option<&CryptoHash>,
) -> anyhow::Result<Option<Nonce>> {
    let block_ref = match block_hash {
        Some(h) => BlockReference::BlockId(BlockId::Hash(h.clone())),
        None => BlockReference::Finality(Finality::None),
    };
    match view_client
        .send(Query::new(
            block_ref,
            QueryRequest::ViewAccessKey {
                account_id: account_id.clone(),
                public_key: public_key.clone(),
            },
        ))
        .await?
    {
        Ok(res) => match res.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(Some(access_key.nonce)),
            other => {
                tracing::error!(target: "mirror", "Received unexpected QueryResponse after Querying Access Key: {:?}", other);
                Ok(None)
            }
        },
        Err(_) => Ok(None),
    }
}

impl TxMirror {
    pub fn new<P: AsRef<Path>>(
        source_home: &P,
        target_home: &P,
        secret_file: &P,
    ) -> anyhow::Result<Self> {
        let target_config =
            nearcore::config::load_config(target_home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| {
                    format!("Error loading target config from {:?}", target_home.as_ref())
                })?;
        let secret = crate::secret::load(secret_file)
            .with_context(|| format!("Failed to load secret from {:?}", secret_file.as_ref()))?;
        let db = open_db(target_home, &target_config).context("failed to open mirror DB")?;
        let source_config =
            nearcore::config::load_config(source_home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| {
                    format!("Error loading source config from {:?}", source_home.as_ref())
                })?;

        let source_node = nearcore::start_with_config(source_home.as_ref(), source_config.clone())
            .context("failed to start source chain NEAR node")?;

        let target_indexer = Indexer::new(near_indexer::IndexerConfig {
            home_dir: target_home.as_ref().to_path_buf(),
            sync_mode: near_indexer::SyncModeEnum::LatestSynced,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        })
        .context("failed to start target chain indexer")?;
        let (target_view_client, target_client) = target_indexer.client_actors();
        let target_stream = target_indexer.streamer();

        Ok(Self {
            source_view_client: source_node.view_client,
            source_client: source_node.client,
            target_client,
            target_view_client,
            target_stream,
            db,
            target_genesis_height: target_config.genesis.config.genesis_height,
            tracked_shards: target_config.config.tracked_shards.clone(),
            secret,
            next_source_height: None,
        })
    }

    fn get_next_source_height(&mut self) -> anyhow::Result<BlockHeight> {
        if let Some(height) = self.next_source_height {
            return Ok(height);
        }
        let height =
            self.db.get_cf(self.db.cf_handle(DBCol::Misc.name()).unwrap(), "next_source_height")?;
        match height {
            Some(h) => {
                let height = BlockHeight::try_from_slice(&h).unwrap();
                self.next_source_height = Some(height);
                Ok(height)
            }
            None => Ok(self.target_genesis_height),
        }
    }

    async fn send_transactions(
        &mut self,
        block: &MappedBlock,
    ) -> anyhow::Result<Vec<SignedTransaction>> {
        let mut sent = vec![];
        for chunk in block.chunks.iter() {
            for tx in chunk.txs.iter() {
                match self
                    .target_client
                    .send(NetworkClientMessages::Transaction {
                        transaction: tx.clone(),
                        is_forwarded: false,
                        check_only: false,
                    })
                    .await?
                {
                    NetworkClientResponses::RequestRouted => {
                        tracing::debug!(
                            target: "mirror", "sent source #{} shard {} tx {}",
                            block.source_height, chunk.shard_id, tx.get_hash()
                        );
                        crate::metrics::TRANSACTIONS_SENT.with_label_values(&["ok"]).inc();
                        sent.push(tx.clone());
                    }
                    NetworkClientResponses::InvalidTx(e) => {
                        // TODO: here if we're getting an error because the tx was already included, it is possible
                        // that some other instance of this code ran and made progress already. For now we can assume
                        // only once instance of this code will run, but this is the place to detect if that's not the case.
                        tracing::error!(
                            target: "mirror", "Tried to send an invalid tx from source #{} shard {}: {:?}",
                            block.source_height, chunk.shard_id, e
                        );
                        crate::metrics::TRANSACTIONS_SENT.with_label_values(&["invalid"]).inc();
                    }
                    r => {
                        tracing::error!(
                            target: "mirror", "Unexpected response sending tx from source #{} shard {}: {:?}. The transaction was not sent",
                            block.source_height, chunk.shard_id, r
                        );
                        crate::metrics::TRANSACTIONS_SENT
                            .with_label_values(&["internal_error"])
                            .inc();
                    }
                }
            }
        }
        Ok(sent)
    }

    // If the access key was present in the genesis records, just
    // return the same nonce. Otherwise, we need to change the
    // nonce. So check if we already know what the difference is
    // nonces is, and if not, try to fetch that info and store it.
    async fn map_nonce(
        &self,
        signer_id: &AccountId,
        source_public: &PublicKey,
        target_public: &PublicKey,
        nonce: Nonce,
        source_block_hash: &CryptoHash,
    ) -> anyhow::Result<Result<Nonce, MapNonceError>> {
        let db_key = target_public.try_to_vec().unwrap();
        let mut m =
            // TODO: cache this?
            match self.db.get_cf(self.db.cf_handle(DBCol::Nonces.name()).unwrap(), &db_key)? {
                Some(v) => NonceDiff::try_from_slice(&v).unwrap(),
                // If it's not stored in the database, it's an access key that was present in the genesis
                // recods, so we don't need to do anything to the nonce.
                None => return Ok(Ok(nonce)),
            };
        if m.known() {
            return Ok(m.map(nonce));
        }

        let source_nonce = fetch_access_key_nonce(
            &self.source_view_client,
            signer_id,
            source_public,
            Some(source_block_hash),
        )
        .await?;

        let target_nonce =
            fetch_access_key_nonce(&self.target_view_client, signer_id, target_public, None)
                .await?
                // add one because we need to start from a nonce one greater than the current one
                .map(|n| n + 1);

        let mut rewrite = false;
        if m.source_start.is_none() && source_nonce.is_some() {
            rewrite = true;
            m.source_start = source_nonce;
        }
        if m.target_start.is_none() && target_nonce.is_some() {
            rewrite = true;
            m.target_start = target_nonce;
        }
        if rewrite {
            tracing::debug!(target: "mirror", "storing {:?} in DB for ({:?}, {:?})", &m, signer_id, target_public);
            self.db.put_cf(
                self.db.cf_handle(DBCol::Nonces.name()).unwrap(),
                &db_key,
                &m.try_to_vec().unwrap(),
            )?;
        }
        Ok(m.map(nonce))
    }

    fn map_action(&self, a: &ActionView) -> anyhow::Result<Option<Action>> {
        // this try_from() won't fail since the ActionView was constructed from the Action
        let action = Action::try_from(a.clone()).unwrap();

        match &action {
            Action::AddKey(add_key) => {
                let replacement =
                    crate::key_mapping::map_key(&add_key.public_key, self.secret.as_ref());
                let public_key = replacement.public_key();
                self.db.put_cf(
                    self.db.cf_handle(DBCol::Nonces.name()).unwrap(),
                    &public_key.try_to_vec().unwrap(),
                    &NonceDiff::default().try_to_vec().unwrap(),
                )?;

                Ok(Some(Action::AddKey(AddKeyAction {
                    public_key,
                    access_key: add_key.access_key.clone(),
                })))
            }
            Action::DeleteKey(delete_key) => {
                let replacement =
                    crate::key_mapping::map_key(&delete_key.public_key, self.secret.as_ref());
                let public_key = replacement.public_key();
                self.db.delete_cf(
                    self.db.cf_handle(DBCol::Nonces.name()).unwrap(),
                    &public_key.try_to_vec().unwrap(),
                )?;

                Ok(Some(Action::DeleteKey(DeleteKeyAction { public_key })))
            }
            // We don't want to mess with the set of validators in the target chain
            Action::Stake(_) => Ok(None),
            _ => Ok(Some(action)),
        }
    }

    // fetch the source chain block at `source_height`, and prepare a
    // set of transactions that should be valid in the target chain
    // from it.
    async fn fetch_txs(
        &self,
        source_height: BlockHeight,
        ref_hash: CryptoHash,
    ) -> anyhow::Result<Option<MappedBlock>> {
        let source_block_hash = match self
            .source_view_client
            .send(GetBlockHash(BlockReference::BlockId(BlockId::Height(source_height))))
            .await?
        {
            Ok(h) => h,
            Err(e) => match e {
                GetBlockError::UnknownBlock { .. } => return Ok(None),
                e => return Err(e.into()),
            },
        };
        let mut chunks = Vec::new();
        for shard_id in self.tracked_shards.iter() {
            let mut txs = Vec::new();
            let mut txs_awaiting_nonce = HashMap::new();

            let chunk = match self
                .source_view_client
                .send(GetChunk::Height(source_height, *shard_id))
                .await?
            {
                Ok(c) => c,
                Err(e) => match e {
                    GetChunkError::UnknownBlock { .. } => return Ok(None),
                    GetChunkError::UnknownChunk { .. } => {
                        tracing::error!(
                            "Can't fetch source chain shard {} chunk at height {}. Are we tracking all shards?",
                            shard_id, source_height
                        );
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            if chunk.header.height_included != source_height {
                continue;
            }

            for t in chunk.transactions {
                let mapped_key = crate::key_mapping::map_key(&t.public_key, self.secret.as_ref());
                let public_key = mapped_key.public_key();
                let mut actions = Vec::new();
                for action in t.actions.iter() {
                    if let Some(action) = self.map_action(action)? {
                        actions.push(action);
                    }
                }
                if actions.is_empty() {
                    // If this is a tx containing only stake actions, skip it.
                    continue;
                }
                match self
                    .map_nonce(
                        &t.signer_id,
                        &t.public_key,
                        &public_key,
                        t.nonce,
                        &source_block_hash,
                    )
                    .await?
                {
                    Ok(nonce) => {
                        let mut tx = Transaction::new(
                            t.signer_id.clone(),
                            public_key,
                            t.receiver_id.clone(),
                            nonce,
                            ref_hash.clone(),
                        );
                        tx.actions = actions;
                        let tx = SignedTransaction::new(
                            mapped_key.sign(&tx.get_hash_and_size().0.as_ref()),
                            tx,
                        );
                        txs.push(tx);
                    }
                    Err(e) => match e {
                        MapNonceError::AddOverflow(..)
                        | MapNonceError::SubOverflow(..)
                        | MapNonceError::SourceKeyNotOnChain => {
                            tracing::error!(target: "mirror", "error mapping nonce for ({:?}, {:?}): {:?}", &t.signer_id, &public_key, e);
                            continue;
                        }
                        MapNonceError::TargetKeyNotOnChain => {
                            let mut tx = Transaction::new(
                                t.signer_id.clone(),
                                public_key,
                                t.receiver_id.clone(),
                                t.nonce,
                                ref_hash.clone(),
                            );
                            tx.actions = actions;
                            txs_awaiting_nonce.insert(
                                t.hash,
                                TxAwaitingNonce {
                                    tx,
                                    source_public: t.public_key.clone(),
                                    target_private: mapped_key,
                                },
                            );
                        }
                    },
                };
            }
            if txs_awaiting_nonce.is_empty() {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {}",
                    txs.len(), source_height, shard_id
                );
            } else {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {} and still waiting \
                    for the corresponding access keys to make it on chain to prepare {} more transactions",
                    txs.len(), source_height, shard_id, txs_awaiting_nonce.len()
                );
            }
            chunks.push(MappedChunk { txs, txs_awaiting_nonce, shard_id: *shard_id });
        }
        Ok(Some(MappedBlock { source_height, source_block_hash, chunks }))
    }

    // Up to a certain capacity, prepare and queue up batches of
    // transactions that we want to send to the target chain.
    async fn queue_txs(
        &mut self,
        tracker: &mut crate::chain_tracker::TxTracker,
        ref_hash: CryptoHash,
        check_send_time: bool,
    ) -> anyhow::Result<()> {
        if tracker.num_blocks_queued() > 100 {
            return Ok(());
        }

        let next_batch_time = tracker.next_batch_time();
        let source_head =
            self.get_source_height().await.context("can't fetch source chain HEAD")?;
        let start_height = match tracker.height_queued() {
            Some(h) => h + 1,
            None => self.get_next_source_height()?,
        };

        for height in start_height..=source_head {
            if let Some(b) = self
                .fetch_txs(height, ref_hash)
                .await
                .with_context(|| format!("Can't fetch source #{} transactions", height))?
            {
                tracker.queue_block(b);
                if tracker.num_blocks_queued() > 100 {
                    return Ok(());
                }
            };

            if check_send_time
                && tracker.num_blocks_queued() > 0
                && Instant::now() > next_batch_time - Duration::from_millis(20)
            {
                return Ok(());
            }
        }
        Ok(())
    }

    fn set_next_source_height(&mut self, height: BlockHeight) -> anyhow::Result<()> {
        self.next_source_height = Some(height);
        self.db.put_cf(
            self.db.cf_handle(DBCol::Misc.name()).unwrap(),
            "next_source_height",
            height.try_to_vec().unwrap(),
        )?;
        Ok(())
    }

    // Go through any upcoming batches of transactions that we haven't
    // been able to set a valid nonce for yet, and see if we can now
    // do that.
    async fn set_nonces(
        &self,
        tracker: &mut crate::chain_tracker::TxTracker,
    ) -> anyhow::Result<()> {
        if tracker.num_txs_awaiting_nonce() == 0 {
            return Ok(());
        }

        let next_batch_time = tracker.next_batch_time();
        let mut txs_ready = Vec::new();

        for (i, b) in tracker.queued_blocks_iter().enumerate() {
            for (shard_id, c) in b.chunks.iter().enumerate() {
                for (hash, tx) in c.txs_awaiting_nonce.iter() {
                    match self
                        .map_nonce(
                            &tx.tx.signer_id,
                            &tx.source_public,
                            &tx.tx.public_key,
                            tx.tx.nonce,
                            &b.source_block_hash,
                        )
                        .await?
                    {
                        Ok(nonce) => {
                            txs_ready.push((i, shard_id, *hash, nonce));
                        }
                        Err(e) => match &e {
                            MapNonceError::AddOverflow(..)
                            | MapNonceError::SubOverflow(..)
                            | MapNonceError::SourceKeyNotOnChain => {
                                tracing::error!(
                                    target: "mirror", "error mapping nonce for ({:?}, {:?}): {:?}",
                                    &tx.tx.signer_id, &tx.tx.public_key, e
                                );
                            }
                            // still not ready
                            MapNonceError::TargetKeyNotOnChain => {}
                        },
                    };
                }
            }
            if Instant::now() > next_batch_time - Duration::from_millis(20) {
                break;
            }
        }
        for (i, shard_id, hash, nonce) in txs_ready.iter() {
            tracker.set_tx_nonce(*i, *shard_id, hash, *nonce);
        }
        Ok(())
    }

    async fn main_loop(
        &mut self,
        mut tracker: crate::chain_tracker::TxTracker,
        mut target_height: BlockHeight,
        mut target_head: CryptoHash,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                // time to send a batch of transactions
                mapped_block = tracker.next_batch(), if tracker.num_blocks_queued() > 0 => {
                    let mapped_block = mapped_block.unwrap();
                    let sent = self.send_transactions(&mapped_block).await?;
                    tracker.on_txs_sent(&sent, mapped_block.source_height, target_height);

                    // now we have one second left until we need to send more transactions. In the
                    // meantime, we might as well prepare some more batches of transactions.
                    // TODO: continue in best effort fashion on error
                    self.set_next_source_height(mapped_block.source_height+1)?;
                    self.queue_txs(&mut tracker, target_head, true).await?;
                }
                msg = self.target_stream.recv() => {
                    let msg = msg.unwrap();
                    tracker.on_target_block(&msg);
                    self.set_nonces(&mut tracker).await?;
                    target_head = msg.block.header.hash;
                    target_height = msg.block.header.height;
                }
                // If we don't have any upcoming sets of transactions to send already built, we probably fell behind in the source
                // chain and can't fetch the transactions. Check if we have them now here.
                _ = tokio::time::sleep(Duration::from_millis(200)), if tracker.num_blocks_queued() == 0 => {
                    self.queue_txs(&mut tracker, target_head, true).await?;
                }
            };
        }
    }

    async fn get_source_height(&self) -> Option<BlockHeight> {
        self.source_client
            .send(near_client::Status { is_health_check: false, detailed: false })
            .await
            .unwrap()
            .ok()
            .map(|s| s.sync_info.latest_block_height)
    }

    // wait until HEAD moves. We don't really need it to be fully synced.
    async fn wait_source_ready(&self) {
        let mut first_height = None;
        loop {
            if let Some(head) = self.get_source_height().await {
                match first_height {
                    Some(h) => {
                        if h != head {
                            return;
                        }
                    }
                    None => {
                        first_height = Some(head);
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn wait_target_synced(&mut self) -> (BlockHeight, CryptoHash) {
        let msg = self.target_stream.recv().await.unwrap();
        (msg.block.header.height, msg.block.header.hash)
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut tracker = crate::chain_tracker::TxTracker::new();
        self.wait_source_ready().await;
        let (target_height, target_head) = self.wait_target_synced().await;

        self.queue_txs(&mut tracker, target_head, false).await?;

        self.main_loop(tracker, target_height, target_head).await
    }
}

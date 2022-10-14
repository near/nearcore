use actix::Addr;
use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client_primitives::types::{
    GetBlock, GetBlockError, GetChunk, GetChunkError, GetExecutionOutcome,
    GetExecutionOutcomeError, GetExecutionOutcomeResponse, Query, QueryError,
};
use near_crypto::{PublicKey, SecretKey};
use near_indexer::{Indexer, StreamerMessage};
use near_network::types::{NetworkClientMessages, NetworkClientResponses};
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteKeyAction, SignedTransaction, Transaction,
};
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, Finality, TransactionOrReceiptId,
};
use near_primitives::views::{
    ExecutionStatusView, QueryRequest, QueryResponseKind, SignedTransactionView,
};
use near_primitives_core::types::{Nonce, ShardId};
use nearcore::config::NearConfig;
use rocksdb::DB;
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};
use strum::IntoEnumIterator;
use tokio::sync::mpsc;

mod chain_tracker;
pub mod genesis;
mod key_mapping;
mod metrics;
pub mod secret;

#[derive(strum::EnumIter)]
enum DBCol {
    Misc,
    // This tracks nonces for Access Keys added by AddKey transactions
    // or transfers to implicit accounts (not present in the genesis state).
    // For a given (account ID, public key), if we're preparing a transaction
    // and there's no entry in the DB, then the key was present in the genesis
    // state. Otherwise, we map tx nonces according to the values in this column.
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

// returns bytes that serve as the key corresponding to this pair in the Nonces column
fn nonce_col_key(account_id: &AccountId, public_key: &PublicKey) -> Vec<u8> {
    (account_id.clone(), public_key.clone()).try_to_vec().unwrap()
}

#[derive(Clone, BorshDeserialize, BorshSerialize, Debug, PartialEq, Eq, PartialOrd, Hash)]
struct TxIds {
    tx_hash: CryptoHash,
    signer_id: AccountId,
    receiver_id: AccountId,
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
    pending_source_txs: HashSet<TxIds>,
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
    fn set_source(&mut self, nonce: Nonce) {
        self.source_start = Some(nonce);
        self.pending_source_txs.clear();
    }

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

struct TxMirror {
    target_stream: mpsc::Receiver<StreamerMessage>,
    source_view_client: Addr<ViewClientActor>,
    source_client: Addr<ClientActor>,
    target_view_client: Addr<ViewClientActor>,
    target_client: Addr<ClientActor>,
    db: DB,
    target_genesis_height: BlockHeight,
    target_min_block_production_delay: Duration,
    tracked_shards: Vec<ShardId>,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
    next_source_height: Option<BlockHeight>,
}

fn open_db<P: AsRef<Path>>(home: P, config: &NearConfig) -> anyhow::Result<DB> {
    let db_path =
        near_store::NodeStorage::opener(home.as_ref(), &config.config.store).path().join("mirror");
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
    source_signer_id: AccountId,
    target_private: SecretKey,
    tx: Transaction,
}

#[derive(Debug)]
enum TargetChainTx {
    Ready(SignedTransaction),
    AwaitingNonce(TxAwaitingNonce),
}

impl TargetChainTx {
    // For an AwaitingNonce(_), set the nonce and sign the transaction, changing self into Ready(_).
    // must not be called if self is Ready(_)
    fn set_nonce(&mut self, nonce: Nonce) {
        match self {
            Self::AwaitingNonce(t) => {
                t.tx.nonce = nonce;
                let tx = SignedTransaction::new(
                    t.target_private.sign(&t.tx.get_hash_and_size().0.as_ref()),
                    t.tx.clone(),
                );
                tracing::debug!(
                    target: "mirror", "prepared a transaction for ({:?}, {:?}) that was previously waiting for the access key to appear on chain",
                    &tx.transaction.signer_id, &tx.transaction.public_key
                );
                *self = Self::Ready(tx);
            }
            Self::Ready(_) => unreachable!(),
        }
    }
}

#[derive(Debug)]
struct MappedChunk {
    txs: Vec<TargetChainTx>,
    shard_id: ShardId,
}

#[derive(Debug)]
struct MappedBlock {
    source_height: BlockHeight,
    chunks: Vec<MappedChunk>,
}

async fn account_exists(
    view_client: &Addr<ViewClientActor>,
    account_id: &AccountId,
    prev_block: &CryptoHash,
) -> anyhow::Result<bool> {
    match view_client
        .send(
            Query::new(
                BlockReference::BlockId(BlockId::Hash(prev_block.clone())),
                QueryRequest::ViewAccount { account_id: account_id.clone() },
            )
            .with_span_context(),
        )
        .await?
    {
        Ok(res) => match res.kind {
            QueryResponseKind::ViewAccount(_) => Ok(true),
            other => {
                panic!("Received unexpected QueryResponse after Querying Account: {:?}", other);
            }
        },
        Err(e) => match &e {
            QueryError::UnknownAccount { .. } => Ok(false),
            _ => Err(e.into()),
        },
    }
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
        .send(
            Query::new(
                block_ref,
                QueryRequest::ViewAccessKey {
                    account_id: account_id.clone(),
                    public_key: public_key.clone(),
                },
            )
            .with_span_context(),
        )
        .await?
    {
        Ok(res) => match res.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(Some(access_key.nonce)),
            other => {
                panic!("Received unexpected QueryResponse after Querying Access Key: {:?}", other);
            }
        },
        Err(_) => Ok(None),
    }
}

#[derive(Clone, Debug)]
enum TxOutcome {
    Success(CryptoHash),
    Pending,
    Failure,
}

async fn fetch_tx_outcome(
    view_client: &Addr<ViewClientActor>,
    transaction_hash: CryptoHash,
    signer_id: &AccountId,
    receiver_id: &AccountId,
) -> anyhow::Result<TxOutcome> {
    let receipt_id = match view_client
        .send(
            GetExecutionOutcome {
                id: TransactionOrReceiptId::Transaction {
                    transaction_hash,
                    sender_id: signer_id.clone(),
                },
            }
            .with_span_context(),
        )
        .await
        .unwrap()
    {
        Ok(GetExecutionOutcomeResponse { outcome_proof, .. }) => {
            match outcome_proof.outcome.status {
                ExecutionStatusView::SuccessReceiptId(id) => id,
                ExecutionStatusView::SuccessValue(_) => unreachable!(),
                ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => {
                    return Ok(TxOutcome::Failure)
                }
            }
        }
        Err(
            GetExecutionOutcomeError::NotConfirmed { .. }
            | GetExecutionOutcomeError::UnknownBlock { .. },
        ) => return Ok(TxOutcome::Pending),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed fetching outcome for tx {}", transaction_hash))
        }
    };
    match view_client
        .send(
            GetExecutionOutcome {
                id: TransactionOrReceiptId::Receipt {
                    receipt_id,
                    receiver_id: receiver_id.clone(),
                },
            }
            .with_span_context(),
        )
        .await
        .unwrap()
    {
        Ok(GetExecutionOutcomeResponse { outcome_proof, .. }) => {
            match outcome_proof.outcome.status {
                ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_) => {
                    // the view client code actually modifies the outcome's block_hash field to be the
                    // next block with a new chunk in the relevant shard, so go backwards one block,
                    // since that's what we'll want to give in the query for AccessKeys
                    let block = view_client
                        .send(
                            GetBlock(BlockReference::BlockId(BlockId::Hash(
                                outcome_proof.block_hash,
                            )))
                            .with_span_context(),
                        )
                        .await
                        .unwrap()
                        .with_context(|| {
                            format!("failed fetching block {}", &outcome_proof.block_hash)
                        })?;
                    Ok(TxOutcome::Success(block.header.prev_hash))
                }
                ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => {
                    Ok(TxOutcome::Failure)
                }
            }
        }
        Err(
            GetExecutionOutcomeError::NotConfirmed { .. }
            | GetExecutionOutcomeError::UnknownBlock { .. }
            | GetExecutionOutcomeError::UnknownTransactionOrReceipt { .. },
        ) => Ok(TxOutcome::Pending),
        Err(e) => {
            Err(e).with_context(|| format!("failed fetching outcome for receipt {}", &receipt_id))
        }
    }
}

async fn block_hash_to_height(
    view_client: &Addr<ViewClientActor>,
    hash: &CryptoHash,
) -> anyhow::Result<BlockHeight> {
    Ok(view_client
        .send(GetBlock(BlockReference::BlockId(BlockId::Hash(hash.clone()))).with_span_context())
        .await
        .unwrap()?
        .header
        .height)
}

impl TxMirror {
    fn new<P: AsRef<Path>>(
        source_home: P,
        target_home: P,
        secret: Option<[u8; crate::secret::SECRET_LEN]>,
    ) -> anyhow::Result<Self> {
        let target_config =
            nearcore::config::load_config(target_home.as_ref(), GenesisValidationMode::UnsafeFast)
                .with_context(|| {
                    format!("Error loading target config from {:?}", target_home.as_ref())
                })?;
        let db =
            open_db(target_home.as_ref(), &target_config).context("failed to open mirror DB")?;
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
            target_min_block_production_delay: target_config
                .client_config
                .min_block_production_delay,
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
                match tx {
                    TargetChainTx::Ready(tx) => {
                        match self
                            .target_client
                            .send(
                                NetworkClientMessages::Transaction {
                                    transaction: tx.clone(),
                                    is_forwarded: false,
                                    check_only: false,
                                }
                                .with_span_context(),
                            )
                            .await?
                        {
                            NetworkClientResponses::RequestRouted => {
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
                                crate::metrics::TRANSACTIONS_SENT
                                    .with_label_values(&["invalid"])
                                    .inc();
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
                    TargetChainTx::AwaitingNonce(tx) => {
                        // TODO: here we should just save this transaction for later and send it when it's known
                        tracing::warn!(target: "mirror", "skipped sending transaction with signer {} because valid target chain nonce not known", &tx.source_signer_id)
                    }
                }
            }
        }
        Ok(sent)
    }

    fn read_nonce_diff(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> anyhow::Result<Option<NonceDiff>> {
        let db_key = nonce_col_key(account_id, public_key);
        // TODO: cache this?
        Ok(self
            .db
            .get_cf(self.db.cf_handle(DBCol::Nonces.name()).unwrap(), &db_key)?
            .map(|v| NonceDiff::try_from_slice(&v).unwrap()))
    }

    fn put_nonce_diff(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
        diff: &NonceDiff,
    ) -> anyhow::Result<()> {
        tracing::debug!(target: "mirror", "storing {:?} in DB for ({:?}, {:?})", &diff, account_id, public_key);
        let db_key = nonce_col_key(account_id, public_key);
        self.db.put_cf(
            self.db.cf_handle(DBCol::Nonces.name()).unwrap(),
            &db_key,
            &diff.try_to_vec().unwrap(),
        )?;
        Ok(())
    }

    // If the access key was present in the genesis records, just
    // return the same nonce. Otherwise, we need to change the
    // nonce. So check if we already know what the difference in
    // nonces is, and if not, try to fetch that info and store it.
    // `source_signer_id` and `target_signer_id` are the same unless
    // it's an implicit account
    async fn map_nonce(
        &self,
        source_signer_id: &AccountId,
        target_signer_id: &AccountId,
        source_public: &PublicKey,
        target_public: &PublicKey,
        nonce: Nonce,
    ) -> anyhow::Result<Result<Nonce, MapNonceError>> {
        let mut diff = match self.read_nonce_diff(source_signer_id, source_public)? {
            Some(m) => m,
            // If it's not stored in the database, it's an access key that was present in the genesis
            // records, so we don't need to do anything to the nonce.
            None => return Ok(Ok(nonce)),
        };
        if diff.known() {
            return Ok(diff.map(nonce));
        }

        self.update_nonces(
            source_signer_id,
            target_signer_id,
            source_public,
            target_public,
            &mut diff,
        )
        .await?;
        Ok(diff.map(nonce))
    }

    async fn update_nonces(
        &self,
        source_signer_id: &AccountId,
        target_signer_id: &AccountId,
        source_public: &PublicKey,
        target_public: &PublicKey,
        diff: &mut NonceDiff,
    ) -> anyhow::Result<()> {
        let mut rewrite = false;
        if diff.source_start.is_none() {
            self.update_source_nonce(source_signer_id, source_public, diff).await?;
            rewrite |= diff.source_start.is_some();
        }
        if diff.target_start.is_none() {
            diff.target_start = fetch_access_key_nonce(
                &self.target_view_client,
                target_signer_id,
                target_public,
                None,
            )
            .await?;
            rewrite |= diff.target_start.is_some();
        }

        if rewrite {
            self.put_nonce_diff(source_signer_id, source_public, diff)?;
        }
        Ok(())
    }

    async fn update_source_nonce(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
        diff: &mut NonceDiff,
    ) -> anyhow::Result<()> {
        let mut block_height = 0;
        let mut block_hash = CryptoHash::default();
        let mut failed_txs = Vec::new();

        // first find the earliest block hash where the access key should exist
        for tx in diff.pending_source_txs.iter() {
            match fetch_tx_outcome(
                &self.source_view_client,
                tx.tx_hash.clone(),
                &tx.signer_id,
                &tx.receiver_id,
            )
            .await?
            {
                TxOutcome::Success(hash) => {
                    let height =
                        block_hash_to_height(&self.source_view_client, &hash).await.with_context(
                            || format!("failed fetching block height of block {}", &hash),
                        )?;
                    if &block_hash == &CryptoHash::default() || block_height > height {
                        block_height = height;
                        block_hash = hash;
                    }
                }
                TxOutcome::Failure => {
                    failed_txs.push(tx.clone());
                }
                TxOutcome::Pending => {}
            }
        }
        if &block_hash == &CryptoHash::default() {
            // no need to do this if block_hash is set because set_source() below will clear it
            for tx in failed_txs.iter() {
                diff.pending_source_txs.remove(tx);
            }
            return Ok(());
        }
        let nonce = fetch_access_key_nonce(
            &self.source_view_client,
            account_id,
            public_key,
            Some(&block_hash),
        )
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "expected access key to exist for {}, {} after finding successful receipt in {}",
                &account_id,
                &public_key,
                &block_hash
            )
        })?;
        diff.set_source(nonce);
        Ok(())
    }

    // we have a situation where nonces need to be mapped (AddKey actions
    // or implicit account transfers). So store the initial nonce data in the DB.
    async fn store_source_nonce(
        &self,
        tx: &SignedTransactionView,
        public_key: &PublicKey,
    ) -> anyhow::Result<()> {
        // TODO: probably better to use a merge operator here. Not urgent, though.
        let mut diff = self.read_nonce_diff(&tx.receiver_id, &public_key)?.unwrap_or_default();
        if diff.source_start.is_some() {
            return Ok(());
        }
        diff.pending_source_txs.insert(TxIds {
            tx_hash: tx.hash.clone(),
            signer_id: tx.signer_id.clone(),
            receiver_id: tx.receiver_id.clone(),
        });
        self.update_source_nonce(&tx.receiver_id, &public_key, &mut diff).await?;
        self.put_nonce_diff(&tx.receiver_id, &public_key, &diff)
    }

    async fn map_actions(
        &self,
        tx: &SignedTransactionView,
        prev_block: &CryptoHash,
    ) -> anyhow::Result<Vec<Action>> {
        let mut actions = Vec::new();

        for a in tx.actions.iter() {
            // this try_from() won't fail since the ActionView was constructed from the Action
            let action = Action::try_from(a.clone()).unwrap();

            match &action {
                Action::AddKey(add_key) => {
                    self.store_source_nonce(tx, &add_key.public_key).await?;

                    let replacement =
                        crate::key_mapping::map_key(&add_key.public_key, self.secret.as_ref());

                    actions.push(Action::AddKey(AddKeyAction {
                        public_key: replacement.public_key(),
                        access_key: add_key.access_key.clone(),
                    }));
                }
                Action::DeleteKey(delete_key) => {
                    let replacement =
                        crate::key_mapping::map_key(&delete_key.public_key, self.secret.as_ref());
                    let public_key = replacement.public_key();

                    actions.push(Action::DeleteKey(DeleteKeyAction { public_key }));
                }
                Action::Transfer(_) => {
                    if tx.receiver_id.is_implicit()
                        && !account_exists(&self.source_view_client, &tx.receiver_id, prev_block)
                            .await
                            .with_context(|| {
                                format!("failed checking existence for account {}", &tx.receiver_id)
                            })?
                    {
                        let public_key = crate::key_mapping::implicit_account_key(&tx.receiver_id);
                        self.store_source_nonce(tx, &public_key).await?;
                    }
                    actions.push(action);
                }
                // We don't want to mess with the set of validators in the target chain
                Action::Stake(_) => {}
                _ => actions.push(action),
            };
        }
        Ok(actions)
    }

    // fetch the source chain block at `source_height`, and prepare a
    // set of transactions that should be valid in the target chain
    // from it.
    async fn fetch_txs(
        &self,
        source_height: BlockHeight,
        ref_hash: CryptoHash,
    ) -> anyhow::Result<Option<MappedBlock>> {
        let prev_hash = match self
            .source_view_client
            .send(
                GetBlock(BlockReference::BlockId(BlockId::Height(source_height)))
                    .with_span_context(),
            )
            .await
            .unwrap()
        {
            Ok(b) => b.header.prev_hash,
            Err(GetBlockError::UnknownBlock { .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut chunks = Vec::new();
        for shard_id in self.tracked_shards.iter() {
            let mut txs = Vec::new();

            let chunk = match self
                .source_view_client
                .send(GetChunk::Height(source_height, *shard_id).with_span_context())
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

            let mut num_not_ready = 0;
            for t in chunk.transactions {
                let actions = self.map_actions(&t, &prev_hash).await?;
                if actions.is_empty() {
                    // If this is a tx containing only stake actions, skip it.
                    continue;
                }
                let mapped_key = crate::key_mapping::map_key(&t.public_key, self.secret.as_ref());
                let public_key = mapped_key.public_key();

                let target_signer_id =
                    crate::key_mapping::map_account(&t.signer_id, self.secret.as_ref());
                match self
                    .map_nonce(&t.signer_id, &target_signer_id, &t.public_key, &public_key, t.nonce)
                    .await?
                {
                    Ok(nonce) => {
                        let mut tx = Transaction::new(
                            target_signer_id,
                            public_key,
                            crate::key_mapping::map_account(&t.receiver_id, self.secret.as_ref()),
                            nonce,
                            ref_hash.clone(),
                        );
                        tx.actions = actions;
                        let tx = SignedTransaction::new(
                            mapped_key.sign(&tx.get_hash_and_size().0.as_ref()),
                            tx,
                        );
                        txs.push(TargetChainTx::Ready(tx));
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
                                crate::key_mapping::map_account(&t.signer_id, self.secret.as_ref()),
                                public_key,
                                crate::key_mapping::map_account(
                                    &t.receiver_id,
                                    self.secret.as_ref(),
                                ),
                                t.nonce,
                                ref_hash.clone(),
                            );
                            tx.actions = actions;
                            txs.push(TargetChainTx::AwaitingNonce(TxAwaitingNonce {
                                tx,
                                source_public: t.public_key.clone(),
                                source_signer_id: t.signer_id.clone(),
                                target_private: mapped_key,
                            }));
                            num_not_ready += 1;
                        }
                    },
                };
            }
            if num_not_ready == 0 {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {}",
                    txs.len(), source_height, shard_id
                );
            } else {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {} {} of which are \
                    still waiting for the corresponding access keys to make it on chain",
                    txs.len(), source_height, shard_id, num_not_ready,
                );
            }
            chunks.push(MappedChunk { txs, shard_id: *shard_id });
        }
        Ok(Some(MappedBlock { source_height, chunks }))
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
        // TODO: we should instead save something like the
        // (block_height, shard_id, idx_in_chunk) of the last
        // transaction sent. Currently we set next_source_height after
        // sending all of the transactions in that chunk, so if we get
        // SIGTERM or something in the middle of sending a batch of
        // txs, we'll send some that we already sent next time we
        // start. Not a giant problem but kind of unclean.
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
        let next_batch_time = tracker.next_batch_time();
        let mut txs_ready = Vec::new();
        let mut keys_mapped = HashSet::new();

        for (source_signer_id, source_public_key) in tracker.pending_access_keys_iter() {
            let mut diff = self.read_nonce_diff(source_signer_id, source_public_key)?.unwrap();
            let target_signer_id =
                crate::key_mapping::map_account(source_signer_id, self.secret.as_ref());
            let target_public_key =
                crate::key_mapping::map_key(source_public_key, self.secret.as_ref()).public_key();
            self.update_nonces(
                &source_signer_id,
                &target_signer_id,
                &source_public_key,
                &target_public_key,
                &mut diff,
            )
            .await?;
            if diff.known() {
                keys_mapped.insert((source_signer_id.clone(), source_public_key.clone()));
            }
        }
        for (tx_ref, tx) in tracker.tx_awaiting_nonce_iter() {
            if keys_mapped.contains(&(tx.source_signer_id.clone(), tx.source_public.clone())) {
                let nonce = self
                    .map_nonce(
                        &tx.source_signer_id,
                        &tx.tx.signer_id,
                        &tx.source_public,
                        &tx.tx.public_key,
                        tx.tx.nonce,
                    )
                    .await?
                    .unwrap();
                txs_ready.push((tx_ref.clone(), nonce));
            }

            if Instant::now() > next_batch_time - Duration::from_millis(20) {
                break;
            }
        }
        for (tx_ref, nonce) in txs_ready {
            tracker.set_tx_nonce(&tx_ref, nonce);
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
            .send(
                near_client::Status { is_health_check: false, detailed: false }.with_span_context(),
            )
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

    async fn run(mut self) -> anyhow::Result<()> {
        let mut tracker =
            crate::chain_tracker::TxTracker::new(self.target_min_block_production_delay);
        self.wait_source_ready().await;
        let (target_height, target_head) = self.wait_target_synced().await;

        self.queue_txs(&mut tracker, target_head, false).await?;

        self.main_loop(tracker, target_height, target_head).await
    }
}

pub async fn run<P: AsRef<Path>>(
    source_home: P,
    target_home: P,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
) -> anyhow::Result<()> {
    let m = TxMirror::new(source_home, target_home, secret)?;
    m.run().await
}

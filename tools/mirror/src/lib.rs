use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_configs::GenesisValidationMode;
use near_client::{ClientActor, ViewClientActor};
use near_client::{ProcessTxRequest, ProcessTxResponse};
use near_client_primitives::types::{
    GetBlockError, GetChunkError, GetExecutionOutcome, GetExecutionOutcomeError,
    GetExecutionOutcomeResponse, Query, QueryError,
};
use near_crypto::{PublicKey, SecretKey};
use near_indexer::{Indexer, StreamerMessage};
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, DeleteKeyAction, SignedTransaction, Transaction,
};
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, Finality, TransactionOrReceiptId,
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
pub mod cli;
mod genesis;
mod key_mapping;
mod metrics;
mod offline;
mod online;
mod secret;

pub use cli::MirrorCommand;

#[derive(strum::EnumIter)]
enum DBCol {
    Misc,
    // This tracks nonces for Access Keys added by AddKey transactions
    // or transfers to implicit accounts (not present in the genesis state).
    // For a given (account ID, public key), if we're preparing a transaction
    // and there's no entry in the DB, then the key was present in the genesis
    // state. Otherwise, we map tx nonces according to the values in this column.
    Nonces,
    AccessKeyOutcomes,
}

impl DBCol {
    fn name(&self) -> &'static str {
        match self {
            Self::Misc => "miscellaneous",
            Self::Nonces => "nonces",
            Self::AccessKeyOutcomes => "access_key_outcomes",
        }
    }
}

// TODO: maybe move these type defs to `mod types` or something
#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
struct TxInfo {
    hash: CryptoHash,
    signer_id: AccountId,
    receiver_id: AccountId,
}

#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
struct ReceiptInfo {
    id: CryptoHash,
    receiver_id: AccountId,
}

#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, PartialEq, Eq, PartialOrd, Hash)]
enum ChainObjectId {
    Tx(TxInfo),
    Receipt(ReceiptInfo),
}

// we want a reference to transactions in .queued_blocks that need to have nonces
// set later. To avoid having the struct be self referential we keep this struct
// with enough info to look it up later.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TxRef {
    source_height: BlockHeight,
    shard_id: ShardId,
    tx_idx: usize,
}

impl std::fmt::Display for TxRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "source #{} shard {} tx {}", self.source_height, self.shard_id, self.tx_idx)
    }
}

impl PartialOrd for TxRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TxRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.source_height
            .cmp(&other.source_height)
            .then_with(|| self.tx_idx.cmp(&other.tx_idx))
            .then_with(|| self.shard_id.cmp(&other.shard_id))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum NonceUpdater {
    TxRef(TxRef),
    ChainObjectId(ChainObjectId),
}

// returns bytes that serve as the key corresponding to this pair in the Nonces column
fn nonce_col_key(account_id: &AccountId, public_key: &PublicKey) -> Vec<u8> {
    (account_id.clone(), public_key.clone()).try_to_vec().unwrap()
}

// this serves a similar purpose to `LatestTargetNonce`. The difference is
// that this one keeps track of what's in memory. So for example if the last
// height we sent transactions for was 10, and we have a set of transactions
// queued up for height 12, one of which is an AddKey for ('foo.near', 'ed25519:...'),
// then we'lll want to remember that for txs of height > 12 that use that
// signer id and public key, but we don't want to store that on disk. The `LatestTargetNonce`
// on disk will only record the transactions/receipts updating the nonce that we actually sent
// or saw on chain

#[derive(Clone, Debug)]
struct TargetNonce {
    nonce: Option<Nonce>,
    pending_outcomes: HashSet<NonceUpdater>,
}

// For a given AddKey Action, records the starting nonces of the
// resulting Access Keys.  We need this because when an AddKey receipt
// is processed, the nonce field of the AddKey action is actually
// ignored, and it's set to block_height*1000000, so to generate
// transactions with valid nonces, we need to map valid source chain
// nonces to valid target chain nonces.
#[derive(BorshDeserialize, BorshSerialize, Debug)]
struct LatestTargetNonce {
    nonce: Option<Nonce>,
    pending_outcomes: HashSet<ChainObjectId>,
}

// TODO: move DB related stuff to its own file and add a way to
// keep track of updates in memory and write them all in one transaction
fn read_target_nonce(
    db: &DB,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> anyhow::Result<Option<LatestTargetNonce>> {
    let db_key = nonce_col_key(account_id, public_key);
    Ok(db
        .get_cf(db.cf_handle(DBCol::Nonces.name()).unwrap(), &db_key)?
        .map(|v| LatestTargetNonce::try_from_slice(&v).unwrap()))
}

fn put_target_nonce(
    db: &DB,
    account_id: &AccountId,
    public_key: &PublicKey,
    nonce: &LatestTargetNonce,
) -> anyhow::Result<()> {
    tracing::debug!(target: "mirror", "storing {:?} in DB for ({}, {:?})", &nonce, account_id, public_key);
    let db_key = nonce_col_key(account_id, public_key);
    db.put_cf(db.cf_handle(DBCol::Nonces.name()).unwrap(), &db_key, &nonce.try_to_vec().unwrap())?;
    Ok(())
}

fn read_access_key_outcome(
    db: &DB,
    id: &ChainObjectId,
) -> anyhow::Result<Option<HashSet<(AccountId, PublicKey)>>> {
    Ok(db
        .get_cf(db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(), &id.try_to_vec().unwrap())?
        .map(|v| HashSet::try_from_slice(&v).unwrap()))
}

fn put_access_key_outcome(
    db: &DB,
    id: ChainObjectId,
    access_keys: HashSet<(AccountId, PublicKey)>,
) -> anyhow::Result<()> {
    tracing::debug!(target: "mirror", "storing {:?} in DB for {:?}", &access_keys, &id);
    Ok(db.put_cf(
        db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(),
        &id.try_to_vec().unwrap(),
        &access_keys.try_to_vec().unwrap(),
    )?)
}

fn delete_access_key_outcome(db: &DB, id: &ChainObjectId) -> anyhow::Result<()> {
    tracing::debug!(target: "mirror", "deleting {:?} from DB", &id);
    Ok(db.delete_cf(
        db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(),
        &id.try_to_vec().unwrap(),
    )?)
}

fn set_next_source_height(db: &DB, height: BlockHeight) -> anyhow::Result<()> {
    // TODO: we should instead save something like the
    // (block_height, shard_id, idx_in_chunk) of the last
    // transaction sent. Currently we set next_source_height after
    // sending all of the transactions in that chunk, so if we get
    // SIGTERM or something in the middle of sending a batch of
    // txs, we'll send some that we already sent next time we
    // start. Not a giant problem but kind of unclean.
    db.put_cf(
        db.cf_handle(DBCol::Misc.name()).unwrap(),
        "next_source_height",
        height.try_to_vec().unwrap(),
    )?;
    Ok(())
}

struct ChunkTxs {
    shard_id: ShardId,
    transactions: Vec<SignedTransactionView>,
}

#[derive(thiserror::Error, Debug)]
enum ChainError {
    #[error("block unknown")]
    UnknownBlock,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ChainError {
    fn other<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Other(anyhow::Error::from(error))
    }
}

impl From<GetBlockError> for ChainError {
    fn from(err: GetBlockError) -> Self {
        match err {
            GetBlockError::UnknownBlock { .. } => Self::UnknownBlock,
            _ => Self::other(err),
        }
    }
}

impl From<GetChunkError> for ChainError {
    fn from(err: GetChunkError) -> Self {
        match err {
            GetChunkError::UnknownBlock { .. } => Self::UnknownBlock,
            _ => Self::other(err),
        }
    }
}

impl From<near_chain_primitives::Error> for ChainError {
    fn from(err: near_chain_primitives::Error) -> Self {
        match err {
            near_chain_primitives::Error::DBNotFoundErr(_) => Self::UnknownBlock,
            _ => Self::other(err),
        }
    }
}

#[async_trait(?Send)]
trait ChainAccess {
    async fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn head_height(&self) -> anyhow::Result<BlockHeight>;

    async fn get_txs(
        &self,
        height: BlockHeight,
        shards: &[ShardId],
    ) -> Result<Vec<ChunkTxs>, ChainError>;
}

struct TxMirror<T: ChainAccess> {
    target_stream: mpsc::Receiver<StreamerMessage>,
    source_chain_access: T,
    // TODO: separate out the code that uses the target chain clients, and
    // make it an option to send the transactions to some RPC node.
    // that way it would be possible to run this code and send transactions with an
    // old binary not caught up to the current protocol version, since the
    // transactions we're constructing should stay valid.
    target_view_client: Addr<ViewClientActor>,
    target_client: Addr<ClientActor>,
    db: DB,
    target_genesis_height: BlockHeight,
    target_min_block_production_delay: Duration,
    tracked_shards: Vec<ShardId>,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
}

fn open_db<P: AsRef<Path>>(home: P, config: &NearConfig) -> anyhow::Result<DB> {
    let db_path = near_store::NodeStorage::opener(home.as_ref(), &config.config.store, None)
        .path()
        .join("mirror");
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
// action that we haven't seen on chain yet. The target_tx field is complete
// except for the nonce field.
#[derive(Debug)]
struct TxAwaitingNonce {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    source_tx_index: usize,
    target_private: SecretKey,
    target_tx: Transaction,
    nonce_updates: HashSet<(AccountId, PublicKey)>,
    target_nonce: TargetNonce,
}

impl TxAwaitingNonce {
    fn new(
        source_tx: &SignedTransactionView,
        source_tx_index: usize,
        actions: Vec<Action>,
        target_nonce: &TargetNonce,
        ref_hash: &CryptoHash,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_private: SecretKey,
        target_public_key: PublicKey,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        let mut target_tx = Transaction::new(
            target_signer_id,
            target_public_key,
            target_receiver_id,
            source_tx.nonce,
            ref_hash.clone(),
        );
        target_tx.actions = actions;
        Self {
            source_signer_id: source_tx.signer_id.clone(),
            source_receiver_id: source_tx.receiver_id.clone(),
            source_tx_index,
            target_private,
            target_tx,
            nonce_updates,
            target_nonce: target_nonce.clone(),
        }
    }
}

// A transaction meant for the target chain that is complete/ready to send.
// We keep some extra info about the transaction for the purposes of logging
// later on when we find it on chain.
#[derive(Debug)]
struct MappedTx {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    source_tx_index: usize,
    target_tx: SignedTransaction,
    nonce_updates: HashSet<(AccountId, PublicKey)>,
    sent_successfully: bool,
}

impl MappedTx {
    fn new(
        source_tx: &SignedTransactionView,
        source_tx_index: usize,
        actions: Vec<Action>,
        nonce: Nonce,
        ref_hash: &CryptoHash,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        target_public_key: PublicKey,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        let mut target_tx = Transaction::new(
            target_signer_id,
            target_public_key,
            target_receiver_id,
            nonce,
            ref_hash.clone(),
        );
        target_tx.actions = actions;
        let target_tx = SignedTransaction::new(
            target_secret_key.sign(&target_tx.get_hash_and_size().0.as_ref()),
            target_tx,
        );
        Self {
            source_signer_id: source_tx.signer_id.clone(),
            source_receiver_id: source_tx.receiver_id.clone(),
            source_tx_index,
            target_tx,
            nonce_updates,
            sent_successfully: false,
        }
    }
}

#[derive(Debug)]
enum TargetChainTx {
    Ready(MappedTx),
    AwaitingNonce(TxAwaitingNonce),
}

impl TargetChainTx {
    fn set_nonce(&mut self, nonce: Nonce) {
        match self {
            Self::AwaitingNonce(t) => {
                t.target_tx.nonce = nonce;
                let target_tx = SignedTransaction::new(
                    t.target_private.sign(&t.target_tx.get_hash_and_size().0.as_ref()),
                    t.target_tx.clone(),
                );
                *self = Self::Ready(MappedTx {
                    source_signer_id: t.source_signer_id.clone(),
                    source_receiver_id: t.source_receiver_id.clone(),
                    source_tx_index: t.source_tx_index,
                    target_tx,
                    nonce_updates: t.nonce_updates.clone(),
                    sent_successfully: false,
                });
            }
            Self::Ready(_) => unreachable!(),
        };
    }

    // For an AwaitingNonce(_), set the nonce and sign the transaction, changing self into Ready(_).
    // must not be called if self is Ready(_)
    fn try_set_nonce(&mut self, nonce: Option<Nonce>) {
        let nonce = match self {
            Self::AwaitingNonce(t) => match std::cmp::max(t.target_nonce.nonce, nonce) {
                Some(n) => n,
                None => return,
            },
            Self::Ready(_) => unreachable!(),
        };
        self.set_nonce(nonce);
    }

    fn new_ready(
        source_tx: &SignedTransactionView,
        source_tx_index: usize,
        actions: Vec<Action>,
        nonce: Nonce,
        ref_hash: &CryptoHash,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        target_public_key: PublicKey,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        Self::Ready(MappedTx::new(
            source_tx,
            source_tx_index,
            actions,
            nonce,
            &ref_hash,
            target_signer_id,
            target_receiver_id,
            target_secret_key,
            target_public_key,
            nonce_updates,
        ))
    }

    fn new_awaiting_nonce(
        source_tx: &SignedTransactionView,
        source_tx_index: usize,
        actions: Vec<Action>,
        target_nonce: &TargetNonce,
        ref_hash: &CryptoHash,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_private: SecretKey,
        target_public_key: PublicKey,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        Self::AwaitingNonce(TxAwaitingNonce::new(
            source_tx,
            source_tx_index,
            actions,
            target_nonce,
            &ref_hash,
            target_signer_id,
            target_receiver_id,
            target_private,
            target_public_key,
            nonce_updates,
        ))
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
) -> anyhow::Result<bool> {
    match view_client
        .send(
            Query::new(
                BlockReference::Finality(Finality::None),
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
) -> anyhow::Result<Option<Nonce>> {
    match view_client
        .send(
            Query::new(
                BlockReference::Finality(Finality::None),
                QueryRequest::ViewAccessKey {
                    account_id: account_id.clone(),
                    public_key: public_key.clone(),
                },
            )
            .with_span_context(),
        )
        .await
        .unwrap()
    {
        Ok(res) => match res.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(Some(access_key.nonce)),
            other => {
                panic!("Received unexpected QueryResponse after Querying Access Key: {:?}", other);
            }
        },
        Err(e) => match &e {
            QueryError::UnknownAccessKey { .. } => Ok(None),
            _ => Err(e.into()),
        },
    }
}

#[derive(Clone, Debug)]
enum TxOutcome {
    Unknown,
    TxPending(TxInfo),
    ReceiptPending(ReceiptInfo),
    Success,
    Failure,
}

async fn fetch_outcome(
    view_client: &Addr<ViewClientActor>,
    id: &ChainObjectId,
) -> anyhow::Result<TxOutcome> {
    match id.clone() {
        ChainObjectId::Tx(id) => fetch_tx_outcome(view_client, id).await,
        ChainObjectId::Receipt(id) => fetch_receipt_outcome(view_client, id).await,
    }
}

async fn fetch_tx_outcome(
    view_client: &Addr<ViewClientActor>,
    id: TxInfo,
) -> anyhow::Result<TxOutcome> {
    match view_client
        .send(
            GetExecutionOutcome {
                id: TransactionOrReceiptId::Transaction {
                    transaction_hash: id.hash,
                    sender_id: id.signer_id.clone(),
                },
            }
            .with_span_context(),
        )
        .await
        .unwrap()
    {
        Ok(GetExecutionOutcomeResponse { outcome_proof, .. }) => {
            match outcome_proof.outcome.status {
                ExecutionStatusView::SuccessReceiptId(receipt_id) => {
                    fetch_receipt_outcome(
                        view_client,
                        ReceiptInfo { id: receipt_id, receiver_id: id.receiver_id.clone() },
                    )
                    .await
                }
                ExecutionStatusView::SuccessValue(_) => unreachable!(),
                ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => {
                    Ok(TxOutcome::Failure)
                }
            }
        }
        Err(GetExecutionOutcomeError::UnknownTransactionOrReceipt { .. }) => Ok(TxOutcome::Unknown),
        Err(
            GetExecutionOutcomeError::NotConfirmed { .. }
            | GetExecutionOutcomeError::UnknownBlock { .. },
        ) => Ok(TxOutcome::TxPending(id)),
        Err(e) => Err(e).with_context(|| format!("failed fetching outcome for tx {}", &id.hash)),
    }
}

async fn fetch_receipt_outcome(
    view_client: &Addr<ViewClientActor>,
    id: ReceiptInfo,
) -> anyhow::Result<TxOutcome> {
    match view_client
        .send(
            GetExecutionOutcome {
                id: TransactionOrReceiptId::Receipt {
                    receipt_id: id.id.clone(),
                    receiver_id: id.receiver_id.clone(),
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
                    Ok(TxOutcome::Success)
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
        ) => Ok(TxOutcome::ReceiptPending(id)),
        Err(e) => Err(e).with_context(|| format!("failed fetching outcome for receipt {}", &id.id)),
    }
}

impl<T: ChainAccess> TxMirror<T> {
    fn new<P: AsRef<Path>>(
        source_chain_access: T,
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
        let target_indexer = Indexer::new(near_indexer::IndexerConfig {
            home_dir: target_home.as_ref().to_path_buf(),
            sync_mode: near_indexer::SyncModeEnum::LatestSynced,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        })
        .context("failed to start target chain indexer")?;
        let (target_view_client, target_client) = target_indexer.client_actors();
        let target_stream = target_indexer.streamer();

        Ok(Self {
            source_chain_access,
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
        })
    }

    fn get_next_source_height(&mut self) -> anyhow::Result<BlockHeight> {
        let height =
            self.db.get_cf(self.db.cf_handle(DBCol::Misc.name()).unwrap(), "next_source_height")?;
        match height {
            Some(h) => Ok(BlockHeight::try_from_slice(&h).unwrap()),
            None => Ok(self.target_genesis_height),
        }
    }

    async fn send_transactions(&mut self, block: &mut MappedBlock) -> anyhow::Result<()> {
        for chunk in block.chunks.iter_mut() {
            for tx in chunk.txs.iter_mut() {
                match tx {
                    TargetChainTx::Ready(tx) => {
                        match self
                            .target_client
                            .send(
                                ProcessTxRequest {
                                    transaction: tx.target_tx.clone(),
                                    is_forwarded: false,
                                    check_only: false,
                                }
                                .with_span_context(),
                            )
                            .await?
                        {
                            ProcessTxResponse::RequestRouted => {
                                crate::metrics::TRANSACTIONS_SENT.with_label_values(&["ok"]).inc();
                                tx.sent_successfully = true;
                            }
                            ProcessTxResponse::InvalidTx(e) => {
                                // TODO: here if we're getting an error because the tx was already included, it is possible
                                // that some other instance of this code ran and made progress already. For now we can assume
                                // only once instance of this code will run, but this is the place to detect if that's not the case.
                                tracing::error!(
                                    target: "mirror", "Tried to send an invalid tx for ({}, {:?}) from source #{} shard {} tx {}: {:?}",
                                    &tx.target_tx.transaction.signer_id, &tx.target_tx.transaction.public_key, block.source_height, chunk.shard_id, tx.source_tx_index, e
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
                        tracing::warn!(
                            target: "mirror", "skipped sending transaction for ({}, {:?}) because valid target chain nonce not known",
                            &tx.target_tx.signer_id, &tx.target_tx.public_key
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn map_actions(
        &self,
        tx: &SignedTransactionView,
    ) -> anyhow::Result<(Vec<Action>, HashSet<(AccountId, PublicKey)>)> {
        let mut actions = Vec::new();
        let mut nonce_updates = HashSet::new();

        for a in tx.actions.iter() {
            // this try_from() won't fail since the ActionView was constructed from the Action
            let action = Action::try_from(a.clone()).unwrap();

            match &action {
                Action::AddKey(add_key) => {
                    let public_key =
                        crate::key_mapping::map_key(&add_key.public_key, self.secret.as_ref())
                            .public_key();
                    let receiver_id =
                        crate::key_mapping::map_account(&tx.receiver_id, self.secret.as_ref());

                    nonce_updates.insert((receiver_id, public_key.clone()));
                    actions.push(Action::AddKey(AddKeyAction {
                        public_key,
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
                    if tx.receiver_id.is_implicit() && tx.actions.len() == 1 {
                        let target_account =
                            crate::key_mapping::map_account(&tx.receiver_id, self.secret.as_ref());
                        if !account_exists(&self.target_view_client, &target_account)
                            .await
                            .with_context(|| {
                                format!("failed checking existence for account {}", &tx.receiver_id)
                            })?
                        {
                            let public_key =
                                crate::key_mapping::implicit_account_key(&target_account);
                            nonce_updates.insert((target_account, public_key));
                        }
                    }
                    actions.push(action);
                }
                // We don't want to mess with the set of validators in the target chain
                Action::Stake(_) => {}
                _ => actions.push(action),
            };
        }
        Ok((actions, nonce_updates))
    }

    // fetch the source chain block at `source_height`, and prepare a
    // set of transactions that should be valid in the target chain
    // from it.
    async fn fetch_txs(
        &self,
        source_height: BlockHeight,
        ref_hash: CryptoHash,
        tracker: &mut crate::chain_tracker::TxTracker,
    ) -> anyhow::Result<Option<MappedBlock>> {
        let source_chunks =
            match self.source_chain_access.get_txs(source_height, &self.tracked_shards).await {
                Ok(x) => x,
                Err(e) => match e {
                    ChainError::UnknownBlock => return Ok(None),
                    ChainError::Other(e) => return Err(e),
                },
            };

        let mut num_not_ready = 0;
        let mut chunks = Vec::new();
        for ch in source_chunks {
            let mut txs = Vec::new();

            for (idx, source_tx) in ch.transactions.into_iter().enumerate() {
                let (actions, tx_nonce_updates) = self.map_actions(&source_tx).await?;
                if actions.is_empty() {
                    // If this is a tx containing only stake actions, skip it.
                    continue;
                }
                let mapped_key =
                    crate::key_mapping::map_key(&source_tx.public_key, self.secret.as_ref());
                let public_key = mapped_key.public_key();

                let target_signer_id =
                    crate::key_mapping::map_account(&source_tx.signer_id, self.secret.as_ref());
                let target_receiver_id =
                    crate::key_mapping::map_account(&source_tx.receiver_id, self.secret.as_ref());

                let target_nonce = tracker
                    .next_nonce(
                        &self.target_view_client,
                        &self.db,
                        &target_signer_id,
                        &public_key,
                        source_height,
                    )
                    .await?;
                if target_nonce.pending_outcomes.is_empty() {
                    match target_nonce.nonce {
                        Some(nonce) => {
                            let target_tx = TargetChainTx::new_ready(
                                &source_tx,
                                idx,
                                actions,
                                nonce,
                                &ref_hash,
                                target_signer_id,
                                target_receiver_id,
                                &mapped_key,
                                public_key,
                                tx_nonce_updates,
                            );
                            txs.push(target_tx);
                        }
                        None => {
                            num_not_ready += 1;
                            let target_tx = TargetChainTx::new_awaiting_nonce(
                                &source_tx,
                                idx,
                                actions,
                                target_nonce,
                                &ref_hash,
                                target_signer_id,
                                target_receiver_id,
                                mapped_key,
                                public_key,
                                tx_nonce_updates,
                            );
                            txs.push(target_tx);
                        }
                    }
                } else {
                    num_not_ready += 1;
                    let target_tx = TargetChainTx::new_awaiting_nonce(
                        &source_tx,
                        idx,
                        actions,
                        target_nonce,
                        &ref_hash,
                        target_signer_id,
                        target_receiver_id,
                        mapped_key,
                        public_key,
                        tx_nonce_updates,
                    );
                    txs.push(target_tx);
                }
            }
            if num_not_ready == 0 {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {}",
                    txs.len(), source_height, ch.shard_id
                );
            } else {
                tracing::debug!(
                    target: "mirror", "prepared {} transacations for source chain #{} shard {} with {} \
                    still waiting for the corresponding access keys to make it on chain",
                    txs.len(), source_height, ch.shard_id, num_not_ready,
                );
            }
            chunks.push(MappedChunk { txs, shard_id: ch.shard_id });
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
        let source_head = self
            .source_chain_access
            .head_height()
            .await
            .context("can't fetch source chain HEAD")?;
        let start_height = match tracker.height_queued() {
            Some(h) => h + 1,
            None => self.get_next_source_height()?,
        };

        for height in start_height..=source_head {
            if let Some(b) = self
                .fetch_txs(height, ref_hash, tracker)
                .await
                .with_context(|| format!("Can't fetch source #{} transactions", height))?
            {
                tracker.queue_block(b, &self.target_view_client, &self.db).await?;
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

    async fn main_loop(
        &mut self,
        mut tracker: crate::chain_tracker::TxTracker,
        mut target_height: BlockHeight,
        mut target_head: CryptoHash,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                // time to send a batch of transactions
                mapped_block = tracker.next_batch(&self.target_view_client, &self.db), if tracker.num_blocks_queued() > 0 => {
                    self.send_transactions(mapped_block?).await?;
                    tracker.on_txs_sent(&self.target_view_client, &self.db, target_height).await?;

                    // now we have one second left until we need to send more transactions. In the
                    // meantime, we might as well prepare some more batches of transactions.
                    // TODO: continue in best effort fashion on error
                    self.queue_txs(&mut tracker, target_head, true).await?;
                }
                msg = self.target_stream.recv() => {
                    let msg = msg.unwrap();
                    target_head = msg.block.header.hash;
                    target_height = msg.block.header.height;
                    tracker.on_target_block(&self.target_view_client, &self.db, msg).await?;
                }
                // If we don't have any upcoming sets of transactions to send already built, we probably fell behind in the source
                // chain and can't fetch the transactions. Check if we have them now here.
                _ = tokio::time::sleep(Duration::from_millis(200)), if tracker.num_blocks_queued() == 0 => {
                    self.queue_txs(&mut tracker, target_head, true).await?;
                }
            };
            if tracker.finished() {
                tracing::info!(target: "mirror", "finished sending all transactions");
                return Ok(());
            }
        }
    }

    async fn wait_target_synced(&mut self) -> (BlockHeight, CryptoHash) {
        let msg = self.target_stream.recv().await.unwrap();
        (msg.block.header.height, msg.block.header.hash)
    }

    async fn run(mut self, stop_height: Option<BlockHeight>) -> anyhow::Result<()> {
        let mut tracker = crate::chain_tracker::TxTracker::new(
            self.target_min_block_production_delay,
            stop_height,
        );
        self.source_chain_access.init().await?;

        let (target_height, target_head) = self.wait_target_synced().await;

        self.queue_txs(&mut tracker, target_head, false).await?;

        self.main_loop(tracker, target_height, target_head).await
    }
}

async fn run<P: AsRef<Path>>(
    source_home: P,
    target_home: P,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
    stop_height: Option<BlockHeight>,
    online_source: bool,
) -> anyhow::Result<()> {
    if !online_source {
        let source_chain_access = crate::offline::ChainAccess::new(source_home)?;
        let stop_height = stop_height.unwrap_or(source_chain_access.head_height().await?);
        TxMirror::new(source_chain_access, target_home, secret)?.run(Some(stop_height)).await
    } else {
        TxMirror::new(crate::online::ChainAccess::new(source_home)?, target_home, secret)?
            .run(stop_height)
            .await
    }
}

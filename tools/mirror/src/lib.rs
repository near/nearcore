use actix::Addr;
use anyhow::Context;
use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_configs::GenesisValidationMode;
use near_chain_primitives::error::QueryError as RuntimeQueryError;
use near_client::{ClientActor, ViewClientActor};
use near_client::{ProcessTxRequest, ProcessTxResponse};
use near_client_primitives::types::{
    GetBlock, GetBlockError, GetChunkError, GetExecutionOutcomeError, GetReceiptError, Query,
    QueryError, Status,
};
use near_crypto::{PublicKey, SecretKey};
use near_indexer::{Indexer, StreamerMessage};
use near_o11y::WithSpanContextExt;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    SignedTransaction, StakeAction, Transaction,
};
use near_primitives::types::{
    AccountId, BlockHeight, BlockReference, Finality, TransactionOrReceiptId,
};
use near_primitives::views::{
    ExecutionOutcomeWithIdView, ExecutionStatusView, QueryRequest, QueryResponseKind,
};
use near_primitives_core::account::id::AccountType;
use near_primitives_core::account::{AccessKey, AccessKeyPermission};
use near_primitives_core::types::{Nonce, ShardId};
use rocksdb::DB;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use strum::IntoEnumIterator;
use tokio::sync::mpsc;

mod chain_tracker;
pub mod cli;
pub mod genesis;
pub mod key_mapping;
mod key_util;
mod metrics;
mod offline;
mod online;
pub mod secret;

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
    ChainObjectId(CryptoHash),
}

// returns bytes that serve as the key corresponding to this pair in the Nonces column
fn nonce_col_key(account_id: &AccountId, public_key: &PublicKey) -> Vec<u8> {
    borsh::to_vec(&(account_id.clone(), public_key.clone())).unwrap()
}

// this serves a similar purpose to `LatestTargetNonce`. The difference is
// that this one keeps track of what's in memory. So for example if the last
// height we sent transactions for was 10, and we have a set of transactions
// queued up for height 12, one of which is an AddKey for ('foo.near', 'ed25519:...'),
// then we'lll want to remember that for txs of height > 12 that use that
// signer id and public key, but we don't want to store that on disk. The `LatestTargetNonce`
// on disk will only record the transactions/receipts updating the nonce that we actually sent
// or saw on chain

#[derive(Clone, Debug, Default)]
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
    pending_outcomes: HashSet<CryptoHash>,
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
    tracing::trace!(target: "mirror", "storing {:?} in DB for ({}, {:?})", &nonce, account_id, public_key);
    let db_key = nonce_col_key(account_id, public_key);
    db.put_cf(
        db.cf_handle(DBCol::Nonces.name()).unwrap(),
        &db_key,
        &borsh::to_vec(&nonce).unwrap(),
    )?;
    Ok(())
}

// we store a value (empty HashSet if it affects no access keys) in the database for any tx we send,
// as well as all generated descendant receipts. Once we see it on chain, we update any access keys
// that might be updated by it, and we insert any generated receipts in the DB. When we see an
// execution outcome that adds stake actions, the presence/absence of that receipt's ID in the DB
// will tell us whether it resulted from one of our txs that we mirrored from the source so that
// we can send a stake tx to reverse it

fn read_pending_outcome(
    db: &DB,
    id: &CryptoHash,
) -> anyhow::Result<Option<HashSet<(AccountId, PublicKey)>>> {
    Ok(db
        .get_cf(
            db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(),
            &borsh::to_vec(&id).unwrap(),
        )?
        .map(|v| HashSet::try_from_slice(&v).unwrap()))
}

fn put_pending_outcome(
    db: &DB,
    id: CryptoHash,
    access_keys: HashSet<(AccountId, PublicKey)>,
) -> anyhow::Result<()> {
    tracing::trace!(target: "mirror", "storing {:?} in DB for {:?}", &access_keys, &id);
    Ok(db.put_cf(
        db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(),
        &borsh::to_vec(&id).unwrap(),
        &borsh::to_vec(&access_keys).unwrap(),
    )?)
}

fn delete_pending_outcome(db: &DB, id: &CryptoHash) -> anyhow::Result<()> {
    tracing::trace!(target: "mirror", "deleting {:?} from DB", &id);
    Ok(db.delete_cf(
        db.cf_handle(DBCol::AccessKeyOutcomes.name()).unwrap(),
        &borsh::to_vec(&id).unwrap(),
    )?)
}

fn set_last_source_height(db: &DB, height: BlockHeight) -> anyhow::Result<()> {
    // TODO: we should instead save something like the
    // (block_height, shard_id, idx_in_chunk) of the last
    // transaction sent. Currently we set last_source_height after
    // sending all of the transactions in that chunk, so if we get
    // SIGTERM or something in the middle of sending a batch of
    // txs, we'll send some that we already sent next time we
    // start. Not a giant problem but kind of unclean.
    db.put_cf(
        db.cf_handle(DBCol::Misc.name()).unwrap(),
        "last_source_height",
        borsh::to_vec(&height).unwrap(),
    )?;
    Ok(())
}

fn get_last_source_height(db: &DB) -> anyhow::Result<Option<BlockHeight>> {
    Ok(db
        .get_cf(db.cf_handle(DBCol::Misc.name()).unwrap(), "last_source_height")?
        .map(|v| BlockHeight::try_from_slice(&v).unwrap()))
}

struct SourceChunk {
    shard_id: ShardId,
    transactions: Vec<SignedTransaction>,
    receipts: Vec<Receipt>,
}

struct SourceBlock {
    hash: CryptoHash,
    chunks: Vec<SourceChunk>,
}

#[derive(thiserror::Error, Debug)]
enum ChainError {
    #[error("unknown")]
    Unknown,
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
            GetBlockError::UnknownBlock { .. } => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<GetChunkError> for ChainError {
    fn from(err: GetChunkError) -> Self {
        match err {
            GetChunkError::UnknownBlock { .. } => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<near_chain_primitives::Error> for ChainError {
    fn from(err: near_chain_primitives::Error) -> Self {
        match err {
            near_chain_primitives::Error::DBNotFoundErr(_) => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<GetExecutionOutcomeError> for ChainError {
    fn from(err: GetExecutionOutcomeError) -> Self {
        match err {
            GetExecutionOutcomeError::UnknownBlock { .. }
            | GetExecutionOutcomeError::UnknownTransactionOrReceipt { .. }
            | GetExecutionOutcomeError::NotConfirmed { .. } => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<GetReceiptError> for ChainError {
    fn from(err: GetReceiptError) -> Self {
        match err {
            GetReceiptError::UnknownReceipt(_) => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<QueryError> for ChainError {
    fn from(err: QueryError) -> Self {
        match err {
            QueryError::UnavailableShard { .. }
            | QueryError::UnknownAccount { .. }
            | QueryError::NoContractCode { .. }
            | QueryError::UnknownAccessKey { .. }
            | QueryError::GarbageCollectedBlock { .. }
            | QueryError::UnknownBlock { .. } => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

impl From<RuntimeQueryError> for ChainError {
    fn from(err: RuntimeQueryError) -> Self {
        match err {
            RuntimeQueryError::UnknownAccount { .. }
            | RuntimeQueryError::NoContractCode { .. }
            | RuntimeQueryError::UnknownAccessKey { .. } => Self::Unknown,
            _ => Self::other(err),
        }
    }
}

#[async_trait(?Send)]
trait ChainAccess {
    // this should return the first `num_initial_blocks` valid block heights in the
    // chain after last_height. It should only return fewer than that if there are no
    // more, which only happens if we're not starting a source node (--online-source is
    // not given), and the last height we sent txs for is close to the HEAD of the chain
    async fn init(
        &self,
        last_height: BlockHeight,
        num_initial_blocks: usize,
    ) -> anyhow::Result<Vec<BlockHeight>>;

    async fn block_height_to_hash(&self, height: BlockHeight) -> Result<CryptoHash, ChainError>;

    async fn head_height(&self) -> Result<BlockHeight, ChainError>;

    async fn get_txs(&self, height: BlockHeight) -> Result<SourceBlock, ChainError>;

    async fn get_next_block_height(&self, height: BlockHeight) -> Result<BlockHeight, ChainError>;

    async fn get_outcome(
        &self,
        id: TransactionOrReceiptId,
    ) -> Result<ExecutionOutcomeWithIdView, ChainError>;

    // for transactions with the same signer and receiver, we
    // want the receipt that results from applying it, if successful,
    // since that receipt doesn't show up if you just look through
    // the receipts in the next block.
    // If the tx wasn't successful, just returns None
    async fn get_tx_receipt_id(
        &self,
        tx_hash: &CryptoHash,
        signer_id: &AccountId,
    ) -> Result<Option<CryptoHash>, ChainError> {
        match self
            .get_outcome(TransactionOrReceiptId::Transaction {
                transaction_hash: *tx_hash,
                sender_id: signer_id.clone(),
            })
            .await?
            .outcome
            .status
        {
            ExecutionStatusView::SuccessReceiptId(id) => Ok(Some(id)),
            ExecutionStatusView::Failure(_) | ExecutionStatusView::Unknown => Ok(None),
            ExecutionStatusView::SuccessValue(_) => unreachable!(),
        }
    }

    async fn get_receipt(&self, id: &CryptoHash) -> Result<Arc<Receipt>, ChainError>;

    // returns all public keys with full permissions for the given account
    async fn get_full_access_keys(
        &self,
        account_id: &AccountId,
        block_hash: &CryptoHash,
    ) -> Result<Vec<PublicKey>, ChainError>;
}

fn execution_status_good(status: &ExecutionStatusView) -> bool {
    matches!(
        status,
        ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_)
    )
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct MirrorConfig {
    /// Delay between sending batches of mainnet transactions. If this is
    /// given, then instead of trying to get the transactions in consecutive
    /// mainnet blocks to appear in consecutive target chain blocks, we will
    /// wait this long before sending each mainnet block's worth of transactions.
    /// TODO: add an option to target a specific number of transactions per second
    tx_batch_interval: Option<Duration>,
}

const CREATE_ACCOUNT_DELTA: usize = 5;

// TODO: separate out the code that uses the target chain clients, and
// make it an option to send the transactions to some RPC node.
// that way it would be possible to run this code and send transactions with an
// old binary not caught up to the current protocol version, since the
// transactions we're constructing should stay valid.
struct TxMirror<T: ChainAccess> {
    source_chain_access: T,
    db: Arc<DB>,
    target_genesis_height: BlockHeight,
    target_min_block_production_delay: Duration,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
    default_extra_key: SecretKey,
    config: MirrorConfig,
}

fn open_db<P: AsRef<Path>>(home: P) -> anyhow::Result<DB> {
    let mut options = rocksdb::Options::default();
    options.create_missing_column_families(true);
    options.create_if_missing(true);
    let cf_descriptors = DBCol::iter()
        .map(|col| rocksdb::ColumnFamilyDescriptor::new(col.name(), options.clone()))
        .collect::<Vec<_>>();
    Ok(DB::open_cf_descriptors(&options, home.as_ref(), cf_descriptors)?)
}

#[derive(Clone, Copy, Debug)]
enum MappedTxProvenance {
    MappedSourceTx(BlockHeight, ShardId, usize),
    TxAddKey(BlockHeight, ShardId, usize),
    ReceiptAddKey(BlockHeight, ShardId, usize),
    TxCreateAccount(BlockHeight, ShardId, usize),
    ReceiptCreateAccount(BlockHeight, ShardId, usize),
    Unstake(CryptoHash),
}

impl MappedTxProvenance {
    fn is_create_account(&self) -> bool {
        matches!(
            self,
            MappedTxProvenance::TxCreateAccount(_, _, _)
                | MappedTxProvenance::ReceiptCreateAccount(_, _, _)
        )
    }

    fn is_add_key(&self) -> bool {
        matches!(
            self,
            MappedTxProvenance::TxAddKey(_, _, _) | MappedTxProvenance::ReceiptAddKey(_, _, _)
        )
    }

    fn is_unstake(&self) -> bool {
        matches!(self, MappedTxProvenance::Unstake(_))
    }
}

impl std::fmt::Display for MappedTxProvenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MappedSourceTx(height, shard_id, idx) => {
                write!(f, "source #{} shard {} tx #{}", height, shard_id, idx)
            }
            Self::TxAddKey(height, shard_id, idx) => {
                write!(f, "extra AddKey for source #{} shard {} tx #{}", height, shard_id, idx)
            }
            Self::ReceiptAddKey(height, shard_id, idx) => {
                write!(f, "extra AddKey for source #{} shard {} receipt #{}", height, shard_id, idx)
            }
            Self::TxCreateAccount(height, shard_id, idx) => write!(
                f,
                "extra CreateAccount for source #{} shard {} tx #{}",
                height, shard_id, idx
            ),
            Self::ReceiptCreateAccount(height, shard_id, idx) => {
                write!(
                    f,
                    "extra CreateAccount for source #{} shard {} receipt #{}",
                    height, shard_id, idx
                )
            }
            Self::Unstake(hash) => {
                write!(f, "unstake after stake receipt in target block {}", hash,)
            }
        }
    }
}

// a transaction that's almost prepared, except that we don't yet know
// what nonce to use because the public key was added in an AddKey
// action that we haven't seen on chain yet. The target_tx field is complete
// except for the nonce field.
#[derive(Clone, Debug)]
struct TxAwaitingNonce {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    provenance: MappedTxProvenance,
    target_secret_key: SecretKey,
    target_tx: Transaction,
    nonce_updates: HashSet<(AccountId, PublicKey)>,
    target_nonce: TargetNonce,
}

impl TxAwaitingNonce {
    fn new(
        source_signer_id: AccountId,
        source_receiver_id: AccountId,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: SecretKey,
        target_public_key: PublicKey,
        actions: Vec<Action>,
        target_nonce: &TargetNonce,
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        let mut target_tx = Transaction::new_v0(
            target_signer_id,
            target_public_key,
            target_receiver_id,
            0,
            *ref_hash,
        );
        *target_tx.actions_mut() = actions;
        Self {
            source_signer_id,
            source_receiver_id,
            provenance,
            target_secret_key,
            target_tx,
            nonce_updates,
            target_nonce: target_nonce.clone(),
        }
    }
}

// A transaction meant for the target chain that is complete/ready to send.
// We keep some extra info about the transaction for the purposes of logging
// later on when we find it on chain.
#[derive(Clone, Debug)]
struct MappedTx {
    source_signer_id: AccountId,
    source_receiver_id: AccountId,
    provenance: MappedTxProvenance,
    target_tx: SignedTransaction,
    nonce_updates: HashSet<(AccountId, PublicKey)>,
    sent_successfully: bool,
}

impl MappedTx {
    fn new(
        source_signer_id: AccountId,
        source_receiver_id: AccountId,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        target_public_key: PublicKey,
        actions: Vec<Action>,
        nonce: Nonce,
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        let mut target_tx = Transaction::new_v0(
            target_signer_id,
            target_public_key,
            target_receiver_id,
            nonce,
            *ref_hash,
        );
        *target_tx.actions_mut() = actions;
        let target_tx = SignedTransaction::new(
            target_secret_key.sign(&target_tx.get_hash_and_size().0.as_ref()),
            target_tx,
        );
        Self {
            source_signer_id,
            source_receiver_id,
            provenance,
            target_tx,
            nonce_updates,
            sent_successfully: false,
        }
    }

    fn inc_nonce(&mut self, target_secret_key: &SecretKey) {
        let mut tx = self.target_tx.transaction.clone();
        *tx.nonce_mut() += 1;
        self.target_tx =
            SignedTransaction::new(target_secret_key.sign(&tx.get_hash_and_size().0.as_ref()), tx);
    }
}

#[derive(Clone, Debug)]
enum TargetChainTx {
    Ready(MappedTx),
    AwaitingNonce(TxAwaitingNonce),
}

impl TargetChainTx {
    fn set_nonce(&mut self, nonce: Nonce) {
        match self {
            Self::AwaitingNonce(t) => {
                *t.target_tx.nonce_mut() = nonce;
                let target_tx = SignedTransaction::new(
                    t.target_secret_key.sign(&t.target_tx.get_hash_and_size().0.as_ref()),
                    t.target_tx.clone(),
                );
                *self = Self::Ready(MappedTx {
                    source_signer_id: t.source_signer_id.clone(),
                    source_receiver_id: t.source_receiver_id.clone(),
                    provenance: t.provenance,
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
        source_signer_id: AccountId,
        source_receiver_id: AccountId,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        target_public_key: PublicKey,
        actions: Vec<Action>,
        nonce: Nonce,
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        Self::Ready(MappedTx::new(
            source_signer_id,
            source_receiver_id,
            target_signer_id,
            target_receiver_id,
            target_secret_key,
            target_public_key,
            actions,
            nonce,
            ref_hash,
            provenance,
            nonce_updates,
        ))
    }

    fn new_awaiting_nonce(
        source_signer_id: AccountId,
        source_receiver_id: AccountId,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        target_public_key: PublicKey,
        actions: Vec<Action>,
        target_nonce: &TargetNonce,
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> Self {
        Self::AwaitingNonce(TxAwaitingNonce::new(
            source_signer_id,
            source_receiver_id,
            target_signer_id,
            target_receiver_id,
            target_secret_key.clone(),
            target_public_key,
            actions,
            target_nonce,
            &ref_hash,
            provenance,
            nonce_updates,
        ))
    }

    fn target_nonce(&self) -> TargetNonce {
        match self {
            Self::Ready(t) => TargetNonce {
                nonce: Some(t.target_tx.transaction.nonce()),
                pending_outcomes: HashSet::new(),
            },
            Self::AwaitingNonce(t) => t.target_nonce.clone(),
        }
    }

    fn inc_target_nonce(&mut self, target_secret_key: &SecretKey) {
        match self {
            Self::Ready(t) => t.inc_nonce(target_secret_key),
            Self::AwaitingNonce(t) => {
                if let Some(n) = &mut t.target_nonce.nonce {
                    *n += 1;
                }
            }
        }
    }
}

#[derive(Debug)]
struct MappedChunk {
    txs: Vec<TargetChainTx>,
    shard_id: ShardId,
}

// TODO: get rid of this struct and just store the transactions more simply in TxTracker
#[derive(Debug)]
struct MappedBlock {
    source_height: BlockHeight,
    source_hash: CryptoHash,
    chunks: Vec<MappedChunk>,
}

#[derive(Debug)]
struct TxBatch {
    source_height: BlockHeight,
    source_hash: CryptoHash,
    txs: Vec<(TxRef, TargetChainTx)>,
}

impl From<&MappedBlock> for TxBatch {
    fn from(block: &MappedBlock) -> Self {
        Self {
            source_height: block.source_height,
            source_hash: block.source_hash,
            txs: block
                .chunks
                .iter()
                .flat_map(|c| {
                    c.txs.iter().enumerate().map(move |(tx_idx, tx)| {
                        (
                            TxRef {
                                source_height: block.source_height,
                                shard_id: c.shard_id,
                                tx_idx,
                            },
                            tx.clone(),
                        )
                    })
                })
                .collect(),
        }
    }
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

impl<T: ChainAccess> TxMirror<T> {
    fn new(
        source_chain_access: T,
        target_home: &Path,
        mirror_db_path: Option<&Path>,
        secret: Option<[u8; crate::secret::SECRET_LEN]>,
        config: MirrorConfig,
    ) -> anyhow::Result<Self> {
        let target_config =
            nearcore::config::load_config(target_home, GenesisValidationMode::UnsafeFast)
                .with_context(|| format!("Error loading target config from {:?}", target_home))?;
        if !target_config.client_config.archive {
            // this is probably not going to come up, but we want to avoid a situation where
            // we go offline for a long time and then come back online, and we state sync to
            // the head of the target chain without looking for our outcomes that made it on
            // chain right before we went offline
            anyhow::bail!("config file in {} has archive: false, but archive must be set to true for the target chain", target_home.display());
        }
        let db = match mirror_db_path {
            Some(mirror_db_path) => open_db(mirror_db_path),
            None => {
                // keep backward compatibility
                let mirror_db_path = near_store::NodeStorage::opener(
                    target_home,
                    target_config.config.archive,
                    &target_config.config.store,
                    None,
                )
                .path()
                .join("mirror");
                open_db(&mirror_db_path)
            }
        };
        let db = db.context("failed to open mirror DB")?;
        let db = Arc::new(db);
        let default_extra_key = crate::key_mapping::default_extra_key(secret.as_ref());

        Ok(Self {
            source_chain_access,
            db,
            target_genesis_height: target_config.genesis.config.genesis_height,
            target_min_block_production_delay: target_config
                .client_config
                .min_block_production_delay
                .unsigned_abs(),
            secret,
            default_extra_key,
            config,
        })
    }

    async fn send_transactions<'a, I: Iterator<Item = &'a mut TargetChainTx>>(
        target_client: &Addr<ClientActor>,
        txs: I,
    ) -> anyhow::Result<()> {
        for tx in txs {
            match tx {
                TargetChainTx::Ready(tx) => {
                    match target_client
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
                                target: "mirror", "Tried to send an invalid tx for ({}, {:?}) from {}: {:?}",
                                tx.target_tx.transaction.signer_id(), tx.target_tx.transaction.public_key(), &tx.provenance, e
                            );
                            crate::metrics::TRANSACTIONS_SENT.with_label_values(&["invalid"]).inc();
                        }
                        r => {
                            tracing::error!(
                                target: "mirror", "Unexpected response sending tx from {}: {:?}. The transaction was not sent",
                                &tx.provenance, r
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
                        tx.target_tx.signer_id(), tx.target_tx.public_key()
                    );
                }
            }
        }
        Ok(())
    }

    async fn map_actions(
        &self,
        target_view_client: &Addr<ViewClientActor>,
        tx: &SignedTransaction,
    ) -> anyhow::Result<(Vec<Action>, HashSet<(AccountId, PublicKey)>)> {
        let mut actions = Vec::new();
        let mut nonce_updates = HashSet::new();

        let mut account_created = false;
        let mut full_key_added = false;
        for action in tx.transaction.actions().iter() {
            match &action {
                Action::AddKey(add_key) => {
                    if add_key.access_key.permission == AccessKeyPermission::FullAccess {
                        full_key_added = true;
                    }
                    let public_key =
                        crate::key_mapping::map_key(&add_key.public_key, self.secret.as_ref())
                            .public_key();
                    let receiver_id = crate::key_mapping::map_account(
                        &tx.transaction.receiver_id(),
                        self.secret.as_ref(),
                    );

                    nonce_updates.insert((receiver_id, public_key.clone()));
                    actions.push(Action::AddKey(Box::new(AddKeyAction {
                        public_key,
                        access_key: add_key.access_key.clone(),
                    })));
                }
                Action::DeleteKey(delete_key) => {
                    let replacement =
                        crate::key_mapping::map_key(&delete_key.public_key, self.secret.as_ref());
                    let public_key = replacement.public_key();

                    actions.push(Action::DeleteKey(Box::new(DeleteKeyAction { public_key })));
                }
                Action::Transfer(_) => {
                    // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                    if tx.transaction.receiver_id().get_account_type()
                        == AccountType::NearImplicitAccount
                        && tx.transaction.actions().len() == 1
                    {
                        let target_account = crate::key_mapping::map_account(
                            &tx.transaction.receiver_id(),
                            self.secret.as_ref(),
                        );
                        if !account_exists(target_view_client, &target_account).await.with_context(
                            || format!("failed checking existence for account {}", &target_account),
                        )? {
                            if target_account.get_account_type() == AccountType::NearImplicitAccount
                            {
                                let public_key =
                                    PublicKey::from_near_implicit_account(&target_account)
                                        .expect("must be near-implicit");
                                nonce_updates.insert((target_account, public_key));
                            }
                        }
                    }
                    actions.push(action.clone());
                }
                // We don't want to mess with the set of validators in the target chain
                Action::Stake(_) => {}
                Action::CreateAccount(_) => {
                    account_created = true;
                    actions.push(action.clone());
                }
                Action::DeleteAccount(d) => {
                    actions.push(Action::DeleteAccount(DeleteAccountAction {
                        beneficiary_id: crate::key_mapping::map_account(
                            &d.beneficiary_id,
                            self.secret.as_ref(),
                        ),
                    }));
                }
                // TODO: handle delegate actions
                _ => actions.push(action.clone()),
            };
        }
        if account_created && !full_key_added {
            actions.push(Action::AddKey(Box::new(AddKeyAction {
                public_key: self.default_extra_key.public_key(),
                access_key: AccessKey::full_access(),
            })));
        }
        Ok((actions, nonce_updates))
    }

    async fn prepare_tx(
        &self,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        source_signer_id: AccountId,
        source_receiver_id: AccountId,
        target_signer_id: AccountId,
        target_receiver_id: AccountId,
        target_secret_key: &SecretKey,
        actions: Vec<Action>,
        ref_hash: &CryptoHash,
        source_height: Option<BlockHeight>,
        provenance: MappedTxProvenance,
        nonce_updates: HashSet<(AccountId, PublicKey)>,
    ) -> anyhow::Result<TargetChainTx> {
        let target_public_key = target_secret_key.public_key();
        let target_nonce = match source_height.as_ref() {
            Some(_) => None,
            None => Some(
                tracker
                    .insert_nonce(
                        tx_block_queue,
                        target_view_client,
                        &self.db,
                        &target_signer_id,
                        &target_public_key,
                        target_secret_key,
                    )
                    .await?,
            ),
        };
        let target_nonce = match source_height {
            Some(source_height) => {
                tracker
                    .next_nonce(
                        target_view_client,
                        &self.db,
                        &target_signer_id,
                        &target_public_key,
                        source_height,
                    )
                    .await?
            }
            None => target_nonce.as_ref().unwrap(),
        };

        if target_nonce.pending_outcomes.is_empty() && target_nonce.nonce.is_some() {
            Ok(TargetChainTx::new_ready(
                source_signer_id,
                source_receiver_id,
                target_signer_id,
                target_receiver_id,
                &target_secret_key,
                target_public_key,
                actions,
                target_nonce.nonce.unwrap(),
                &ref_hash,
                provenance,
                nonce_updates,
            ))
        } else {
            Ok(TargetChainTx::new_awaiting_nonce(
                source_signer_id,
                source_receiver_id,
                target_signer_id,
                target_receiver_id,
                target_secret_key,
                target_public_key,
                actions,
                target_nonce,
                &ref_hash,
                provenance,
                nonce_updates,
            ))
        }
    }

    // add extra AddKey transactions that come from function call. If we don't do this,
    // then the only keys we will have mapped are the ones added by regular AddKey transactions.
    async fn push_extra_tx(
        &self,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        block_hash: CryptoHash,
        txs: &mut Vec<TargetChainTx>,
        predecessor_id: AccountId,
        receiver_id: AccountId,
        actions: &[Action],
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        source_height: Option<BlockHeight>,
    ) -> anyhow::Result<()> {
        let target_signer_id =
            crate::key_mapping::map_account(&predecessor_id, self.secret.as_ref());

        let target_secret_key = match self
            .source_chain_access
            .get_full_access_keys(&predecessor_id, &block_hash)
            .await
        {
            Ok(keys) => {
                let mut key = None;
                let mut first_key = None;
                for k in keys.iter() {
                    let target_secret_key = crate::key_mapping::map_key(k, self.secret.as_ref());
                    if fetch_access_key_nonce(
                        target_view_client,
                        &target_signer_id,
                        &target_secret_key.public_key(),
                    )
                    .await?
                    .is_some()
                    {
                        key = Some(target_secret_key);
                        break;
                    }
                    if first_key.is_none() {
                        first_key = Some(target_secret_key);
                    }
                }
                // here none of them have equivalents in the target chain. Just use the first one and hope that
                // it will become available. shouldn't happen much in practice
                if key.is_none() {
                    if let Some(k) = first_key {
                        tracing::warn!(
                            target: "mirror", "preparing a transaction for {} for signer {} with key {} even though it is not yet known in the target chain",
                            &provenance, &target_signer_id, &k.public_key(),
                        );
                        key = Some(k);
                    }
                }
                match key {
                    Some(key) => key,
                    None => {
                        tracing::debug!(
                            target: "mirror", "trying to prepare a transaction with the default extra key for {} because no full access key for {} in the source chain is known at block {}",
                            &provenance, &target_signer_id, &block_hash,
                        );
                        self.default_extra_key.clone()
                    }
                }
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("failed fetching access key for {}", &predecessor_id))
            }
        };

        let target_receiver_id =
            crate::key_mapping::map_account(&receiver_id, self.secret.as_ref());

        let mut nonce_updates = HashSet::new();
        let mut target_actions = Vec::new();
        let mut full_key_added = false;
        let mut account_created = false;

        for a in actions {
            match a {
                Action::AddKey(a) => {
                    if a.access_key.permission == AccessKeyPermission::FullAccess {
                        full_key_added = true;
                    }
                    let target_public_key =
                        crate::key_mapping::map_key(&a.public_key, self.secret.as_ref())
                            .public_key();

                    nonce_updates.insert((target_receiver_id.clone(), target_public_key.clone()));
                    target_actions.push(Action::AddKey(Box::new(AddKeyAction {
                        public_key: target_public_key,
                        access_key: a.access_key.clone(),
                    })));
                }
                Action::CreateAccount(_) => {
                    account_created = true;
                    target_actions.push(Action::CreateAccount(CreateAccountAction {}))
                }
                Action::Transfer(_) => {
                    if provenance.is_create_account() {
                        target_actions.push(a.clone())
                    }
                }
                Action::Stake(_) => {
                    if provenance.is_unstake() {
                        target_actions.push(a.clone());
                    }
                }
                _ => {}
            };
        }
        if account_created && !full_key_added {
            target_actions.push(Action::AddKey(Box::new(AddKeyAction {
                public_key: self.default_extra_key.public_key(),
                access_key: AccessKey::full_access(),
            })));
        }

        tracing::debug!(
            target: "mirror", "preparing {} for ({}, {}) with actions: {:?}",
            &provenance, &target_signer_id, target_secret_key.public_key(), &target_actions,
        );
        let target_tx = self
            .prepare_tx(
                tracker,
                tx_block_queue,
                target_view_client,
                predecessor_id,
                receiver_id,
                target_signer_id,
                target_receiver_id,
                &target_secret_key,
                target_actions,
                ref_hash,
                source_height,
                provenance,
                nonce_updates,
            )
            .await?;
        txs.push(target_tx);
        Ok(())
    }

    async fn add_function_call_keys(
        &self,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        txs: &mut Vec<TargetChainTx>,
        receipt_id: &CryptoHash,
        receiver_id: &AccountId,
        ref_hash: &CryptoHash,
        provenance: MappedTxProvenance,
        source_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let outcome = self
            .source_chain_access
            .get_outcome(TransactionOrReceiptId::Receipt {
                receipt_id: *receipt_id,
                receiver_id: receiver_id.clone(),
            })
            .await
            .with_context(|| format!("failed fetching outcome for receipt {}", receipt_id))?;
        if !execution_status_good(&outcome.outcome.status) {
            return Ok(());
        }

        for id in outcome.outcome.receipt_ids.iter() {
            let receipt = match self.source_chain_access.get_receipt(id).await {
                Ok(r) => r,
                Err(ChainError::Unknown) => {
                    tracing::warn!(
                        target: "mirror", "receipt {} appears in the list output by receipt {}, but can't find it in the source chain",
                        id, receipt_id,
                    );
                    continue;
                }
                Err(ChainError::Other(e)) => return Err(e),
            };

            if let ReceiptEnum::Action(r) | ReceiptEnum::PromiseYield(r) = receipt.receipt() {
                if (provenance.is_create_account()
                    && receipt.predecessor_id() == receipt.receiver_id())
                    || (!provenance.is_create_account()
                        && receipt.predecessor_id() != receipt.receiver_id())
                {
                    continue;
                }
                // TODO: should also take care of delete key actions, and deploy contract actions to
                // implicit accounts, etc...
                let mut key_added = false;
                let mut account_created = false;
                for a in r.actions.iter() {
                    match a {
                        Action::AddKey(_) => key_added = true,
                        Action::CreateAccount(_) => account_created = true,
                        _ => {}
                    };
                }
                if provenance.is_create_account() {
                    if !account_created {
                        continue;
                    }
                } else {
                    if !key_added {
                        continue;
                    }
                    if account_created {
                        tracing::warn!(
                            target: "mirror", "for receipt {} predecessor and receiver are the same but there's a create account in the actions: {:?}",
                            receipt.receipt_id(), &r.actions,
                        );
                    }
                }
                let outcome = self
                    .source_chain_access
                    .get_outcome(TransactionOrReceiptId::Receipt {
                        receipt_id: *receipt.receipt_id(),
                        receiver_id: receipt.receiver_id().clone(),
                    })
                    .await
                    .with_context(|| {
                        format!("failed fetching outcome for receipt {}", receipt.receipt_id())
                    })?;
                if !execution_status_good(&outcome.outcome.status) {
                    continue;
                }

                self.push_extra_tx(
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    outcome.block_hash,
                    txs,
                    receipt.predecessor_id().clone(),
                    receipt.receiver_id().clone(),
                    &r.actions,
                    ref_hash,
                    provenance,
                    Some(source_height),
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn add_tx_function_call_keys(
        &self,
        tx: &SignedTransaction,
        provenance: MappedTxProvenance,
        source_height: BlockHeight,
        ref_hash: &CryptoHash,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        txs: &mut Vec<TargetChainTx>,
    ) -> anyhow::Result<()> {
        // if signer and receiver are the same then the resulting local receipt
        // is only logically included, and we won't see it in the receipts in any chunk,
        // so handle that case here
        if tx.transaction.signer_id() == tx.transaction.receiver_id()
            && tx.transaction.actions().iter().any(|a| matches!(a, Action::FunctionCall(_)))
        {
            let tx_hash = tx.get_hash();
            if let Some(receipt_id) = self
                .source_chain_access
                .get_tx_receipt_id(&tx_hash, &tx.transaction.signer_id())
                .await
                .with_context(|| format!("failed fetching local receipt ID for tx {}", &tx_hash))?
            {
                self.add_function_call_keys(
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    txs,
                    &receipt_id,
                    &tx.transaction.receiver_id(),
                    ref_hash,
                    provenance,
                    source_height,
                )
                .await
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    async fn add_receipt_function_call_keys(
        &self,
        receipt: &Receipt,
        provenance: MappedTxProvenance,
        source_height: BlockHeight,
        ref_hash: &CryptoHash,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        txs: &mut Vec<TargetChainTx>,
    ) -> anyhow::Result<()> {
        if let ReceiptEnum::Action(r) | ReceiptEnum::PromiseYield(r) = receipt.receipt() {
            if r.actions.iter().any(|a| matches!(a, Action::FunctionCall(_))) {
                self.add_function_call_keys(
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    txs,
                    receipt.receipt_id(),
                    receipt.receiver_id(),
                    ref_hash,
                    provenance,
                    source_height,
                )
                .await
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    async fn add_create_account_txs(
        &self,
        create_account_height: BlockHeight,
        ref_hash: CryptoHash,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        txs: &mut Vec<TargetChainTx>,
    ) -> anyhow::Result<()> {
        let source_block =
            self.source_chain_access.get_txs(create_account_height).await.with_context(|| {
                format!("Failed fetching chunks for source chain #{}", create_account_height)
            })?;
        for ch in source_block.chunks {
            for (idx, source_tx) in ch.transactions.into_iter().enumerate() {
                self.add_tx_function_call_keys(
                    &source_tx,
                    MappedTxProvenance::TxCreateAccount(create_account_height, ch.shard_id, idx),
                    create_account_height,
                    &ref_hash,
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    txs,
                )
                .await?;
            }
            for (idx, r) in ch.receipts.iter().enumerate() {
                // TODO: we're scanning the list of receipts for each block twice. Once here and then again
                // when we queue that height's txs. Prob not a big deal but could fix that.
                self.add_receipt_function_call_keys(
                    r,
                    MappedTxProvenance::ReceiptCreateAccount(
                        create_account_height,
                        ch.shard_id,
                        idx,
                    ),
                    create_account_height,
                    &ref_hash,
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    txs,
                )
                .await?;
            }
        }
        Ok(())
    }

    // fetch the source chain block at `source_height`, and prepare a
    // set of transactions that should be valid in the target chain
    // from it.
    async fn fetch_txs(
        &self,
        source_height: BlockHeight,
        create_account_height: Option<BlockHeight>,
        ref_hash: CryptoHash,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
    ) -> anyhow::Result<MappedBlock> {
        let source_block =
            self.source_chain_access.get_txs(source_height).await.with_context(|| {
                format!("Failed fetching chunks for source chain #{}", source_height)
            })?;

        let mut chunks = Vec::new();
        for ch in source_block.chunks {
            let mut txs = Vec::new();

            for (idx, source_tx) in ch.transactions.into_iter().enumerate() {
                let (actions, nonce_updates) =
                    self.map_actions(target_view_client, &source_tx).await?;
                if actions.is_empty() {
                    // If this is a tx containing only stake actions, skip it.
                    continue;
                }
                let target_private_key = crate::key_mapping::map_key(
                    &source_tx.transaction.public_key(),
                    self.secret.as_ref(),
                );

                let target_signer_id = crate::key_mapping::map_account(
                    &source_tx.transaction.signer_id(),
                    self.secret.as_ref(),
                );
                let target_receiver_id = crate::key_mapping::map_account(
                    &source_tx.transaction.receiver_id(),
                    self.secret.as_ref(),
                );

                let target_tx = self
                    .prepare_tx(
                        tracker,
                        tx_block_queue,
                        target_view_client,
                        source_tx.transaction.signer_id().clone(),
                        source_tx.transaction.receiver_id().clone(),
                        target_signer_id,
                        target_receiver_id,
                        &target_private_key,
                        actions,
                        &ref_hash,
                        Some(source_height),
                        MappedTxProvenance::MappedSourceTx(source_height, ch.shard_id, idx),
                        nonce_updates,
                    )
                    .await?;
                txs.push(target_tx);
                self.add_tx_function_call_keys(
                    &source_tx,
                    MappedTxProvenance::TxAddKey(source_height, ch.shard_id, idx),
                    source_height,
                    &ref_hash,
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    &mut txs,
                )
                .await?;
            }
            for (idx, r) in ch.receipts.iter().enumerate() {
                self.add_receipt_function_call_keys(
                    r,
                    MappedTxProvenance::ReceiptAddKey(source_height, ch.shard_id, idx),
                    source_height,
                    &ref_hash,
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    &mut txs,
                )
                .await?;
            }
            tracing::debug!(
                target: "mirror", "prepared {} transacations for source chain #{} shard {}",
                txs.len(), source_height, ch.shard_id
            );
            chunks.push(MappedChunk { txs, shard_id: ch.shard_id });
        }
        if let Some(create_account_height) = create_account_height {
            if !chunks.is_empty() {
                // just add them to shard 0's transactions instead of caring about which one to put it in. Doesn't really
                // matter since in the end we're just sending all of them
                self.add_create_account_txs(
                    create_account_height,
                    ref_hash,
                    tracker,
                    tx_block_queue,
                    target_view_client,
                    &mut chunks[0].txs,
                )
                .await?;
            } else {
                // shouldn't happen
                tracing::warn!("something is wrong as there are no chunks to send transactions for at height {}", source_height);
            }
        }
        Ok(MappedBlock { source_height, source_hash: source_block.hash, chunks })
    }

    // Up to a certain capacity, prepare and queue up batches of
    // transactions that we want to send to the target chain.
    // Returns the number of blocks worth of txs queued at the end.
    // `have_stop_height` refers to whether we're going to stop sending transactions and exit after a particular height
    async fn queue_txs(
        &mut self,
        tracker: &Mutex<crate::chain_tracker::TxTracker>,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_view_client: &Addr<ViewClientActor>,
        ref_hash: CryptoHash,
        have_stop_height: bool,
    ) -> anyhow::Result<()> {
        let mut num_blocks_queued = {
            let tx_block_queue = tx_block_queue.lock().unwrap();
            tx_block_queue.len()
        };
        if num_blocks_queued > 100 {
            return Ok(());
        }

        loop {
            let (next_height, create_account_height) =
                crate::chain_tracker::TxTracker::next_heights(&tracker, &self.source_chain_access)
                    .await?;

            let mut tracker = tracker.lock().unwrap();
            let next_height = match next_height {
                Some(h) => h,
                None => return Ok(()),
            };
            // if we have a stop height, just send the last few blocks without worrying about
            // extra create account txs, otherwise wait until we get more blocks
            if !have_stop_height && create_account_height.is_none() {
                return Ok(());
            }
            let b = self
                .fetch_txs(
                    next_height,
                    create_account_height,
                    ref_hash,
                    &mut tracker,
                    tx_block_queue,
                    target_view_client,
                )
                .await
                .with_context(|| format!("Can't fetch source #{} transactions", next_height))?;
            tracker.queue_block(&tx_block_queue, b, target_view_client, &self.db).await?;

            num_blocks_queued += 1;
            if num_blocks_queued > 100 {
                return Ok(());
            }
        }
    }

    // send stake txs for zero stake for each of the stake actions we just saw in
    // the last block's processed receipts. These would have come from function calls
    // rather than normal stake txs, since we skip sending those.
    // TODO: here we're just sending it and forgetting about it, but would be good to
    // retry later if the tx got lost for some reason
    async fn unstake(
        &mut self,
        tracker: &mut crate::chain_tracker::TxTracker,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_client: &Addr<ClientActor>,
        target_view_client: &Addr<ViewClientActor>,
        stakes: HashMap<(AccountId, PublicKey), AccountId>,
        source_hash: &CryptoHash,
        target_hash: &CryptoHash,
        target_height: BlockHeight,
    ) -> anyhow::Result<()> {
        let mut txs = Vec::new();
        for ((receiver_id, public_key), predecessor_id) in stakes {
            self.push_extra_tx(
                tracker,
                tx_block_queue,
                target_view_client,
                *source_hash,
                &mut txs,
                predecessor_id,
                receiver_id.clone(),
                &[Action::Stake(Box::new(StakeAction { public_key, stake: 0 }))],
                target_hash,
                MappedTxProvenance::Unstake(*target_hash),
                None,
            )
            .await?;
        }
        if !txs.is_empty() {
            Self::send_transactions(target_client, txs.iter_mut()).await?;
            tracker
                .on_txs_sent(
                    tx_block_queue,
                    &self.db,
                    crate::chain_tracker::SentBatch::ExtraTxs(txs),
                    target_height,
                )
                .await?;
        }
        Ok(())
    }

    async fn send_txs_loop(
        db: Arc<DB>,
        blocks_sent: mpsc::Sender<TxBatch>,
        tx_block_queue: Arc<Mutex<VecDeque<MappedBlock>>>,
        mut send_time: Pin<Box<tokio::time::Sleep>>,
        send_delay: Arc<Mutex<Duration>>,
        target_client: Addr<ClientActor>,
    ) -> anyhow::Result<()> {
        let mut sent_source_height = None;

        loop {
            (&mut send_time).await;

            let tx_batch = {
                let tx_block_queue = tx_block_queue.lock().unwrap();
                let b = match sent_source_height {
                    Some(sent_source_height) => {
                        let mut block_idx = None;
                        for (idx, b) in tx_block_queue.iter().enumerate() {
                            if b.source_height > sent_source_height {
                                block_idx = Some(idx);
                                break;
                            }
                        }
                        match block_idx {
                            Some(idx) => tx_block_queue.get(idx),
                            None => None,
                        }
                    }
                    None => tx_block_queue.get(0),
                };
                b.map(|b| TxBatch::from(b))
            };

            let mut tx_batch = match tx_batch {
                Some(b) => b,
                None => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };

            let start_time = tokio::time::Instant::now();

            tracing::debug!(target: "mirror", "Sending transactions for source block #{}", tx_batch.source_height);
            Self::send_transactions(
                &target_client,
                tx_batch.txs.iter_mut().map(|(_tx_ref, tx)| tx),
            )
            .await?;
            set_last_source_height(&db, tx_batch.source_height)?;
            sent_source_height = Some(tx_batch.source_height);

            blocks_sent.send(tx_batch).await.unwrap();

            let send_delay = *send_delay.lock().unwrap();
            tracing::debug!(target: "mirror", "Sleeping for {:?} until sending more transactions", &send_delay);
            let next_send_time = start_time + send_delay;
            send_time.as_mut().reset(next_send_time);
        }
    }

    async fn index_target_loop(
        tracker: Arc<Mutex<crate::chain_tracker::TxTracker>>,
        tx_block_queue: Arc<Mutex<VecDeque<MappedBlock>>>,
        home_dir: PathBuf,
        db: Arc<DB>,
        clients_tx: tokio::sync::oneshot::Sender<(Addr<ClientActor>, Addr<ViewClientActor>)>,
        accounts_to_unstake: mpsc::Sender<HashMap<(AccountId, PublicKey), AccountId>>,
        target_height: Arc<RwLock<BlockHeight>>,
        target_head: Arc<RwLock<CryptoHash>>,
    ) -> anyhow::Result<()> {
        let target_indexer = Indexer::new(near_indexer::IndexerConfig {
            home_dir,
            sync_mode: near_indexer::SyncModeEnum::FromInterruption,
            await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
            validate_genesis: false,
        })
        .context("failed to start target chain indexer")?;
        let (target_view_client, target_client) = target_indexer.client_actors();
        let mut target_stream = target_indexer.streamer();
        let (first_target_height, first_target_head) = Self::index_target_chain(
            &tracker,
            &tx_block_queue,
            &mut target_stream,
            db.as_ref(),
            &target_view_client,
            &target_client,
        )
        .await?;
        *target_height.write().unwrap() = first_target_height;
        *target_head.write().unwrap() = first_target_head;
        clients_tx.send((target_client.clone(), target_view_client.clone())).unwrap();

        loop {
            let msg = target_stream.recv().await.unwrap();
            *target_head.write().unwrap() = msg.block.header.hash;
            *target_height.write().unwrap() = msg.block.header.height;
            let target_block_info = {
                let mut tracker = tracker.lock().unwrap();
                tracker.on_target_block(&tx_block_queue, db.as_ref(), msg)?
            };
            if !target_block_info.staked_accounts.is_empty() {
                accounts_to_unstake.send(target_block_info.staked_accounts).await.unwrap();
            }
            for access_key_update in target_block_info.access_key_updates {
                let nonce = crate::fetch_access_key_nonce(
                    &target_view_client,
                    &access_key_update.account_id,
                    &access_key_update.public_key,
                )
                .await?;
                let mut tracker = tracker.lock().unwrap();
                tracker.try_set_nonces(&tx_block_queue, db.as_ref(), access_key_update, nonce)?;
            }
        }
    }

    async fn queue_txs_loop(
        &mut self,
        tracker: Arc<Mutex<crate::chain_tracker::TxTracker>>,
        tx_block_queue: Arc<Mutex<VecDeque<MappedBlock>>>,
        target_client: Addr<ClientActor>,
        target_view_client: Addr<ViewClientActor>,
        mut blocks_sent: mpsc::Receiver<TxBatch>,
        mut accounts_to_unstake: mpsc::Receiver<HashMap<(AccountId, PublicKey), AccountId>>,
        send_delay: Arc<Mutex<Duration>>,
        target_height: Arc<RwLock<BlockHeight>>,
        target_head: Arc<RwLock<CryptoHash>>,
        mut source_hash: CryptoHash,
        have_stop_height: bool,
    ) -> anyhow::Result<()> {
        let mut queue_txs_time = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                // time to send a batch of transactions
                _ = queue_txs_time.tick() => {
                    self.queue_txs(&tracker, &tx_block_queue, &target_view_client, *target_head.read().unwrap(), have_stop_height).await?;
                }
                tx_batch = blocks_sent.recv() => {
                    let tx_batch = tx_batch.unwrap();
                    source_hash = tx_batch.source_hash;
                    // lock the tracker before removing the block from the queue so that
                    // we don't call on_target_block() in the other thread between removing the block
                    // and calling on_txs_sent(), because that could lead to a bug looking up transactions
                    // in TxTracker::get_tx()
                    let mut tracker = tracker.lock().unwrap();
                    {
                        let mut tx_block_queue = tx_block_queue.lock().unwrap();
                        let b = tx_block_queue.pop_front().unwrap();
                        assert!(b.source_height == tx_batch.source_height);
                    };
                    let target_height = *target_height.read().unwrap();
                    let new_delay = tracker.on_txs_sent(
                        &tx_block_queue,
                        &self.db,
                        crate::chain_tracker::SentBatch::MappedBlock(tx_batch),
                        target_height,
                    ).await?;
                    *send_delay.lock().unwrap() = new_delay;
                }
                msg = accounts_to_unstake.recv() => {
                    let staked_accounts = msg.unwrap();
                    let mut tracker = tracker.lock().unwrap();
                    self.unstake(
                        &mut tracker, &tx_block_queue, &target_client,
                        &target_view_client, staked_accounts, &source_hash,
                        &target_head.read().unwrap(), *target_height.read().unwrap()
                    ).await?;
                }
            };
            // TODO: this locking of the mutex before continuing the loop is kind of unnecessary since we should be able to tell
            // exactly when we've done the thing that makes finished() return true, usually after a call to on_target_block()
            {
                let tracker = tracker.lock().unwrap();
                if tracker.finished() {
                    tracing::info!(target: "mirror", "finished sending all transactions");
                    return Ok(());
                }
            }
        }
    }

    async fn target_chain_syncing(target_client: &Addr<ClientActor>) -> bool {
        target_client
            .send(Status { is_health_check: false, detailed: false }.with_span_context())
            .await
            .unwrap()
            .map(|s| s.sync_info.syncing)
            .unwrap_or(true)
    }

    async fn target_chain_head(
        target_view_client: &Addr<ViewClientActor>,
    ) -> anyhow::Result<(BlockHeight, CryptoHash)> {
        let header = target_view_client
            .send(GetBlock(BlockReference::Finality(Finality::Final)).with_span_context())
            .await
            .unwrap()
            .context("failed fetching target chain HEAD")?
            .header;
        Ok((header.height, header.hash))
    }

    // call tracker.on_target_block() on each target chain block until that client is synced
    async fn index_target_chain(
        tracker: &Mutex<crate::chain_tracker::TxTracker>,
        tx_block_queue: &Mutex<VecDeque<MappedBlock>>,
        target_stream: &mut mpsc::Receiver<StreamerMessage>,
        db: &DB,
        target_view_client: &Addr<ViewClientActor>,
        target_client: &Addr<ClientActor>,
    ) -> anyhow::Result<(BlockHeight, CryptoHash)> {
        let mut head = None;

        loop {
            let msg = target_stream.recv().await.unwrap();
            let height = msg.block.header.height;

            {
                let mut tracker = tracker.lock().unwrap();
                // TODO: handle the return value. it is possible we want to unstake or update nonces
                // after a restart.
                tracker.on_target_block(&tx_block_queue, db, msg)?;
            }

            match head {
                Some((head_height, head_hash)) => {
                    if height >= head_height {
                        return Ok((head_height, head_hash));
                    }
                }
                None => {
                    if !Self::target_chain_syncing(target_client).await {
                        head = Some(Self::target_chain_head(target_view_client).await?);
                    }
                }
            }
        }
    }

    async fn run(
        mut self,
        stop_height: Option<BlockHeight>,
        target_home: PathBuf,
    ) -> anyhow::Result<()> {
        let last_stored_height = get_last_source_height(&self.db)?;
        let last_height = last_stored_height.unwrap_or(self.target_genesis_height - 1);

        let next_heights =
            self.source_chain_access.init(last_height, CREATE_ACCOUNT_DELTA + 1).await?;

        if next_heights.is_empty() {
            anyhow::bail!("no new blocks after #{}", last_height);
        }
        if let Some(stop_height) = stop_height {
            if next_heights[0] > stop_height {
                anyhow::bail!(
                    "--stop-height was {} and the next height to send txs for is {}",
                    stop_height,
                    next_heights[0]
                );
            }
        }
        let source_hash = self
            .source_chain_access
            .block_height_to_hash(next_heights[0])
            .await
            .with_context(|| format!("error fetching hash of block #{}", next_heights[0]))?;

        tracing::debug!(target: "mirror", "source chain initialized with first heights: {:?}", &next_heights);

        let tracker = Arc::new(Mutex::new(crate::chain_tracker::TxTracker::new(
            self.target_min_block_production_delay,
            self.config.tx_batch_interval,
            next_heights.iter(),
            stop_height,
        )));
        let target_height = Arc::new(RwLock::new(0));
        let target_head = Arc::new(RwLock::new(CryptoHash::default()));
        let (clients_tx, clients_rx) = tokio::sync::oneshot::channel();
        let (target_indexer_done_tx, target_indexer_done_rx) =
            tokio::sync::oneshot::channel::<anyhow::Result<()>>();
        let (unstake_tx, unstake_rx) = mpsc::channel(10);

        let db = self.db.clone();
        let target_height2 = target_height.clone();
        let target_head2 = target_head.clone();
        let tracker2 = tracker.clone();
        let arbiter = actix::Arbiter::new();

        let tx_block_queue = Arc::new(Mutex::new(VecDeque::new()));

        let tx_block_queue2 = tx_block_queue.clone();
        arbiter.spawn(async move {
            let res = Self::index_target_loop(
                tracker2,
                tx_block_queue2,
                target_home,
                db,
                clients_tx,
                unstake_tx,
                target_height2,
                target_head2,
            )
            .await;
            target_indexer_done_tx.send(res).unwrap();
        });

        // wait til we set the values in target_height and target_head after receiving a message from the indexer
        let (target_client, target_view_client) = clients_rx.await.unwrap();

        // Wait at least 15 seconds before sending any transactions because for
        // a few seconds after the node starts, transaction routing requests
        // will be silently dropped by the peer manager.
        let mut send_time = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(15)));
        let mut send_delay = self
            .config
            .tx_batch_interval
            .unwrap_or(self.target_min_block_production_delay + Duration::from_millis(100));

        if last_stored_height.is_none() {
            // send any extra function call-initiated create accounts for the first few blocks right now
            // we set source_hash to 0 because we don't actually care about it here, and it doesn't even exist since these are
            // not transactions corresponding to some actual block, but just extra txs create account actions in the first few blocks.
            let mut block = MappedBlock {
                source_hash: CryptoHash::default(),
                source_height: last_height,
                chunks: vec![MappedChunk { shard_id: 0, txs: Vec::new() }],
            };
            let mut tracker = tracker.lock().unwrap();
            for h in next_heights {
                self.add_create_account_txs(
                    h,
                    *target_head.read().unwrap(),
                    &mut tracker,
                    &tx_block_queue,
                    &target_view_client,
                    &mut block.chunks[0].txs,
                )
                .await?;
            }
            if block.chunks.iter().any(|c| !c.txs.is_empty()) {
                tracing::debug!(target: "mirror", "sending extra create account transactions for the first {} blocks", CREATE_ACCOUNT_DELTA);
                let mut b = {
                    tracker
                        .queue_block(&tx_block_queue, block, &target_view_client, &self.db)
                        .await?;
                    (&mut send_time).await;
                    let mut tx_block_queue = tx_block_queue.lock().unwrap();
                    TxBatch::from(&tx_block_queue.pop_front().unwrap())
                };
                Self::send_transactions(&target_client, b.txs.iter_mut().map(|(_tx_ref, tx)| tx))
                    .await?;
                send_delay = tracker
                    .on_txs_sent(
                        &tx_block_queue,
                        &self.db,
                        crate::chain_tracker::SentBatch::MappedBlock(b),
                        *target_height.read().unwrap(),
                    )
                    .await?;
            }
        }
        self.queue_txs(
            &tracker,
            &tx_block_queue,
            &target_view_client,
            *target_head.read().unwrap(),
            stop_height.is_some(),
        )
        .await?;

        let send_delay = Arc::new(Mutex::new(send_delay));
        let send_delay2 = send_delay.clone();
        let (blocks_sent_tx, blocks_sent_rx) = mpsc::channel(10);
        let tx_block_queue2 = tx_block_queue.clone();
        let target_client2 = target_client.clone();
        let db = self.db.clone();
        let arbiter2 = actix::Arbiter::new();
        let (send_txs_done_tx, send_txs_done_rx) =
            tokio::sync::oneshot::channel::<anyhow::Result<()>>();
        arbiter2.spawn(async move {
            let res = Self::send_txs_loop(
                db,
                blocks_sent_tx,
                tx_block_queue2,
                send_time,
                send_delay2,
                target_client2,
            )
            .await;
            send_txs_done_tx.send(res).unwrap();
        });
        tokio::select! {
            res = self.queue_txs_loop(
                tracker, tx_block_queue, target_client, target_view_client,
                blocks_sent_rx, unstake_rx, send_delay, target_height, target_head,
                source_hash, stop_height.is_some(),
            ) => {
                // TODO: cancel other threads
                res
            }
            res = target_indexer_done_rx => {
                let res = res.unwrap();
                tracing::error!("target indexer thread exited");
                res.context("target indexer thread failure")
            }
            res = send_txs_done_rx => {
                let res = res.unwrap();
                tracing::error!("transaction sending thread exited");
                res.context("target indexer thread failure")
            }
        }
    }
}

async fn run<P: AsRef<Path>>(
    source_home: P,
    target_home: P,
    mirror_db_path: Option<PathBuf>,
    secret: Option<[u8; crate::secret::SECRET_LEN]>,
    stop_height: Option<BlockHeight>,
    online_source: bool,
    config_path: Option<P>,
) -> anyhow::Result<()> {
    let config: MirrorConfig = match config_path {
        Some(p) => {
            let c = std::fs::read_to_string(p.as_ref())
                .with_context(|| format!("Could not read config from {}", p.as_ref().display()))?;
            serde_json::from_str(&c)
                .with_context(|| format!("Could not parse config from {}", p.as_ref().display()))?
        }
        None => Default::default(),
    };
    if !online_source {
        let source_chain_access = crate::offline::ChainAccess::new(source_home)?;
        let stop_height = stop_height.unwrap_or(
            source_chain_access.head_height().await.context("could not fetch source chain head")?,
        );
        TxMirror::new(
            source_chain_access,
            target_home.as_ref(),
            mirror_db_path.as_deref(),
            secret,
            config,
        )?
        .run(Some(stop_height), target_home.as_ref().to_path_buf())
        .await
    } else {
        TxMirror::new(
            crate::online::ChainAccess::new(source_home)?,
            target_home.as_ref(),
            mirror_db_path.as_deref(),
            secret,
            config,
        )?
        .run(stop_height, target_home.as_ref().to_path_buf())
        .await
    }
}

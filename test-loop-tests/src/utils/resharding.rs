use super::sharding::this_block_has_new_shard_layout;
use crate::setup::block_observer::BlockSource;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use crate::utils::resharding_check_trace;
use crate::utils::sharding::{get_memtrie_for_shard, next_block_has_new_shard_layout};
use crate::utils::transactions::get_anchor_hash;
use assert_matches::assert_matches;
use borsh::BorshDeserialize;
use bytesize::ByteSize;
use itertools::Itertools;
use near_chain::ChainStoreAccess;
use near_chain::types::Tip;
use near_client::Client;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptOrStateStoredReceipt;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::{create_user_test_signer, encode};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, Gas, Nonce, NumShards, ShardId,
};
use near_primitives::views::{FinalExecutionStatus, QueryRequest};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::db::refcount::decode_value_with_rc;
use near_store::flat::FlatStorageStatus;
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{DBCol, ShardUId, StorageError, Trie, TrieDBStorage, get};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::env::var;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZero;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;

/// A config to tell what shards will be tracked by the client at the given index.
/// For more details, see `TrackedShardsConfig::Schedule`.
#[derive(Clone, Debug)]
pub(crate) struct TrackedShardSchedule {
    pub client_index: usize,
    pub schedule: Vec<Vec<ShardId>>,
}

/// Sends random transfers between `account_ids` at every block of the request node. The seed comes
/// from `NEAR_TEST_RESHARDING_MONEY_TRANSFERS_SEED` when it is set, so that a run can be repeated.
#[derive(Clone)]
pub(crate) struct MoneyTransfersTraffic {
    account_ids: Vec<AccountId>,
    seed: u64,
    num_submitted_transfers: Rc<Cell<usize>>,
}

impl MoneyTransfersTraffic {
    pub(crate) fn new(account_ids: Vec<AccountId>) -> Self {
        let seed = match var("NEAR_TEST_RESHARDING_MONEY_TRANSFERS_SEED") {
            Ok(seed) => seed.parse().unwrap(),
            Err(_) => rand::thread_rng().r#gen::<u64>(),
        };
        println!("Random seed: {}", seed);
        Self { account_ids, seed, num_submitted_transfers: Rc::default() }
    }

    /// The action to register on the request node's blocks.
    pub(crate) fn block_action(&self) -> Self {
        self.clone()
    }

    pub(crate) fn num_submitted_transfers(&self) -> usize {
        self.num_submitted_transfers.get()
    }
}

impl BlockAction for MoneyTransfersTraffic {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        const NUM_TRANSFERS_PER_BLOCK: usize = 20;
        let node = nodes.request;
        let height = node.head().height;
        let mut rng_seed = [0u8; 32];
        rng_seed[0..8].copy_from_slice(&self.seed.to_le_bytes());
        rng_seed[8..16].copy_from_slice(&height.to_le_bytes());
        let mut rng: ChaCha20Rng = SeedableRng::from_seed(rng_seed);
        let clients = nodes.all.iter().map(|node| node.client()).collect_vec();
        for _ in 0..NUM_TRANSFERS_PER_BLOCK {
            let sender = self.account_ids.choose(&mut rng).unwrap().clone();
            let receiver = self.account_ids.choose(&mut rng).unwrap().clone();
            let anchor_hash = get_anchor_hash(&clients);
            let nonce = node.get_next_nonce(&sender);
            let amount = Balance::from_near(1).checked_mul(rng.gen_range(1..=10)).unwrap();
            let tx = SignedTransaction::send_money(
                nonce,
                sender.clone(),
                receiver.clone(),
                &create_user_test_signer(&sender).into(),
                amount,
                anchor_hash,
            );
            node.submit_tx(tx);
            self.num_submitted_transfers.set(self.num_submitted_transfers.get() + 1);
        }
        ControlFlow::Continue(())
    }
}

/// Sends one transaction per new block of the request node that reads a key and writes a key-value
/// pair in the contract storage, and asserts each transaction succeeded once it is more than
/// `TX_CHECK_DEADLINE` blocks old. The transactions younger than that when the test ends stay
/// unchecked.
#[derive(Clone)]
pub(crate) struct StorageOperationsTraffic {
    sender_id: AccountId,
    contract_id: AccountId,
    state: Rc<RefCell<StorageOperationsState>>,
}

struct StorageOperationsState {
    next_nonce: Nonce,
    unchecked_txs: Vec<(CryptoHash, BlockHeight)>,
    num_submitted_calls: usize,
}

impl StorageOperationsTraffic {
    pub(crate) fn new(sender_id: AccountId, contract_id: AccountId) -> Self {
        let state = StorageOperationsState {
            next_nonce: 103,
            unchecked_txs: Vec::new(),
            num_submitted_calls: 0,
        };
        Self { sender_id, contract_id, state: Rc::new(RefCell::new(state)) }
    }

    /// The action to register on the request node's blocks.
    pub(crate) fn block_action(&self) -> Self {
        self.clone()
    }

    pub(crate) fn num_submitted_calls(&self) -> usize {
        self.state.borrow().num_submitted_calls
    }
}

impl BlockAction for StorageOperationsTraffic {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        const TX_CHECK_DEADLINE: BlockHeightDelta = 5;
        let node = nodes.request;
        let height = node.head().height;
        let state = &mut *self.state.borrow_mut();
        state.unchecked_txs.retain(|(tx_hash, tx_height)| {
            if tx_height + TX_CHECK_DEADLINE >= height {
                return true;
            }
            let outcome = node.client().chain.get_partial_transaction_result(tx_hash);
            let status = outcome.as_ref().map(|outcome| outcome.status.clone());
            assert_matches!(status, Ok(FinalExecutionStatus::SuccessValue(_)));
            false
        });

        let clients = nodes.all.iter().map(|node| node.client()).collect_vec();
        let anchor_hash = get_anchor_hash(&clients);
        let gas = Gas::from_teragas(20);
        let salt = 2 * height;
        let read_action = Action::FunctionCall(Box::new(FunctionCallAction {
            args: encode(&[salt]),
            method_name: "read_value".to_string(),
            gas,
            deposit: Balance::ZERO,
        }));
        let write_action = Action::FunctionCall(Box::new(FunctionCallAction {
            args: encode(&[salt + 1, salt * 10]),
            method_name: "write_key_value".to_string(),
            gas,
            deposit: Balance::ZERO,
        }));
        let tx = SignedTransaction::from_actions(
            state.next_nonce,
            self.sender_id.clone(),
            self.contract_id.clone(),
            &create_user_test_signer(&self.sender_id).into(),
            vec![read_action, write_action],
            anchor_hash,
        );
        state.next_nonce += 1;
        let tx_hash = node.submit_tx(tx);
        resharding_check_trace::submitted_tx(height, &self.sender_id, &self.contract_id, &tx_hash);
        state.unchecked_txs.push((tx_hash, height));
        state.num_submitted_calls += 1;
        ControlFlow::Continue(())
    }
}

/// Calls the test contract `CALLS_PER_BLOCK` times per new block of the request node, up to and
/// including the first block of the new shard layout, to pile up receipts at the split.
///
/// The signer of each call is taken in sequential order from `signers`, and the receiver from
/// `receivers`. Note that if the number of signers and receivers is the same then the traffic will
/// always flow the same way. It would be nice to randomize it a bit.
#[derive(Clone)]
pub(crate) struct BurnGasTraffic {
    signers: Vec<AccountId>,
    receivers: Vec<AccountId>,
    gas_burnt_per_call: Gas,
    state: Rc<RefCell<BurnGasTrafficState>>,
}

struct BurnGasTrafficState {
    nonce: Nonce,
    resharding_height: Option<BlockHeight>,
    submitted_txs: Vec<CryptoHash>,
}

impl BurnGasTraffic {
    pub(crate) fn new(
        signers: Vec<AccountId>,
        receivers: Vec<AccountId>,
        gas_burnt_per_call: Gas,
    ) -> Self {
        Self {
            signers,
            receivers,
            gas_burnt_per_call,
            state: Rc::new(RefCell::new(BurnGasTrafficState {
                nonce: 102,
                resharding_height: None,
                submitted_txs: Vec::new(),
            })),
        }
    }

    /// The action to register on the request node's blocks.
    pub(crate) fn block_action(&self) -> Self {
        self.clone()
    }

    pub(crate) fn submitted_txs(&self) -> Vec<CryptoHash> {
        self.state.borrow().submitted_txs.clone()
    }
}

impl BlockAction for BurnGasTraffic {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        const CALLS_PER_BLOCK: usize = 5;
        let node = nodes.request;
        let tip = nodes.tip();
        let mut state = self.state.borrow_mut();

        if state.resharding_height.is_none()
            && next_block_has_new_shard_layout(node.client().epoch_manager.as_ref(), &tip)
        {
            tracing::debug!(target: "test", height = tip.height, "resharding height set");
            state.resharding_height = Some(tip.height);
        }
        // One more block of traffic after the split, then the receipts are piled up.
        if let Some(resharding_height) = state.resharding_height {
            if tip.height > resharding_height + 1 {
                return ControlFlow::Break(());
            }
        }

        for call_index in 0..CALLS_PER_BLOCK {
            let signer_id = &self.signers[call_index % self.signers.len()];
            let receiver_id = &self.receivers[call_index % self.receivers.len()];
            let signer: Signer = create_user_test_signer(signer_id).into();
            state.nonce += 1;
            let tx = SignedTransaction::call(
                state.nonce,
                signer_id.clone(),
                receiver_id.clone(),
                &signer,
                Balance::from_yoctonear(1),
                "burn_gas_raw".to_owned(),
                self.gas_burnt_per_call.as_gas().to_le_bytes().to_vec(),
                self.gas_burnt_per_call.checked_add(Gas::from_teragas(10)).unwrap(),
                tip.last_block_hash,
            );
            let tx_hash = node.submit_tx(tx);
            resharding_check_trace::submitted_tx(tip.height, signer_id, receiver_id, &tx_hash);
            state.submitted_txs.push(tx_hash);
        }
        ControlFlow::Continue(())
    }
}

/// Reads the indices trie node of the parent shard and of both child shards at every block, and
/// asserts the read succeeds on each shard the request node tracks. A shard without a chunk extra
/// reads as `None`, and a missing key reads as the default value, as in the checks this replaces.
pub(crate) struct IndicesTrieNodeReadableCheck<Indices> {
    trie_key: TrieKey,
    parent_shard_uid: ShardUId,
    left_child_shard_uid: ShardUId,
    right_child_shard_uid: ShardUId,
    indices_type: PhantomData<Indices>,
}

impl<Indices> IndicesTrieNodeReadableCheck<Indices> {
    pub(crate) fn new(
        trie_key: TrieKey,
        shard_layout_after_resharding: &ShardLayout,
        left_child_account: &AccountId,
        right_child_account: &AccountId,
    ) -> Self {
        let (parent_shard_uid, left_child_shard_uid, right_child_shard_uid) =
            get_resharded_shard_uids(
                left_child_account,
                right_child_account,
                shard_layout_after_resharding,
            );
        Self {
            trie_key,
            parent_shard_uid,
            left_child_shard_uid,
            right_child_shard_uid,
            indices_type: PhantomData,
        }
    }
}

impl<Indices: BorshDeserialize + Default + Debug + 'static> BlockAction
    for IndicesTrieNodeReadableCheck<Indices>
{
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        let node = nodes.request;
        let head = node.head();
        let read_indices = |shard_uid| {
            get_trie_node_value::<Indices>(
                node.client(),
                shard_uid,
                &head.prev_block_hash,
                self.trie_key.clone(),
            )
        };
        let indices_parent_shard = read_indices(self.parent_shard_uid);
        let indices_left_child_shard = read_indices(self.left_child_shard_uid);
        let indices_right_child_shard = read_indices(self.right_child_shard_uid);
        let parent = format!("{indices_parent_shard:?}");
        let left_child = format!("{indices_left_child_shard:?}");
        let right_child = format!("{indices_right_child_shard:?}");
        tracing::debug!(target: "test", height = head.height, epoch = ?head.epoch_id,
                parent, left_child, right_child, "indices node");
        resharding_check_trace::indices_node(
            head.height,
            &format!("{:?}", self.trie_key),
            &parent,
            &left_child,
            &right_child,
        );
        assert_matches!(indices_parent_shard, Some(Ok(_)) | None);
        assert_matches!(indices_left_child_shard, Some(Ok(_)) | None);
        assert_matches!(indices_right_child_shard, Some(Ok(_)) | None);
        ControlFlow::Continue(())
    }
}

/// Asserts every buffered outgoing receipt of every shard carries its congestion metadata, at each
/// block of the request node, and logs the sizes.
pub(crate) struct OutgoingReceiptBufferCheck;

impl OutgoingReceiptBufferCheck {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl BlockAction for OutgoingReceiptBufferCheck {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        let client = nodes.request.client();
        let head = nodes.request.head();
        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        for shard_uid in shard_layout.shard_uids() {
            let mut outgoing_receipt_sizes: BTreeMap<ShardId, Vec<ByteSize>> = BTreeMap::new();
            let memtrie = get_memtrie_for_shard(client, &shard_uid, &head.prev_block_hash);
            let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(&memtrie).unwrap();
            for target_shard in outgoing_buffers.shards() {
                let mut receipt_sizes = Vec::new();
                for receipt in outgoing_buffers.to_shard(target_shard).iter(&memtrie, false) {
                    let receipt_size = match receipt {
                        Ok(ReceiptOrStateStoredReceipt::StateStoredReceipt(
                            state_stored_receipt,
                        )) => state_stored_receipt.metadata().congestion_size,
                        _ => panic!("receipt is {:?}", receipt),
                    };
                    receipt_sizes.push(ByteSize::b(receipt_size));
                }
                if !receipt_sizes.is_empty() {
                    outgoing_receipt_sizes.insert(target_shard, receipt_sizes);
                }
            }
            tracing::info!(target: "test", shard_id = %shard_uid.shard_id(), ?outgoing_receipt_sizes, "outgoing buffers from shard");
        }
        ControlFlow::Continue(())
    }
}

/// The test's nodes at the observed block.
pub(crate) struct BlockNodes<'a> {
    /// Node whose new block this is.
    pub(crate) observed: &'a TestLoopNode<'a>,
    pub(crate) all: &'a [TestLoopNode<'a>],
    /// Node that sends transactions and answers queries.
    pub(crate) request: &'a TestLoopNode<'a>,
    pub(crate) archival: Option<&'a TestLoopNode<'a>>,
}

impl<'a> BlockNodes<'a> {
    /// Head of the node whose new block this is.
    pub(crate) fn tip(&self) -> Arc<Tip> {
        self.observed.head()
    }

    pub(crate) fn clients(&self) -> Vec<&'a Client> {
        self.all.iter().map(|node| node.client()).collect_vec()
    }

    /// Number of shards in the shard layout of the observed block's epoch.
    pub(crate) fn num_shards_at_tip(&self) -> NumShards {
        let tip = self.tip();
        self.request.client().epoch_manager.get_shard_layout(&tip.epoch_id).unwrap().num_shards()
    }

    pub(crate) fn epoch_height_at_tip(&self) -> EpochHeight {
        let tip = self.tip();
        self.request
            .client()
            .epoch_manager
            .get_epoch_height_from_prev_block(&tip.prev_block_hash)
            .unwrap()
    }

    pub(crate) fn account_id_of(&self, node: &TestLoopNode<'_>) -> AccountId {
        node.client().validator_signer.get().map(|signer| signer.validator_id().clone()).unwrap()
    }
}

/// Code a resharding test runs on each new block of one node: a check, traffic, or output.
pub(crate) trait BlockAction: 'static {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()>;
}

/// Indexes of the nodes with a role in the test, used to build `BlockNodes` for every call.
#[derive(Clone, Copy)]
pub(crate) struct NodeRoles {
    pub(crate) request_node_index: usize,
    pub(crate) archival_node_index: Option<usize>,
}

/// Registers `action`, called on each new block of `source`.
pub(crate) fn install_block_action(
    env: &mut TestLoopEnv,
    source: BlockSource,
    roles: NodeRoles,
    mut action: Box<dyn BlockAction>,
) {
    env.on_each_block(source, move |block| {
        let nodes = BlockNodes {
            observed: block.observed_node,
            all: block.nodes,
            request: &block.nodes[roles.request_node_index],
            archival: roles.archival_node_index.map(|index| &block.nodes[index]),
        };
        action.on_new_block(&nodes)
    });
}

/// Submits a transaction that creates `new_account_id` from `originator`.
pub(crate) fn create_account(
    env: &TestLoopEnv,
    rpc_id: &AccountId,
    originator: &AccountId,
    new_account_id: &AccountId,
    amount: Balance,
    nonce: u64,
) -> CryptoHash {
    let node = env.node_for_account(rpc_id);
    let signer = create_user_test_signer(originator);
    let new_signer: Signer = create_user_test_signer(new_account_id);

    let tx = SignedTransaction::create_account(
        nonce,
        originator.clone(),
        new_account_id.clone(),
        amount,
        new_signer.public_key(),
        &signer,
        node.head().last_block_hash,
    );

    node.submit_tx(tx)
}

#[derive(Clone)]
pub(crate) struct AccountDeletedAfterSplit {
    account_id: AccountId,
    beneficiary_id: AccountId,
    state: Rc<RefCell<AccountDeletionState>>,
}

#[derive(Debug)]
enum AccountDeletionState {
    WaitingForNewShardLayout,
    WaitingForDeleteOutcome { delete_tx_hash: CryptoHash, deleted_at_height: BlockHeight },
    WaitingForGarbageCollection { deleted_at_height: BlockHeight },
    Completed,
}

impl AccountDeletedAfterSplit {
    /// `account_id` is a sub-account; its parent account created it and gets its balance back.
    pub(crate) fn new(account_id: AccountId) -> Self {
        let beneficiary_id = account_id
            .get_parent_account_id()
            .unwrap_or_else(|| panic!("{account_id} must be a sub-account"))
            .to_owned();
        Self {
            account_id,
            beneficiary_id,
            state: Rc::new(RefCell::new(AccountDeletionState::WaitingForNewShardLayout)),
        }
    }

    /// The action to register on the request node's blocks: it deletes the account at the first
    /// block of the new shard layout, then follows it to the garbage collection query.
    pub(crate) fn block_action(&self) -> Self {
        self.clone()
    }

    /// Submits the transaction that creates the account, and returns its hash. The parent account
    /// creates it with 10 NEAR, using its first nonce.
    pub(crate) fn submit_create_transaction(
        &self,
        env: &TestLoopEnv,
        request_node_account_id: &AccountId,
    ) -> CryptoHash {
        create_account(
            env,
            request_node_account_id,
            &self.beneficiary_id,
            &self.account_id,
            Balance::from_near(10),
            2,
        )
    }

    /// Asserts that the account was deleted at the first block of the new shard layout, that its
    /// delete transaction succeeded one epoch later, and that after the garbage collection window
    /// the request node no longer served its state at the deletion height.
    pub(crate) fn assert_deleted_and_state_garbage_collected(&self) {
        assert_matches!(
            *self.state.borrow(),
            AccountDeletionState::Completed,
            "{} did not reach the last step of its deletion check",
            self.account_id
        );
    }
}

impl BlockAction for AccountDeletedAfterSplit {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        let account_id = &self.account_id;
        let beneficiary_id = &self.beneficiary_id;
        let state = &self.state;
        {
            let request_node = nodes.request;
            let tip = nodes.tip();
            let client_config = &request_node.client().config;
            let epoch_length = client_config.epoch_length;
            let gc_num_epochs_to_keep = client_config.gc.gc_num_epochs_to_keep;
            let next_state = match *state.borrow() {
                AccountDeletionState::WaitingForNewShardLayout => {
                    if !this_block_has_new_shard_layout(
                        request_node.client().epoch_manager.as_ref(),
                        &tip,
                    ) {
                        return ControlFlow::Continue(());
                    }
                    // We construct the tx manually instead of using node.tx_delete_account()
                    // because that method uses node.head().last_block_hash, which is the
                    // head of a single node. With shard shuffling enabled, nodes can be at
                    // different heights, and the chunk producer that processes the tx might
                    // not know about that block hash yet. The block at the minimum head
                    // height across all nodes is known by every node.
                    let block_hash = nodes
                        .all
                        .iter()
                        .map(|node| node.head())
                        .min_by_key(|head| head.height)
                        .unwrap()
                        .last_block_hash;
                    let signer = create_user_test_signer(&account_id);
                    let nonce = request_node.get_next_nonce(&account_id);
                    let tx = SignedTransaction::delete_account(
                        nonce,
                        account_id.clone(),
                        account_id.clone(),
                        beneficiary_id.clone(),
                        &signer,
                        block_hash,
                    );
                    let delete_tx_hash = request_node.submit_tx(tx);
                    resharding_check_trace::deleted_account_step(
                        "submitted delete",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::WaitingForDeleteOutcome {
                        delete_tx_hash,
                        deleted_at_height: tip.height,
                    }
                }
                AccountDeletionState::WaitingForDeleteOutcome {
                    delete_tx_hash,
                    deleted_at_height,
                } => {
                    if tip.height != deleted_at_height + epoch_length {
                        return ControlFlow::Continue(());
                    }
                    let status = request_node
                        .client()
                        .chain
                        .get_partial_transaction_result(&delete_tx_hash)
                        .unwrap()
                        .status;
                    assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
                    resharding_check_trace::deleted_account_step(
                        "checked delete outcome",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::WaitingForGarbageCollection { deleted_at_height }
                }
                AccountDeletionState::WaitingForGarbageCollection { deleted_at_height } => {
                    let garbage_collected_height =
                        deleted_at_height + (gc_num_epochs_to_keep + 1) * epoch_length;
                    if tip.height < garbage_collected_height {
                        return ControlFlow::Continue(());
                    }
                    let query = QueryRequest::ViewAccount { account_id: account_id.clone() };
                    let request_node_result =
                        request_node.runtime_query_at_height(deleted_at_height, query.clone());
                    assert_matches!(request_node_result, Err(..));
                    if let Some(archival_node) = nodes.archival {
                        let _archival_node_result =
                            archival_node.runtime_query_at_height(deleted_at_height, query);
                        // TODO(cloud_archival) Assert that the archival node still has the account.
                    }
                    resharding_check_trace::deleted_account_step(
                        "checked state garbage collected",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::Completed
                }
                AccountDeletionState::Completed => return ControlFlow::Break(()),
            };
            let completed = matches!(next_state, AccountDeletionState::Completed);
            *state.borrow_mut() = next_state;
            if completed { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        }
    }
}

/// The uid of `shard_id` in the shard layout of the node's head.
pub(crate) fn shard_uid_at_head(node: &TestLoopNode<'_>, shard_id: ShardId) -> ShardUId {
    let head = node.head();
    shard_id_to_uid(node.client().epoch_manager.as_ref(), shard_id, &head.epoch_id).unwrap()
}

/// Asserts that the parent shard's flat storage is still ready, which is what happens when a node
/// tracks no child shard after the split and its resharding is skipped.
pub(crate) fn assert_parent_flat_storage_ready(
    node: &TestLoopNode<'_>,
    parent_shard_uid: ShardUId,
) {
    let flat_store = node.client().runtime_adapter.store().flat_store();
    let status = flat_store.get_flat_storage_status(parent_shard_uid);
    assert_matches!(
        status,
        FlatStorageStatus::Ready(_),
        "unexpected parent shard status for {parent_shard_uid}"
    );
}

/// Deletes the state of every shard other than `kept_shard_uid`, so that a test can later assert
/// that only the state of the shard a node tracks is left. Genesis writes the state of all shards.
pub(crate) fn delete_state_of_other_shards(node: &TestLoopNode<'_>, kept_shard_uid: ShardUId) {
    let client = node.client();
    let the_only_shard_uid = kept_shard_uid;
    let store = client.chain.chain_store.store().trie_store();
    let mut store_update = store.store_update();
    for (key, value) in store.store().iter_raw_bytes(DBCol::State) {
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        if shard_uid == the_only_shard_uid {
            continue;
        }
        let (_, rc) = decode_value_with_rc(&value);
        assert!(rc > 0);
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        store_update.decrement_refcount_by(shard_uid, &node_hash, NonZero::new(rc as u32).unwrap());
    }
    store_update.commit();
}

/// Asserts that the state of every shard other than `the_only_shard_uid` has been cleaned up.
pub(crate) fn assert_only_shard_state_left(node: &TestLoopNode<'_>, the_only_shard_uid: ShardUId) {
    let client = node.client();
    let store = client.chain.chain_store.store();
    let mut shard_uid_prefixes = HashSet::new();
    for (key, _) in store.iter_raw_bytes(DBCol::State) {
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        shard_uid_prefixes.insert(shard_uid);
    }
    assert_eq!(shard_uid_prefixes.into_iter().collect_vec(), [the_only_shard_uid]);
}

/// Returns a tuple with the shard uids of: parent, left child, right child.
fn get_resharded_shard_uids(
    left_child_account: &AccountId,
    right_child_account: &AccountId,
    shard_layout_after_resharding: &ShardLayout,
) -> (ShardUId, ShardUId, ShardUId) {
    let left_child_shard_uid =
        shard_layout_after_resharding.account_id_to_shard_uid(&left_child_account);
    let right_child_shard_uid =
        shard_layout_after_resharding.account_id_to_shard_uid(&right_child_account);
    let parent_shard_uid = ShardUId::new(
        3,
        shard_layout_after_resharding.get_parent_shard_id(left_child_shard_uid.shard_id()).unwrap(),
    );
    (parent_shard_uid, left_child_shard_uid, right_child_shard_uid)
}

// Helper function to retrieve any key from the trie. This bypasses all intermediate layers
// (caching, memtrie, flat-storage).
fn get_trie_node_value<I: borsh::BorshDeserialize + Default>(
    client: &Client,
    shard_uid: ShardUId,
    prev_block_hash: &CryptoHash,
    key: TrieKey,
) -> Option<Result<I, StorageError>> {
    client.chain.get_chunk_extra(prev_block_hash, &shard_uid).ok().map(|chunk_extra| {
        let trie = Trie::new(
            Arc::new(TrieDBStorage::new(
                TrieStoreAdapter::new(client.runtime_adapter.store().clone()),
                shard_uid,
            )),
            *chunk_extra.state_root(),
            None,
        );
        Ok(get(&trie, &key)?.unwrap_or_default())
    })
}

/// Deterministic test gas-key signer for an account. Setup and verification
/// share this helper so both sides agree on the public key.
pub(crate) fn gas_key_signer_for_account(account_id: &AccountId) -> Signer {
    const GAS_KEY_SIGNER_SEED: &str = "gas_key_resharding";
    InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, GAS_KEY_SIGNER_SEED).into()
}

//! The two loads every scenario runs: cross-shard transfers and contract calls. They are
//! always on and always together, so a boundary that only works with empty blocks, or with
//! transfers but not contract calls, does not pass.
//!
//! Each load has its own accounts, so nonces never interleave. The accounts follow the
//! builder's two-shard layout, which splits at "test1": ids starting with "low_" are on
//! the shard with index 0 and ids starting with "upper_" on the one with index 1. The
//! layout's shard ids are a shuffle of the indices, so the loads only assume the grouping
//! and read the ids off the layout.

use super::scenario::Progress;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use near_chain::{Block, ChainStoreAccess};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, BlockHeight};
use std::collections::{HashMap, HashSet};

/// The deposit of each transfer.
pub const TRANSFER_AMOUNT: Balance = Balance::from_yoctonear(1);
/// The gas each contract call attaches: far more than the call burns, so most of it is
/// refunded. The chunk gas limit bounds what a chunk burns, not what its calls attach.
const CONTRACT_CALL_GAS: Gas = Gas::from_teragas(300);
/// The first block of the loads, as an offset from the estimated first spice height. Seven
/// heights before it is what the mixed-pattern test of the hand-written tests uses: enough
/// that every load has transactions included below the widest pre-spice gap the catalog
/// admits, and short enough that a silenced shard accumulates a backlog of a few dozen
/// transactions, not a few epochs' worth.
const LOAD_START_OFFSET: i64 = -7;
/// The epoch after which the loads stop, so the next epoch drains and nothing is pending
/// at `done`.
const LOAD_END_EPOCH: u32 = 1;
const CONTRACT_METHOD: &str = "log_something";
/// The shard index the contract account lives on: the upper shard. A fault that silences
/// it keeps the deploy and the call backlog in the pool, and the shard's first spice
/// chunks congest until the pool's transactions expire, so such faults reject it.
pub const CONTRACT_SHARD_INDEX: usize = 1;

fn low_sender() -> AccountId {
    create_account_id("low_sender")
}

fn upper_sender() -> AccountId {
    create_account_id("upper_sender")
}

fn low_receiver() -> AccountId {
    create_account_id("low_receiver")
}

fn upper_receiver() -> AccountId {
    create_account_id("upper_receiver")
}

fn upper_contract() -> AccountId {
    create_account_id("upper_contract")
}

/// Adds the accounts of the loads to `builder`.
pub fn add_accounts(builder: TestLoopBuilder) -> TestLoopBuilder {
    let balance = Balance::from_near(100);
    builder
        .add_user_account(&low_sender(), balance)
        .add_user_account(&upper_sender(), balance)
        .add_user_account(&low_receiver(), balance)
        .add_user_account(&upper_receiver(), balance)
        // Calls reserve their attached gas at the gas price, which the loads push up, so
        // the contract account has room for several in flight at a price far above the
        // minimum.
        .add_user_account(&upper_contract(), Balance::from_near(1000))
}

/// One of the loads, as the oracles and the load's own preconditions name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoadKind {
    /// A transfer from the low shard to an account on the upper shard.
    TransferLowToUpper,
    /// A transfer from the upper shard to an account on the low shard.
    TransferUpperToLow,
    /// The contract account on the upper shard calls itself.
    ContractCall,
}

impl LoadKind {
    pub const ALL: [LoadKind; 3] =
        [LoadKind::TransferLowToUpper, LoadKind::TransferUpperToLow, LoadKind::ContractCall];
    pub const TRANSFERS: [LoadKind; 2] =
        [LoadKind::TransferLowToUpper, LoadKind::TransferUpperToLow];

    pub fn name(self) -> &'static str {
        match self {
            LoadKind::TransferLowToUpper => "transfers from the low shard",
            LoadKind::TransferUpperToLow => "transfers from the upper shard",
            LoadKind::ContractCall => "contract calls",
        }
    }

    /// The account signing the load's transactions, and so the shard they are included in.
    pub fn sender(self) -> AccountId {
        match self {
            LoadKind::TransferLowToUpper => low_sender(),
            LoadKind::TransferUpperToLow => upper_sender(),
            LoadKind::ContractCall => upper_contract(),
        }
    }

    /// The account receiving the load's transactions: the one a transfer deposits to, the
    /// contract for the calls.
    pub fn receiver(self) -> AccountId {
        match self {
            LoadKind::TransferLowToUpper => upper_receiver(),
            LoadKind::TransferUpperToLow => low_receiver(),
            LoadKind::ContractCall => upper_contract(),
        }
    }

    fn is_transfer(self) -> bool {
        self != LoadKind::ContractCall
    }
}

/// A transaction a load submitted.
#[derive(Clone, Debug)]
pub struct SubmittedTx {
    pub kind: LoadKind,
    pub hash: CryptoHash,
    /// The head height of the driver node when the transaction was submitted.
    pub submitted_height: BlockHeight,
}

/// The loads of a run: what they submitted, and once the run is over, where each
/// transaction was included. Each load proves it fired through two oracles: at least one of
/// its transactions is in the last new chunk of its sender's shard before the boundary
/// (`load_in_flight`), and every one is included exactly once (`exactly_once`).
pub struct Loads {
    rate: usize,
    started: bool,
    /// Set by the progress point that ends the loads: the current tick is the last.
    ending: bool,
    finished: bool,
    submitted: Vec<SubmittedTx>,
    /// The balance of each transfer receiver before the first transfer.
    initial_receiver_balances: HashMap<AccountId, Balance>,
    /// The heights each submitted transaction was included at, in new chunks. Filled by
    /// [`Self::scan_inclusions`] after the terminal condition.
    inclusion_heights: HashMap<CryptoHash, Vec<BlockHeight>>,
    /// The deploy of the test contract, from `warmed_up` on.
    deploy_tx: Option<CryptoHash>,
}

impl Loads {
    pub fn new(rate: usize) -> Self {
        Self {
            rate,
            started: false,
            ending: false,
            finished: false,
            submitted: Vec::new(),
            initial_receiver_balances: HashMap::new(),
            inclusion_heights: HashMap::new(),
            deploy_tx: None,
        }
    }

    /// Reacts to a progress point: checks the accounts' shards at `Built`, deploys the contract at
    /// `WarmedUp`, and starts and ends the ticking.
    pub fn on_progress(&mut self, env: &TestLoopEnv, progress: &Progress) {
        match progress {
            Progress::Built => assert_accounts_on_expected_shards(env),
            Progress::WarmedUp => self.deploy_tx = Some(deploy_contract(env)),
            Progress::Height { offset: LOAD_START_OFFSET, .. } => self.start(env),
            Progress::EpochAfter(epoch) if *epoch == LOAD_END_EPOCH => self.ending = true,
            _ => {}
        }
    }

    fn start(&mut self, env: &TestLoopEnv) {
        assert!(!self.started, "the loads start once");
        self.started = true;
        for kind in LoadKind::TRANSFERS {
            let receiver = kind.receiver();
            let balance = env.rpc_node().view_account_query(&receiver).unwrap().amount;
            self.initial_receiver_balances.insert(receiver, balance);
        }
    }

    /// Submits one round of transactions, `rate` per load, if the loads are running. The
    /// driver calls it for every block from the start point through the end point.
    pub fn tick(&mut self, env: &TestLoopEnv) {
        if !self.started || self.finished {
            return;
        }
        let node = env.rpc_node();
        let submitted_height = node.head().height;
        for _ in 0..self.rate {
            for kind in LoadKind::ALL {
                let tx = match kind {
                    LoadKind::TransferLowToUpper | LoadKind::TransferUpperToLow => {
                        node.tx_send_money(&kind.sender(), &kind.receiver(), TRANSFER_AMOUNT)
                    }
                    LoadKind::ContractCall => node.tx_call(
                        &kind.sender(),
                        &kind.receiver(),
                        CONTRACT_METHOD,
                        vec![],
                        Balance::ZERO,
                        CONTRACT_CALL_GAS,
                    ),
                };
                let hash = node.submit_tx(tx);
                self.submitted.push(SubmittedTx { kind, hash, submitted_height });
            }
        }
        if self.ending {
            self.finished = true;
        }
    }

    /// Finds where each submitted transaction was included, scanning every block of the
    /// driver node up to `head_height`. Call after the terminal condition.
    pub fn scan_inclusions(&mut self, env: &TestLoopEnv, head_height: BlockHeight) {
        let mut submitted: HashSet<CryptoHash> = self.submitted.iter().map(|tx| tx.hash).collect();
        submitted.insert(self.deploy_tx());
        self.inclusion_heights = scan_inclusions(&env.rpc_node(), &submitted, head_height);
    }

    /// The deploy of the test contract, submitted at `warmed_up`.
    pub fn deploy_tx(&self) -> CryptoHash {
        self.deploy_tx.expect("the contract is deployed at warmed_up")
    }

    /// The transactions `kind` submitted, in submission order.
    pub fn txs(&self, kind: LoadKind) -> impl Iterator<Item = &SubmittedTx> {
        self.submitted.iter().filter(move |tx| tx.kind == kind)
    }

    /// The heights `tx_hash` was included at: one, when the transaction was included once.
    pub fn inclusion_heights(&self, tx_hash: &CryptoHash) -> &[BlockHeight] {
        self.inclusion_heights.get(tx_hash).map_or(&[], Vec::as_slice)
    }

    /// The balance `kind`'s receiver had before the first transfer. Panics for the contract
    /// calls, which deposit nothing.
    pub fn initial_receiver_balance(&self, kind: LoadKind) -> Balance {
        assert!(kind.is_transfer(), "{} deposit nothing", kind.name());
        self.initial_receiver_balances[&kind.receiver()]
    }
}

/// The heights each of `submitted` was included at, found by scanning the new chunks of
/// every block of `node`'s chain from just above genesis to `head_height`.
pub fn scan_inclusions(
    node: &TestLoopNode<'_>,
    submitted: &HashSet<CryptoHash>,
    head_height: BlockHeight,
) -> HashMap<CryptoHash, Vec<BlockHeight>> {
    let mut inclusion_heights: HashMap<CryptoHash, Vec<BlockHeight>> = HashMap::new();
    let genesis_height = node.client().chain.chain_store.get_genesis_height();
    for height in genesis_height + 1..=head_height {
        let Ok(block_hash) = node.client().chain.chain_store.get_block_hash_by_height(height)
        else {
            continue;
        };
        let block = node.client().chain.get_block(&block_hash).unwrap();
        for chunk_header in block.chunks().iter() {
            if !chunk_header.is_new_chunk() {
                continue;
            }
            let chunk = node.client().chain.get_chunk(&chunk_header.chunk_hash()).unwrap();
            for tx in chunk.to_transactions() {
                if submitted.contains(&tx.get_hash()) {
                    inclusion_heights.entry(tx.get_hash()).or_default().push(height);
                }
            }
        }
    }
    inclusion_heights
}

/// The gas price must move at the boundary, which the replay-pricing check relies on: the
/// boundary replay must price gas with the prev block's `next_gas_price`, as the pre-spice
/// apply did, and if that equals the block's own a replay using the block's own would
/// still certify. The chunk gas limit is what makes the loads move it, and a change in
/// costs that breaks that fails here, so the constant is adjusted by hand.
///
/// The check sizes the load, so it only applies when the two blocks at the boundary have
/// every chunk. A missing chunk leaves nothing to replay with a price, and the chunk after
/// it reports no gas used, so with a gap at either block the price can sit at its floor on
/// both sides whatever the load; the plain scenario then carries the check.
pub fn check_gas_price_moves(env: &TestLoopEnv, last_pre_spice: &Block) -> Result<(), String> {
    let before_last_pre_spice = env.rpc_node().block(*last_pre_spice.header().prev_hash());
    let gap_at_boundary = [last_pre_spice, before_last_pre_spice.as_ref()]
        .iter()
        .any(|block| block.header().chunk_mask().iter().any(|present| !*present));
    if gap_at_boundary {
        tracing::info!(
            target: "test",
            "a chunk is missing at the boundary, so the gas price check does not apply",
        );
        return Ok(());
    }
    let before = before_last_pre_spice.header().next_gas_price();
    let at = last_pre_spice.header().next_gas_price();
    if before == at {
        return Err(format!(
            "the gas price did not move at the boundary: {} yoctoNEAR per gas before the last \
             pre-spice block and {} at it; the loads must use more than half of the summed chunk \
             gas limit, adjust the gas limit",
            before.as_yoctonear(),
            at.as_yoctonear(),
        ));
    }
    Ok(())
}

/// The accounts of each load must sit where the loads assume: the transfers cross shards,
/// and the contract shares the upper shard.
fn assert_accounts_on_expected_shards(env: &TestLoopEnv) {
    let node = env.rpc_node();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap();
    let shard_of = |account_id: AccountId| shard_layout.account_id_to_shard_id(&account_id);
    let low_shard_id = shard_of(low_sender());
    let upper_shard_id = shard_of(upper_sender());
    assert_ne!(low_shard_id, upper_shard_id, "the transfers must cross shards");
    assert_eq!(shard_of(low_receiver()), low_shard_id, "low_receiver must be on the low shard");
    assert_eq!(
        shard_of(upper_receiver()),
        upper_shard_id,
        "upper_receiver must be on the upper shard"
    );
    assert_eq!(
        shard_of(upper_contract()),
        upper_shard_id,
        "upper_contract must be on the upper shard"
    );
    assert_eq!(
        shard_layout.get_shard_index(upper_shard_id).unwrap(),
        CONTRACT_SHARD_INDEX,
        "the contract's shard must have index {CONTRACT_SHARD_INDEX}",
    );
}

/// Submits the deploy of the test contract on the contract account in the genesis epoch,
/// so it is settled long before the boundary. It is not waited for: a fault silencing the
/// contract's shard keeps it in the pool until the shard's first spice chunk, where it
/// lands ahead of the calls, in nonce order; `exactly_once` checks it was included once.
fn deploy_contract(env: &TestLoopEnv) -> CryptoHash {
    let deploy_tx = env.rpc_node().tx_deploy_test_contract(&upper_contract());
    env.rpc_node().submit_tx(deploy_tx)
}

//! The fixed set of invariants every scenario is checked against, and the bookkeeping of
//! what a fault is allowed to violate.
//!
//! Every oracle runs at `done` on the driver node unless noted. A fault may relax an oracle
//! it legitimately violates, narrowed to exactly what it injected ([`Relaxations`]); an
//! oracle the design lists as never relaxed ignores relaxations, and
//! [`Relaxations::from_faults`] rejects a fault that names one.
//!
//! The helpers lifted from the hand-written tests of `activation_boundary.rs` live here
//! too, as `check_*` functions returning the failure as a message and `assert_*` wrappers
//! that panic with it, which those tests keep calling.

use super::driver::{RunRecord, gate_counts};
use super::loads::{LoadKind, TRANSFER_AMOUNT};
use super::scenario::{Fault, Narrowing, OracleId};
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use near_chain::spice::all_stake_fallback::SPICE_FALLBACK_CERTIFICATION_DELAY;
use near_chain::spice::boundary_synthesis::{
    execution_result_and_receipt_proofs_from_pre_spice_apply, get_undelivered_receipt_proofs,
    get_undelivered_receipts,
};
use near_chain::{Block, Chain, ChainStoreAccess, ReceiptFilter, get_incoming_receipts_for_shard};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ChunkHash;
use near_primitives::transaction::ExecutionStatus;
use near_primitives::types::{Balance, BlockHeight, ShardId, SpiceChunkId};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

/// Returns the formatted message as the error of the enclosing function unless `condition`
/// holds.
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

/// An invariant checked at `done`.
pub trait Oracle {
    fn id(&self) -> OracleId;

    /// The name a failure prints. The id's name, unless the oracle belongs to a fault and
    /// has none of its own.
    fn name(&self) -> &'static str {
        self.id().name()
    }

    /// `Err` with what was violated.
    fn check(
        &self,
        env: &TestLoopEnv,
        run: &RunRecord,
        relaxations: &Relaxations,
    ) -> Result<(), String>;
}

/// Everything the faults of a scenario relax.
#[derive(Default)]
pub struct Relaxations {
    by_oracle: HashMap<OracleId, Vec<Narrowing>>,
}

impl Relaxations {
    /// No relaxation: the oracles check everything.
    pub fn none() -> Self {
        Self::default()
    }

    /// Collects what every fault relaxes. Panics when a fault relaxes an oracle the design
    /// lists as never relaxed.
    pub fn from_faults(faults: &[Box<dyn Fault>]) -> Self {
        let mut by_oracle: HashMap<OracleId, Vec<Narrowing>> = HashMap::new();
        for fault in faults {
            for relaxation in fault.relaxes() {
                assert!(
                    relaxation.oracle.is_relaxable(),
                    "fault {} relaxes {}, which is never relaxed",
                    fault.tag(),
                    relaxation.oracle.name(),
                );
                by_oracle.entry(relaxation.oracle).or_default().push(relaxation.narrowing);
            }
        }
        Self { by_oracle }
    }

    fn narrowings(&self, oracle: OracleId) -> &[Narrowing] {
        self.by_oracle.get(&oracle).map_or(&[], Vec::as_slice)
    }

    /// Whether `oracle`, or the half of it documented as relaxable, is switched off.
    pub fn is_whole(&self, oracle: OracleId) -> bool {
        self.narrowings(oracle).contains(&Narrowing::Whole)
    }

    /// Whether the checks of `oracle` at `height` are switched off, for every shard.
    pub fn height_relaxed(&self, oracle: OracleId, height: BlockHeight) -> bool {
        self.narrowings(oracle).iter().any(|narrowing| match narrowing {
            Narrowing::Whole => true,
            Narrowing::Heights(heights) => heights.contains(&height),
            Narrowing::Shards(_) | Narrowing::ShardHeights(_) | Narrowing::Node(_) => false,
        })
    }

    /// Whether the checks of `oracle` for `shard_id` are switched off, at every height.
    pub fn shard_relaxed(&self, oracle: OracleId, shard_id: ShardId) -> bool {
        self.narrowings(oracle).iter().any(|narrowing| match narrowing {
            Narrowing::Whole => true,
            Narrowing::Shards(shards) => shards.contains(&shard_id),
            Narrowing::Heights(_) | Narrowing::ShardHeights(_) | Narrowing::Node(_) => false,
        })
    }

    /// Whether the check of `oracle` for `shard_id` at `height` is switched off.
    pub fn chunk_relaxed(&self, oracle: OracleId, shard_id: ShardId, height: BlockHeight) -> bool {
        self.narrowings(oracle).iter().any(|narrowing| match narrowing {
            Narrowing::Whole => true,
            Narrowing::Heights(heights) => heights.contains(&height),
            Narrowing::Shards(shards) => shards.contains(&shard_id),
            Narrowing::ShardHeights(shard_heights) => shard_heights.contains(&(shard_id, height)),
            Narrowing::Node(_) => false,
        })
    }
}

/// An oracle that is a function of what the run recorded.
struct FnOracle {
    id: OracleId,
    check: fn(&TestLoopEnv, &RunRecord, &Relaxations) -> Result<(), String>,
}

impl Oracle for FnOracle {
    fn id(&self) -> OracleId {
        self.id
    }

    fn check(
        &self,
        env: &TestLoopEnv,
        run: &RunRecord,
        relaxations: &Relaxations,
    ) -> Result<(), String> {
        (self.check)(env, run, relaxations)
    }
}

/// The oracles every scenario runs, in the order of the design's table.
pub fn always_on_oracles() -> Vec<Box<dyn Oracle>> {
    let mut oracles: Vec<Box<dyn Oracle>> = Vec::new();
    let mut add = |id, check| oracles.push(Box::new(FnOracle { id, check }));
    add(OracleId::Crossed, crossed);
    add(OracleId::HeightContiguity, height_contiguity);
    add(OracleId::ChunkCompleteness, chunk_completeness);
    add(OracleId::SupplyIdentity, supply_identity);
    add(OracleId::BoundaryResults, boundary_results);
    add(OracleId::StatelessEndorser, stateless_endorser);
    add(OracleId::WithinWindow, within_window);
    add(OracleId::LoadInFlight, load_in_flight);
    add(OracleId::ExactlyOnce, exactly_once);
    add(OracleId::ViewQueries, view_queries);
    add(OracleId::CodeRequested, code_requested);
    add(OracleId::UndeliveredReceipts, undelivered_receipts);
    add(OracleId::NodeAgreement, node_agreement);
    add(OracleId::GateQuiet, gate_quiet);
    add(OracleId::NoPanics, no_panics);
    oracles
}

pub fn prev_block(env: &TestLoopEnv, block: &Block) -> Arc<Block> {
    env.rpc_node().block(*block.header().prev_hash())
}

/// The blocks above `height` on `node`'s chain, oldest first, up to the head.
pub fn blocks_after(node: &TestLoopNode<'_>, height: BlockHeight) -> Vec<Arc<Block>> {
    let mut blocks = Vec::new();
    let mut block = node.head_block();
    while block.header().height() > height {
        let prev_hash = *block.header().prev_hash();
        blocks.push(block);
        block = node.block(prev_hash);
    }
    blocks.reverse();
    blocks
}

fn panic_on_error(result: Result<(), String>) {
    if let Err(message) = result {
        panic!("{message}");
    }
}

// -- crossed, height_contiguity, chunk_completeness --

fn crossed(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    check_crossed(env, run.last_pre_spice())
}

fn height_contiguity(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    check_height_contiguity(env, run.last_pre_spice(), relaxations)
}

fn chunk_completeness(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    check_chunk_completeness(env, run.last_pre_spice(), relaxations)
}

/// From the last pre-spice block to the head no height is skipped and no chunk is
/// missing: the boundary costs nothing in liveness. Execution advanced past it.
pub fn assert_boundary_crossed_without_missing_chunks(env: &TestLoopEnv, last_pre_spice: &Block) {
    let relaxations = Relaxations::none();
    panic_on_error(
        check_crossed(env, last_pre_spice)
            .and_then(|()| check_height_contiguity(env, last_pre_spice, &relaxations))
            .and_then(|()| check_chunk_completeness(env, last_pre_spice, &relaxations)),
    );
}

/// The head is spice and the spice final execution head is above the last pre-spice height.
fn check_crossed(env: &TestLoopEnv, last_pre_spice: &Block) -> Result<(), String> {
    let node = env.rpc_node();
    ensure!(
        node.head_block().is_spice_block(),
        "the head at height {} is not a spice block",
        node.head().height,
    );
    let final_execution_head = node
        .client()
        .chain
        .chain_store
        .spice_final_execution_head()
        .map_err(|err| format!("the spice final execution head is unavailable: {err}"))?;
    let last_pre_spice_height = last_pre_spice.header().height();
    ensure!(
        final_execution_head.height > last_pre_spice_height,
        "execution must advance past the boundary: the spice final execution head is at height \
         {} and the last pre-spice block at {last_pre_spice_height}",
        final_execution_head.height,
    );
    Ok(())
}

/// No height is skipped from the last pre-spice block to the head, except the heights
/// `relaxations` allows.
fn check_height_contiguity(
    env: &TestLoopEnv,
    last_pre_spice: &Block,
    relaxations: &Relaxations,
) -> Result<(), String> {
    let node = env.rpc_node();
    let last_pre_spice_height = last_pre_spice.header().height();
    let present: HashSet<BlockHeight> = blocks_after(&node, last_pre_spice_height)
        .iter()
        .map(|block| block.header().height())
        .collect();
    let skipped: Vec<BlockHeight> = (last_pre_spice_height + 1..=node.head().height)
        .filter(|height| !present.contains(height))
        .filter(|height| !relaxations.height_relaxed(OracleId::HeightContiguity, *height))
        .collect();
    ensure!(skipped.is_empty(), "height skipped crossing the boundary: {skipped:?}");
    Ok(())
}

/// No chunk is missing from the last pre-spice block to the head, except the
/// `(shard, height)` pairs `relaxations` allows.
fn check_chunk_completeness(
    env: &TestLoopEnv,
    last_pre_spice: &Block,
    relaxations: &Relaxations,
) -> Result<(), String> {
    let node = env.rpc_node();
    let epoch_manager = node.client().epoch_manager.as_ref();
    for block in blocks_after(&node, last_pre_spice.header().height()) {
        let height = block.header().height();
        let shard_layout = epoch_manager
            .get_shard_layout(block.header().epoch_id())
            .map_err(|err| format!("no shard layout at height {height}: {err}"))?;
        for (shard_index, present) in block.header().chunk_mask().iter().enumerate() {
            if *present {
                continue;
            }
            let shard_id = shard_layout.get_shard_id(shard_index).unwrap();
            ensure!(
                relaxations.chunk_relaxed(OracleId::ChunkCompleteness, shard_id, height),
                "chunk of shard {shard_id} missing at height {height} crossing the boundary",
            );
        }
    }
    Ok(())
}

// -- supply_identity --

fn supply_identity(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    check_supply_subtracts_certified_burn_once(env, run.last_pre_spice()).map(|_| ())
}

/// Every block after the last pre-spice block subtracts from the supply exactly the burn
/// of the blocks its core statements newly certify. So the last pre-spice block's burn,
/// the one its pre-spice apply wrote, enters the supply once, at the block certifying
/// it, and never again. Returns that block's height.
pub fn assert_supply_subtracts_certified_burn_once(
    env: &TestLoopEnv,
    last_pre_spice: &Block,
) -> BlockHeight {
    check_supply_subtracts_certified_burn_once(env, last_pre_spice)
        .unwrap_or_else(|message| panic!("{message}"))
}

/// The expected burn comes from the core statements alone, independent of the production
/// code: a block certifies in the block whose core statements carry its last missing
/// shard's result. Inflation is zero, so there is no minting term. Assumes the shard
/// layout does not change from the last pre-spice block to the head. Returns the height of
/// the block certifying the last pre-spice block.
fn check_supply_subtracts_certified_burn_once(
    env: &TestLoopEnv,
    last_pre_spice: &Block,
) -> Result<BlockHeight, String> {
    let node = env.rpc_node();
    let client = node.client();
    let shard_layout = client
        .epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;
    let num_shards = shard_layout.num_shards() as usize;

    // The burn in the last pre-spice block's chunk extras. A missing chunk's extra
    // repeats the burn of the last chunk applied on its shard.
    let mut last_pre_spice_burnt = Balance::ZERO;
    for shard_uid in shard_layout.shard_uids() {
        let chunk_extra =
            client.chain.chain_store.get_chunk_extra(last_pre_spice.hash(), &shard_uid).map_err(
                |err| format!("no chunk extra of {shard_uid} at the last pre-spice: {err}"),
            )?;
        last_pre_spice_burnt =
            last_pre_spice_burnt.checked_add(chunk_extra.balance_burnt()).unwrap();
    }
    ensure!(
        last_pre_spice_burnt > Balance::ZERO,
        "the last pre-spice block must burn gas for the supply identity to be meaningful",
    );

    let mut shards_by_block: HashMap<CryptoHash, HashSet<ShardId>> = HashMap::new();
    let mut burnt_by_block: HashMap<CryptoHash, Balance> = HashMap::new();
    let mut certifying_height = None;
    let mut prev_supply = last_pre_spice.header().total_supply();
    for block in blocks_after(&node, last_pre_spice.header().height()) {
        let mut expected_burn = Balance::ZERO;
        for (chunk_id, execution_result) in block.spice_core_statements().iter_execution_results() {
            let shards = shards_by_block.entry(chunk_id.block_hash).or_default();
            ensure!(
                shards.insert(chunk_id.shard_id),
                "result of {chunk_id:?} appears in two blocks' core statements",
            );
            let burnt = burnt_by_block.entry(chunk_id.block_hash).or_default();
            *burnt = burnt.checked_add(execution_result.chunk_extra.balance_burnt()).unwrap();
            if shards.len() == num_shards {
                expected_burn = expected_burn.checked_add(*burnt).unwrap();
                if chunk_id.block_hash == *last_pre_spice.hash() {
                    certifying_height = Some(block.header().height());
                }
            }
        }
        ensure!(
            block.header().total_supply() == prev_supply.checked_sub(expected_burn).unwrap(),
            "block {} must subtract exactly the burn of the blocks it newly certifies",
            block.header().height(),
        );
        prev_supply = block.header().total_supply();
    }
    let certifying_height =
        certifying_height.ok_or_else(|| "the last pre-spice block must certify".to_string())?;
    ensure!(
        burnt_by_block.get(last_pre_spice.hash()) == Some(&last_pre_spice_burnt),
        "the certified burn of the last pre-spice block differs from its pre-spice apply",
    );
    Ok(certifying_height)
}

// -- boundary_results, stateless_endorser --

fn boundary_results(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    check_boundary_results(env, run.last_pre_spice())
}

fn stateless_endorser(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    if relaxations.is_whole(OracleId::StatelessEndorser) {
        return Ok(());
    }
    check_stateless_endorsers(env, run.last_pre_spice())
}

/// The certified execution results of the last pre-spice block must be exactly what its
/// pre-spice apply produced, for every shard, and each must be endorsed by at least one
/// validator that never ran that apply. Such a validator can only have endorsed by
/// validating the boundary witness, replaying the apply from the witness and arriving at
/// the same result.
pub fn assert_boundary_results_validated(env: &TestLoopEnv, last_pre_spice: &Block) {
    panic_on_error(
        check_boundary_results(env, last_pre_spice)
            .and_then(|()| check_stateless_endorsers(env, last_pre_spice)),
    );
}

/// The certified results of the last pre-spice block equal the local pre-spice apply, for
/// every shard.
fn check_boundary_results(env: &TestLoopEnv, last_pre_spice: &Block) -> Result<(), String> {
    let rpc_node = env.rpc_node();
    let client = rpc_node.client();
    let epoch_manager = client.epoch_manager.as_ref();
    let core_reader = &client.chain.spice_core_reader;
    let shard_layout = epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;

    let certified = core_reader
        .get_execution_results_by_shard_id(last_pre_spice.header())
        .map_err(|err| format!("the certified results are unavailable: {err}"))?;
    ensure!(
        certified.len() == shard_layout.num_shards() as usize,
        "every chunk of the last pre-spice block must be certified, {} of {} are",
        certified.len(),
        shard_layout.num_shards(),
    );

    for shard_id in shard_layout.shard_ids() {
        let certified = certified[&shard_id].as_ref();
        // The rpc node tracks every shard, so it applied the last pre-spice block in
        // full the pre-spice way; this is the result the boundary attests.
        let (applied, _) = execution_result_and_receipt_proofs_from_pre_spice_apply(
            &client.chain.chain_store,
            epoch_manager,
            last_pre_spice,
            shard_id,
        )
        .map_err(|err| format!("no pre-spice apply result for shard {shard_id}: {err}"))?;
        ensure!(
            certified == &applied,
            "certified result of shard {shard_id} differs from the pre-spice apply",
        );
    }
    Ok(())
}

/// For every shard some endorser of the boundary chunk never ran the pre-spice apply, and
/// endorsed the certified result.
fn check_stateless_endorsers(env: &TestLoopEnv, last_pre_spice: &Block) -> Result<(), String> {
    let rpc_node = env.rpc_node();
    let client = rpc_node.client();
    let core_reader = &client.chain.spice_core_reader;
    let shard_layout = client
        .epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;
    let certified = core_reader
        .get_execution_results_by_shard_id(last_pre_spice.header())
        .map_err(|err| format!("the certified results are unavailable: {err}"))?;

    for shard_id in shard_layout.shard_ids() {
        let certified = certified
            .get(&shard_id)
            .ok_or_else(|| format!("the boundary chunk of shard {shard_id} is not certified"))?;
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let mut stateless_endorsers = Vec::new();
        for (index, data) in env.node_datas.iter().enumerate() {
            let applied_locally = env
                .node(index)
                .client()
                .chain
                .chain_store
                .get_chunk_extra(last_pre_spice.hash(), &shard_uid)
                .is_ok();
            if applied_locally {
                continue;
            }
            let Some(endorsement) =
                core_reader.get_endorsement(last_pre_spice.hash(), shard_id, &data.account_id)
            else {
                continue;
            };
            ensure!(
                endorsement.execution_result_hash == certified.compute_hash(),
                "{} endorsed a different result for shard {shard_id} than certified",
                data.account_id,
            );
            stateless_endorsers.push(data.account_id.clone());
        }
        ensure!(
            !stateless_endorsers.is_empty(),
            "no validator without the pre-spice apply of shard {shard_id} endorsed its boundary \
             chunk; the boundary witness validation went unexercised",
        );
    }
    Ok(())
}

// -- within_window --

/// The boundary chunks certify before `SPICE_FALLBACK_CERTIFICATION_DELAY` blocks pass.
fn within_window(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    if relaxations.is_whole(OracleId::WithinWindow) {
        return Ok(());
    }
    let node = env.rpc_node();
    let last_pre_spice = run.last_pre_spice();
    let last_pre_spice_height = last_pre_spice.header().height();
    let shard_layout = node
        .client()
        .epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;
    for shard_id in shard_layout.shard_ids() {
        let chunk_id = SpiceChunkId { block_hash: *last_pre_spice.hash(), shard_id };
        let (certifying_height, _) = node
            .certifying_height_and_endorsers(&chunk_id)
            .ok_or_else(|| format!("the boundary chunk of shard {shard_id} never certified"))?;
        ensure!(
            certifying_height < last_pre_spice_height + SPICE_FALLBACK_CERTIFICATION_DELAY,
            "the boundary chunk of shard {shard_id} certified at height {certifying_height}, \
             with the fallback open from height {}",
            last_pre_spice_height + SPICE_FALLBACK_CERTIFICATION_DELAY,
        );
    }
    Ok(())
}

// -- load_in_flight --

/// Each load has a transaction in the last new chunk of its sender's shard at or before the
/// last pre-spice block, so its receipt is in flight at the boundary whatever chunks are
/// missing. A fault that silences a shard relaxes it for the loads whose sender lives there.
fn load_in_flight(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    let node = env.rpc_node();
    let last_pre_spice = run.last_pre_spice();
    let shard_layout = node
        .client()
        .epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;
    let mut errors = Vec::new();
    for kind in LoadKind::ALL {
        let shard_id = shard_layout.account_id_to_shard_id(&kind.sender());
        if relaxations.shard_relaxed(OracleId::LoadInFlight, shard_id) {
            continue;
        }
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let last_new_chunk_height = last_pre_spice.chunks()[shard_index].height_included();
        let included_there = run
            .loads
            .txs(kind)
            .any(|tx| run.loads.inclusion_heights(&tx.hash).contains(&last_new_chunk_height));
        if !included_there {
            errors.push(format!(
                "none of the {} was included in the last new chunk of shard {shard_id} before \
                 the boundary, at height {last_new_chunk_height}, so the load has no receipt in \
                 flight at the boundary",
                kind.name(),
            ));
        }
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

// -- exactly_once --

fn succeeded(status: &ExecutionStatus) -> bool {
    matches!(status, ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_))
}

/// Every transaction of every load is included once, every outcome succeeded, and every
/// deposit lands once: each transfer receiver's balance grew by the amount times the
/// number of transfers to it.
fn exactly_once(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    const MAX_REPORTED_PROBLEMS: usize = 5;
    let node = env.rpc_node();
    let chain = &node.client().chain;
    let mut problems = Vec::new();
    for kind in LoadKind::ALL {
        for tx in run.loads.txs(kind) {
            let inclusion_heights = run.loads.inclusion_heights(&tx.hash);
            if inclusion_heights.len() != 1 {
                problems.push(format!(
                    "transaction {} of the {} submitted at height {} was included at \
                     {inclusion_heights:?}, not once",
                    tx.hash,
                    kind.name(),
                    tx.submitted_height,
                ));
                continue;
            }
            let outcome = match chain.get_execution_outcome(&tx.hash) {
                Ok(outcome) => outcome.outcome_with_id.outcome,
                Err(err) => {
                    problems.push(format!("no outcome of transaction {}: {err}", tx.hash));
                    continue;
                }
            };
            if !succeeded(&outcome.status) {
                problems.push(format!(
                    "transaction {} of the {} failed: {:?}",
                    tx.hash,
                    kind.name(),
                    outcome.status,
                ));
            }
            for receipt_id in &outcome.receipt_ids {
                match chain.get_execution_outcome(receipt_id) {
                    Ok(receipt_outcome)
                        if succeeded(&receipt_outcome.outcome_with_id.outcome.status) => {}
                    Ok(receipt_outcome) => problems.push(format!(
                        "receipt {receipt_id} of transaction {} of the {} failed: {:?}",
                        tx.hash,
                        kind.name(),
                        receipt_outcome.outcome_with_id.outcome.status,
                    )),
                    Err(err) => problems.push(format!(
                        "no outcome of receipt {receipt_id} of transaction {}: {err}",
                        tx.hash,
                    )),
                }
            }
        }
    }
    // The deploy is submitted once at warmup and must have landed exactly once, whatever
    // kept it in the pool.
    let deploy_tx = run.loads.deploy_tx();
    let deploy_inclusions = run.loads.inclusion_heights(&deploy_tx);
    if deploy_inclusions.len() != 1 {
        problems.push(format!(
            "the contract deploy {deploy_tx} was included at {deploy_inclusions:?}, not once"
        ));
    }
    match chain.get_execution_outcome(&deploy_tx) {
        Ok(outcome) if succeeded(&outcome.outcome_with_id.outcome.status) => {}
        Ok(outcome) => problems.push(format!(
            "the contract deploy {deploy_tx} failed: {:?}",
            outcome.outcome_with_id.outcome.status
        )),
        Err(err) => problems.push(format!("no outcome of the contract deploy {deploy_tx}: {err}")),
    }
    for kind in LoadKind::TRANSFERS {
        let receiver = kind.receiver();
        let final_balance = node.view_account_query(&receiver).unwrap().amount;
        let expected_increase =
            TRANSFER_AMOUNT.checked_mul(run.loads.txs(kind).count() as u128).unwrap();
        let initial_balance = run.loads.initial_receiver_balance(kind);
        if final_balance.checked_sub(initial_balance) != Some(expected_increase) {
            problems.push(format!(
                "{receiver} gained {} yoctoNEAR, expected exactly {} yoctoNEAR: every deposit \
                 must land once",
                final_balance.as_yoctonear().saturating_sub(initial_balance.as_yoctonear()),
                expected_increase.as_yoctonear(),
            ));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    let hidden = problems.len().saturating_sub(MAX_REPORTED_PROBLEMS);
    problems.truncate(MAX_REPORTED_PROBLEMS);
    let mut message = problems.join("; ");
    if hidden > 0 {
        message.push_str(&format!("; and {hidden} more"));
    }
    Err(message)
}

// -- view_queries --

/// Balances as of heights on both sides of the boundary for the transfer receivers, an
/// outcome pair spanning the boundary for the transfer load, and both outcomes of the
/// calls on either side of it. The heights are resolved through `get_block_hash_by_height`
/// and those with no canonical block are left out; the last pre-spice block, the first
/// spice block and the block below the terminal head always exist and are always queried.
fn view_queries(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    let node = env.rpc_node();
    let chain = &node.client().chain;
    let boundary_height = run.last_pre_spice().header().height();
    let first_spice_height = run.first_spice().header().height();
    let last_queried_height = run.terminal_height - 1;
    let required = [boundary_height, first_spice_height, last_queried_height];

    let outcome_height = |hash: CryptoHash| -> Result<BlockHeight, String> {
        let outcome = chain
            .get_execution_outcome(&hash)
            .map_err(|err| format!("no outcome of {hash}: {err}"))?;
        chain
            .get_block_header(&outcome.block_hash)
            .map(|header| header.height())
            .map_err(|err| format!("no block of the outcome of {hash}: {err}"))
    };
    let receipt_ids = |tx_hash: CryptoHash| -> Result<Vec<CryptoHash>, String> {
        chain
            .get_execution_outcome(&tx_hash)
            .map(|outcome| outcome.outcome_with_id.outcome.receipt_ids)
            .map_err(|err| format!("no outcome of transaction {tx_hash}: {err}"))
    };
    let single_receipt = |tx_hash: CryptoHash| -> Result<CryptoHash, String> {
        match receipt_ids(tx_hash)?[..] {
            [receipt_id] => Ok(receipt_id),
            ref ids => {
                Err(format!("transaction {tx_hash} produced {} receipts, not one", ids.len()))
            }
        }
    };

    // Where each transfer's deposit landed: the height of the block whose chunk applied
    // the receipt. With every chunk present that is the block after the inclusion; a
    // missing chunk on the receiving shard defers it to that shard's next new chunk, which
    // past the boundary is its first spice chunk.
    let mut deposit_heights: HashMap<LoadKind, Vec<BlockHeight>> = HashMap::new();
    for kind in LoadKind::TRANSFERS {
        for tx in run.loads.txs(kind) {
            let height = outcome_height(single_receipt(tx.hash)?)?;
            deposit_heights.entry(kind).or_default().push(height);
        }
    }

    // The receiver's balance as of the block at height h reflects exactly the deposits
    // applied at or below h.
    for queried_height in [
        boundary_height - 2,
        boundary_height - 1,
        boundary_height,
        boundary_height + 1,
        boundary_height + 2,
        last_queried_height,
    ] {
        let Ok(block_hash) = chain.chain_store.get_block_hash_by_height(queried_height) else {
            ensure!(
                !required.contains(&queried_height),
                "no canonical block at height {queried_height}, which view queries must cover",
            );
            continue;
        };
        for kind in LoadKind::TRANSFERS {
            let receiver = kind.receiver();
            let balance = node.view_account_balance_at(block_hash, &receiver).map_err(|err| {
                format!("view query of {receiver} at height {queried_height} must work: {err:?}")
            })?;
            let landed = deposit_heights
                .get(&kind)
                .map_or(0, |heights| heights.iter().filter(|h| **h <= queried_height).count());
            let expected = TRANSFER_AMOUNT.checked_mul(landed as u128).unwrap();
            let initial_balance = run.loads.initial_receiver_balance(kind);
            ensure!(
                balance.checked_sub(initial_balance) == Some(expected),
                "balance of {receiver} as of height {queried_height} is {} yoctoNEAR above its \
                 initial balance, it must reflect exactly the {landed} deposits applied by then",
                balance.as_yoctonear().saturating_sub(initial_balance.as_yoctonear()),
            );
        }
    }

    // An outcome pair spanning the boundary: a transfer whose transaction outcome sits in
    // a pre-spice block and whose receipt outcome sits in a spice block. There always is
    // one: the transfer in the last pre-spice chunk of the shard whose last chunk is the
    // later one has its receipt wait for the other shard's next new chunk, which is that
    // shard's first spice chunk.
    let mut spanning = None;
    'transfers: for kind in LoadKind::TRANSFERS {
        for tx in run.loads.txs(kind) {
            let tx_height = outcome_height(tx.hash)?;
            if tx_height > boundary_height {
                continue;
            }
            let receipt_id = single_receipt(tx.hash)?;
            let receipt_height = outcome_height(receipt_id)?;
            if receipt_height > boundary_height {
                spanning = Some((tx.hash, tx_height, receipt_id, receipt_height));
                break 'transfers;
            }
        }
    }
    let (spanning_tx, tx_height, receipt_id, receipt_height) = spanning.ok_or_else(|| {
        format!(
            "no transfer has its transaction outcome at or below the last pre-spice height \
             {boundary_height} and its receipt outcome above it; some receipt must be in \
             flight across the boundary",
        )
    })?;
    ensure!(
        run.loads.inclusion_heights(&spanning_tx).contains(&tx_height),
        "the outcome of transaction {spanning_tx} is at height {tx_height}, not at its \
         inclusion height {:?}",
        run.loads.inclusion_heights(&spanning_tx),
    );
    ensure!(
        chain
            .get_block_header(&chain.chain_store.get_block_hash_by_height(receipt_height).unwrap())
            .map_or(false, |header| header.is_spice()),
        "the receipt {receipt_id} of transaction {spanning_tx} executed at height \
         {receipt_height}, above the boundary, in a block that is not a spice block",
    );

    // The last call included at or below the boundary and the first included above it:
    // both outcomes of each are retrievable. The call's receipt is local, so it executes
    // in the same block as the transaction and need not span the boundary.
    let call_heights: Vec<(CryptoHash, BlockHeight)> = run
        .loads
        .txs(LoadKind::ContractCall)
        .filter_map(|tx| run.loads.inclusion_heights(&tx.hash).first().map(|h| (tx.hash, *h)))
        .collect();
    let last_pre_spice_call =
        call_heights.iter().filter(|(_, h)| *h <= boundary_height).max_by_key(|(_, h)| *h);
    let first_spice_call =
        call_heights.iter().filter(|(_, h)| *h >= first_spice_height).min_by_key(|(_, h)| *h);
    for (side, call) in [("at or below", last_pre_spice_call), ("above", first_spice_call)] {
        let (call_tx, height) = call.ok_or_else(|| {
            format!("no contract call was included {side} the last pre-spice height")
        })?;
        outcome_height(*call_tx)?;
        let receipt_ids = receipt_ids(*call_tx)?;
        ensure!(
            !receipt_ids.is_empty(),
            "the contract call {call_tx} included at height {height} produced no receipt",
        );
        for receipt_id in receipt_ids {
            outcome_height(receipt_id)?;
        }
    }
    Ok(())
}

// -- code_requested --

/// At least one validator fetched the contract code with a code request for a call chunk
/// at or after the last pre-spice block, and the certified first spice chunk of the
/// contract's shard was endorsed by a validator that did not apply it. Relaxed whole when
/// every validator applies every chunk itself, since nobody then validates from a witness
/// or requests code.
fn code_requested(
    env: &TestLoopEnv,
    run: &RunRecord,
    relaxations: &Relaxations,
) -> Result<(), String> {
    if relaxations.is_whole(OracleId::CodeRequested) {
        return Ok(());
    }
    let node = env.rpc_node();
    let chain = &node.client().chain;
    let last_pre_spice_height = run.last_pre_spice().header().height();
    let requested_at_boundary = run.code_requests.iter().any(|chunk_id| {
        chain
            .chain_store
            .get_block_header(&chunk_id.block_hash)
            .is_ok_and(|header| header.height() >= last_pre_spice_height)
    });
    ensure!(
        requested_at_boundary,
        "no contract code request for a chunk at or after the last pre-spice block (height \
         {last_pre_spice_height}) was observed, among {} requests",
        run.code_requests.len(),
    );

    // A validator that did not apply the contract shard's first spice chunk endorsed its
    // certified result, so it validated the chunk from the witness, which carries the
    // contract accesses, and fetched the code it lacked.
    let first_spice = run.first_spice();
    let shard_layout = chain
        .epoch_manager
        .get_shard_layout(first_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the first spice block: {err}"))?;
    let contract_shard_id = shard_layout.account_id_to_shard_id(&LoadKind::ContractCall.receiver());
    let certified = chain
        .spice_core_reader
        .get_execution_results_by_shard_id(first_spice.header())
        .map_err(|err| format!("the certified results are unavailable: {err}"))?
        .remove(&contract_shard_id)
        .ok_or_else(|| {
            format!("the first spice chunk of shard {contract_shard_id} is not certified")
        })?;
    let shard_uid = ShardUId::from_shard_id_and_layout(contract_shard_id, &shard_layout);
    let stateless_endorsers = env
        .node_datas
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            env.node(*index)
                .client()
                .chain
                .chain_store
                .get_chunk_extra(first_spice.hash(), &shard_uid)
                .is_err()
        })
        .filter(|(_, data)| {
            chain
                .spice_core_reader
                .get_endorsement(first_spice.hash(), contract_shard_id, &data.account_id)
                .is_some_and(|endorsement| {
                    endorsement.execution_result_hash == certified.compute_hash()
                })
        })
        .count();
    ensure!(
        stateless_endorsers > 0,
        "no validator endorsed shard {contract_shard_id}'s first spice chunk from its witness",
    );
    Ok(())
}

// -- undelivered_receipts --

/// For every ordered shard pair, the carriers of undelivered receipts are the source's
/// chunks after the target's last pre-spice chunk, each receipt was pending at the
/// boundary, and executed in the first spice block. The expectations are derived from the
/// chain, not from the tags: the target's last included height at the last pre-spice
/// block says which chunks were missing, whatever made them so. When every chunk is new it
/// asserts that nothing is undelivered.
fn undelivered_receipts(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    let node = env.rpc_node();
    let chain = &node.client().chain;
    let epoch_manager = chain.epoch_manager.as_ref();
    let last_pre_spice = run.last_pre_spice();
    let first_spice = run.first_spice();
    let last_pre_spice_height = last_pre_spice.header().height();
    let first_spice_height = first_spice.header().height();
    let shard_layout = epoch_manager
        .get_shard_layout(last_pre_spice.header().epoch_id())
        .map_err(|err| format!("no shard layout at the last pre-spice block: {err}"))?;
    let shard_ids: Vec<ShardId> = shard_layout.shard_ids().collect();
    let last_included_height = |shard_id: ShardId| {
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        last_pre_spice.chunks()[shard_index].height_included()
    };

    let mut undelivered_receipt_ids_by_receiving_shard: HashMap<ShardId, Vec<CryptoHash>> =
        HashMap::new();
    for &source_shard_id in &shard_ids {
        let source_shard_index = shard_layout.get_shard_index(source_shard_id).unwrap();
        let undelivered_receipt_proofs = get_undelivered_receipt_proofs(
            &chain.chain_store,
            epoch_manager,
            last_pre_spice,
            source_shard_id,
        )
        .map_err(|err| {
            format!("no undelivered receipt proofs of shard {source_shard_id}: {err}")
        })?;
        for &target_shard_id in &shard_ids {
            if target_shard_id == source_shard_id {
                continue;
            }
            // The source's chunks after the target's last one carry what the target
            // missed, each with one proof to the target. When the target's chunk at the
            // boundary is new there are none.
            let target_last_included_height = last_included_height(target_shard_id);
            let expected_carriers: HashSet<ChunkHash> = (target_last_included_height + 1
                ..=last_pre_spice_height)
                .filter_map(|height| {
                    let block = chain.get_block_by_height(height).ok()?;
                    let chunks = block.chunks();
                    let chunk_header = &chunks[source_shard_index];
                    chunk_header.is_new_chunk(height).then(|| chunk_header.chunk_hash().clone())
                })
                .collect();
            let carriers: HashSet<ChunkHash> = undelivered_receipt_proofs
                .iter()
                .filter(|(_, proofs)| {
                    proofs.iter().any(|proof| proof.1.to_shard_id == target_shard_id)
                })
                .map(|(chunk_hash, _)| chunk_hash.clone())
                .collect();
            ensure!(
                carriers == expected_carriers,
                "the carriers of shard {source_shard_id} for shard {target_shard_id} are \
                 {carriers:?}, they must be its chunks after shard {target_shard_id}'s last one at \
                 height {target_last_included_height}: {expected_carriers:?}",
            );
            for (chunk_hash, proofs) in &undelivered_receipt_proofs {
                let to_target =
                    proofs.iter().filter(|proof| proof.1.to_shard_id == target_shard_id).count();
                ensure!(
                    to_target <= 1,
                    "chunk {chunk_hash:?} of shard {source_shard_id} must carry one proof per \
                     target that missed it, it has {to_target} for shard {target_shard_id}",
                );
            }
        }

        let undelivered_receipts = get_undelivered_receipts(
            &chain.chain_store,
            epoch_manager,
            last_pre_spice,
            source_shard_id,
        )
        .map_err(|err| format!("no undelivered receipts of shard {source_shard_id}: {err}"))?;
        for receipt in &undelivered_receipts {
            let receipt_id = receipt.receipt_id();
            let receiving_shard_id = shard_layout.account_id_to_shard_id(receipt.receiver_id());
            let receipt_outcome = chain
                .get_execution_outcome(receipt_id)
                .map_err(|err| format!("no outcome of {receipt_id}: {err}"))?;
            if receipt_outcome.block_hash != *first_spice.hash() {
                // Delivered to the first spice chunk but applied later. That is legitimate
                // only when the chunk ran out of gas, on the backlog a gap leaves behind,
                // and pushed its incoming receipts to the delayed queue.
                let outcome_height = chain
                    .get_block_header(&receipt_outcome.block_hash)
                    .map(|header| header.height())
                    .map_err(|err| format!("no block of the outcome of {receipt_id}: {err}"))?;
                ensure!(
                    outcome_height > first_spice_height,
                    "the undelivered receipt {receipt_id} executed at height {outcome_height}, \
                     before the first spice block at {first_spice_height}",
                );
                let shard_uid =
                    ShardUId::from_shard_id_and_layout(receiving_shard_id, &shard_layout);
                let chunk_extra = chain
                    .chain_store
                    .get_chunk_extra(first_spice.hash(), &shard_uid)
                    .map_err(|err| {
                        format!("no chunk extra of {shard_uid} at the first spice block: {err}")
                    })?;
                ensure!(
                    chunk_extra.gas_used() >= chunk_extra.gas_limit(),
                    "the undelivered receipt {receipt_id} executed at height {outcome_height}, \
                     not in the first spice block at {first_spice_height}, though shard \
                     {receiving_shard_id}'s first spice chunk had gas to spare: {} of {} used",
                    chunk_extra.gas_used(),
                    chunk_extra.gas_limit(),
                );
            }
            undelivered_receipt_ids_by_receiving_shard
                .entry(receiving_shard_id)
                .or_default()
                .push(*receipt_id);
        }
    }
    for (receiving_shard_id, undelivered_receipt_ids) in undelivered_receipt_ids_by_receiving_shard
    {
        // The receipts were pending for the receiving shard at the boundary: sent after its
        // last pre-spice chunk, so no pre-spice chunk of it applied them.
        let receiving_last_included_height = last_included_height(receiving_shard_id);
        ensure!(
            receiving_last_included_height < last_pre_spice_height,
            "receipts are undelivered to shard {receiving_shard_id}, whose chunk at the last \
             pre-spice block is new",
        );
        let pending_receipt_ids: HashSet<CryptoHash> = get_incoming_receipts_for_shard(
            &chain.chain_store,
            epoch_manager,
            receiving_shard_id,
            &shard_layout,
            *last_pre_spice.hash(),
            receiving_last_included_height,
            ReceiptFilter::TargetShard,
        )
        .map_err(|err| format!("no pending receipts of shard {receiving_shard_id}: {err}"))?
        .iter()
        .flat_map(|response| response.1.iter())
        .flat_map(|proof| proof.0.iter())
        .map(|receipt| *receipt.receipt_id())
        .collect();
        for receipt_id in &undelivered_receipt_ids {
            ensure!(
                pending_receipt_ids.contains(receipt_id),
                "receipt {receipt_id} carried to shard {receiving_shard_id} must be pending for \
                 it at the boundary",
            );
        }
    }
    Ok(())
}

// -- node_agreement --

/// Every node has the same chain at the height the driver drove all of them to, and the
/// same certified results for the last pre-spice block. The driver runs every node to that
/// height first, because the loop stops the instant the driver's predicate holds and the
/// others may not have processed the block yet; heads can then differ by a block, so the
/// comparison is of the canonical block at that height, not of the heads.
fn node_agreement(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    let height = run.agreement_height;
    let block_hash_at = |node: &TestLoopNode<'_>| {
        node.client().chain.chain_store.get_block_hash_by_height(height).map_err(|err| {
            format!("no canonical block at height {height} on {}: {err}", node.node_data.account_id)
        })
    };
    let driver = env.rpc_node();
    let driver_block_hash = block_hash_at(&driver)?;
    let last_pre_spice_header = run.last_pre_spice().header();
    let driver_results = driver
        .client()
        .chain
        .spice_core_reader
        .get_execution_results_by_shard_id(last_pre_spice_header)
        .map_err(|err| format!("the certified results of the driver are unavailable: {err}"))?;
    for index in 0..env.node_datas.len() {
        let node = env.node(index);
        ensure!(
            node.head().height >= height,
            "{} is at height {}, below the agreement height {height}",
            node.node_data.account_id,
            node.head().height,
        );
        let block_hash = block_hash_at(&node)?;
        ensure!(
            block_hash == driver_block_hash,
            "{} has block {block_hash} at height {height}, the driver has {driver_block_hash}",
            node.node_data.account_id,
        );
        let results = node
            .client()
            .chain
            .spice_core_reader
            .get_execution_results_by_shard_id(last_pre_spice_header)
            .map_err(|err| {
                format!(
                    "the certified results of {} are unavailable: {err}",
                    node.node_data.account_id
                )
            })?;
        ensure!(
            results == driver_results,
            "{} has different certified results for the last pre-spice block than the driver",
            node.node_data.account_id,
        );
    }
    Ok(())
}

// -- gate_quiet --

/// No node's `SpiceMessageGate` drops anything from the moment that node's head first
/// reaches the last pre-spice block. The driver snapshots each node's per-kind drop counts
/// when its head gets there; drops before that point are legitimate.
fn gate_quiet(env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
    let mut errors = Vec::new();
    for (index, snapshot) in &run.gate_snapshots {
        let now = gate_counts(env, *index);
        for ((actor, kind, before), (_, _, after)) in snapshot.iter().zip(&now) {
            if before != after {
                errors.push(format!(
                    "the {actor} of {} dropped {} {} messages after its head reached the last \
                     pre-spice block",
                    env.node_datas[*index].account_id,
                    after - before,
                    kind.as_str(),
                ));
            }
        }
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

// -- no_panics --

/// No node panicked: the test loop reached `done`, which is what makes the oracles run.
fn no_panics(_: &TestLoopEnv, _: &RunRecord, _: &Relaxations) -> Result<(), String> {
    Ok(())
}

// -- helpers shared with the hand-written tests --

/// The chunk presence of `shard_index` at `heights`: 'x' missing, '.' present.
pub fn observed_chunk_pattern(
    chain: &Chain,
    shard_index: usize,
    heights: Range<BlockHeight>,
) -> String {
    heights
        .map(|height| {
            let block = chain.get_block_by_height(height).unwrap();
            if block.chunks()[shard_index].is_new_chunk(height) { '.' } else { 'x' }
        })
        .collect()
}

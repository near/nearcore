use crate::config::Config;
use crate::estimator_context::{EstimatorContext, Testbed};
use crate::gas_cost::GasCost;
use crate::transaction_builder::TransactionBuilder;

use std::collections::HashMap;

use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_vm_logic::ExtCosts;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rand_xorshift::XorShiftRng;

pub fn read_resource(path: &str) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(dir).join(path);
    std::fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to load test resource: {}, {}", path.display(), err))
}

pub(crate) fn transaction_cost(
    testbed: Testbed,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
) -> GasCost {
    let block_size = 100;
    let (gas_cost, _ext_costs) = transaction_cost_ext(testbed, block_size, make_transaction);
    gas_cost
}

pub(crate) fn transaction_cost_ext(
    mut testbed: Testbed,
    block_size: usize,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let blocks = {
        let n_blocks = testbed.config.warmup_iters_per_block + testbed.config.iter_per_block;
        let mut blocks = Vec::with_capacity(n_blocks);
        for _ in 0..n_blocks {
            let mut block = Vec::with_capacity(block_size);
            for _ in 0..block_size {
                let tx = make_transaction(testbed.transaction_builder());
                block.push(tx)
            }
            blocks.push(block)
        }
        blocks
    };

    let measurements = testbed.measure_blocks(blocks);
    let measurements =
        measurements.into_iter().skip(testbed.config.warmup_iters_per_block).collect::<Vec<_>>();

    aggregate_per_block_measurements(testbed.config, block_size, measurements)
}

pub(crate) fn fn_cost(
    ctx: &mut EstimatorContext,
    method: &str,
    ext_cost: ExtCosts,
    count: u64,
) -> GasCost {
    let (total_cost, measured_count) = fn_cost_count(ctx, method, ext_cost);
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

pub(crate) fn fn_cost_count(
    ctx: &mut EstimatorContext,
    method: &str,
    ext_cost: ExtCosts,
) -> (GasCost, u64) {
    let block_size = 2;
    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_unused_account();
        tb.transaction_from_function_call(sender, method, Vec::new())
    };
    let testbed = ctx.testbed();
    let (gas_cost, ext_costs) = transaction_cost_ext(testbed, block_size, &mut make_transaction);
    let ext_cost = ext_costs[&ext_cost];
    (gas_cost, ext_cost)
}

pub(crate) fn noop_function_call_cost(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.noop_function_call_cost.clone() {
        return cost;
    }

    let cost = {
        let testbed = ctx.testbed();

        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", Vec::new())
        };
        transaction_cost(testbed, &mut make_transaction)
    };

    ctx.cached.noop_function_call_cost = Some(cost.clone());
    cost
}

/// Estimates the cost to call `method`, but makes sure that `setup` is called
/// before.
///
/// Used for storage costs -- `setup` writes stuff into the storage, where
/// `method` can then find it. We take care to make sure that `setup` is run in
/// a separate block, to make sure we hit the database and not an in-memory hash
/// map.
pub(crate) fn fn_cost_with_setup(
    ctx: &mut EstimatorContext,
    setup: &str,
    method: &str,
    ext_cost: ExtCosts,
    count: u64,
) -> GasCost {
    let (total_cost, measured_count) = {
        let block_size = 2usize;
        let n_blocks = ctx.config.warmup_iters_per_block + ctx.config.iter_per_block;

        let mut testbed = ctx.testbed();

        let blocks = {
            let mut blocks = Vec::with_capacity(2 * n_blocks);
            for _ in 0..n_blocks {
                let tb = testbed.transaction_builder();
                let mut setup_block = Vec::new();
                let mut block = Vec::new();
                for _ in 0..block_size {
                    let sender = tb.random_unused_account();
                    let setup_tx =
                        tb.transaction_from_function_call(sender.clone(), setup, Vec::new());
                    let tx = tb.transaction_from_function_call(sender, method, Vec::new());

                    setup_block.push(setup_tx);
                    block.push(tx);
                }
                blocks.push(setup_block);
                blocks.push(block);
            }
            blocks
        };

        let measurements = testbed.measure_blocks(blocks);
        // Filter out setup blocks.
        let measurements: Vec<_> = measurements
            .into_iter()
            .skip(ctx.config.warmup_iters_per_block * 2)
            .enumerate()
            .filter(|(i, _)| i % 2 == 1)
            .map(|(_, m)| m)
            .collect();

        let (gas_cost, ext_costs) =
            aggregate_per_block_measurements(ctx.config, block_size, measurements);
        (gas_cost, ext_costs[&ext_cost])
    };
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

pub(crate) fn aggregate_per_block_measurements(
    config: &Config,
    block_size: usize,
    measurements: Vec<(GasCost, HashMap<ExtCosts, u64>)>,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let mut block_costs = Vec::new();
    let mut total_ext_costs: HashMap<ExtCosts, u64> = HashMap::new();
    let mut total = GasCost::zero(config.metric);
    let mut n = 0;
    for (gas_cost, ext_cost) in measurements {
        block_costs.push(gas_cost.to_gas() as f64);
        total += gas_cost;
        n += block_size as u64;
        for (c, v) in ext_cost {
            *total_ext_costs.entry(c).or_default() += v;
        }
    }
    for v in total_ext_costs.values_mut() {
        *v /= n;
    }
    let mut gas_cost = total / n;
    gas_cost.set_uncertain(is_high_variance(&block_costs));
    (gas_cost, total_ext_costs)
}

/// We expect our cost computations to be fairly reproducible, and just flag
/// "high-variance" measurements as suspicious. To make results easily
/// explainable, we just require that all the samples don't deviate from the
/// mean by more than 15%, where the number 15 is somewhat arbitrary.
///
/// Note that this looks at block processing times, and each block contains
/// multiples of things we are actually measuring. As low block variance doesn't
/// guarantee low within-block variance, this is necessary an approximate sanity
/// check.
pub(crate) fn is_high_variance(samples: &[f64]) -> bool {
    let threshold = 0.15;

    let mean = samples.iter().copied().sum::<f64>() / (samples.len() as f64);

    let all_below_threshold =
        samples.iter().copied().all(|it| (mean - it).abs() < mean * threshold);

    !all_below_threshold
}

/// Get account id from its index.
pub(crate) fn get_account_id(account_index: usize) -> AccountId {
    AccountId::try_from(format!("near_{}_{}", account_index, account_index)).unwrap()
}

/// Produce a valid function name with `len` letters
pub(crate) fn generate_fn_name(index: usize, len: usize) -> Vec<u8> {
    let mut name = Vec::new();
    let mut index = index;
    name.push((b'A'..=b'Z').chain(b'a'..=b'z').nth(index % 52).unwrap());
    for _ in 1..len {
        index = index / 52;
        name.push((b'A'..=b'Z').chain(b'a'..=b'z').nth(index % 52).unwrap());
    }
    name
}

/// Create a WASM module that is empty except for a main method and a single data entry with n characters
pub(crate) fn generate_data_only_contract(data_size: usize) -> Vec<u8> {
    // Using pseudo-random stream with fixed seed to create deterministic, incompressable payload.
    let prng: XorShiftRng = rand::SeedableRng::seed_from_u64(0xdeadbeef);
    let payload = prng.sample_iter(&Alphanumeric).take(data_size).collect::<String>();
    let wat_code = format!("(module (data \"{payload}\") (func (export \"main\")))");
    wat::parse_str(wat_code).unwrap()
}

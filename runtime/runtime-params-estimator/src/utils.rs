use crate::apply_block_cost;
use crate::estimator_context::EstimatorContext;
use crate::gas_cost::{GasCost, NonNegativeTolerance};
use crate::transaction_builder::TransactionBuilder;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_vm_runner::internal::VMKind;
use near_vm_runner::logic::{ExtCosts, VMConfig};
use rand::distributions::Alphanumeric;
use rand::Rng;
use rand_xorshift::XorShiftRng;
use std::collections::HashMap;
use std::iter;

pub fn read_resource(path: &str) -> Vec<u8> {
    let dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(dir).join(path);
    std::fs::read(&path)
        .unwrap_or_else(|err| panic!("failed to load test resource: {}, {}", path.display(), err))
}

/// Attempts to clear OS page cache on Linux based system. Will fail on
/// other systems. Requires write access to /proc/sys/vm/drop_caches
#[cfg(target_os = "linux")]
pub fn clear_linux_page_cache() -> std::io::Result<()> {
    unsafe {
        libc::sync();
    }
    std::fs::write("/proc/sys/vm/drop_caches", b"1")
}

#[track_caller]
pub(crate) fn transaction_cost(
    ctx: &mut EstimatorContext,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
) -> GasCost {
    let block_size = 100;
    let (gas_cost, _ext_costs) = transaction_cost_ext(ctx, block_size, make_transaction, 0);
    gas_cost
}

#[track_caller]
pub(crate) fn transaction_cost_ext(
    ctx: &mut EstimatorContext,
    block_size: usize,
    make_transaction: &mut dyn FnMut(&mut TransactionBuilder) -> SignedTransaction,
    block_latency: usize,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let verbose = ctx.config.debug;
    let measurement_overhead = overhead_per_measured_block(ctx, block_latency);

    let mut testbed = ctx.testbed();
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

    let measurements = testbed.measure_blocks(blocks, block_latency);
    if verbose {
        // prints individual block measurements (without division by number of
        // inner items) which helps understanding issue with high variance
        eprint!("|warmup|");
        for (gas, _ext) in &measurements[..testbed.config.warmup_iters_per_block] {
            eprint!(" {gas:>#7.2?}");
        }
        eprintln!();
        eprint!("|proper|");
        for (gas, _ext) in &measurements[testbed.config.warmup_iters_per_block..] {
            eprint!(" {gas:>#7.2?}");
        }
        eprintln!();
    }
    let measurements =
        measurements.into_iter().skip(testbed.config.warmup_iters_per_block).collect::<Vec<_>>();

    aggregate_per_block_measurements(block_size, measurements, Some(measurement_overhead))
}

/// Returns the total measurement overhead for a measured block.
pub(crate) fn overhead_per_measured_block(
    ctx: &mut EstimatorContext,
    block_latency: usize,
) -> GasCost {
    let per_block_overhead = apply_block_cost(ctx);
    let measurement_overhead = per_block_overhead * (1 + block_latency) as u64;
    measurement_overhead
}

#[track_caller]
pub(crate) fn fn_cost(
    ctx: &mut EstimatorContext,
    method: &str,
    ext_cost: ExtCosts,
    count: u64,
) -> GasCost {
    // Most functions finish execution in a single block. Other measurements
    // should use `fn_cost_count`.
    let block_latency = 0;
    let (total_cost, measured_count) = fn_cost_count(ctx, method, ext_cost, block_latency);
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    total_cost.saturating_sub(&base_cost, &NonNegativeTolerance::PER_MILLE) / count
}

#[track_caller]
pub(crate) fn fn_cost_count(
    ctx: &mut EstimatorContext,
    method: &str,
    ext_cost: ExtCosts,
    block_latency: usize,
) -> (GasCost, u64) {
    let block_size = 20;
    let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
        let sender = tb.random_unused_account();
        tb.transaction_from_function_call(sender, method, Vec::new())
    };
    let (gas_cost, ext_costs) =
        transaction_cost_ext(ctx, block_size, &mut make_transaction, block_latency);
    let ext_cost = ext_costs[&ext_cost];
    (gas_cost, ext_cost)
}

pub(crate) fn noop_function_call_cost(ctx: &mut EstimatorContext) -> GasCost {
    if let Some(cost) = ctx.cached.noop_function_call_cost.clone() {
        return cost;
    }

    let cost = {
        let mut make_transaction = |tb: &mut TransactionBuilder| -> SignedTransaction {
            let sender = tb.random_unused_account();
            tb.transaction_from_function_call(sender, "noop", Vec::new())
        };
        transaction_cost(ctx, &mut make_transaction)
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
        let block_latency = 0;
        let overhead = overhead_per_measured_block(ctx, block_latency);
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

        let measurements = testbed.measure_blocks(blocks, 0);
        // Filter out setup blocks.
        let measurements: Vec<_> = measurements
            .into_iter()
            .skip(ctx.config.warmup_iters_per_block * 2)
            .enumerate()
            .filter(|(i, _)| i % 2 == 1)
            .map(|(_, m)| m)
            .collect();

        let (gas_cost, ext_costs) =
            aggregate_per_block_measurements(block_size, measurements, Some(overhead));

        let is_write = [ExtCosts::storage_write_base, ExtCosts::storage_remove_base]
            .iter()
            .any(|cost| *ext_costs.get(cost).unwrap_or(&0) > 0);
        if !is_write {
            assert_eq!(
                0,
                *ext_costs.get(&ExtCosts::touching_trie_node).unwrap_or(&0),
                "flat storage not working"
            );
        }

        (gas_cost, ext_costs[&ext_cost])
    };
    assert_eq!(measured_count, count);

    let base_cost = noop_function_call_cost(ctx);

    (total_cost - base_cost) / count
}

/// Estimates the cost to call `method`, on given contract.
///
/// Used for costs that specifically need to deploy a contract first. Note that
/// this causes the contract to be deployed once in every iteration. Therefore,
/// whenever possible, prefer to add a method to the generic test contract.
///
/// The measured cost is the pure cost of executing a function call action. The
/// overhead of block, transaction, and receipt processing is already subtracted
/// in the returned result. It does so by executing the method n+1 times in a
/// single transaction, and subtract the cost of a transaction that calls the
/// method once, before dividing by n.
pub(crate) fn fn_cost_in_contract(
    ctx: &mut EstimatorContext,
    method: &str,
    code: &[u8],
    n_actions: usize,
) -> GasCost {
    let n_warmup_blocks = ctx.config.warmup_iters_per_block;
    let n_blocks = n_warmup_blocks + ctx.config.iter_per_block;
    let mut testbed = ctx.testbed();

    let mut chosen_accounts = {
        let tb = testbed.transaction_builder();
        iter::repeat_with(|| tb.random_unused_account()).take(n_blocks + 1).collect::<Vec<_>>()
    };
    testbed.clear_caches();

    for account in &chosen_accounts {
        let tb = testbed.transaction_builder();
        let setup = vec![Action::DeployContract(DeployContractAction { code: code.to_vec() })];
        let setup_tx = tb.transaction_from_actions(account.clone(), account.clone(), setup);

        testbed.process_block(vec![setup_tx], 0);
    }

    let mut blocks = Vec::with_capacity(n_blocks);
    // Measurement blocks with single tx with many actions.
    for account in chosen_accounts.drain(..n_blocks) {
        let actions = iter::repeat_with(|| function_call_action(method.to_string()))
            .take(n_actions)
            .collect();
        let tx = testbed.transaction_builder().transaction_from_actions(
            account.clone(),
            account,
            actions,
        );
        blocks.push(vec![tx]);
    }
    // Base with single tx with single action. Insert it after warm-up blocks.
    let final_account = chosen_accounts.pop().unwrap();
    let base_tx = testbed.transaction_builder().transaction_from_actions(
        final_account.clone(),
        final_account,
        vec![function_call_action(method.to_string())],
    );
    blocks.insert(n_warmup_blocks, vec![base_tx]);

    let mut measurements = testbed.measure_blocks(blocks, 0);
    measurements.drain(0..n_warmup_blocks);

    let (base_gas_cost, _base_ext_costs) = measurements.remove(0);
    // Do not subtract block overhead because we already subtract the base.
    let overhead = None;
    let block_size = 1;
    let (gas_cost, _ext_costs) =
        aggregate_per_block_measurements(block_size, measurements, overhead);
    gas_cost.saturating_sub(&base_gas_cost, &NonNegativeTolerance::Strict) / (n_actions - 1) as u64
}

fn function_call_action(method_name: String) -> Action {
    Action::FunctionCall(FunctionCallAction {
        method_name,
        args: Vec::new(),
        gas: 10u64.pow(15),
        deposit: 0,
    })
}

/// Takes a list of measurements of input blocks and returns the cost for a
/// single work item.
///
/// Inputs measurements cover the work to ingest and fully process it. Note that
/// the processing can span multiple block ticks but the measured work is
/// defined in a single block.
///
/// Each block is assumed to contain `block_size` amount of work
/// items to be measured. Usually, one such work item is a transaction, or an
/// action within a transaction.
///
/// The output is the cost of a single work item.
pub(crate) fn aggregate_per_block_measurements(
    block_size: usize,
    block_measurements: Vec<(GasCost, HashMap<ExtCosts, u64>)>,
    overhead: Option<GasCost>,
) -> (GasCost, HashMap<ExtCosts, u64>) {
    let mut block_costs = Vec::new();
    let mut total_ext_costs: HashMap<ExtCosts, u64> = HashMap::new();
    let mut total = GasCost::zero();
    let num_blocks = block_measurements.len() as u64;
    for (block_cost, block_ext_cost) in block_measurements {
        block_costs.push(block_cost.to_gas() as f64);
        total += block_cost;
        for (c, v) in block_ext_cost {
            *total_ext_costs.entry(c).or_default() += v;
        }
    }

    let work_item_ext_cost = {
        for v in total_ext_costs.values_mut() {
            let n = num_blocks * block_size as u64;
            *v /= n;
        }
        total_ext_costs
    };

    let mut avg_block_cost = total / num_blocks;
    if is_high_variance(&block_costs) {
        avg_block_cost.set_uncertain("HIGH-VARIANCE");
    }
    if let Some(overhead) = overhead {
        avg_block_cost = avg_block_cost.saturating_sub(&overhead, &NonNegativeTolerance::PER_MILLE);
    }
    let work_item_gas_cost = avg_block_cost / block_size as u64;

    (work_item_gas_cost, work_item_ext_cost)
}

pub(crate) fn average_cost(measurements: Vec<GasCost>) -> GasCost {
    let scalar_costs = measurements.iter().map(|cost| cost.to_gas() as f64).collect::<Vec<_>>();
    let total: GasCost = measurements.into_iter().sum();
    let mut avg = total / scalar_costs.len() as u64;
    if is_high_variance(&scalar_costs) {
        avg.set_uncertain("HIGH-VARIANCE");
    }
    avg
}

/// We expect our cost computations to be fairly reproducible, and just flag
/// "high-variance" measurements as suspicious. We require that sample standard
/// deviation is no more than 10% of the mean.
///
/// Note that this looks at block processing times, and each block contains
/// multiples of things we are actually measuring. As low block variance doesn't
/// guarantee low within-block variance, this is necessary an approximate sanity
/// check.
pub(crate) fn is_high_variance(samples: &[f64]) -> bool {
    let threshold = 0.1;

    if samples.len() <= 1 {
        return true;
    }
    let mean = samples.iter().copied().sum::<f64>() / (samples.len() as f64);
    let s2 = samples.iter().map(|value| (mean - *value).powi(2)).sum::<f64>()
        / (samples.len() - 1) as f64;
    let stddev = s2.sqrt();
    stddev / mean > threshold
}

/// Returns several percentile values from the given vector of costs. For
/// example, the input 0.9 represents the 90th percentile, which is the largest
/// gas cost in the vector for which no more than 90% of all values are smaller.
pub(crate) fn percentiles(
    mut costs: Vec<GasCost>,
    percentiles: &[f32],
) -> impl Iterator<Item = GasCost> + '_ {
    costs.sort();
    let sample_size = costs.len();
    percentiles
        .into_iter()
        .map(move |p| (p * sample_size as f32).ceil() as usize - 1)
        .map(move |idx| costs[idx].clone())
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
pub(crate) fn generate_data_only_contract(data_size: usize, config: &VMConfig) -> Vec<u8> {
    // Using pseudo-random stream with fixed seed to create deterministic, incompressable payload.
    let prng: XorShiftRng = rand::SeedableRng::seed_from_u64(0xdeadbeef);
    let payload = prng.sample_iter(&Alphanumeric).take(data_size).collect();
    let payload = String::from_utf8(payload).unwrap();
    let wat_code = format!(
        r#"(module
            (memory 1)
            (func (export "main"))
            (data (i32.const 0) "{payload}")
        )"#
    );
    let wasm = wat::parse_str(wat_code).unwrap();
    // Validate generated code is valid.
    near_vm_runner::prepare::prepare_contract(&wasm, config, VMKind::NearVm).unwrap();
    wasm
}

pub(crate) fn random_vec(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen()).collect()
}

#[cfg(test)]
mod test {
    use super::percentiles;
    use crate::{config::GasMetric, gas_cost::GasCost};
    use rand::prelude::SliceRandom;

    #[track_caller]
    fn check_percentiles(gas_values: &[u64], p_values: &[f32], expected_gas_results: &[u64]) {
        let costs =
            gas_values.iter().map(|n| GasCost::from_gas((*n).into(), GasMetric::Time)).collect();

        let results = percentiles(costs, p_values).map(|cost| cost.to_gas()).collect::<Vec<_>>();

        assert_eq!(results, expected_gas_results,)
    }

    #[test]
    fn test_percentiles() {
        let mut one_to_thousand = (1..=1000u64).collect::<Vec<_>>();
        one_to_thousand.shuffle(&mut rand::thread_rng());
        check_percentiles(&one_to_thousand, &[0.1, 0.5, 0.995], &[100, 500, 995]);

        let mut one_to_ninety_nine = (1..=99u64).collect::<Vec<_>>();
        one_to_ninety_nine.shuffle(&mut rand::thread_rng());
        check_percentiles(&one_to_ninety_nine, &[0.1, 0.5, 0.995], &[10, 50, 99]);

        let mut one_to_one_o_one = (1..=101u64).collect::<Vec<_>>();
        one_to_one_o_one.shuffle(&mut rand::thread_rng());
        check_percentiles(&one_to_one_o_one, &[0.1, 0.5, 0.995], &[11, 51, 101]);
    }
}

use crate::node::Node;
use once_cell::sync::Lazy;
use std::process::Output;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

static HEAVY_TESTS_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub fn heavy_test<F>(f: F)
where
    F: FnOnce(),
{
    let _guard = HEAVY_TESTS_LOCK.lock();
    f();
}

pub fn check_result(output: Output) -> Result<String, String> {
    let mut result = String::from_utf8_lossy(output.stdout.as_slice());
    if !output.status.success() {
        if result.is_empty() {
            result = String::from_utf8_lossy(output.stderr.as_slice());
        }
        return Err(result.into_owned());
    }
    Ok(result.into_owned())
}

pub fn wait<F>(mut f: F, check_interval_ms: u64, max_wait_ms: u64)
where
    F: FnMut() -> bool,
{
    let mut ms_slept = 0;
    while !f() {
        thread::sleep(Duration::from_millis(check_interval_ms));
        ms_slept += check_interval_ms;
        if ms_slept > max_wait_ms {
            println!("BBBB Slept {}; max_wait_ms {}", ms_slept, max_wait_ms);
            panic!("Timed out waiting for the condition");
        }
    }
}

/// TODO it makes sense to have three types of wait checks:
/// Wait until sufficient number of nodes is caught up (> 2/3). This can be checked by looking at the block heights and verifying that the blocks are produced;
/// Wait until a certain node is caught up and participating in a consensus. Check first-layer BLS signatures;
/// Wait until all nodes are more-or-less caught up. Check that the max_height - min_height < threshold;
///
pub fn wait_for_catchup(nodes: &[Arc<RwLock<dyn Node>>]) {
    wait(
        || {
            let tips: Vec<_> = nodes
                .iter()
                .filter(|node| node.read().unwrap().is_running())
                .map(|node| node.read().unwrap().user().get_best_height())
                .collect();
            tips.iter().min() == tips.iter().max()
        },
        1000,
        10000,
    );
}

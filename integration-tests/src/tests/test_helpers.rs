use once_cell::sync::Lazy;
use std::sync::Mutex;
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

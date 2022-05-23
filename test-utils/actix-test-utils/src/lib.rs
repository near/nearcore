use once_cell::sync::Lazy;
use std::sync::Mutex;

pub struct ShutdownableThread {
    pub join: Option<std::thread::JoinHandle<()>>,
    pub actix_system: actix_rt::System,
}

impl ShutdownableThread {
    pub fn start<F>(_name: &'static str, f: F) -> ShutdownableThread
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            run_actix(async move {
                f();
                tx.send(actix_rt::System::current()).unwrap();
            });
        });

        let actix_system = rx.recv().unwrap();
        ShutdownableThread { join: Some(join), actix_system }
    }

    pub fn shutdown(&self) {
        self.actix_system.stop();
    }
}

impl Drop for ShutdownableThread {
    fn drop(&mut self) {
        self.shutdown();
        self.join.take().unwrap().join().unwrap();
    }
}

#[inline]
pub fn spawn_interruptible<F: std::future::Future + 'static>(
    f: F,
) -> actix_rt::task::JoinHandle<F::Output> {
    actix_rt::spawn(f)
}

// Number of actix instances that are currently running.
pub(crate) static ACTIX_INSTANCES_COUNTER: Lazy<Mutex<usize>> = Lazy::new(|| (Mutex::new(0)));

pub fn setup_actix() -> actix_rt::SystemRunner {
    static SET_PANIC_HOOK: std::sync::Once = std::sync::Once::new();

    // This is a workaround to make actix/tokio runtime stop when a task panics.
    // See: https://github.com/actix/actix-net/issues/80
    SET_PANIC_HOOK.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if actix_rt::System::is_registered() {
                actix_rt::System::current().stop_with_code(1);
            }
            default_hook(info);
        }));
    });

    actix_rt::System::new()
}

pub fn block_on_interruptible<F: std::future::Future>(
    sys: &actix_rt::SystemRunner,
    f: F,
) -> F::Output {
    sys.block_on(f)
}

pub fn run_actix<F: std::future::Future>(f: F) {
    {
        let mut value = ACTIX_INSTANCES_COUNTER.lock().unwrap();
        *value += 1;
    }

    let sys = setup_actix();
    block_on_interruptible(&sys, f);
    sys.run().unwrap();

    {
        let mut value = ACTIX_INSTANCES_COUNTER.lock().unwrap();
        *value -= 1;
        if *value == 0 {
            // If we're the last instance - make sure to wait for all RocksDB handles to be dropped.
            near_store::db::RocksDB::block_until_all_instances_are_dropped();
        }
    }
}

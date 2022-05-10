use actix_rt::signal;
use futures::{future, select, task::Poll, FutureExt};
use once_cell::sync::Lazy;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

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

static CAUGHT_SIGINT: AtomicBool = AtomicBool::new(false);

macro_rules! handle_interrupt {
    ($future:expr) => {
        async move {
            assert!(!CAUGHT_SIGINT.load(Ordering::SeqCst), "SIGINT received");
            select! {
                _ = {
                    future::poll_fn(|_| {
                        if CAUGHT_SIGINT.load(Ordering::SeqCst) {
                            return Poll::Ready(());
                        }
                        Poll::Pending
                    })
                }.fuse() => panic!("SIGINT received"),
                _ = $future.fuse() => {},
            }
        }
    };
}

#[inline]
pub fn spawn_interruptible<F: std::future::Future + 'static>(
    f: F,
) -> actix_rt::task::JoinHandle<()> {
    actix_rt::spawn(handle_interrupt!(f))
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
                let exit_code = if CAUGHT_SIGINT.load(Ordering::SeqCst) { 130 } else { 1 };
                actix_rt::System::current().stop_with_code(exit_code);
            }
            default_hook(info);
        }));
    });

    static TRAP_SIGINT_HOOK: std::sync::Once = std::sync::Once::new();

    // This is a workaround to ensure all threads get the exit memo.
    // Plainly polling ctrl_c() on busy threads like ours can be problematic.
    TRAP_SIGINT_HOOK.call_once(|| {
        std::thread::Builder::new()
            .name("SIGINT trap".into())
            .spawn(|| {
                let sys = actix_rt::System::new();
                sys.block_on(async {
                    signal::ctrl_c().await.expect("failed to listen for SIGINT");
                    CAUGHT_SIGINT.store(true, Ordering::SeqCst);
                });
                sys.run().unwrap();
            })
            .expect("failed to spawn SIGINT handler thread");
    });
    actix_rt::System::new()
}

pub fn block_on_interruptible<F: std::future::Future>(
    sys: &actix_rt::SystemRunner,
    f: F,
) {
    sys.block_on(handle_interrupt!(f))
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

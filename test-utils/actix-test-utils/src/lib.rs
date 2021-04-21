use actix_rt::signal;
use futures::{future, select, task::Poll, FutureExt};
use std::sync::atomic::{AtomicBool, Ordering};

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
            run_actix_until_stop(async move {
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

fn run_actix_until<F: std::future::Future>(f: F, expect_panic: bool) {
    static SET_PANIC_HOOK: std::sync::Once = std::sync::Once::new();

    // This is a workaround to make actix/tokio runtime stop when a task panics.
    // See: https://github.com/actix/actix-net/issues/80
    SET_PANIC_HOOK.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !expect_panic {
                default_hook(info);
            }
            if actix_rt::System::is_registered() {
                let exit_code = if CAUGHT_SIGINT.load(Ordering::SeqCst) { 130 } else { 1 };
                actix_rt::System::current().stop_with_code(exit_code);
            }
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

    let sys = actix_rt::System::new();
    sys.block_on(handle_interrupt!(f));
    sys.run().unwrap();
}

pub fn run_actix_until_stop<F: std::future::Future>(f: F) {
    run_actix_until(f, false)
}

pub fn run_actix_until_panic<F: std::future::Future>(f: F) {
    run_actix_until(f, true)
}

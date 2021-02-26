pub struct ShutdownableThread {
    pub join: Option<std::thread::JoinHandle<()>>,
    pub actix_system: actix::System,
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
                tx.send(actix::System::current()).unwrap();
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
            if actix::System::is_registered() {
                actix::System::current().stop_with_code(1);
            }
        }));
    });

    let sys = actix::System::new();
    sys.block_on(f);
    sys.run().unwrap();
}

pub fn run_actix_until_stop<F: std::future::Future>(f: F) {
    run_actix_until(f, false)
}

pub fn run_actix_until_panic<F: std::future::Future>(f: F) {
    run_actix_until(f, true)
}

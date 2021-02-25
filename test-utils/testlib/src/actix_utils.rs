use std::sync::mpsc;

use actix::System;

pub struct ShutdownableThread {
    pub join: Option<std::thread::JoinHandle<()>>,
    pub actix_system: System,
}

impl ShutdownableThread {
    pub fn start<F>(_name: &'static str, f: F) -> ShutdownableThread
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            System::new().block_on(async move {
                f();
                tx.send(System::current()).unwrap();
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

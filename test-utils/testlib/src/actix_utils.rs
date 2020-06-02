use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use actix::System;

pub struct ShutdownableThread {
    pub join: Option<std::thread::JoinHandle<()>>,
    pub actix_system: System,
}

impl ShutdownableThread {
    pub fn start<F>(name: &'static str, f: F) -> ShutdownableThread
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            let system = System::new(name);
            f();
            tx.send(System::current()).unwrap();
            system.run().unwrap();
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
        // Leaving some time for all threads to stop after system is stopped.
        thread::sleep(Duration::from_millis(100));
        self.join.take().unwrap().join().unwrap();
    }
}

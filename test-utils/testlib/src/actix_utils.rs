use std::sync::mpsc;

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
            System::builder()
                .name(name)
                .stop_on_panic(true)
                .run(move || {
                    f();
                    tx.send(System::current()).unwrap();
                })
                .unwrap();
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

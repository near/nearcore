use std::sync::mpsc;
use std::thread::JoinHandle;
use std::{panic, thread};

use actix::{Actor, Addr, System};
use futures::future::IntoFuture;
use futures::stream::Stream;
use log::{debug, error};
use tokio::prelude::Future;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

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
            system.run();
            ()
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

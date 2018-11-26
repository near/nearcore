use std::fmt::Debug;
use futures::Future;

/// Spawns the given subtask. This allows the part of `TxFlowTask` that spawns sub-tasks
/// to be mockable which we use to enforce determinism for testing and debugging.
/// Unfortunately, we cannot use `tokio:runtime::current_thread::Runtime` because it would
/// put the spawned task at the end of the queue instead of executing it immediately.
pub trait SpawnerLike {
    fn new() -> Self;
    fn spawn<F, I, E>(f: F) where
        E: Debug,
        F: Future<Item=I, Error=E> + 'static + Send;
}

pub struct Spawner {}

impl SpawnerLike for Spawner {
    fn new() -> Self {
       Spawner {}
    }

    fn spawn<F, I, E>(f: F) where
        E: Debug,
        F: Future<Item=I, Error=E> + 'static + Send, {
        // Consumes the item.
        tokio::spawn(f.map(|_| ()).map_err(|e| {
           error!("Failure in the sub-task {:?}", e);
        }));
    }
}

pub struct WaitSpawner {}

impl SpawnerLike for WaitSpawner {
    fn new() -> Self {
        WaitSpawner {}
    }
    fn spawn<F, I, E>(f: F) where
        E: Debug,
        F: Future<Item=I, Error=E> + 'static + Send {
        match f.wait() {
            Ok(_) => {},
            Err(e) => error!("Failure in the sub-task {:?}", e)
        };
    }
}

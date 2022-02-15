use std::future::Future;
use std::sync::RwLock;

fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}

#[allow(dead_code)]
fn test<T: Clone + Send + Sync>() {
    is_send::<Once<T>>();
    is_sync::<Once<T>>();
}

// Once is a synchronization primitive, which stores a single value.
// This value can be set at most once, and multiple consumers are
// allowed to wait for that value.
pub struct Once<T: Clone + Send + Sync> {
    value: RwLock<Option<T>>,
    notify: tokio::sync::Notify,
}

impl<T: Clone + Send + Sync> Once<T> {
    pub fn new() -> Once<T> {
        return Once { value: RwLock::new(None), notify: tokio::sync::Notify::new() };
    }

    // set() sets the value of Once to x and returns true.
    // If the value was already set, it just returns false.
    pub fn set(self: &Self, x: T) -> bool {
        let mut v = self.value.write().unwrap();
        if v.is_some() {
            return false;
        }
        *v = Some(x.clone());
        self.notify.notify_waiters();
        drop(v);
        return true;
    }

    // get() gets a clone of the value, or returns None if not set yet.
    pub fn get(self: &Self) -> Option<T> {
        self.value.read().unwrap().clone()
    }

    // wait() waits for Once to be set, then returns a clone of the value.
    pub fn wait(self: &Self) -> impl Future<Output = T> + Send + '_ {
        let l = self.value.read().unwrap();
        let v = (*l).clone();
        let n = self.notify.notified();
        drop(l);
        async move {
            if let Some(v) = v {
                return v;
            }
            n.await;
            return self.get().unwrap();
        }
    }
}

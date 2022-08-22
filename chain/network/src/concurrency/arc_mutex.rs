use std::sync::{Arc,Mutex};
use arc_swap::ArcSwap;

/// Mutex which only synchronizes on writes.
/// Reads always succeed and return the latest written version. 
pub struct ArcMutex<T> {
    value: ArcSwap<T>,
    mutex: Mutex<()>,
}

impl<T:Clone> ArcMutex<T> {
    pub fn new(v:T) -> Self { Self { value: ArcSwap::new(Arc::new(v)), mutex: Mutex::new(()) } }
    // non-blocking
    pub fn load(&self) -> Arc<T> { self.value.load_full() }
    // blocking
    pub fn update<R>(&self, f:impl FnOnce(&mut T) -> R) -> R {
        let _guard = self.mutex.lock().unwrap();
        let mut value = self.value.load().as_ref().clone();
        let res = f(&mut value);
        self.value.store(Arc::new(value));
        res
    }
}

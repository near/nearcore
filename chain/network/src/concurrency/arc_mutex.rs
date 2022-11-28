use arc_swap::ArcSwap;
use std::sync::{Arc, Mutex};

/// Mutex which only synchronizes on writes.
/// Reads always succeed and return the latest written version.
pub struct ArcMutex<T> {
    value: ArcSwap<T>,
    mutex: Mutex<()>,
}

impl<T: Clone> ArcMutex<T> {
    pub fn new(v: T) -> Self {
        Self { value: ArcSwap::new(Arc::new(v)), mutex: Mutex::new(()) }
    }

    /// Loads the last value stored. Non-blocking.
    pub fn load(&self) -> Arc<T> {
        self.value.load_full()
    }

    /// Atomic update of the value. Blocking.
    pub fn update<R>(&self, f: impl FnOnce(T) -> (R,T)) -> R {
        let _guard = self.mutex.lock().unwrap();
        let (res,val) = f(self.value.load().as_ref().clone());
        self.value.store(Arc::new(val));
        res
    }

    /// Atomic update of the value. Value is not modified if an error is returned.
    /// Blocking.
    pub fn try_update<R,E>(&self, f: impl FnOnce(T) -> Result<(R,T),E>) -> Result<R,E> {
        let _guard = self.mutex.lock().unwrap();
        match f(self.value.load().as_ref().clone()) {
            Ok((res,val)) => {
                self.value.store(Arc::new(val));
                Ok(res)
            }
            Err(e) => Err(e),
        }
    }
}

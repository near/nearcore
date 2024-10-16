use std::sync::Mutex;

// AtomicCell narrows down a Mutex API to load/store calls.
pub(crate) struct AtomicCell<T>(Mutex<T>);

impl<T: Clone> AtomicCell<T> {
    pub(crate) fn new(v: T) -> Self {
        Self(Mutex::new(v))
    }
    pub(crate) fn load(&self) -> T {
        self.0.lock().unwrap().clone()
    }
    pub(crate) fn store(&self, v: T) {
        *self.0.lock().unwrap() = v;
    }
}

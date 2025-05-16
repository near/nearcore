use std::sync::Mutex;

// AtomicCell narrows down a Mutex API to load/store calls.
pub(crate) struct AtomicCell<T>(Mutex<T>);

impl<T: Clone> AtomicCell<T> {
    pub fn new(v: T) -> Self {
        Self(Mutex::new(v))
    }
    pub fn load(&self) -> T {
        self.0.lock().unwrap().clone()
    }
    pub fn store(&self, v: T) {
        *self.0.lock().unwrap() = v;
    }
}

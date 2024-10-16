/// Sink is a handler/wrapper of a function Fn(T) -> ().
/// It supports composition with functions Fn(U) -> Fn(T).
/// It is a test-only feature for aggregating internal events of a component under test
/// which are otherwise not observable via public API.
/// Ideally tests should rely solely on public API, however it is not the case as of today.
/// TODO(gprusak): once all network integration tests are migrated to crate, Sink should
/// be hidden behind #[cfg(test)].
use std::sync::Arc;

pub struct Sink<T>(Option<Arc<Box<dyn Fn(T) + Sync + Send + 'static>>>);

impl<T> Clone for Sink<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Sink<T> {
    pub fn null() -> Self {
        Self(None)
    }

    pub fn push(&self, t: T) {
        if let Some(f) = &self.0 {
            f(t)
        }
    }

    pub fn new(f: impl Fn(T) + Sync + Send + 'static) -> Self {
        Sink(Some(Arc::new(Box::new(f))))
    }
}

impl<T: 'static + std::fmt::Debug + Send> Sink<T> {
    pub fn compose<U>(&self, f: impl Send + Sync + 'static + Fn(U) -> T) -> Sink<U> {
        match self.0.clone() {
            None => Sink::null(),
            Some(s) => Sink::new(Box::new(move |u| s(f(u)))),
        }
    }
}

/// Sink is a handler/wrapper of a function Fn(T) -> ().
/// It supports composition with functions Fn(U) -> Fn(T).
/// It is a test-only feature for aggregating internal events of a component under test
/// which are otherwise not observable via public API.
/// Ideally tests should rely solely on public API, however it is not the case as of today.
/// TODO(gprusak): once all network integration tests are migrated to near_network, Sink should
/// be hidden behind #[cfg(test)].
use std::sync::Arc;

pub struct Sink<T>(Arc<Box<dyn Fn(T) + Sync + Send + 'static>>);

impl<T> Clone for Sink<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Sink<T> {
    pub fn null() -> Self {
        Self::new(|_| {})
    }

    pub fn push(&self, t: T) {
        (self.0)(t);
    }

    pub fn new(f: impl Fn(T) + Sync + Send + 'static) -> Self {
        Sink(Arc::new(Box::new(f)))
    }
}

impl<T: 'static + std::fmt::Debug + Send> Sink<T> {
    pub fn compose<U>(&self, f: impl Send + Sync + 'static + Fn(U) -> T) -> Sink<U> {
        let s = self.0.clone();
        Sink(Arc::new(Box::new(move |u| s(f(u)))))
    }
}

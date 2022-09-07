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

impl<T: Send + 'static> Sink<T> {
    // Accepts a constructor of the value to push.
    // Returns a function which does the push.
    // If sink is null it doesn't call make() at all,
    // therefore you can use this to skip expensive computation
    // in non-test env.
    pub fn delayed_push(&self, make: impl FnOnce() -> T) -> Box<dyn Send + 'static + FnOnce()> {
        let maybe_ev = self.0.as_ref().map(|_| make());
        let this = self.clone();
        Box::new(move || {
            maybe_ev.map(|ev| this.push(ev));
        })
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

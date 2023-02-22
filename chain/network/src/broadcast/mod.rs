//! TEST-ONLY: unbounded version of tokio::sync::broadcast channel.
//! It never forgets values, so should be only used in tests.
use crate::sink::Sink;
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    let ch = Arc::new(Channel {
        stream: std::sync::RwLock::new(vec![]),
        notify: tokio::sync::Notify::new(),
    });
    (Sender(ch.clone()), Receiver { next: 0, channel: ch })
}

struct Channel<T> {
    stream: std::sync::RwLock<Vec<T>>,
    notify: tokio::sync::Notify,
}

pub struct Sender<T>(Arc<Channel<T>>);

#[derive(Clone)]
pub struct Receiver<T> {
    channel: Arc<Channel<T>>,
    next: usize,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + Sync + 'static> Sender<T> {
    pub fn send(&self, v: T) {
        let mut l = self.0.stream.write().unwrap();
        l.push(v);
        self.0.notify.notify_waiters();
    }

    pub fn sink(&self) -> Sink<T> {
        let s = self.clone();
        Sink::new(move |v| s.send(v))
    }
}

impl<T: Clone + Send> Receiver<T> {
    /// Returns a copy of the receiver which ignores all the events until now.
    // TODO(gprusak): this still doesn't solve
    // the events being mixed in the stream.
    // Without actix, awaiting the expected state
    // should get way easier.
    pub fn from_now(&self) -> Self {
        Self { channel: self.channel.clone(), next: self.channel.stream.read().unwrap().len() }
    }

    /// recv() extracts a value from the channel.
    /// If channel is empty, awaits until a value is pushed to the channel.
    pub async fn recv(&mut self) -> T {
        self.next += 1;
        let new_value_pushed = {
            // The lock has to be inside a block without await,
            // because otherwise recv() is not Send.
            let l = self.channel.stream.read().unwrap();
            let new_value_pushed = self.channel.notify.notified();
            // Synchronically check if the channel is non-empty.
            // If so, pop a value and return immediately.
            if let Some(v) = l.get(self.next - 1) {
                return v.clone();
            }
            new_value_pushed
        };
        // Channel was empty, so we wait for the new value.
        new_value_pushed.await;
        let v = self.channel.stream.read().unwrap()[self.next - 1].clone();
        v
    }

    /// Calls recv() in a loop until the returned value satisfies the predicate `pred`
    /// (predicate is satisfied iff it returns `Some(u)`). Returns `u`.
    /// All the values popped from the channel in the process are dropped silently.
    pub async fn recv_until<U>(&mut self, mut pred: impl FnMut(T) -> Option<U>) -> U {
        loop {
            if let Some(u) = pred(self.recv().await) {
                return u;
            }
        }
    }

    /// Non-blocking version of recv(): pops a value from the channel,
    /// or returns None if channel is empty.
    pub fn try_recv(&mut self) -> Option<T> {
        let l = self.channel.stream.read().unwrap();
        if l.len() <= self.next {
            return None;
        }
        self.next += 1;
        Some(l[self.next - 1].clone())
    }
}

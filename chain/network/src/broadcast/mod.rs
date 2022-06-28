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
    pub async fn recv(&mut self) -> T {
        self.next += 1;
        let l = self.channel.stream.read().unwrap();
        let n = self.channel.notify.notified();
        let v = if l.len() > self.next - 1 { Some(l[self.next - 1].clone()) } else { None };
        if let Some(v) = v {
            return v;
        }
        drop(l);
        n.await;
        let v = self.channel.stream.read().unwrap()[self.next - 1].clone();
        v
    }

    pub async fn recv_until<U>(&mut self, pred: impl Fn(T) -> Option<U>) -> U {
        loop {
            if let Some(u) = pred(self.recv().await) {
                return u;
            }
        }
    }
}

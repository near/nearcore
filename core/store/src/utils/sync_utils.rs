use parking_lot::{Condvar, Mutex, MutexGuard};
use std::ops::{Deref, DerefMut};

/// A convenience wrapper around a Mutex and a Condvar.
///
/// It enables blocking while waiting for the underlying value to be updated.
/// The implementation ensures that any modification results in all blocked
/// threads being notified.
pub(crate) struct Monitor<T> {
    cvar: Condvar,
    mutex: Mutex<T>,
}

pub(crate) struct MonitorReadGuard<'a, T> {
    guard: MutexGuard<'a, T>,
}

pub(crate) struct MonitorWriteGuard<'a, T> {
    guard: MutexGuard<'a, T>,
    cvar: &'a Condvar,
}

impl<T> Monitor<T> {
    pub fn new(t: T) -> Self {
        Self { mutex: Mutex::new(t), cvar: Condvar::new() }
    }

    pub fn lock(&self) -> MonitorReadGuard<'_, T> {
        let guard = self.mutex.lock();
        MonitorReadGuard { guard }
    }

    pub fn lock_mut(&self) -> MonitorWriteGuard<'_, T> {
        let guard = self.mutex.lock();
        MonitorWriteGuard { guard, cvar: &self.cvar }
    }

    pub fn wait<'a>(&'a self, guard: MonitorReadGuard<'a, T>) -> MonitorReadGuard<'a, T> {
        let mut guard = guard.guard;
        self.cvar.wait(&mut guard);
        MonitorReadGuard { guard }
    }
}

impl<T> Deref for MonitorReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.guard
    }
}

impl<T> Deref for MonitorWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.guard
    }
}

impl<T> DerefMut for MonitorWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.guard
    }
}

impl<T> Drop for MonitorWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.cvar.notify_all();
    }
}

use crate::futures::DelayedActionRunner;

type FakeDelayedActionTask<T> =
    Box<dyn FnOnce(&mut T, &mut dyn DelayedActionRunner<T>) + Send + 'static>;

pub struct FakeDelayedActionRunner<T> {
    tasks: Vec<FakeDelayedActionTask<T>>,
}

// For some reason deriving Default sets a constrain for T to implement Default so instead we
// implement Default trait by hand.
impl<T> Default for FakeDelayedActionRunner<T> {
    fn default() -> Self {
        FakeDelayedActionRunner { tasks: Vec::new() }
    }
}

impl<T> DelayedActionRunner<T> for FakeDelayedActionRunner<T> {
    fn run_later_boxed(
        &mut self,
        _name: &'static str,
        _dur: crate::time::Duration,
        f: FakeDelayedActionTask<T>,
    ) {
        self.tasks.push(f);
    }
}

impl<T> FakeDelayedActionRunner<T> {
    pub fn trigger(&mut self, actor: &mut T) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task(actor, self);
        }
    }
}

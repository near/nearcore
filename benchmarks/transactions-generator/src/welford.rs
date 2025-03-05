use std::ops::{AddAssign, Div, Sub};

#[derive(Debug, Clone)]
pub struct Mean<T> {
    mean: T,
    count: i64,
}

impl<T> Mean<T>
where
    T: Default + Clone,
    T: Sub,
    <T as Sub>::Output: Div<i64>,
    T: AddAssign<<<T as Sub>::Output as Div<i64>>::Output>,
{
    pub fn new() -> Self {
        Self { mean: T::default(), count: 0 }
    }

    pub fn add_measurement(&mut self, x: T) {
        self.count += 1;
        self.mean += (x - self.mean.clone()) / self.count;
    }

    pub fn mean(&self) -> &T {
        &self.mean
    }
}

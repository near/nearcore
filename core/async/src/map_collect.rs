use rayon::iter::{IntoParallelIterator, ParallelIterator};

/// Parallel or sequential mapping and collecting operations.
/// Default behavior uses Rayon, but can be configured to use sequential processing instead.
#[derive(Clone, Copy)]
pub enum MapCollect {
    Sequential,
    Rayon,
}

impl MapCollect {
    /// Maps each element of the input collection using the provided function `f` and collects the
    /// results into a vector. The mapping is done in parallel if `MapCollect::Rayon` is used,
    /// otherwise it is done sequentially.
    pub fn map_collect<T, U, F>(&self, input: Vec<T>, f: F) -> Vec<U>
    where
        F: Fn(T) -> U + Sync + Send,
        T: Send,
        U: Send,
    {
        match self {
            MapCollect::Sequential => input.into_iter().map(f).collect(),
            MapCollect::Rayon => input.into_par_iter().map(f).collect(),
        }
    }
}

use std::any::{Any, TypeId};
use std::collections::HashMap;

#[derive(Default)]
/// Accepts any typed data as block info to be shared across shards.
pub struct BlockInfo {
    data: HashMap<TypeId, Box<dyn Any>>,
}

impl BlockInfo {
    pub fn get<T: Any>(&self) -> Option<&T> {
        self.data.get(&TypeId::of::<T>()).and_then(|boxed| boxed.downcast_ref())
    }

    pub fn insert<T: Any>(&mut self, obj: T) {
        self.data.insert(TypeId::of::<T>(), Box::new(obj));
    }
}

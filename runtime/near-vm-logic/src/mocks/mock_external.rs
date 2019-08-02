use crate::dependencies::ExternalError;
use crate::External;
use std::collections::btree_map::Range;
use std::collections::{BTreeMap, HashMap};
use std::intrinsics::transmute;

#[derive(Default)]
/// Emulates the trie and the mock handling code.
pub struct MockedExternal {
    pub fake_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    iterators: HashMap<u64, Range<'static, Vec<u8>, Vec<u8>>>,
    next_iterator_index: u64,
}

impl MockedExternal {
    pub fn new() -> Self {
        Self::default()
    }
}

impl External for MockedExternal {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.fake_trie.insert(key.to_vec(), value.to_vec()))
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.fake_trie.get(key).cloned())
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.fake_trie.remove(key))
    }

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool, ExternalError> {
        Ok(self.fake_trie.contains_key(key))
    }

    fn storage_iter(&mut self, prefix: &[u8]) -> Result<u64, ExternalError> {
        let res = self.iterators.len() as u64;
        let iterator = self.fake_trie.range(prefix.to_vec()..);
        let iterator = unsafe {
            transmute::<Range<'_, Vec<u8>, Vec<u8>>, Range<'static, Vec<u8>, Vec<u8>>>(iterator)
        };
        self.iterators.insert(self.next_iterator_index, iterator);
        self.next_iterator_index += 1;
        Ok(res)
    }

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, ExternalError> {
        let res = self.iterators.len() as u64;
        let iterator = self.fake_trie.range(start.to_vec()..end.to_vec());
        let iterator = unsafe {
            transmute::<Range<'_, Vec<u8>, Vec<u8>>, Range<'static, Vec<u8>, Vec<u8>>>(iterator)
        };
        self.iterators.insert(self.next_iterator_index, iterator);
        self.next_iterator_index += 1;
        Ok(res)
    }

    fn storage_iter_next(
        &mut self,
        iterator_idx: u64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, ExternalError> {
        match self.iterators.get_mut(&iterator_idx) {
            Some(iter) => Ok(iter.next().map(|(k, v)| (k.clone(), v.clone()))),
            None => Err(ExternalError::InvalidIteratorIndex),
        }
    }

    fn storage_iter_drop(&mut self, iterator_idx: u64) -> Result<(), ExternalError> {
        if self.iterators.remove(&iterator_idx).is_none() {
            Err(ExternalError::InvalidIteratorIndex)
        } else {
            Ok(())
        }
    }

    fn promise_create(
        &mut self,
        _account_id: String,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _amount: u128,
        _gas: u64,
    ) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn promise_then(
        &mut self,
        _promise_id: u64,
        _account_id: String,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _amount: u128,
        _gas: u64,
    ) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn promise_and(&mut self, _promise_indices: &[u64]) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn storage_usage(&self) -> u64 {
        unimplemented!()
    }
}

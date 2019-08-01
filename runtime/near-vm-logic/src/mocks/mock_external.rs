use crate::dependencies::ExternalError;
use crate::External;
use std::collections::btree_map::Range;
use std::collections::BTreeMap;
use std::intrinsics::transmute;

#[derive(Default)]
/// Emulates the trie and the mock handling code.
pub struct MockedExternal {
    fake_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    iterators: Vec<Range<'static, Vec<u8>, Vec<u8>>>,
    valid_iterators_start_at: usize,
}

impl External for MockedExternal {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        self.valid_iterators_start_at = self.iterators.len();
        Ok(self.fake_trie.insert(key.to_vec(), value.to_vec()))
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.fake_trie.get(key).cloned())
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        self.valid_iterators_start_at = self.iterators.len();
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
        self.iterators.push(iterator);
        Ok(res)
    }

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, ExternalError> {
        let res = self.iterators.len() as u64;
        let iterator = self.fake_trie.range(start.to_vec()..end.to_vec());
        let iterator = unsafe {
            transmute::<Range<'_, Vec<u8>, Vec<u8>>, Range<'static, Vec<u8>, Vec<u8>>>(iterator)
        };
        self.iterators.push(iterator);
        Ok(res)
    }

    fn storage_iter_next(
        &mut self,
        iterator_idx: u64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, ExternalError> {
        if iterator_idx < self.valid_iterators_start_at as _ {
            Err(ExternalError::IteratorWasInvalidated)
        } else if iterator_idx >= self.iterators.len() as _ {
            Err(ExternalError::InvalidIteratorIndex)
        } else {
            Ok(self.iterators[iterator_idx as usize].next().map(|(k, v)| (k.clone(), v.clone())))
        }
    }

    fn promise_create(
        &mut self,
        account_id: String,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: u128,
        gas: u64,
    ) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn promise_then(
        &mut self,
        promise_id: u64,
        account_id: String,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: u128,
        gas: u64,
    ) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn promise_and(&mut self, promise_indices: &[u64]) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn storage_usage(&self) -> u64 {
        unimplemented!()
    }
}

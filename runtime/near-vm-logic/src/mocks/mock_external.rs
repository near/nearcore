use crate::dependencies::ExternalError;
use crate::External;
use serde::{Deserialize, Serialize};
use std::collections::btree_map::Range;
use std::collections::{BTreeMap, HashMap};
use std::intrinsics::transmute;

/// Encapsulates fake iterator. Optionally stores, if this iterator is built from prefix.
struct FakeIterator {
    iterator: Range<'static, Vec<u8>, Vec<u8>>,
    prefix: Option<Vec<u8>>,
}

#[derive(Default)]
/// Emulates the trie and the mock handling code.
pub struct MockedExternal {
    pub fake_trie: BTreeMap<Vec<u8>, Vec<u8>>,
    iterators: HashMap<u64, FakeIterator>,
    next_iterator_index: u64,
    receipt_create_calls: Vec<ReceiptCreateCall>,
    next_receipt_index: u64,
}

impl MockedExternal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get calls to receipt create that were performed during contract call.
    pub fn get_receipt_create_calls(&self) -> &Vec<ReceiptCreateCall> {
        &self.receipt_create_calls
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
        let res = self.next_iterator_index;
        let iterator = self.fake_trie.range(prefix.to_vec()..);
        let iterator = unsafe {
            transmute::<Range<'_, Vec<u8>, Vec<u8>>, Range<'static, Vec<u8>, Vec<u8>>>(iterator)
        };
        self.iterators.insert(
            self.next_iterator_index,
            FakeIterator { iterator, prefix: Some(prefix.to_vec()) },
        );
        self.next_iterator_index += 1;
        Ok(res)
    }

    fn storage_iter_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, ExternalError> {
        let res = self.next_iterator_index;
        let iterator = self.fake_trie.range(start.to_vec()..end.to_vec());
        let iterator = unsafe {
            transmute::<Range<'_, Vec<u8>, Vec<u8>>, Range<'static, Vec<u8>, Vec<u8>>>(iterator)
        };
        self.iterators.insert(self.next_iterator_index, FakeIterator { iterator, prefix: None });
        self.next_iterator_index += 1;
        Ok(res)
    }

    fn storage_iter_next(
        &mut self,
        iterator_idx: u64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, ExternalError> {
        match self.iterators.get_mut(&iterator_idx) {
            Some(FakeIterator { iterator, prefix }) => match iterator.next() {
                Some((k, v)) => {
                    if let Some(prefix) = prefix {
                        if k.starts_with(prefix) {
                            Ok(Some((k.clone(), v.clone())))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(Some((k.clone(), v.clone())))
                    }
                }
                None => Ok(None),
            },
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

    fn receipt_create(
        &mut self,
        receipt_indices: Vec<u64>,
        receiver_id: String,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: u64,
    ) -> Result<u64, ExternalError> {
        if receipt_indices.iter().any(|el| *el >= self.next_receipt_index) {
            return Err(ExternalError::InvalidReceiptIndex);
        }
        self.receipt_create_calls.push(ReceiptCreateCall {
            receipt_indices,
            receiver_id,
            method_name,
            arguments,
            attached_deposit,
            prepaid_gas,
        });
        let res = self.next_receipt_index;
        self.next_receipt_index += 1;
        Ok(res)
    }

    fn sha256(&self, data: &[u8]) -> Result<Vec<u8>, ExternalError> {
        let value_hash = sodiumoxide::crypto::hash::sha256::hash(data);
        Ok(value_hash.as_ref().to_vec())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ReceiptCreateCall {
    receipt_indices: Vec<u64>,
    receiver_id: String,
    #[serde(with = "crate::serde_with::bytes_as_str")]
    method_name: Vec<u8>,
    #[serde(with = "crate::serde_with::bytes_as_str")]
    arguments: Vec<u8>,
    attached_deposit: u128,
    prepaid_gas: u64,
}

// Copyright 2017-2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! Test implementation for Externalities.

use std::collections::HashMap;
use std::iter::FromIterator;
use hash_db::Hasher;
use heapsize::HeapSizeOf;
use trie::trie_root;
use backend::InMemory;
use changes_trie::{compute_changes_trie_root, InMemoryStorage as ChangesTrieInMemoryStorage};
use primitives::storage::well_known_keys::CHANGES_TRIE_CONFIG;
use super::{Externalities, OverlayedChanges};

/// Simple HashMap-based Externalities impl.
pub struct TestExternalities<H: Hasher> where H::Out: HeapSizeOf {
	inner: HashMap<Vec<u8>, Vec<u8>>,
	changes_trie_storage: ChangesTrieInMemoryStorage<H>,
	changes: OverlayedChanges,
}

impl<H: Hasher> TestExternalities<H> where H::Out: HeapSizeOf {
	/// Create a new instance of `TestExternalities`
	pub fn new(inner: HashMap<Vec<u8>, Vec<u8>>) -> Self {
		let mut overlay = OverlayedChanges::default();
		super::set_changes_trie_config(
			&mut overlay,
			inner.get(&CHANGES_TRIE_CONFIG.to_vec()).cloned(),
			false,
		).expect("changes trie configuration is correct in test env; qed");

		TestExternalities {
			inner,
			changes_trie_storage: ChangesTrieInMemoryStorage::new(),
			changes: overlay,
		}
	}

	/// Insert key/value
	pub fn insert(&mut self, k: Vec<u8>, v: Vec<u8>) -> Option<Vec<u8>> {
		self.inner.insert(k, v)
	}
}

impl<H: Hasher> ::std::fmt::Debug for TestExternalities<H> where H::Out: HeapSizeOf {
	fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
		write!(f, "{:?}", self.inner)
	}
}

impl<H: Hasher> PartialEq for TestExternalities<H> where H::Out: HeapSizeOf {
	fn eq(&self, other: &TestExternalities<H>) -> bool {
		self.inner.eq(&other.inner)
	}
}

impl<H: Hasher> FromIterator<(Vec<u8>, Vec<u8>)> for TestExternalities<H> where H::Out: HeapSizeOf {
	fn from_iter<I: IntoIterator<Item=(Vec<u8>, Vec<u8>)>>(iter: I) -> Self {
		let mut t = Self::new(Default::default());
		for i in iter {
			t.inner.insert(i.0, i.1);
		}
		t
	}
}

impl<H: Hasher> Default for TestExternalities<H> where H::Out: HeapSizeOf {
	fn default() -> Self { Self::new(Default::default()) }
}

impl<H: Hasher> From<TestExternalities<H>> for HashMap<Vec<u8>, Vec<u8>> where H::Out: HeapSizeOf {
	fn from(tex: TestExternalities<H>) -> Self {
		tex.inner.into()
	}
}

impl<H: Hasher> From< HashMap<Vec<u8>, Vec<u8>> > for TestExternalities<H> where H::Out: HeapSizeOf {
	fn from(hashmap: HashMap<Vec<u8>, Vec<u8>>) -> Self {
		TestExternalities {
			inner: hashmap,
			changes_trie_storage: ChangesTrieInMemoryStorage::new(),
			changes: Default::default(),
		}
	}
}

impl<H: Hasher> Externalities<H> for TestExternalities<H> where H::Out: Ord + HeapSizeOf {
	fn storage(&self, key: &[u8]) -> Option<Vec<u8>> {
		self.inner.get(key).map(|x| x.to_vec())
	}

	fn child_storage(&self, _storage_key: &[u8], _key: &[u8]) -> Option<Vec<u8>> {
		None
	}

	fn place_storage(&mut self, key: Vec<u8>, maybe_value: Option<Vec<u8>>) {
		self.changes.set_storage(key.clone(), maybe_value.clone());
		match maybe_value {
			Some(value) => { self.inner.insert(key, value); }
			None => { self.inner.remove(&key); }
		}
	}

	fn place_child_storage(&mut self, _storage_key: Vec<u8>, _key: Vec<u8>, _value: Option<Vec<u8>>) -> bool {
		false
	}

	fn kill_child_storage(&mut self, _storage_key: &[u8]) { }

	fn clear_prefix(&mut self, prefix: &[u8]) {
		self.changes.clear_prefix(prefix);
		self.inner.retain(|key, _| !key.starts_with(prefix));
	}

	fn chain_id(&self) -> u64 { 42 }

	fn storage_root(&mut self) -> H::Out {
		trie_root::<H, _, _, _>(self.inner.clone())
	}

	fn child_storage_root(&mut self, _storage_key: &[u8]) -> Option<Vec<u8>> {
		None
	}

	fn storage_changes_root(&mut self, block: u64) -> Option<H::Out> {
		compute_changes_trie_root::<_, _, H>(
			&InMemory::default(),
			Some(&self.changes_trie_storage),
			&self.changes,
			block,
		).map(|(root, _)| root.clone())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use primitives::{Blake2Hasher, H256};

	#[test]
	fn commit_should_work() {
		let mut ext = TestExternalities::<Blake2Hasher>::default();
		ext.set_storage(b"doe".to_vec(), b"reindeer".to_vec());
		ext.set_storage(b"dog".to_vec(), b"puppy".to_vec());
		ext.set_storage(b"dogglesworth".to_vec(), b"cat".to_vec());
		const ROOT: [u8; 32] = hex!("0b41e488cccbd67d1f1089592c2c235f5c5399b053f7fe9152dd4b5f279914cd");
		assert_eq!(ext.storage_root(), H256(ROOT));
	}
}

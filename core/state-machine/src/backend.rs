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

//! State machine backends. These manage the code and storage of contracts.

use std::{error, fmt};
use std::cmp::Ord;
use std::collections::HashMap;
use std::marker::PhantomData;
use hash_db::Hasher;
use trie_backend::TrieBackend;
use trie_backend_essence::TrieBackendStorage;
use substrate_trie::{TrieDBMut, TrieMut, MemoryDB, trie_root, child_trie_root};
use heapsize::HeapSizeOf;

/// A state backend is used to read state data and can have changes committed
/// to it.
///
/// The clone operation (if implemented) should be cheap.
pub trait Backend<H: Hasher> {
	/// An error type when fetching data is not possible.
	type Error: super::Error;

	/// Storage changes to be applied if committing
	type Transaction: Consolidate + Default;

	/// Type of trie backend storage.
	type TrieBackendStorage: TrieBackendStorage<H>;

	/// Get keyed storage associated with specific address, or None if there is nothing associated.
	fn storage(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

	/// Get keyed child storage associated with specific address, or None if there is nothing associated.
	fn child_storage(&self, storage_key: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

	/// true if a key exists in storage.
	fn exists_storage(&self, key: &[u8]) -> Result<bool, Self::Error> {
		Ok(self.storage(key)?.is_some())
	}

	/// true if a key exists in child storage.
	fn exists_child_storage(&self, storage_key: &[u8], key: &[u8]) -> Result<bool, Self::Error> {
		Ok(self.child_storage(storage_key, key)?.is_some())
	}

	/// Retrieve all entries keys of child storage and call `f` for each of those keys.
	fn for_keys_in_child_storage<F: FnMut(&[u8])>(&self, storage_key: &[u8], f: F);

	/// Retrieve all entries keys of which start with the given prefix and
	/// call `f` for each of those keys.
	fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], f: F);

	/// Calculate the storage root, with given delta over what is already stored in
	/// the backend, and produce a "transaction" that can be used to commit.
	fn storage_root<I>(&self, delta: I) -> (H::Out, Self::Transaction)
	where
		I: IntoIterator<Item=(Vec<u8>, Option<Vec<u8>>)>,
		H::Out: Ord;

	/// Calculate the child storage root, with given delta over what is already stored in
	/// the backend, and produce a "transaction" that can be used to commit.
	fn child_storage_root<I>(&self, storage_key: &[u8], delta: I) -> (Vec<u8>, Self::Transaction)
	where
		I: IntoIterator<Item=(Vec<u8>, Option<Vec<u8>>)>,
		H::Out: Ord;

	/// Get all key/value pairs into a Vec.
	fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)>;

	/// Try convert into trie backend.
	fn try_into_trie_backend(self) -> Option<TrieBackend<Self::TrieBackendStorage, H>>;
}

/// Trait that allows consolidate two transactions together.
pub trait Consolidate {
	/// Consolidate two transactions into one.
	fn consolidate(&mut self, other: Self);
}

impl Consolidate for () {
	fn consolidate(&mut self, _: Self) {
		()
	}
}

impl Consolidate for Vec<(Option<Vec<u8>>, Vec<u8>, Option<Vec<u8>>)> {
	fn consolidate(&mut self, mut other: Self) {
		self.append(&mut other);
	}
}

impl<H: Hasher> Consolidate for MemoryDB<H> {
	fn consolidate(&mut self, other: Self) {
		MemoryDB::consolidate(self, other)
	}
}

/// Error impossible.
// TODO: use `!` type when stabilized.
#[derive(Debug)]
pub enum Void {}

impl fmt::Display for Void {
	fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
		match *self {}
	}
}

impl error::Error for Void {
	fn description(&self) -> &str { "unreachable error" }
}

/// In-memory backend. Fully recomputes tries on each commit but useful for
/// tests.
#[derive(Eq)]
pub struct InMemory<H> {
	inner: HashMap<Option<Vec<u8>>, HashMap<Vec<u8>, Vec<u8>>>,
	_hasher: PhantomData<H>,
}

impl<H> Default for InMemory<H> {
	fn default() -> Self {
		InMemory {
			inner: Default::default(),
			_hasher: PhantomData,
		}
	}
}

impl<H> Clone for InMemory<H> {
	fn clone(&self) -> Self {
		InMemory {
			inner: self.inner.clone(),
			_hasher: PhantomData,
		}
	}
}

impl<H> PartialEq for InMemory<H> {
	fn eq(&self, other: &Self) -> bool {
		self.inner.eq(&other.inner)
	}
}

impl<H: Hasher> InMemory<H> where H::Out: HeapSizeOf {
	/// Copy the state, with applied updates
	pub fn update(&self, changes: <Self as Backend<H>>::Transaction) -> Self {
		let mut inner: HashMap<_, _> = self.inner.clone();
		for (storage_key, key, val) in changes {
			match val {
				Some(v) => { inner.entry(storage_key).or_default().insert(key, v); },
				None => { inner.entry(storage_key).or_default().remove(&key); },
			}
		}

		inner.into()
	}
}

impl<H> From<HashMap<Option<Vec<u8>>, HashMap<Vec<u8>, Vec<u8>>>> for InMemory<H> {
	fn from(inner: HashMap<Option<Vec<u8>>, HashMap<Vec<u8>, Vec<u8>>>) -> Self {
		InMemory {
			inner: inner,
			_hasher: PhantomData,
		}
	}
}

impl<H> From<HashMap<Vec<u8>, Vec<u8>>> for InMemory<H> {
	fn from(inner: HashMap<Vec<u8>, Vec<u8>>) -> Self {
		let mut expanded = HashMap::new();
		expanded.insert(None, inner);
		InMemory {
			inner: expanded,
			_hasher: PhantomData,
		}
	}
}

impl super::Error for Void {}

impl<H: Hasher> Backend<H> for InMemory<H> where H::Out: HeapSizeOf {
	type Error = Void;
	type Transaction = Vec<(Option<Vec<u8>>, Vec<u8>, Option<Vec<u8>>)>;
	type TrieBackendStorage = MemoryDB<H>;

	fn storage(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		Ok(self.inner.get(&None).and_then(|map| map.get(key).map(Clone::clone)))
	}

	fn child_storage(&self, storage_key: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
		Ok(self.inner.get(&Some(storage_key.to_vec())).and_then(|map| map.get(key).map(Clone::clone)))
	}

	fn exists_storage(&self, key: &[u8]) -> Result<bool, Self::Error> {
		Ok(self.inner.get(&None).map(|map| map.get(key).is_some()).unwrap_or(false))
	}

	fn for_keys_with_prefix<F: FnMut(&[u8])>(&self, prefix: &[u8], f: F) {
		self.inner.get(&None).map(|map| map.keys().filter(|key| key.starts_with(prefix)).map(|k| &**k).for_each(f));
	}

	fn for_keys_in_child_storage<F: FnMut(&[u8])>(&self, storage_key: &[u8], mut f: F) {
		self.inner.get(&Some(storage_key.to_vec())).map(|map| map.keys().for_each(|k| f(&k)));
	}

	fn storage_root<I>(&self, delta: I) -> (H::Out, Self::Transaction)
	where
		I: IntoIterator<Item=(Vec<u8>, Option<Vec<u8>>)>,
		<H as Hasher>::Out: Ord,
	{
		let existing_pairs = self.inner.get(&None).into_iter().flat_map(|map| map.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));

		let transaction: Vec<_> = delta.into_iter().collect();
		let root = trie_root::<H, _, _, _>(existing_pairs.chain(transaction.iter().cloned())
			.collect::<HashMap<_, _>>()
			.into_iter()
			.filter_map(|(k, maybe_val)| maybe_val.map(|val| (k, val)))
		);

		let full_transaction = transaction.into_iter().map(|(k, v)| (None, k, v)).collect();

		(root, full_transaction)
	}

	fn child_storage_root<I>(&self, storage_key: &[u8], delta: I) -> (Vec<u8>, Self::Transaction)
	where
		I: IntoIterator<Item=(Vec<u8>, Option<Vec<u8>>)>,
		H::Out: Ord
	{
		let storage_key = storage_key.to_vec();

		let existing_pairs = self.inner.get(&Some(storage_key.clone())).into_iter().flat_map(|map| map.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));

		let transaction: Vec<_> = delta.into_iter().collect();
		let root = child_trie_root::<H, _, _, _>(
			&storage_key,
			existing_pairs.chain(transaction.iter().cloned())
				.collect::<HashMap<_, _>>()
				.into_iter()
				.filter_map(|(k, maybe_val)| maybe_val.map(|val| (k, val)))
		);

		let full_transaction = transaction.into_iter().map(|(k, v)| (Some(storage_key.clone()), k, v)).collect();

		(root, full_transaction)
	}

	fn pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.inner.get(&None).into_iter().flat_map(|map| map.iter().map(|(k, v)| (k.clone(), v.clone()))).collect()
	}

	fn try_into_trie_backend(self) -> Option<TrieBackend<Self::TrieBackendStorage, H>> {
		let mut mdb = MemoryDB::default();	// TODO: should be more correct and use ::new()
		let mut root = None;
		for (storage_key, map) in self.inner {
			if storage_key != None {
				let _ = insert_into_memory_db::<H, _>(&mut mdb, map.into_iter())?;
			} else {
				root = Some(insert_into_memory_db::<H, _>(&mut mdb, map.into_iter())?);
			}
		}
		let root = match root {
			Some(root) => root,
			None => insert_into_memory_db::<H, _>(&mut mdb, ::std::iter::empty())?,
		};
		Some(TrieBackend::new(mdb, root))
	}
}

/// Insert input pairs into memory db.
pub(crate) fn insert_into_memory_db<H, I>(mdb: &mut MemoryDB<H>, input: I) -> Option<H::Out>
	where
		H: Hasher,
		H::Out: HeapSizeOf,
		I: IntoIterator<Item=(Vec<u8>, Vec<u8>)>,
{
	let mut root = <H as Hasher>::Out::default();
	{
		let mut trie = TrieDBMut::<H>::new(mdb, &mut root);
		for (key, value) in input {
			if let Err(e) = trie.insert(&key, &value) {
				warn!(target: "trie", "Failed to write to trie: {}", e);
				return None;
			}
		}
	}

	Some(root)
}

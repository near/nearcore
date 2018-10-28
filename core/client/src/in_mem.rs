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

//! In memory client backend

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use error;
use backend::{self, NewBlockState};
use light;
use primitives::AuthorityId;
use runtime_primitives::generic::BlockId;
use runtime_primitives::traits::{Block as BlockT, Header as HeaderT, Zero,
	NumberFor, As, Digest, DigestItem};
use runtime_primitives::Justification;
use blockchain::{self, BlockStatus, HeaderBackend};
use state_machine::backend::{Backend as StateBackend, InMemory};
use state_machine::InMemoryChangesTrieStorage;
use hash_db::Hasher;
use heapsize::HeapSizeOf;
use leaves::LeafSet;
use trie::MemoryDB;

struct PendingBlock<B: BlockT> {
	block: StoredBlock<B>,
	state: NewBlockState,
}

#[derive(PartialEq, Eq, Clone)]
enum StoredBlock<B: BlockT> {
	Header(B::Header, Option<Justification>),
	Full(B, Option<Justification>),
}

impl<B: BlockT> StoredBlock<B> {
	fn new(header: B::Header, body: Option<Vec<B::Extrinsic>>, just: Option<Justification>) -> Self {
		match body {
			Some(body) => StoredBlock::Full(B::new(header, body), just),
			None => StoredBlock::Header(header, just),
		}
	}

	fn header(&self) -> &B::Header {
		match *self {
			StoredBlock::Header(ref h, _) => h,
			StoredBlock::Full(ref b, _) => b.header(),
		}
	}

	fn justification(&self) -> Option<&Justification> {
		match *self {
			StoredBlock::Header(_, ref j) | StoredBlock::Full(_, ref j) => j.as_ref()
		}
	}

	fn extrinsics(&self) -> Option<&[B::Extrinsic]> {
		match *self {
			StoredBlock::Header(_, _) => None,
			StoredBlock::Full(ref b, _) => Some(b.extrinsics()),
		}
	}

	fn into_inner(self) -> (B::Header, Option<Vec<B::Extrinsic>>, Option<Justification>) {
		match self {
			StoredBlock::Header(header, just) => (header, None, just),
			StoredBlock::Full(block, just) => {
				let (header, body) = block.deconstruct();
				(header, Some(body), just)
			}
		}
	}
}

#[derive(Clone)]
struct BlockchainStorage<Block: BlockT> {
	blocks: HashMap<Block::Hash, StoredBlock<Block>>,
	hashes: HashMap<NumberFor<Block>, Block::Hash>,
	best_hash: Block::Hash,
	best_number: NumberFor<Block>,
	finalized_hash: Block::Hash,
	finalized_number: NumberFor<Block>,
	genesis_hash: Block::Hash,
	cht_roots: HashMap<NumberFor<Block>, Block::Hash>,
	leaves: LeafSet<Block::Hash, NumberFor<Block>>,
}

/// In-memory blockchain. Supports concurrent reads.
pub struct Blockchain<Block: BlockT> {
	storage: Arc<RwLock<BlockchainStorage<Block>>>,
	cache: Cache<Block>,
}

struct Cache<Block: BlockT> {
	storage: Arc<RwLock<BlockchainStorage<Block>>>,
	authorities_at: RwLock<HashMap<Block::Hash, Option<Vec<AuthorityId>>>>,
}

impl<Block: BlockT + Clone> Clone for Blockchain<Block> {
	fn clone(&self) -> Self {
		let storage = Arc::new(RwLock::new(self.storage.read().clone()));
		Blockchain {
			storage: storage.clone(),
			cache: Cache {
				storage,
				authorities_at: RwLock::new(self.cache.authorities_at.read().clone()),
			},
		}
	}
}

impl<Block: BlockT> Blockchain<Block> {
	/// Get header hash of given block.
	pub fn id(&self, id: BlockId<Block>) -> Option<Block::Hash> {
		match id {
			BlockId::Hash(h) => Some(h),
			BlockId::Number(n) => self.storage.read().hashes.get(&n).cloned(),
		}
	}

	/// Create new in-memory blockchain storage.
	pub fn new() -> Blockchain<Block> {
		let storage = Arc::new(RwLock::new(
			BlockchainStorage {
				blocks: HashMap::new(),
				hashes: HashMap::new(),
				best_hash: Default::default(),
				best_number: Zero::zero(),
				finalized_hash: Default::default(),
				finalized_number: Zero::zero(),
				genesis_hash: Default::default(),
				cht_roots: HashMap::new(),
				leaves: LeafSet::new(),
			}));
		Blockchain {
			storage: storage.clone(),
			cache: Cache {
				storage: storage,
				authorities_at: Default::default(),
			},
		}
	}

	/// Insert a block header and associated data.
	pub fn insert(
		&self,
		hash: Block::Hash,
		header: <Block as BlockT>::Header,
		justification: Option<Justification>,
		body: Option<Vec<<Block as BlockT>::Extrinsic>>,
		new_state: NewBlockState,
	) -> ::error::Result<()> {
		let number = header.number().clone();
		let best_tree_route = match new_state.is_best() {
			false => None,
			true => {
				let best_hash = self.storage.read().best_hash;
				if &best_hash == header.parent_hash() {
					None
				} else {
					let route = ::blockchain::tree_route(
						self,
						BlockId::Hash(best_hash),
						BlockId::Hash(*header.parent_hash()),
					)?;
					Some(route)
				}
			}
		};

		let mut storage = self.storage.write();

		storage.leaves.import(hash.clone(), number.clone(), header.parent_hash().clone());

		if new_state.is_best() {
			if let Some(tree_route) = best_tree_route {
				// apply retraction and enaction when reorganizing up to parent hash
				let enacted = tree_route.enacted();

				for entry in enacted {
					storage.hashes.insert(entry.number, entry.hash);
				}

				for entry in tree_route.retracted().iter().skip(enacted.len()) {
					storage.hashes.remove(&entry.number);
				}
			}

			storage.best_hash = hash.clone();
			storage.best_number = number.clone();
			storage.hashes.insert(number, hash.clone());
		}

		storage.blocks.insert(hash.clone(), StoredBlock::new(header, body, justification));

		if let NewBlockState::Final = new_state {
			storage.finalized_hash = hash;
			storage.finalized_number = number.clone();
		}

		if number == Zero::zero() {
			storage.genesis_hash = hash;
		}

		Ok(())
	}

	/// Compare this blockchain with another in-mem blockchain
	pub fn equals_to(&self, other: &Self) -> bool {
		self.canon_equals_to(other) && self.storage.read().blocks == other.storage.read().blocks
	}

	/// Compare canonical chain to other canonical chain.
	pub fn canon_equals_to(&self, other: &Self) -> bool {
		let this = self.storage.read();
		let other = other.storage.read();
			this.hashes == other.hashes
			&& this.best_hash == other.best_hash
			&& this.best_number == other.best_number
			&& this.genesis_hash == other.genesis_hash
	}

	/// Insert CHT root.
	pub fn insert_cht_root(&self, block: NumberFor<Block>, cht_root: Block::Hash) {
		self.storage.write().cht_roots.insert(block, cht_root);
	}

	fn finalize_header(&self, id: BlockId<Block>) -> error::Result<()> {
		let hash = match self.header(id)? {
			Some(h) => h.hash(),
			None => return Err(error::ErrorKind::UnknownBlock(format!("{}", id)).into()),
		};

		self.storage.write().finalized_hash = hash;
		Ok(())
	}
}

impl<Block: BlockT> HeaderBackend<Block> for Blockchain<Block> {
	fn header(&self, id: BlockId<Block>) -> error::Result<Option<<Block as BlockT>::Header>> {
		Ok(self.id(id).and_then(|hash| {
			self.storage.read().blocks.get(&hash).map(|b| b.header().clone())
		}))
	}

	fn info(&self) -> error::Result<blockchain::Info<Block>> {
		let storage = self.storage.read();
		Ok(blockchain::Info {
			best_hash: storage.best_hash,
			best_number: storage.best_number,
			genesis_hash: storage.genesis_hash,
			finalized_hash: storage.finalized_hash,
			finalized_number: storage.finalized_number,
		})
	}

	fn status(&self, id: BlockId<Block>) -> error::Result<BlockStatus> {
		match self.id(id).map_or(false, |hash| self.storage.read().blocks.contains_key(&hash)) {
			true => Ok(BlockStatus::InChain),
			false => Ok(BlockStatus::Unknown),
		}
	}

	fn number(&self, hash: Block::Hash) -> error::Result<Option<NumberFor<Block>>> {
		Ok(self.storage.read().blocks.get(&hash).map(|b| *b.header().number()))
	}

	fn hash(&self, number: <<Block as BlockT>::Header as HeaderT>::Number) -> error::Result<Option<Block::Hash>> {
		Ok(self.id(BlockId::Number(number)))
	}
}


impl<Block: BlockT> blockchain::Backend<Block> for Blockchain<Block> {
	fn body(&self, id: BlockId<Block>) -> error::Result<Option<Vec<<Block as BlockT>::Extrinsic>>> {
		Ok(self.id(id).and_then(|hash| {
			self.storage.read().blocks.get(&hash)
				.and_then(|b| b.extrinsics().map(|x| x.to_vec()))
		}))
	}

	fn justification(&self, id: BlockId<Block>) -> error::Result<Option<Justification>> {
		Ok(self.id(id).and_then(|hash| self.storage.read().blocks.get(&hash).and_then(|b|
			b.justification().map(|x| x.clone()))
		))
	}

	fn last_finalized(&self) -> error::Result<Block::Hash> {
		Ok(self.storage.read().finalized_hash.clone())
	}

	fn cache(&self) -> Option<&blockchain::Cache<Block>> {
		Some(&self.cache)
	}

	fn leaves(&self) -> error::Result<Vec<Block::Hash>> {
		Ok(self.storage.read().leaves.hashes())
	}
}

impl<Block: BlockT> light::blockchain::Storage<Block> for Blockchain<Block>
	where
		Block::Hash: From<[u8; 32]>,
{
	fn import_header(
		&self,
		header: Block::Header,
		authorities: Option<Vec<AuthorityId>>,
		state: NewBlockState,
	) -> error::Result<()> {
		let hash = header.hash();
		let parent_hash = *header.parent_hash();
		self.insert(hash, header, None, None, state)?;
		if state.is_best() {
			self.cache.insert(parent_hash, authorities);
		}

		Ok(())
	}

	fn last_finalized(&self) -> error::Result<Block::Hash> {
		Ok(self.storage.read().finalized_hash.clone())
	}

	fn finalize_header(&self, id: BlockId<Block>) -> error::Result<()> {
		Blockchain::finalize_header(self, id)
	}

	fn cht_root(&self, _cht_size: u64, block: NumberFor<Block>) -> error::Result<Block::Hash> {
		self.storage.read().cht_roots.get(&block).cloned()
			.ok_or_else(|| error::ErrorKind::Backend(format!("CHT for block {} not exists", block)).into())
	}

	fn cache(&self) -> Option<&blockchain::Cache<Block>> {
		Some(&self.cache)
	}
}

/// In-memory operation.
pub struct BlockImportOperation<Block: BlockT, H: Hasher> {
	pending_block: Option<PendingBlock<Block>>,
	pending_authorities: Option<Vec<AuthorityId>>,
	old_state: InMemory<H>,
	new_state: Option<InMemory<H>>,
	changes_trie_update: Option<MemoryDB<H>>,
}

impl<Block, H> backend::BlockImportOperation<Block, H> for BlockImportOperation<Block, H>
where
	Block: BlockT,
	H: Hasher,

	H::Out: HeapSizeOf,
{
	type State = InMemory<H>;

	fn state(&self) -> error::Result<Option<&Self::State>> {
		Ok(Some(&self.old_state))
	}

	fn set_block_data(
		&mut self,
		header: <Block as BlockT>::Header,
		body: Option<Vec<<Block as BlockT>::Extrinsic>>,
		justification: Option<Justification>,
		state: NewBlockState,
	) -> error::Result<()> {
		assert!(self.pending_block.is_none(), "Only one block per operation is allowed");
		self.pending_block = Some(PendingBlock {
			block: StoredBlock::new(header, body, justification),
			state,
		});
		Ok(())
	}

	fn update_authorities(&mut self, authorities: Vec<AuthorityId>) {
		self.pending_authorities = Some(authorities);
	}

	fn update_storage(&mut self, update: <InMemory<H> as StateBackend<H>>::Transaction) -> error::Result<()> {
		self.new_state = Some(self.old_state.update(update));
		Ok(())
	}

	fn update_changes_trie(&mut self, update: MemoryDB<H>) -> error::Result<()> {
		self.changes_trie_update = Some(update);
		Ok(())
	}

	fn reset_storage<I: Iterator<Item=(Vec<u8>, Vec<u8>)>>(&mut self, iter: I) -> error::Result<()> {
		self.new_state = Some(InMemory::from(iter.collect::<HashMap<_, _>>()));
		Ok(())
	}
}

/// In-memory backend. Keeps all states and blocks in memory. Useful for testing.
pub struct Backend<Block, H>
where
	Block: BlockT,
	H: Hasher,

	H::Out: HeapSizeOf + From<Block::Hash>,
{
	states: RwLock<HashMap<Block::Hash, InMemory<H>>>,
	changes_trie_storage: InMemoryChangesTrieStorage<H>,
	blockchain: Blockchain<Block>,
	aux: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl<Block, H> Backend<Block, H>
where
	Block: BlockT,
	H: Hasher,

	H::Out: HeapSizeOf + From<Block::Hash>,
{
	/// Create a new instance of in-mem backend.
	pub fn new() -> Backend<Block, H> {
		Backend {
			states: RwLock::new(HashMap::new()),
			changes_trie_storage: InMemoryChangesTrieStorage::new(),
			blockchain: Blockchain::new(),
			aux: RwLock::new(HashMap::new()),
		}
	}
}

impl<Block, H> backend::Backend<Block, H> for Backend<Block, H>
where
	Block: BlockT,
	H: Hasher,
	H::Out: HeapSizeOf + From<Block::Hash>,
{
	type BlockImportOperation = BlockImportOperation<Block, H>;
	type Blockchain = Blockchain<Block>;
	type State = InMemory<H>;
	type ChangesTrieStorage = InMemoryChangesTrieStorage<H>;

	fn begin_operation(&self, block: BlockId<Block>) -> error::Result<Self::BlockImportOperation> {
		let state = match block {
			BlockId::Hash(ref h) if h.clone() == Default::default() => Self::State::default(),
			_ => self.state_at(block)?,
		};

		Ok(BlockImportOperation {
			pending_block: None,
			pending_authorities: None,
			old_state: state,
			new_state: None,
			changes_trie_update: None,
		})
	}

	fn commit_operation(&self, operation: Self::BlockImportOperation) -> error::Result<()> {
		if let Some(pending_block) = operation.pending_block {
			let old_state = &operation.old_state;
			let (header, body, justification) = pending_block.block.into_inner();

			let hash = header.hash();
			let parent_hash = *header.parent_hash();

			self.states.write().insert(hash, operation.new_state.unwrap_or_else(|| old_state.clone()));

			let changes_trie_root = header.digest().log(DigestItem::as_changes_trie_root).cloned();
			if let Some(changes_trie_root) = changes_trie_root {
				if let Some(changes_trie_update) = operation.changes_trie_update {
					let changes_trie_root: H::Out = changes_trie_root.into();
					self.changes_trie_storage.insert(header.number().as_(), changes_trie_root, changes_trie_update);
				}
			}

			self.blockchain.insert(hash, header, justification, body, pending_block.state)?;
			// dumb implementation - store value for each block
			if pending_block.state.is_best() {
				self.blockchain.cache.insert(parent_hash, operation.pending_authorities);
			}
		}
		Ok(())
	}

	fn finalize_block(&self, block: BlockId<Block>) -> error::Result<()> {
		self.blockchain.finalize_header(block)
	}

	fn blockchain(&self) -> &Self::Blockchain {
		&self.blockchain
	}

	fn changes_trie_storage(&self) -> Option<&Self::ChangesTrieStorage> {
		Some(&self.changes_trie_storage)
	}

	fn state_at(&self, block: BlockId<Block>) -> error::Result<Self::State> {
		match self.blockchain.id(block).and_then(|id| self.states.read().get(&id).cloned()) {
			Some(state) => Ok(state),
			None => Err(error::ErrorKind::UnknownBlock(format!("{}", block)).into()),
		}
	}

	fn revert(&self, _n: NumberFor<Block>) -> error::Result<NumberFor<Block>> {
		Ok(As::sa(0))
	}

	fn insert_aux<'a, 'b: 'a, 'c: 'a, I: IntoIterator<Item=&'a (&'c [u8], &'c [u8])>, D: IntoIterator<Item=&'a &'b [u8]>>(&self, insert: I, delete: D) -> error::Result<()> {
		let mut aux = self.aux.write();
		for (k, v) in insert {
			aux.insert(k.to_vec(), v.to_vec());
		}
		for k in delete {
			aux.remove(*k);
		}
		Ok(())
	}

	fn get_aux(&self, key: &[u8]) -> error::Result<Option<Vec<u8>>> {
		Ok(self.aux.read().get(key).cloned())
	}
}

impl<Block, H> backend::LocalBackend<Block, H> for Backend<Block, H>
where
	Block: BlockT,
	H: Hasher,
	H::Out: HeapSizeOf + From<Block::Hash>,
{}

impl<Block: BlockT> Cache<Block> {
	fn insert(&self, at: Block::Hash, authorities: Option<Vec<AuthorityId>>) {
		self.authorities_at.write().insert(at, authorities);
	}
}

impl<Block: BlockT> blockchain::Cache<Block> for Cache<Block> {
	fn authorities_at(&self, block: BlockId<Block>) -> Option<Vec<AuthorityId>> {
		let hash = match block {
			BlockId::Hash(hash) => hash,
			BlockId::Number(number) => self.storage.read().hashes.get(&number).cloned()?,
		};

		self.authorities_at.read().get(&hash).cloned().unwrap_or(None)
	}
}

/// Insert authorities entry into in-memory blockchain cache. Extracted as a separate function to use it in tests.
pub fn cache_authorities_at<Block: BlockT>(
	blockchain: &Blockchain<Block>,
	at: Block::Hash,
	authorities: Option<Vec<AuthorityId>>
) {
	blockchain.cache.insert(at, authorities);
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;
	use test_client;
	use primitives::Blake2Hasher;

	type TestBackend = test_client::client::in_mem::Backend<test_client::runtime::Block, Blake2Hasher>;

	#[test]
	fn test_leaves_with_complex_block_tree() {
		let backend = Arc::new(TestBackend::new());

		test_client::trait_tests::test_leaves_for_backend(backend);
	}

	#[test]
	fn test_blockchain_query_by_number_gets_canonical() {
		let backend = Arc::new(TestBackend::new());

		test_client::trait_tests::test_blockchain_query_by_number_gets_canonical(backend);
	}
}

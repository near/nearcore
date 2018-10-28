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

//! Blockchain access trait

use client::{self, Client as SubstrateClient, ClientInfo, BlockStatus, CallExecutor};
use client::error::Error;
use consensus::BlockImport;
use runtime_primitives::traits::{Block as BlockT, Header as HeaderT, NumberFor};
use runtime_primitives::generic::{BlockId};
use consensus::{ImportBlock, ImportResult};
use runtime_primitives::Justification;
use primitives::{Blake2Hasher, AuthorityId};

/// Local client abstraction for the network.
pub trait Client<Block: BlockT>: Send + Sync {
	/// Import a new block. Parent is supposed to be existing in the blockchain.
	fn import(&self, block: ImportBlock<Block>, new_authorities: Option<Vec<AuthorityId>>)
		-> Result<ImportResult, Error>;

	/// Get blockchain info.
	fn info(&self) -> Result<ClientInfo<Block>, Error>;

	/// Get block status.
	fn block_status(&self, id: &BlockId<Block>) -> Result<BlockStatus, Error>;

	/// Get block hash by number.
	fn block_hash(&self, block_number: <Block::Header as HeaderT>::Number) -> Result<Option<Block::Hash>, Error>;

	/// Get block header.
	fn header(&self, id: &BlockId<Block>) -> Result<Option<Block::Header>, Error>;

	/// Get block body.
	fn body(&self, id: &BlockId<Block>) -> Result<Option<Vec<Block::Extrinsic>>, Error>;

	/// Get block justification.
	fn justification(&self, id: &BlockId<Block>) -> Result<Option<Justification>, Error>;

	/// Get block header proof.
	fn header_proof(&self, block_number: <Block::Header as HeaderT>::Number) -> Result<(Block::Header, Vec<Vec<u8>>), Error>;

	/// Get storage read execution proof.
	fn read_proof(&self, block: &Block::Hash, key: &[u8]) -> Result<Vec<Vec<u8>>, Error>;

	/// Get method execution proof.
	fn execution_proof(&self, block: &Block::Hash, method: &str, data: &[u8]) -> Result<(Vec<u8>, Vec<Vec<u8>>), Error>;

	/// Get key changes proof.
	fn key_changes_proof(
		&self,
		first: Block::Hash,
		last: Block::Hash,
		max: Block::Hash,
		key: &[u8]
	) -> Result<(NumberFor<Block>, Vec<Vec<u8>>), Error>;
}

impl<B, E, Block> Client<Block> for SubstrateClient<B, E, Block> where
	B: client::backend::Backend<Block, Blake2Hasher> + Send + Sync + 'static,
	E: CallExecutor<Block, Blake2Hasher> + Send + Sync + 'static,
	Self: BlockImport<Block, Error=Error>,
	Block: BlockT
{
	fn import(&self, block: ImportBlock<Block>, new_authorities: Option<Vec<AuthorityId>>)
		-> Result<ImportResult, Error>
	{
		(self as &SubstrateClient<B, E, Block>).import_block(block, new_authorities)
	}

	fn info(&self) -> Result<ClientInfo<Block>, Error> {
		(self as &SubstrateClient<B, E, Block>).info()
	}

	fn block_status(&self, id: &BlockId<Block>) -> Result<BlockStatus, Error> {
		(self as &SubstrateClient<B, E, Block>).block_status(id)
	}

	fn block_hash(&self, block_number: <Block::Header as HeaderT>::Number) -> Result<Option<Block::Hash>, Error> {
		(self as &SubstrateClient<B, E, Block>).block_hash(block_number)
	}

	fn header(&self, id: &BlockId<Block>) -> Result<Option<Block::Header>, Error> {
		(self as &SubstrateClient<B, E, Block>).header(id)
	}

	fn body(&self, id: &BlockId<Block>) -> Result<Option<Vec<Block::Extrinsic>>, Error> {
		(self as &SubstrateClient<B, E, Block>).body(id)
	}

	fn justification(&self, id: &BlockId<Block>) -> Result<Option<Justification>, Error> {
		(self as &SubstrateClient<B, E, Block>).justification(id)
	}

	fn header_proof(&self, block_number: <Block::Header as HeaderT>::Number) -> Result<(Block::Header, Vec<Vec<u8>>), Error> {
		(self as &SubstrateClient<B, E, Block>).header_proof(&BlockId::Number(block_number))
	}

	fn read_proof(&self, block: &Block::Hash, key: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
		(self as &SubstrateClient<B, E, Block>).read_proof(&BlockId::Hash(block.clone()), key)
	}

	fn execution_proof(&self, block: &Block::Hash, method: &str, data: &[u8]) -> Result<(Vec<u8>, Vec<Vec<u8>>), Error> {
		(self as &SubstrateClient<B, E, Block>).execution_proof(&BlockId::Hash(block.clone()), method, data)
	}

	fn key_changes_proof(
		&self,
		first: Block::Hash,
		last: Block::Hash,
		max: Block::Hash,
		key: &[u8]
	) -> Result<(NumberFor<Block>, Vec<Vec<u8>>), Error> {
		(self as &SubstrateClient<B, E, Block>).key_changes_proof(first, last, max, key)
	}
}

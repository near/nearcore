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

//! Db-based backend utility structures and functions, used by both
//! full and light storages.

use std::sync::Arc;
use std::io;

use kvdb::{KeyValueDB, DBTransaction};
use kvdb_rocksdb::{Database, DatabaseConfig};

use client;
use codec::Decode;
use trie::DBValue;
use runtime_primitives::generic::BlockId;
use runtime_primitives::traits::{As, Block as BlockT, Header as HeaderT, Zero};
use DatabaseSettings;

/// Number of columns in the db. Must be the same for both full && light dbs.
/// Otherwise RocksDb will fail to open database && check its type.
pub const NUM_COLUMNS: u32 = 9;
/// Meta column. The set of keys in the column is shared by full && light storages.
pub const COLUMN_META: Option<u32> = Some(0);

/// Keys of entries in COLUMN_META.
pub mod meta_keys {
	/// Type of storage (full or light).
	pub const TYPE: &[u8; 4] = b"type";
	/// Best block key.
	pub const BEST_BLOCK: &[u8; 4] = b"best";
	/// Last finalized block key.
	pub const FINALIZED_BLOCK: &[u8; 5] = b"final";
	/// Meta information prefix for list-based caches.
	pub const CACHE_META_PREFIX: &[u8; 5] = b"cache";
	/// Genesis block hash.
	pub const GENESIS_HASH: &[u8; 3] = b"gen";
	/// Leaves prefix list key.
	pub const LEAF_PREFIX: &[u8; 4] = b"leaf";
}

/// Database metadata.
#[derive(Debug)]
pub struct Meta<N, H> {
	/// Hash of the best known block.
	pub best_hash: H,
	/// Number of the best known block.
	pub best_number: N,
	/// Hash of the best finalized block.
	pub finalized_hash: H,
	/// Number of the best finalized block.
	pub finalized_number: N,
	/// Hash of the genesis block.
	pub genesis_hash: H,
}

/// A block lookup key: used for canonical lookup from block number to hash
pub type ShortBlockLookupKey = [u8; 4];

/// Convert block number into short lookup key (LE representation) for
/// blocks that are in the canonical chain.
pub fn number_to_lookup_key<N>(n: N) -> ShortBlockLookupKey where N: As<u64> {
	let n: u64 = n.as_();
	assert!(n & 0xffffffff00000000 == 0);

	[
		(n >> 24) as u8,
		((n >> 16) & 0xff) as u8,
		((n >> 8) & 0xff) as u8,
		(n & 0xff) as u8
	]
}

/// Convert number and hash into long lookup key for blocks that are
/// not in the canonical chain.
pub fn number_and_hash_to_lookup_key<N, H>(number: N, hash: H) -> Vec<u8> where
	N: As<u64>,
	H: AsRef<[u8]>
{
	let mut lookup_key = number_to_lookup_key(number).to_vec();
	lookup_key.extend_from_slice(hash.as_ref());
	lookup_key
}

/// Convert block lookup key into block number.
/// all block lookup keys start with the block number.
pub fn lookup_key_to_number<N>(key: &[u8]) -> client::error::Result<N> where N: As<u64> {
	if key.len() < 4 {
		return Err(client::error::ErrorKind::Backend("Invalid block key".into()).into());
	}
	Ok((key[0] as u64) << 24
		| (key[1] as u64) << 16
		| (key[2] as u64) << 8
		| (key[3] as u64)).map(As::sa)
}

/// Convert block id to block lookup key.
/// block lookup key is the DB-key header, block and justification are stored under.
/// looks up lookup key by hash from DB as necessary.
pub fn block_id_to_lookup_key<Block>(
	db: &KeyValueDB,
	hash_lookup_col: Option<u32>,
	id: BlockId<Block>
) -> Result<Option<Vec<u8>>, client::error::Error> where
	Block: BlockT,
{
	match id {
		// numbers are solely looked up in canonical chain
		BlockId::Number(n) => Ok(Some(number_to_lookup_key(n).to_vec())),
		BlockId::Hash(h) => db.get(hash_lookup_col, h.as_ref()).map(|v|
			v.map(|v| { v.into_vec() })
		).map_err(db_err),
	}
}


/// Maps database error to client error
pub fn db_err(err: io::Error) -> client::error::Error {
	use std::error::Error;
	client::error::ErrorKind::Backend(err.description().into()).into()
}

/// Open RocksDB database.
pub fn open_database(config: &DatabaseSettings, col_meta: Option<u32>, db_type: &str) -> client::error::Result<Arc<KeyValueDB>> {
	let mut db_config = DatabaseConfig::with_columns(Some(NUM_COLUMNS));
	db_config.memory_budget = config.cache_size;
	let path = config.path.to_str().ok_or_else(|| client::error::ErrorKind::Backend("Invalid database path".into()))?;
	let db = Database::open(&db_config, &path).map_err(db_err)?;

	// check database type
	match db.get(col_meta, meta_keys::TYPE).map_err(db_err)? {
		Some(stored_type) => {
			if db_type.as_bytes() != &*stored_type {
				return Err(client::error::ErrorKind::Backend(
					format!("Unexpected database type. Expected: {}", db_type)).into());
			}
		},
		None => {
			let mut transaction = DBTransaction::new();
			transaction.put(col_meta, meta_keys::TYPE, db_type.as_bytes());
			db.write(transaction).map_err(db_err)?;
		},
	}

	Ok(Arc::new(db))
}

/// Read database column entry for the given block.
pub fn read_db<Block>(db: &KeyValueDB, col_index: Option<u32>, col: Option<u32>, id: BlockId<Block>) -> client::error::Result<Option<DBValue>>
	where
		Block: BlockT,
{
	block_id_to_lookup_key(db, col_index, id).and_then(|key| match key {
		Some(key) => db.get(col, key.as_ref()).map_err(db_err),
		None => Ok(None),
	})
}

/// Read a header from the database.
pub fn read_header<Block: BlockT>(
	db: &KeyValueDB,
	col_index: Option<u32>,
	col: Option<u32>,
	id: BlockId<Block>,
) -> client::error::Result<Option<Block::Header>> {
	match read_db(db, col_index, col, id)? {
		Some(header) => match Block::Header::decode(&mut &header[..]) {
			Some(header) => Ok(Some(header)),
			None => return Err(
				client::error::ErrorKind::Backend("Error decoding header".into()).into()
			),
		}
		None => Ok(None),
	}
}

/// Read meta from the database.
pub fn read_meta<Block>(db: &KeyValueDB, col_meta: Option<u32>, col_header: Option<u32>) -> Result<
	Meta<<<Block as BlockT>::Header as HeaderT>::Number, Block::Hash>,
	client::error::Error,
>
	where
		Block: BlockT,
{
	let genesis_hash: Block::Hash = match db.get(col_meta, meta_keys::GENESIS_HASH).map_err(db_err)? {
		Some(h) => match Decode::decode(&mut &h[..]) {
			Some(h) => h,
			None => return Err(client::error::ErrorKind::Backend("Error decoding genesis hash".into()).into()),
		},
		None => return Ok(Meta {
			best_hash: Default::default(),
			best_number: Zero::zero(),
			finalized_hash: Default::default(),
			finalized_number: Zero::zero(),
			genesis_hash: Default::default(),
		}),
	};

	let load_meta_block = |desc, key| -> Result<_, client::error::Error> {
		if let Some(Some(header)) = db.get(col_meta, key).and_then(|id|
			match id {
				Some(id) => db.get(col_header, &id).map(|h| h.map(|b| Block::Header::decode(&mut &b[..]))),
				None => Ok(None),
			}).map_err(db_err)?
		{
			let hash = header.hash();
			debug!("DB Opened blockchain db, fetched {} = {:?} ({})", desc, hash, header.number());
			Ok((hash, *header.number()))
		} else {
			Ok((genesis_hash.clone(), Zero::zero()))
		}
	};

	let (best_hash, best_number) = load_meta_block("best", meta_keys::BEST_BLOCK)?;
	let (finalized_hash, finalized_number) = load_meta_block("final", meta_keys::FINALIZED_BLOCK)?;

	Ok(Meta {
		best_hash,
		best_number,
		finalized_hash,
		finalized_number,
		genesis_hash,
	})
}

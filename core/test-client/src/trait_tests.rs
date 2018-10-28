// Copyright 2018 Parity Technologies (UK) Ltd.
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

//! tests that should hold for all implementations of certain traits.
//! to test implementations without duplication.

#![allow(missing_docs)]

use std::sync::Arc;
use keyring::Keyring;
use consensus::BlockOrigin;
use primitives::Blake2Hasher;
use ::TestClient;
use runtime_primitives::traits::Block as BlockT;
use backend;
use blockchain::{Backend as BlockChainBackendT, HeaderBackend};
use ::BlockBuilderExt;
use runtime::{self, Transfer};
use runtime_primitives::generic::BlockId;

/// helper to test the `leaves` implementation for various backends
pub fn test_leaves_for_backend<B>(backend: Arc<B>) where
	B: backend::LocalBackend<runtime::Block, Blake2Hasher>,
{
	// block tree:
	// G -> A1 -> A2 -> A3 -> A4 -> A5
	//		A1 -> B2 -> B3 -> B4
	//			  B2 -> C3
	//		A1 -> D2

	let client = ::new_with_backend(backend.clone(), false);

	let genesis_hash = client.info().unwrap().chain.genesis_hash;

	assert_eq!(
		client.backend().blockchain().leaves().unwrap(),
		vec![genesis_hash]);

	// G -> A1
	let a1 = client.new_block().unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a1.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a1.hash()]);

	// A1 -> A2
	let a2 = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a2.clone()).unwrap();
	assert_eq!(
		client.backend().blockchain().leaves().unwrap(),
		vec![a2.hash()]);

	// A2 -> A3
	let a3 = client.new_block_at(&BlockId::Hash(a2.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a3.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a3.hash()]);

	// A3 -> A4
	let a4 = client.new_block_at(&BlockId::Hash(a3.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a4.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a4.hash()]);

	// A4 -> A5
	let a5 = client.new_block_at(&BlockId::Hash(a4.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a5.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash()]);

	// A1 -> B2
	let mut builder = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap();
	// this push is required as otherwise B2 has the same hash as A2 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 41,
		nonce: 0,
	}).unwrap();
	let b2 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b2.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash(), b2.hash()]);

	// B2 -> B3
	let b3 = client.new_block_at(&BlockId::Hash(b2.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b3.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash(), b3.hash()]);

	// B3 -> B4
	let b4 = client.new_block_at(&BlockId::Hash(b3.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b4.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash(), b4.hash()]);

	// // B2 -> C3
	let mut builder = client.new_block_at(&BlockId::Hash(b2.hash())).unwrap();
	// this push is required as otherwise C3 has the same hash as B3 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 1,
		nonce: 1,
	}).unwrap();
	let c3 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, c3.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash(), b4.hash(), c3.hash()]);

	// A1 -> D2
	let mut builder = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap();
	// this push is required as otherwise D2 has the same hash as B2 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 1,
		nonce: 0,
	}).unwrap();
	let d2 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, d2.clone()).unwrap();
	assert_eq!(
		backend.blockchain().leaves().unwrap(),
		vec![a5.hash(), b4.hash(), c3.hash(), d2.hash()]);
}


pub fn test_blockchain_query_by_number_gets_canonical<B>(backend: Arc<B>) where
	B: backend::LocalBackend<runtime::Block, Blake2Hasher>,
{
	// block tree:
	// G -> A1 -> A2 -> A3 -> A4 -> A5
	//		A1 -> B2 -> B3 -> B4
	//			  B2 -> C3
	//		A1 -> D2
	let client = ::new_with_backend(backend, false);

	// G -> A1
	let a1 = client.new_block().unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a1.clone()).unwrap();

	// A1 -> A2
	let a2 = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a2.clone()).unwrap();

	// A2 -> A3
	let a3 = client.new_block_at(&BlockId::Hash(a2.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a3.clone()).unwrap();

	// A3 -> A4
	let a4 = client.new_block_at(&BlockId::Hash(a3.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a4.clone()).unwrap();

	// A4 -> A5
	let a5 = client.new_block_at(&BlockId::Hash(a4.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, a5.clone()).unwrap();

	// A1 -> B2
	let mut builder = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap();
	// this push is required as otherwise B2 has the same hash as A2 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 41,
		nonce: 0,
	}).unwrap();
	let b2 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b2.clone()).unwrap();

	// B2 -> B3
	let b3 = client.new_block_at(&BlockId::Hash(b2.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b3.clone()).unwrap();

	// B3 -> B4
	let b4 = client.new_block_at(&BlockId::Hash(b3.hash())).unwrap().bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, b4.clone()).unwrap();

	// // B2 -> C3
	let mut builder = client.new_block_at(&BlockId::Hash(b2.hash())).unwrap();
	// this push is required as otherwise C3 has the same hash as B3 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 1,
		nonce: 1,
	}).unwrap();
	let c3 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, c3.clone()).unwrap();

	// A1 -> D2
	let mut builder = client.new_block_at(&BlockId::Hash(a1.hash())).unwrap();
	// this push is required as otherwise D2 has the same hash as B2 and won't get imported
	builder.push_transfer(Transfer {
		from: Keyring::Alice.to_raw_public().into(),
		to: Keyring::Ferdie.to_raw_public().into(),
		amount: 1,
		nonce: 0,
	}).unwrap();
	let d2 = builder.bake().unwrap();
	client.justify_and_import(BlockOrigin::Own, d2.clone()).unwrap();

	let genesis_hash = client.info().unwrap().chain.genesis_hash;

	assert_eq!(client.backend().blockchain().header(BlockId::Number(0)).unwrap().unwrap().hash(), genesis_hash);
	assert_eq!(client.backend().blockchain().hash(0).unwrap().unwrap(), genesis_hash);

	assert_eq!(client.backend().blockchain().header(BlockId::Number(1)).unwrap().unwrap().hash(), a1.hash());
	assert_eq!(client.backend().blockchain().hash(1).unwrap().unwrap(), a1.hash());

	assert_eq!(client.backend().blockchain().header(BlockId::Number(2)).unwrap().unwrap().hash(), a2.hash());
	assert_eq!(client.backend().blockchain().hash(2).unwrap().unwrap(), a2.hash());

	assert_eq!(client.backend().blockchain().header(BlockId::Number(3)).unwrap().unwrap().hash(), a3.hash());
	assert_eq!(client.backend().blockchain().hash(3).unwrap().unwrap(), a3.hash());

	assert_eq!(client.backend().blockchain().header(BlockId::Number(4)).unwrap().unwrap().hash(), a4.hash());
	assert_eq!(client.backend().blockchain().hash(4).unwrap().unwrap(), a4.hash());

	assert_eq!(client.backend().blockchain().header(BlockId::Number(5)).unwrap().unwrap().hash(), a5.hash());
	assert_eq!(client.backend().blockchain().hash(5).unwrap().unwrap(), a5.hash());
}

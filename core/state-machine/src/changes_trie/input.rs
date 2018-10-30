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

//! Different types of changes trie input pairs.

use codec::{Decode, Encode, Input, Output};

/// Key of { changed key => set of extrinsic indices } mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtrinsicIndex {
	/// Block at which this key has been inserted in the trie.
	pub block: u64,
	/// Storage key this node is responsible for.
	pub key: Vec<u8>,
}

/// Value of { changed key => set of extrinsic indices } mapping.
pub type ExtrinsicIndexValue = Vec<u32>;

/// Key of { changed key => block/digest block numbers } mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestIndex {
	/// Block at which this key has been inserted in the trie.
	pub block: u64,
	/// Storage key this node is responsible for.
	pub key: Vec<u8>,
}

/// Value of { changed key => block/digest block numbers } mapping.
pub type DigestIndexValue = Vec<u64>;

/// Single input pair of changes trie.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputPair {
	/// Element of { key => set of extrinsics where key has been changed } element mapping.
	ExtrinsicIndex(ExtrinsicIndex, ExtrinsicIndexValue),
	/// Element of { key => set of blocks/digest blocks where key has been changed } element mapping.
	DigestIndex(DigestIndex, DigestIndexValue),
}

/// Single input key of changes trie.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputKey {
	/// Key of { key => set of extrinsics where key has been changed } element mapping.
	ExtrinsicIndex(ExtrinsicIndex),
	/// Key of { key => set of blocks/digest blocks where key has been changed } element mapping.
	DigestIndex(DigestIndex),
}

impl Into<(Vec<u8>, Vec<u8>)> for InputPair {
	fn into(self) -> (Vec<u8>, Vec<u8>) {
		match self {
			InputPair::ExtrinsicIndex(key, value) => (key.encode(), value.encode()),
			InputPair::DigestIndex(key, value) => (key.encode(), value.encode()),
		}
	}
}

impl Into<InputKey> for InputPair {
	fn into(self) -> InputKey {
		match self {
			InputPair::ExtrinsicIndex(key, _) => InputKey::ExtrinsicIndex(key),
			InputPair::DigestIndex(key, _) => InputKey::DigestIndex(key),
		}
	}
}

impl ExtrinsicIndex {
	pub fn key_neutral_prefix(block: u64) -> Vec<u8> {
		let mut prefix = vec![1];
		prefix.extend(block.encode());
		prefix
	}
}

impl Encode for ExtrinsicIndex {
	fn encode_to<W: Output>(&self, dest: &mut W) {
		dest.push_byte(1);
		self.block.encode_to(dest);
		self.key.encode_to(dest);
	}
}

impl DigestIndex {
	pub fn key_neutral_prefix(block: u64) -> Vec<u8> {
		let mut prefix = vec![2];
		prefix.extend(block.encode());
		prefix
	}
}


impl Encode for DigestIndex {
	fn encode_to<W: Output>(&self, dest: &mut W) {
		dest.push_byte(2);
		self.block.encode_to(dest);
		self.key.encode_to(dest);
	}
}

impl Decode for InputKey {
	fn decode<I: Input>(input: &mut I) -> Option<Self> {
		match input.read_byte()? {
			1 => Some(InputKey::ExtrinsicIndex(ExtrinsicIndex {
				block: Decode::decode(input)?,
				key: Decode::decode(input)?,
			})),
			2 => Some(InputKey::DigestIndex(DigestIndex {
				block: Decode::decode(input)?,
				key: Decode::decode(input)?,
			})),
			_ => None,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extrinsic_index_serialized_and_deserialized() {
		let original = ExtrinsicIndex { block: 777, key: vec![42] };
		let serialized = original.encode();
		let deserialized: InputKey = Decode::decode(&mut &serialized[..]).unwrap();
		assert_eq!(InputKey::ExtrinsicIndex(original), deserialized);
	}

	#[test]
	fn digest_index_serialized_and_deserialized() {
		let original = DigestIndex { block: 777, key: vec![42] };
		let serialized = original.encode();
		let deserialized: InputKey = Decode::decode(&mut &serialized[..]).unwrap();
		assert_eq!(InputKey::DigestIndex(original), deserialized);
	}
}

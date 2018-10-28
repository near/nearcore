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

//! Rust implementation of Substrate contracts.

use std::collections::HashMap;

use wasmi::{
	Module, ModuleInstance, MemoryInstance, MemoryRef, TableRef, ImportsBuilder
};
use wasmi::RuntimeValue::{I32, I64};
use wasmi::memory_units::{Pages, Bytes};
use state_machine::Externalities;
use error::{Error, ErrorKind, Result};
use wasm_utils::UserError;
use primitives::{blake2_256, twox_128, twox_256, ed25519};
use primitives::hexdisplay::HexDisplay;
use primitives::sandbox as sandbox_primitives;
use primitives::Blake2Hasher;
use trie::ordered_trie_root;
use sandbox;


struct Heap {
	end: u32,
}

impl Heap {
	/// Construct new `Heap` struct with a given number of pages.
	///
	/// Returns `Err` if the heap couldn't allocate required
	/// number of pages.
	///
	/// This could mean that wasm binary specifies memory
	/// limit and we are trying to allocate beyond that limit.
	fn new(memory: &MemoryRef, pages: usize) -> Result<Self> {
		let prev_page_count = memory.initial();
		memory.grow(Pages(pages)).map_err(|_| Error::from(ErrorKind::Runtime))?;
		Ok(Heap {
			end: Bytes::from(prev_page_count).0 as u32,
		})
	}

	fn allocate(&mut self, size: u32) -> u32 {
		let r = self.end;
		self.end += size;
		r
	}

	fn deallocate(&mut self, _offset: u32) {
	}
}

#[cfg(feature="wasm-extern-trace")]
macro_rules! debug_trace {
	( $( $x:tt )* ) => ( trace!( $( $x )* ) )
}
#[cfg(not(feature="wasm-extern-trace"))]
macro_rules! debug_trace {
	( $( $x:tt )* ) => ()
}

struct FunctionExecutor<'e, E: Externalities<Blake2Hasher> + 'e> {
	sandbox_store: sandbox::Store,
	heap: Heap,
	memory: MemoryRef,
	table: Option<TableRef>,
	ext: &'e mut E,
	hash_lookup: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'e, E: Externalities<Blake2Hasher>> FunctionExecutor<'e, E> {
	fn new(m: MemoryRef, heap_pages: usize, t: Option<TableRef>, e: &'e mut E) -> Result<Self> {
		Ok(FunctionExecutor {
			sandbox_store: sandbox::Store::new(),
			heap: Heap::new(&m, heap_pages)?,
			memory: m,
			table: t,
			ext: e,
			hash_lookup: HashMap::new(),
		})
	}
}

impl<'e, E: Externalities<Blake2Hasher>> sandbox::SandboxCapabilities for FunctionExecutor<'e, E> {
	fn store(&self) -> &sandbox::Store {
		&self.sandbox_store
	}
	fn store_mut(&mut self) -> &mut sandbox::Store {
		&mut self.sandbox_store
	}
	fn allocate(&mut self, len: u32) -> u32 {
		self.heap.allocate(len)
	}
	fn deallocate(&mut self, ptr: u32) {
		self.heap.deallocate(ptr)
	}
	fn write_memory(&mut self, ptr: u32, data: &[u8]) -> ::std::result::Result<(), UserError> {
		self.memory.set(ptr, data).map_err(|_| UserError("Invalid attempt to write_memory"))
	}
	fn read_memory(&self, ptr: u32, len: u32) -> ::std::result::Result<Vec<u8>, UserError> {
		self.memory.get(ptr, len as usize).map_err(|_| UserError("Invalid attempt to write_memory"))
	}
}

trait WritePrimitive<T: Sized> {
	fn write_primitive(&self, offset: u32, t: T) -> ::std::result::Result<(), UserError>;
}

impl WritePrimitive<u32> for MemoryInstance {
	fn write_primitive(&self, offset: u32, t: u32) -> ::std::result::Result<(), UserError> {
		use byteorder::{LittleEndian, ByteOrder};
		let mut r = [0u8; 4];
		LittleEndian::write_u32(&mut r, t);
		self.set(offset, &r).map_err(|_| UserError("Invalid attempt to write_primitive"))
	}
}

trait ReadPrimitive<T: Sized> {
	fn read_primitive(&self, offset: u32) -> ::std::result::Result<T, UserError>;
}

impl ReadPrimitive<u32> for MemoryInstance {
	fn read_primitive(&self, offset: u32) -> ::std::result::Result<u32, UserError> {
		use byteorder::{LittleEndian, ByteOrder};
		Ok(LittleEndian::read_u32(&self.get(offset, 4).map_err(|_| UserError("Invalid attempt to read_primitive"))?))
	}
}

// TODO: this macro does not support `where` clauses and that seems somewhat tricky to add
impl_function_executor!(this: FunctionExecutor<'e, E>,
	ext_print_utf8(utf8_data: *const u8, utf8_len: u32) => {
		if let Ok(utf8) = this.memory.get(utf8_data, utf8_len as usize) {
			if let Ok(message) = String::from_utf8(utf8) {
				println!("{}", message);
			}
		}
		Ok(())
	},
	ext_print_hex(data: *const u8, len: u32) => {
		if let Ok(hex) = this.memory.get(data, len as usize) {
			println!("{}", HexDisplay::from(&hex));
		}
		Ok(())
	},
	ext_print_num(number: u64) => {
		println!("{}", number);
		Ok(())
	},
	ext_malloc(size: usize) -> *mut u8 => {
		let r = this.heap.allocate(size);
		debug_trace!(target: "sr-io", "malloc {} bytes at {}", size, r);
		Ok(r)
	},
	ext_free(addr: *mut u8) => {
		this.heap.deallocate(addr);
		debug_trace!(target: "sr-io", "free {}", addr);
		Ok(())
	},
	ext_set_storage(key_data: *const u8, key_len: u32, value_data: *const u8, value_len: u32) => {
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_set_storage"))?;
		let value = this.memory.get(value_data, value_len as usize).map_err(|_| UserError("Invalid attempt to determine value in ext_set_storage"))?;
		if let Some(_preimage) = this.hash_lookup.get(&key) {
			debug_trace!(target: "wasm-trace", "*** Setting storage: %{} -> {}   [k={}]", ::primitives::hexdisplay::ascii_format(&_preimage), HexDisplay::from(&value), HexDisplay::from(&key));
		} else {
			debug_trace!(target: "wasm-trace", "*** Setting storage:  {} -> {}   [k={}]", ::primitives::hexdisplay::ascii_format(&key), HexDisplay::from(&value), HexDisplay::from(&key));
		}
		this.ext.set_storage(key, value);
		Ok(())
	},
	ext_set_child_storage(storage_key_data: *const u8, storage_key_len: u32, key_data: *const u8, key_len: u32, value_data: *const u8, value_len: u32) => {
		let storage_key = this.memory.get(storage_key_data, storage_key_len as usize).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_set_child_storage"))?;
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_set_child_storage"))?;
		let value = this.memory.get(value_data, value_len as usize).map_err(|_| UserError("Invalid attempt to determine value in ext_set_child_storage"))?;
		if let Some(_preimage) = this.hash_lookup.get(&key) {
			debug_trace!(
				target: "wasm-trace", "*** Setting child storage: {} -> %{} -> {}   [k={}]",
				::primitives::hexdisplay::ascii_format(&storage_key),
				::primitives::hexdisplay::ascii_format(&_preimage),
				HexDisplay::from(&value),
				HexDisplay::from(&key)
			);
		} else {
			debug_trace!(
				target: "wasm-trace", "*** Setting child storage: {} ->  {} -> {}   [k={}]",
				::primitives::hexdisplay::ascii_format(&storage_key),
				::primitives::hexdisplay::ascii_format(&key),
				HexDisplay::from(&value),
				HexDisplay::from(&key)
			);
		}
		this.ext.set_child_storage(storage_key, key, value);
		Ok(())
	},
	ext_clear_child_storage(storage_key_data: *const u8, storage_key_len: u32, key_data: *const u8, key_len: u32) => {
		let storage_key = this.memory.get(
			storage_key_data,
			storage_key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_clear_child_storage"))?;
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_clear_child_storage"))?;
		debug_trace!(target: "wasm-trace", "*** Clearing child storage: {} -> {}   [k={}]",
			::primitives::hexdisplay::ascii_format(&storage_key),
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			}, HexDisplay::from(&key));
		this.ext.clear_child_storage(&storage_key, &key);
		Ok(())
	},
	ext_clear_storage(key_data: *const u8, key_len: u32) => {
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_clear_storage"))?;
		debug_trace!(target: "wasm-trace", "*** Clearing storage: {}   [k={}]",
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			}, HexDisplay::from(&key));
		this.ext.clear_storage(&key);
		Ok(())
	},
	ext_exists_storage(key_data: *const u8, key_len: u32) -> u32 => {
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_exists_storage"))?;
		Ok(if this.ext.exists_storage(&key) { 1 } else { 0 })
	},
	ext_exists_child_storage(storage_key_data: *const u8, storage_key_len: u32, key_data: *const u8, key_len: u32) -> u32 => {
		let storage_key = this.memory.get(
			storage_key_data,
			storage_key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_exists_child_storage"))?;
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to determine key in ext_exists_child_storage"))?;
		Ok(if this.ext.exists_child_storage(&storage_key, &key) { 1 } else { 0 })
	},
	ext_clear_prefix(prefix_data: *const u8, prefix_len: u32) => {
		let prefix = this.memory.get(prefix_data, prefix_len as usize).map_err(|_| UserError("Invalid attempt to determine prefix in ext_clear_prefix"))?;
		this.ext.clear_prefix(&prefix);
		Ok(())
	},
	ext_kill_child_storage(storage_key_data: *const u8, storage_key_len: u32) => {
		let storage_key = this.memory.get(
			storage_key_data,
			storage_key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_kill_child_storage"))?;
		this.ext.kill_child_storage(&storage_key);
		Ok(())
	},
	// return 0 and place u32::max_value() into written_out if no value exists for the key.
	ext_get_allocated_storage(key_data: *const u8, key_len: u32, written_out: *mut u32) -> *mut u8 => {
		let key = this.memory.get(
			key_data,
			key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine key in ext_get_allocated_storage"))?;
		let maybe_value = this.ext.storage(&key);

		debug_trace!(target: "wasm-trace", "*** Getting storage: {} == {}   [k={}]",
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			},
			if let Some(ref b) = maybe_value {
				&format!("{}", HexDisplay::from(b))
			} else {
				"<empty>"
			},
			HexDisplay::from(&key)
		);

		if let Some(value) = maybe_value {
			let offset = this.heap.allocate(value.len() as u32) as u32;
			this.memory.set(offset, &value).map_err(|_| UserError("Invalid attempt to set memory in ext_get_allocated_storage"))?;
			this.memory.write_primitive(written_out, value.len() as u32)
				.map_err(|_| UserError("Invalid attempt to write written_out in ext_get_allocated_storage"))?;
			Ok(offset)
		} else {
			this.memory.write_primitive(written_out, u32::max_value())
				.map_err(|_| UserError("Invalid attempt to write failed written_out in ext_get_allocated_storage"))?;
			Ok(0)
		}
	},
	// return 0 and place u32::max_value() into written_out if no value exists for the key.
	ext_get_allocated_child_storage(storage_key_data: *const u8, storage_key_len: u32, key_data: *const u8, key_len: u32, written_out: *mut u32) -> *mut u8 => {
		let storage_key = this.memory.get(
			storage_key_data,
			storage_key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_get_allocated_child_storage"))?;
		let key = this.memory.get(
			key_data,
			key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine key in ext_get_allocated_child_storage"))?;
		let maybe_value = this.ext.child_storage(&storage_key, &key);

		debug_trace!(target: "wasm-trace", "*** Getting child storage: {} -> {} == {}   [k={}]",
			::primitives::hexdisplay::ascii_format(&storage_key),
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			},
			if let Some(ref b) = maybe_value {
				&format!("{}", HexDisplay::from(b))
			} else {
				"<empty>"
			},
			HexDisplay::from(&key)
		);

		if let Some(value) = maybe_value {
			let offset = this.heap.allocate(value.len() as u32) as u32;
			this.memory.set(offset, &value).map_err(|_| UserError("Invalid attempt to set memory in ext_get_allocated_child_storage"))?;
			this.memory.write_primitive(written_out, value.len() as u32)
				.map_err(|_| UserError("Invalid attempt to write written_out in ext_get_allocated_child_storage"))?;
			Ok(offset)
		} else {
			this.memory.write_primitive(written_out, u32::max_value())
				.map_err(|_| UserError("Invalid attempt to write failed written_out in ext_get_allocated_child_storage"))?;
			Ok(0)
		}
	},
	// return u32::max_value() if no value exists for the key.
	ext_get_storage_into(key_data: *const u8, key_len: u32, value_data: *mut u8, value_len: u32, value_offset: u32) -> u32 => {
		let key = this.memory.get(key_data, key_len as usize).map_err(|_| UserError("Invalid attempt to get key in ext_get_storage_into"))?;
		let maybe_value = this.ext.storage(&key);
		debug_trace!(target: "wasm-trace", "*** Getting storage: {} == {}   [k={}]",
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			},
			if let Some(ref b) = maybe_value {
				&format!("{}", HexDisplay::from(b))
			} else {
				"<empty>"
			},
			HexDisplay::from(&key)
		);

		if let Some(value) = maybe_value {
			let value = &value[value_offset as usize..];
			let written = ::std::cmp::min(value_len as usize, value.len());
			this.memory.set(value_data, &value[..written]).map_err(|_| UserError("Invalid attempt to set value in ext_get_storage_into"))?;
			Ok(written as u32)
		} else {
			Ok(u32::max_value())
		}
	},
	// return u32::max_value() if no value exists for the key.
	ext_get_child_storage_into(storage_key_data: *const u8, storage_key_len: u32, key_data: *const u8, key_len: u32, value_data: *mut u8, value_len: u32, value_offset: u32) -> u32 => {
		let storage_key = this.memory.get(
			storage_key_data,
			storage_key_len as usize
		).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_get_child_storage_into"))?;
		let key = this.memory.get(
			key_data,
			key_len as usize
		).map_err(|_| UserError("Invalid attempt to get key in ext_get_child_storage_into"))?;
		let maybe_value = this.ext.child_storage(&storage_key, &key);
		debug_trace!(target: "wasm-trace", "*** Getting storage: {} -> {} == {}   [k={}]",
			::primitives::hexdisplay::ascii_format(&storage_key),
			if let Some(_preimage) = this.hash_lookup.get(&key) {
				format!("%{}", ::primitives::hexdisplay::ascii_format(&_preimage))
			} else {
				format!(" {}", ::primitives::hexdisplay::ascii_format(&key))
			},
			if let Some(ref b) = maybe_value {
				&format!("{}", HexDisplay::from(b))
			} else {
				"<empty>"
			},
			HexDisplay::from(&key)
		);

		if let Some(value) = maybe_value {
			let value = &value[value_offset as usize..];
			let written = ::std::cmp::min(value_len as usize, value.len());
			this.memory.set(value_data, &value[..written]).map_err(|_| UserError("Invalid attempt to set value in ext_get_child_storage_into"))?;
			Ok(written as u32)
		} else {
			Ok(u32::max_value())
		}
	},
	ext_storage_root(result: *mut u8) => {
		let r = this.ext.storage_root();
		this.memory.set(result, r.as_ref()).map_err(|_| UserError("Invalid attempt to set memory in ext_storage_root"))?;
		Ok(())
	},
	ext_child_storage_root(storage_key_data: *const u8, storage_key_len: u32, written_out: *mut u32) -> *mut u8 => {
		let storage_key = this.memory.get(storage_key_data, storage_key_len as usize).map_err(|_| UserError("Invalid attempt to determine storage_key in ext_child_storage_root"))?;
		let r = this.ext.child_storage_root(&storage_key);
		if let Some(value) = r {
			let offset = this.heap.allocate(value.len() as u32) as u32;
			this.memory.set(offset, &value).map_err(|_| UserError("Invalid attempt to set memory in ext_child_storage_root"))?;
			this.memory.write_primitive(written_out, value.len() as u32)
				.map_err(|_| UserError("Invalid attempt to write written_out in ext_child_storage_root"))?;
			Ok(offset)
		} else {
			this.memory.write_primitive(written_out, u32::max_value())
				.map_err(|_| UserError("Invalid attempt to write failed written_out in ext_child_storage_root"))?;
			Ok(0)
		}
	},
	ext_storage_changes_root(block: u64, result: *mut u8) -> u32 => {
		let r = this.ext.storage_changes_root(block);
		if let Some(ref r) = r {
			this.memory.set(result, &r[..]).map_err(|_| UserError("Invalid attempt to set memory in ext_storage_changes_root"))?;
		}
		Ok(if r.is_some() { 1u32 } else { 0u32 })
	},
	ext_blake2_256_enumerated_trie_root(values_data: *const u8, lens_data: *const u32, lens_len: u32, result: *mut u8) => {
		let values = (0..lens_len)
			.map(|i| this.memory.read_primitive(lens_data + i * 4))
			.collect::<::std::result::Result<Vec<u32>, UserError>>()?
			.into_iter()
			.scan(0u32, |acc, v| { let o = *acc; *acc += v; Some((o, v)) })
			.map(|(offset, len)|
				this.memory.get(values_data + offset, len as usize)
					.map_err(|_| UserError("Invalid attempt to get memory in ext_blake2_256_enumerated_trie_root"))
			)
			.collect::<::std::result::Result<Vec<_>, UserError>>()?;
		let r = ordered_trie_root::<Blake2Hasher, _, _>(values.into_iter());
		this.memory.set(result, &r[..]).map_err(|_| UserError("Invalid attempt to set memory in ext_blake2_256_enumerated_trie_root"))?;
		Ok(())
	},
	ext_chain_id() -> u64 => {
		Ok(this.ext.chain_id())
	},
	ext_twox_128(data: *const u8, len: u32, out: *mut u8) => {
		let result = if len == 0 {
			let hashed = twox_128(&[0u8; 0]);
			debug_trace!(target: "xxhash", "XXhash: '' -> {}", HexDisplay::from(&hashed));
			this.hash_lookup.insert(hashed.to_vec(), vec![]);
			hashed
		} else {
			let key = this.memory.get(data, len as usize).map_err(|_| UserError("Invalid attempt to get key in ext_twox_128"))?;
			let hashed_key = twox_128(&key);
			debug_trace!(target: "xxhash", "XXhash: {} -> {}",
				if let Ok(_skey) = ::std::str::from_utf8(&key) {
					_skey
				} else {
					&format!("{}", HexDisplay::from(&key))
				},
				HexDisplay::from(&hashed_key)
			);
			this.hash_lookup.insert(hashed_key.to_vec(), key);
			hashed_key
		};

		this.memory.set(out, &result).map_err(|_| UserError("Invalid attempt to set result in ext_twox_128"))?;
		Ok(())
	},
	ext_twox_256(data: *const u8, len: u32, out: *mut u8) => {
		let result = if len == 0 {
			twox_256(&[0u8; 0])
		} else {
			twox_256(&this.memory.get(data, len as usize).map_err(|_| UserError("Invalid attempt to get data in ext_twox_256"))?)
		};
		this.memory.set(out, &result).map_err(|_| UserError("Invalid attempt to set result in ext_twox_256"))?;
		Ok(())
	},
	ext_blake2_256(data: *const u8, len: u32, out: *mut u8) => {
		let result = if len == 0 {
			blake2_256(&[0u8; 0])
		} else {
			blake2_256(&this.memory.get(data, len as usize).map_err(|_| UserError("Invalid attempt to get data in ext_blake2_256"))?)
		};
		this.memory.set(out, &result).map_err(|_| UserError("Invalid attempt to set result in ext_blake2_256"))?;
		Ok(())
	},
	ext_ed25519_verify(msg_data: *const u8, msg_len: u32, sig_data: *const u8, pubkey_data: *const u8) -> u32 => {
		let mut sig = [0u8; 64];
		this.memory.get_into(sig_data, &mut sig[..]).map_err(|_| UserError("Invalid attempt to get signature in ext_ed25519_verify"))?;
		let mut pubkey = [0u8; 32];
		this.memory.get_into(pubkey_data, &mut pubkey[..]).map_err(|_| UserError("Invalid attempt to get pubkey in ext_ed25519_verify"))?;
		let msg = this.memory.get(msg_data, msg_len as usize).map_err(|_| UserError("Invalid attempt to get message in ext_ed25519_verify"))?;

		Ok(if ed25519::verify(&sig, &msg, &pubkey) {
			0
		} else {
			5
		})
	},
	ext_sandbox_instantiate(
		dispatch_thunk_idx: usize,
		wasm_ptr: *const u8,
		wasm_len: usize,
		imports_ptr: *const u8,
		imports_len: usize,
		state: usize
	) -> u32 => {
		let wasm = this.memory.get(wasm_ptr, wasm_len as usize)
			.map_err(|_| UserError("OOB while ext_sandbox_instantiate: wasm"))?;
		let raw_env_def = this.memory.get(imports_ptr, imports_len as usize)
			.map_err(|_| UserError("OOB while ext_sandbox_instantiate: imports"))?;

		// Extract a dispatch thunk from instance's table by the specified index.
		let dispatch_thunk = {
			let table = this.table.as_ref()
				.ok_or_else(|| UserError("Runtime doesn't have a table; sandbox is unavailable"))?;
			table.get(dispatch_thunk_idx)
				.map_err(|_| UserError("dispatch_thunk_idx is out of the table bounds"))?
				.ok_or_else(|| UserError("dispatch_thunk_idx points on an empty table entry"))?
				.clone()
		};

		let instance_idx_or_err_code =
			match sandbox::instantiate(this, dispatch_thunk, &wasm, &raw_env_def, state) {
				Ok(instance_idx) => instance_idx,
				Err(sandbox::InstantiationError::StartTrapped) => sandbox_primitives::ERR_EXECUTION,
				Err(_) => sandbox_primitives::ERR_MODULE,
			};

		Ok(instance_idx_or_err_code as u32)
	},
	ext_sandbox_instance_teardown(instance_idx: u32) => {
		this.sandbox_store.instance_teardown(instance_idx)?;
		Ok(())
	},
	ext_sandbox_invoke(instance_idx: u32, export_ptr: *const u8, export_len: usize, args_ptr: *const u8, args_len: usize, return_val_ptr: *const u8, return_val_len: usize, state: usize) -> u32 => {
		use codec::{Decode, Encode};

		trace!(target: "sr-sandbox", "invoke, instance_idx={}", instance_idx);
		let export = this.memory.get(export_ptr, export_len as usize)
			.map_err(|_| UserError("OOB while ext_sandbox_invoke: export"))
			.and_then(|b|
				String::from_utf8(b)
					.map_err(|_| UserError("export name should be a valid utf-8 sequence"))
			)?;

		// Deserialize arguments and convert them into wasmi types.
		let serialized_args = this.memory.get(args_ptr, args_len as usize)
			.map_err(|_| UserError("OOB while ext_sandbox_invoke: args"))?;
		let args = Vec::<sandbox_primitives::TypedValue>::decode(&mut &serialized_args[..])
			.ok_or_else(|| UserError("Can't decode serialized arguments for the invocation"))?
			.into_iter()
			.map(Into::into)
			.collect::<Vec<_>>();

		let instance = this.sandbox_store.instance(instance_idx)?;
		let result = instance.invoke(&export, &args, this, state);

		match result {
			Ok(None) => Ok(sandbox_primitives::ERR_OK),
			Ok(Some(val)) => {
				// Serialize return value and write it back into the memory.
				sandbox_primitives::ReturnValue::Value(val.into()).using_encoded(|val| {
					if val.len() > return_val_len as usize {
						Err(UserError("Return value buffer is too small"))?;
					}
					this.memory
						.set(return_val_ptr, val)
						.map_err(|_| UserError("Return value buffer is OOB"))?;
					Ok(sandbox_primitives::ERR_OK)
				})
			}
			Err(_) => Ok(sandbox_primitives::ERR_EXECUTION),
		}
	},
	ext_sandbox_memory_new(initial: u32, maximum: u32) -> u32 => {
		let mem_idx = this.sandbox_store.new_memory(initial, maximum)?;
		Ok(mem_idx)
	},
	ext_sandbox_memory_get(memory_idx: u32, offset: u32, buf_ptr: *mut u8, buf_len: u32) -> u32 => {
		let sandboxed_memory = this.sandbox_store.memory(memory_idx)?;

		match MemoryInstance::transfer(
			&sandboxed_memory,
			offset as usize,
			&this.memory,
			buf_ptr as usize,
			buf_len as usize,
		) {
			Ok(()) => Ok(sandbox_primitives::ERR_OK),
			Err(_) => Ok(sandbox_primitives::ERR_OUT_OF_BOUNDS),
		}
	},
	ext_sandbox_memory_set(memory_idx: u32, offset: u32, val_ptr: *const u8, val_len: u32) -> u32 => {
		let sandboxed_memory = this.sandbox_store.memory(memory_idx)?;

		match MemoryInstance::transfer(
			&this.memory,
			val_ptr as usize,
			&sandboxed_memory,
			offset as usize,
			val_len as usize,
		) {
			Ok(()) => Ok(sandbox_primitives::ERR_OK),
			Err(_) => Ok(sandbox_primitives::ERR_OUT_OF_BOUNDS),
		}
	},
	ext_sandbox_memory_teardown(memory_idx: u32) => {
		this.sandbox_store.memory_teardown(memory_idx)?;
		Ok(())
	},
	=> <'e, E: Externalities<Blake2Hasher> + 'e>
);

/// Wasm rust executor for contracts.
///
/// Executes the provided code in a sandboxed wasm runtime.
#[derive(Debug, Clone)]
pub struct WasmExecutor {
}

impl WasmExecutor {

	/// Create a new instance.
	pub fn new() -> Self {
		WasmExecutor{}
	}

	/// Call a given method in the given code.
	/// This should be used for tests only.
	pub fn call<E: Externalities<Blake2Hasher>>(
		&self,
		ext: &mut E,
		heap_pages: usize,
		code: &[u8],
		method: &str,
		data: &[u8],
		) -> Result<Vec<u8>> {
		let module = ::wasmi::Module::from_buffer(code).expect("all modules compiled with rustc are valid wasm code; qed");
		self.call_in_wasm_module(ext, heap_pages, &module, method, data)
	}

	/// Call a given method in the given wasm-module runtime.
	pub fn call_in_wasm_module<E: Externalities<Blake2Hasher>>(
		&self,
		ext: &mut E,
		heap_pages: usize,
		module: &Module,
		method: &str,
		data: &[u8],
	) -> Result<Vec<u8>> {
		// start module instantiation. Don't run 'start' function yet.
		let intermediate_instance = ModuleInstance::new(
			module,
			&ImportsBuilder::new()
				.with_resolver("env", FunctionExecutor::<E>::resolver())
		)?;

		// extract a reference to a linear memory, optional reference to a table
		// and then initialize FunctionExecutor.
		let memory = intermediate_instance
			.not_started_instance()
			.export_by_name("memory")
			// TODO: with code coming from the blockchain it isn't strictly been compiled with rustc anymore.
			// these assumptions are probably not true anymore
			.expect("all modules compiled with rustc should have an export named 'memory'; qed")
			.as_memory()
			.expect("in module generated by rustc export named 'memory' should be a memory; qed")
			.clone();
		let table: Option<TableRef> = intermediate_instance
			.not_started_instance()
			.export_by_name("__indirect_function_table")
			.and_then(|e| e.as_table().cloned());

		let mut fec = FunctionExecutor::new(memory.clone(), heap_pages, table, ext)?;

		// finish instantiation by running 'start' function (if any).
		let instance = intermediate_instance.run_start(&mut fec)?;
		let size = data.len() as u32;
		let offset = fec.heap.allocate(size);
		memory.set(offset, &data)?;

		let result = instance.invoke_export(
			method,
			&[
				I32(offset as i32),
				I32(size as i32)
			],
			&mut fec
		);
		let returned = match result {
			Ok(x) => x,
			Err(e) => {
				trace!(target: "wasm-executor", "Failed to execute code with {} pages", heap_pages);
				return Err(e.into())
			},
		};

		if let Some(I64(r)) = returned {
			let offset = r as u32;
			let length = (r >> 32) as u32 as usize;
			memory.get(offset, length)
				.map_err(|_| ErrorKind::Runtime.into())
		} else {
			Err(ErrorKind::InvalidReturn.into())
		}
	}
}


#[cfg(test)]
mod tests {
	use super::*;
	use codec::Encode;
	use state_machine::TestExternalities;

	#[test]
	fn returning_should_work() {
		let mut ext = TestExternalities::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");

		let output = WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_empty_return", &[]).unwrap();
		assert_eq!(output, vec![0u8; 0]);
	}

	#[test]
	fn panicking_should_work() {
		let mut ext = TestExternalities::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");

		let output = WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_panic", &[]);
		assert!(output.is_err());

		let output = WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_conditional_panic", &[2]);
		assert!(output.is_err());
	}

	#[test]
	fn storage_should_work() {
		let mut ext = TestExternalities::default();
		ext.set_storage(b"foo".to_vec(), b"bar".to_vec());
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");

		let output = WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_data_in", b"Hello world").unwrap();

		assert_eq!(output, b"all ok!".to_vec());

		let expected = TestExternalities::new(map![
			b"input".to_vec() => b"Hello world".to_vec(),
			b"foo".to_vec() => b"bar".to_vec(),
			b"baz".to_vec() => b"bar".to_vec()
		]);
		assert_eq!(ext, expected);
	}

	#[test]
	fn clear_prefix_should_work() {
		let mut ext = TestExternalities::default();
		ext.set_storage(b"aaa".to_vec(), b"1".to_vec());
		ext.set_storage(b"aab".to_vec(), b"2".to_vec());
		ext.set_storage(b"aba".to_vec(), b"3".to_vec());
		ext.set_storage(b"abb".to_vec(), b"4".to_vec());
		ext.set_storage(b"bbb".to_vec(), b"5".to_vec());
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");

		// This will clear all entries which prefix is "ab".
		let output = WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_clear_prefix", b"ab").unwrap();

		assert_eq!(output, b"all ok!".to_vec());

		let expected: TestExternalities<_> = map![
			b"aaa".to_vec() => b"1".to_vec(),
			b"aab".to_vec() => b"2".to_vec(),
			b"bbb".to_vec() => b"5".to_vec()
		];
		assert_eq!(expected, ext);
	}

	#[test]
	fn blake2_256_should_work() {
		let mut ext = TestExternalities::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_blake2_256", &[]).unwrap(),
			blake2_256(&b""[..]).encode()
		);
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_blake2_256", b"Hello world!").unwrap(),
			blake2_256(&b"Hello world!"[..]).encode()
		);
	}

	#[test]
	fn twox_256_should_work() {
		let mut ext = TestExternalities::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_twox_256", &[]).unwrap(),
			hex!("99e9d85137db46ef4bbea33613baafd56f963c64b1f3685a4eb4abd67ff6203a")
		);
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_twox_256", b"Hello world!").unwrap(),
			hex!("b27dfd7f223f177f2a13647b533599af0c07f68bda23d96d059da2b451a35a74")
		);
	}

	#[test]
	fn twox_128_should_work() {
		let mut ext = TestExternalities::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_twox_128", &[]).unwrap(),
			hex!("99e9d85137db46ef4bbea33613baafd5")
		);
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_twox_128", b"Hello world!").unwrap(),
			hex!("b27dfd7f223f177f2a13647b533599af")
		);
	}

	#[test]
	fn ed25519_verify_should_work() {
		let mut ext = TestExternalities::<Blake2Hasher>::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");
		let key = ed25519::Pair::from_seed(&blake2_256(b"test"));
		let sig = key.sign(b"all ok!");
		let mut calldata = vec![];
		calldata.extend_from_slice(key.public().as_ref());
		calldata.extend_from_slice(sig.as_ref());

		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_ed25519_verify", &calldata).unwrap(),
			vec![1]
		);

		let other_sig = key.sign(b"all is not ok!");
		let mut calldata = vec![];
		calldata.extend_from_slice(key.public().as_ref());
		calldata.extend_from_slice(other_sig.as_ref());

		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_ed25519_verify", &calldata).unwrap(),
			vec![0]
		);
	}

	#[test]
	fn enumerated_trie_root_should_work() {
		let mut ext = TestExternalities::<Blake2Hasher>::default();
		let test_code = include_bytes!("../wasm/target/wasm32-unknown-unknown/release/runtime_test.compact.wasm");
		assert_eq!(
			WasmExecutor::new().call(&mut ext, 8, &test_code[..], "test_enumerated_trie_root", &[]).unwrap(),
			ordered_trie_root::<Blake2Hasher, _, _>(vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()].iter()).0.encode()
		);
	}


}

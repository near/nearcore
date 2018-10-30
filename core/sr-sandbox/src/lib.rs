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

// tag::description[]
//! This crate provides means of instantiation and execution of wasm modules.
//!
//! It works even when the user of this library is itself executes
//! inside the wasm VM. In this case same VM is used for execution
//! of both the sandbox owner and the sandboxed module, without compromising security
//! and without performance penalty of full wasm emulation inside wasm.
//!
//! This is achieved by using bindings to wasm VM which are published by the host API.
//! This API is thin and consists of only handful functions. It contains functions for instantiating
//! modules and executing them and for example doesn't contain functions for inspecting the module
//! structure. The user of this library is supposed to read wasm module by it's own means.
//!
//! When this crate is used in `std` environment all these functions are implemented by directly
//! calling wasm VM.
//!
//! Example of possible use-cases for this library are following:
//!
//! - implementing smart-contract runtimes which uses wasm for contract code
//! - executing wasm substrate runtime inside of a wasm parachain
//! - etc
// end::description[]

#![warn(missing_docs)]
#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(not(feature = "std"), feature(core_intrinsics))]
#![cfg_attr(not(feature = "std"), feature(alloc))]

#[cfg_attr(not(feature = "std"), macro_use)]
extern crate sr_std as rstd;
extern crate substrate_primitives as primitives;
#[cfg(not(feature = "std"))]
extern crate parity_codec as codec;

#[cfg(test)]
extern crate wabt;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use rstd::prelude::*;

pub use primitives::sandbox::{TypedValue, ReturnValue, HostError};

mod imp {
	#[cfg(feature = "std")]
	include!("../with_std.rs");

	#[cfg(not(feature = "std"))]
	include!("../without_std.rs");
}

/// Error that can occur while using this crate.
#[cfg_attr(feature = "std", derive(Debug))]
pub enum Error {
	/// Module is not valid, couldn't be instantiated or it's `start` function trapped
	/// when executed.
	Module,

	/// Access to a memory or table was made with an address or an index which is out of bounds.
	///
	/// Note that if wasm module makes an out-of-bounds access then trap will occur.
	OutOfBounds,

	/// Failed to invoke an exported function for some reason.
	Execution,
}

impl From<Error> for HostError {
	fn from(_e: Error) -> HostError {
		HostError
	}
}

/// Function pointer for specifying functions by the
/// supervisor in [`EnvironmentDefinitionBuilder`].
///
/// [`EnvironmentDefinitionBuilder`]: struct.EnvironmentDefinitionBuilder.html
pub type HostFuncType<T> = fn(&mut T, &[TypedValue]) -> Result<ReturnValue, HostError>;

/// Reference to a sandboxed linear memory, that
/// will be used by the guest module.
///
/// The memory can't be directly accessed by supervisor, but only
/// through designated functions [`get`] and [`set`].
///
/// [`get`]: #method.get
/// [`set`]: #method.set
#[derive(Clone)]
pub struct Memory {
	inner: imp::Memory,
}

impl Memory {
	/// Construct a new linear memory instance.
	///
	/// The memory allocated with initial number of pages specified by `initial`.
	/// Minimal possible value for `initial` is 0 and maximum possible is `65536`.
	/// (Since maximum addressible memory is 2<sup>32</sup> = 4GiB = 65536 * 64KiB).
	///
	/// It is possible to limit maximum number of pages this memory instance can have by specifying
	/// `maximum`. If not specified, this memory instance would be able to allocate up to 4GiB.
	///
	/// Allocated memory is always zeroed.
	pub fn new(initial: u32, maximum: Option<u32>) -> Result<Memory, Error> {
		Ok(Memory {
			inner: imp::Memory::new(initial, maximum)?,
		})
	}

	/// Read a memory area at the address `ptr` with the size of the provided slice `buf`.
	///
	/// Returns `Err` if the range is out-of-bounds.
	pub fn get(&self, ptr: u32, buf: &mut [u8]) -> Result<(), Error> {
		self.inner.get(ptr, buf)
	}

	/// Write a memory area at the address `ptr` with contents of the provided slice `buf`.
	///
	/// Returns `Err` if the range is out-of-bounds.
	pub fn set(&self, ptr: u32, value: &[u8]) -> Result<(), Error> {
		self.inner.set(ptr, value)
	}
}

/// Struct that can be used for defining an environment for a sandboxed module.
///
/// The sandboxed module can access only the entities which were defined and passed
/// to the module at the instantiation time.
pub struct EnvironmentDefinitionBuilder<T> {
	inner: imp::EnvironmentDefinitionBuilder<T>,
}

impl<T> EnvironmentDefinitionBuilder<T> {
	/// Construct a new `EnvironmentDefinitionBuilder`.
	pub fn new() -> EnvironmentDefinitionBuilder<T> {
		EnvironmentDefinitionBuilder {
			inner: imp::EnvironmentDefinitionBuilder::new(),
		}
	}

	/// Register a host function in this environment defintion.
	///
	/// NOTE that there is no constraints on type of this function. An instance
	/// can import function passed here with any signature it wants. It can even import
	/// the same function (i.e. with same `module` and `field`) several times. It's up to
	/// the user code to check or constrain the types of signatures.
	pub fn add_host_func<N1, N2>(&mut self, module: N1, field: N2, f: HostFuncType<T>)
	where
		N1: Into<Vec<u8>>,
		N2: Into<Vec<u8>>,
	{
		self.inner.add_host_func(module, field, f);
	}

	/// Register a memory in this environment definition.
	pub fn add_memory<N1, N2>(&mut self, module: N1, field: N2, mem: Memory)
	where
		N1: Into<Vec<u8>>,
		N2: Into<Vec<u8>>,
	{
		self.inner.add_memory(module, field, mem.inner);
	}
}

/// Sandboxed instance of a wasm module.
///
/// This instance can be used for invoking exported functions.
pub struct Instance<T> {
	inner: imp::Instance<T>,

}

impl<T> Instance<T> {
	/// Instantiate a module with the given [`EnvironmentDefinitionBuilder`]. It will
	/// run the `start` function with the given `state`.
	///
	/// Returns `Err(Error::Module)` if this module can't be instantiated with the given
	/// environment. If execution of `start` function generated a trap, then `Err(Error::Execution)` will
	/// be returned.
	///
	/// [`EnvironmentDefinitionBuilder`]: struct.EnvironmentDefinitionBuilder.html
	pub fn new(code: &[u8], env_def_builder: &EnvironmentDefinitionBuilder<T>, state: &mut T) -> Result<Instance<T>, Error> {
		Ok(Instance {
			inner: imp::Instance::new(code, &env_def_builder.inner, state)?,
		})
	}

	/// Invoke an exported function with the given name.
	///
	/// # Errors
	///
	/// Returns `Err(Error::Execution)` if:
	///
	/// - An export function name isn't a proper utf8 byte sequence,
	/// - This module doesn't have an exported function with the given name,
	/// - If types of the arguments passed to the function doesn't match function signature
	///   then trap occurs (as if the exported function was called via call_indirect),
	/// - Trap occured at the execution time.
	pub fn invoke(
		&mut self,
		name: &[u8],
		args: &[TypedValue],
		state: &mut T,
	) -> Result<ReturnValue, Error> {
		self.inner.invoke(name, args, state)
	}
}

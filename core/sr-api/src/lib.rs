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

//! API's for interfacing with the runtime via native/wasm.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate sr_std as rstd;
extern crate sr_primitives as primitives;
#[doc(hidden)]
pub extern crate parity_codec as codec;
extern crate sr_version as runtime_version;

#[doc(hidden)]
pub use primitives::{traits::Block as BlockT, generic::BlockId, transaction_validity::TransactionValidity, ApplyResult};
use runtime_version::{ApiId, RuntimeVersion};
use rstd::vec::Vec;
#[doc(hidden)]
pub use rstd::slice;
#[doc(hidden)]
pub use codec::{Encode, Decode};

/// Declare the given API traits.
///
/// # Example:
///
/// ```nocompile
/// decl_apis!{
///     pub trait Test<Event> ExtraClientSide<ClientArg> {
///         fn test<AccountId>(event: Event) -> AccountId;
///
///         /// A function that will have the extra parameter `param` on the client side,
///         /// the runtime does not have any parameter.
///         fn testWithExtraParams() ExtraClientSide(param: &Self::ClientArg);
///     }
/// }
/// ```
///
/// Will result in the following declaration:
///
/// ```nocompile
/// mod runtime {
///     pub trait Test<Event, AccountId> {
///         fn test(event: Event) -> AccountId;
///     }
/// }
///
/// pub trait Test<Block: BlockT, Event> {
///     type Error;
///     type ClientArg;
///     fn test<AccountId: Encode + Decode>(&self, at: &BlockId<Block>, event: Event) -> Result<Event, Self::Error>;
///     fn testWithExtraParams(&self, at: &BlockId<Block>, param: &Client) -> Result<Event, Self::Error>;
/// }
/// ```
///
/// The declarations generated in the `runtime` module will be used by `impl_apis!` for implementing
/// the traits for a runtime. The other declarations should be used for implementing the interface
/// in the client.
#[macro_export]
macro_rules! decl_apis {
	(
		$(
			$( #[$attr:meta] )*
			pub trait $name:ident $(< $( $generic_param:ident $( : $generic_bound:ident )* ),* >)*
				$( ExtraClientSide < $( $client_generic_param:ident $( : $client_generic_bound:ident )* ),+ > )*
			{
				$(
					$( #[$fn_attr:meta] )*
					fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
						$( $param_name:ident : $param_type:ty ),*
					)
					$( ExtraClientSide ( $( $client_param_name:ident : $client_param_type:ty ),+ ) )*
					$( -> $return_ty:ty)*;
				)*
			}
		)*
	) => {
		$(
			decl_apis!(
				@ADD_BLOCK_GENERIC
				$( #[$attr] )*
				pub trait $name $(< $( $generic_param $( : $generic_bound )* ),* >)* {
					$( $( type $client_generic_param $( : $client_generic_bound )*; )* )*
					$(
						$( #[$fn_attr] )*
						fn $fn_name $( < $( $fn_generic ),* > )* (
							$( $( $client_param_name: $client_param_type, )* )*
							$( $param_name : &$param_type, )*
						) $( -> $return_ty )*;
					)*
				};
				;
				;
				$( $( $generic_param $( : $generic_bound )* ),* )*
			);
		)*
		decl_apis! {
			@GENERATE_RUNTIME_TRAITS
			$(
				$( #[$attr] )*
				pub trait $name $(< $( $generic_param $( : $generic_bound )* ),* >)* {
					$(
						$( #[$fn_attr] )*
						fn $fn_name $( < $( $fn_generic ),* > )* ($( $param_name : $param_type )* ) $( -> $return_ty )*;
					)*
				};
			)*
		}
	};
	(@ADD_BLOCK_GENERIC
		$( #[$attr:meta] )*
		pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty)*;
			)*
		};
		;
		$( $generic_param_parsed:ident $( : $generic_bound_parsed:ident )* ),*;
		Block: BlockT
		$(, $generic_param_rest:ident $( : $generic_bound_rest:ident )* )*
	) => {
		decl_apis!(
			@ADD_BLOCK_GENERIC
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			Found;
			$( $generic_param_parsed $( : $generic_bound_parsed )* , )* Block: $crate::BlockT;
			$( $generic_param_rest $( : $generic_bound_rest )* ),*
		);
	};
	(@ADD_BLOCK_GENERIC
		$( #[$attr:meta] )*
		pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
	 		$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty )*;
			)*
		};
		$( $block_found:ident )*;
		$( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
		$generic_param:ident $( : $generic_bound:ident )*
		$(, $generic_param_rest:ident $( : $generic_bound_rest:ident )* )*
	) => {
		decl_apis!(
			@ADD_BLOCK_GENERIC
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			$( $block_found )*;
			$( $generic_param_parsed $( : $generic_bound_parsed )* , )* $generic_param $( : $generic_bound )*;
			$( $generic_param_rest $( : $generic_bound_rest )* ),*
		);
	};
	(@ADD_BLOCK_GENERIC
		$( #[$attr:meta] )*
		pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty )*;
			)*
		};
		Found;
	 	$( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
	) => {
		decl_apis!(
			@GENERATE_RETURN_TYPES
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			$( $generic_param_parsed $( : $generic_bound_parsed )* ),*;
			{};
			$( $( $return_ty )*; )*
		);
	};
	(@ADD_BLOCK_GENERIC
		$( #[$attr:meta] )*
		pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty )*;
			)*
		};
		;
		$( $generic_param_parsed:ident $( : $generic_bound_parsed:ident )* ),*;
	) => {
		decl_apis!(
			@GENERATE_RETURN_TYPES
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			// We need to add the required generic Block parameter
			Block: $crate::BlockT $(, $generic_param_parsed $( : $generic_bound_parsed )* )*;
			{};
			$( $( $return_ty )*; )*
		);
	};
	(@GENERATE_RETURN_TYPES
        $( #[$attr:meta] )*
        pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty)*;
			)*
        };
        $( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
		{ $( $result_return_ty:ty; )* };
		$return_ty_current:ty;
		$( $( $return_ty_rest:ty )*; )*
	) => {
		decl_apis!(
			@GENERATE_RETURN_TYPES
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			$( $generic_param_parsed $( : $generic_bound_parsed )* ),*;
			{ $( $result_return_ty; )* Result<$return_ty_current, Self::Error>; };
			$( $( $return_ty_rest )*; )*
		);
	};
	(@GENERATE_RETURN_TYPES
        $( #[$attr:meta] )*
        pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty)*;
			)*
        };
        $( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
		{ $( $result_return_ty:ty; )* };
		;
		$( $( $return_ty_rest:ty )*; )*
	) => {
		decl_apis!(
			@GENERATE_RETURN_TYPES
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			$( $generic_param_parsed $( : $generic_bound_parsed )* ),*;
			{ $( $result_return_ty; )* Result<(), Self::Error>; };
			$( $( $return_ty_rest )*; )*
		);
	};
	(@GENERATE_RETURN_TYPES
        $( #[$attr:meta] )*
        pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty)*;
			)*
        };
        $( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
		{ $( $result_return_ty:ty; )* };
	) => {
		decl_apis!(
			@GENERATE_CLIENT_TRAITS
			$( #[$attr] )*
			pub trait $name $(< $( $generic_param_orig $( : $generic_bound_orig )* ),* >)* {
				$( type $client_generic_param $( : $client_generic_bound )*; )*
				$(
					$( #[$fn_attr] )*
					fn $fn_name $( < $( $fn_generic ),* > )* (
						$( $param_name : $param_type, )*
					) $( -> $return_ty )*;
				)*
			};
			$( $generic_param_parsed $( : $generic_bound_parsed )* ),*;
			{ $( $result_return_ty; )* };
		);
	};
	(@GENERATE_CLIENT_TRAITS
		$( #[$attr:meta] )*
		pub trait $name:ident $(< $( $generic_param_orig:ident $( : $generic_bound_orig:ident )* ),* >)* {
			$( type $client_generic_param:ident $( : $client_generic_bound:ident )*; )*
			$(
				$( #[$fn_attr:meta] )*
				fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
					$( $param_name:ident : $param_type:ty, )*
				) $( -> $return_ty:ty)*;
			)*
		};
		$( $generic_param_parsed:ident $( : $generic_bound_parsed:path )* ),*;
		{ $( $result_return_ty:ty; )* };
	) => {
		$( #[$attr] )*
		pub trait $name < $( $generic_param_parsed $( : $generic_bound_parsed )* ),* > {
			/// The Error type returned by this API.
			type Error;
			$( type $client_generic_param $( : $client_generic_bound )*; )*

			$(
				$( #[$fn_attr] )*
				fn $fn_name $( < $( $fn_generic: $crate::Encode + $crate::Decode ),* > )* (
					&self, at: &$crate::BlockId<Block> $(, $param_name: $param_type )*
				) -> $result_return_ty;
			)*
		}
	};
	(@GENERATE_RUNTIME_TRAITS
		$(
			$( #[$attr:meta] )*
			pub trait $name:ident $(< $( $generic_param:ident $( : $generic_bound:ident )* ),* >)* {
				$(
					$( #[$fn_attr:meta] )*
					fn $fn_name:ident $( < $( $fn_generic:ident ),* > )* (
						$( $param_name:ident : $param_type:ty )*
					) $( -> $return_ty:ty)*;
				)*
			};
		)*
	) => {
		decl_apis! {
			@GENERATE_RUNTIME_TRAITS_WITH_JOINED_GENERICS
			$(
				$( #[$attr] )*
				pub trait $name < $( $( $generic_param $( : $generic_bound )*, )* )* $( $( $( $fn_generic, )* )* )* > {
					$(
						$( #[$fn_attr] )*
						fn $fn_name ($( $param_name: $param_type ),*) $( -> $return_ty )*;
					)*
				}
			)*
		}
	};
	(@GENERATE_RUNTIME_TRAITS_WITH_JOINED_GENERICS
		$(
			$( #[$attr:meta] )*
			pub trait $name:ident < $( $generic_param:ident $( : $generic_bound:ident )*, )* > {
				$(
					$( #[$fn_attr:meta] )*
					fn $fn_name:ident($( $param_name:ident : $param_type:ty ),*) $( -> $return_ty:ty)*;
				)*
			}
		)*
	) => {
		/// The API traits to implement on the runtime side.
		pub mod runtime {
			use super::*;

			$(
				$( #[$attr] )*
				pub trait $name < $( $generic_param $( : $generic_bound )* ),* > {
					$(
						$( #[$fn_attr] )*
						fn $fn_name ($( $param_name: $param_type ),*) $( -> $return_ty )*;
					)*
				}
			)*
		}
	};
}

/// The ApiIds for the various standard runtime APIs.
pub mod id {
	use super::ApiId;
	
	/// ApiId for the BlockBuilder trait.
	pub const BLOCK_BUILDER: ApiId = *b"blkbuild";

	/// ApiId for the TaggedTransactionQueue trait.
	pub const TAGGED_TRANSACTION_QUEUE: ApiId = *b"validatx";

	/// ApiId for the Metadata trait.
	pub const METADATA: ApiId = *b"metadata";
}

decl_apis! {
	/// The `Core` api trait that is mandantory for each runtime.
	pub trait Core<Block: BlockT, AuthorityId> {
		fn version() -> RuntimeVersion;
		fn authorities() -> Vec<AuthorityId>;
		fn execute_block(block: Block);
	}

	/// The `Metadata` api trait that returns metadata for the runtime.
	pub trait Metadata<Data> {
		fn metadata() -> Data;
	}

	/// The `OldTxQueue` api trait for interfering with the old transaction queue.
	pub trait OldTxQueue {
		fn account_nonce<AccountId, Index>(account: AccountId) -> Index;
		fn lookup_address<Address, LookupId>(address: Address) -> Option<LookupId>;
	}

	/// The `TaggedTransactionQueue` api trait for interfering with the new transaction queue.
	pub trait TaggedTransactionQueue<Block: BlockT> {
		fn validate_transaction<TransactionValidity>(tx: <Block as BlockT>::Extrinsic) -> TransactionValidity;
	}

	/// The `BlockBuilder` api trait that provides required functions for building a block for a runtime.
	pub trait BlockBuilder<Block: BlockT> ExtraClientSide <OverlayedChanges> {
		/// Initialise a block with the given header.
		fn initialise_block(header: <Block as BlockT>::Header) ExtraClientSide(changes: &mut Self::OverlayedChanges);
		/// Apply the given extrinsics.
		fn apply_extrinsic(extrinsic: <Block as BlockT>::Extrinsic) ExtraClientSide(changes: &mut Self::OverlayedChanges) -> ApplyResult;
		/// Finish the current block.
		fn finalise_block() ExtraClientSide(changes: &mut Self::OverlayedChanges) -> <Block as BlockT>::Header;
		/// Generate inherent extrinsics.
		fn inherent_extrinsics<InherentExtrinsic, UncheckedExtrinsic>(inherent: InherentExtrinsic) -> Vec<UncheckedExtrinsic>;
		/// Check that the inherents are valid.
		fn check_inherents<InherentData, Error>(block: Block, data: InherentData) -> Result<(), Error>;
		/// Generate a random seed.
		fn random_seed() -> <Block as BlockT>::Hash;
	}
}

/// Implement the given API's for the given runtime.
/// All desired API's need to be implemented in one `impl_apis!` call.
/// Besides generating the implementation for the runtime, there will be also generated an
/// auxiliary module named `api` that contains function for inferring with the API in native/wasm.
/// It is important to use the traits from the `runtime` module with this macro.
///
/// # Example:
///
/// ```nocompile
/// #[macro_use]
/// extern crate sr_api as runtime_api;
///
/// use runtime_api::runtime::{Core, TaggedTransactionQueue};
///
/// impl_apis! {
///     impl Core<Block, AccountId> for Runtime {
///         fn version() -> RuntimeVersion { 1 }
///         fn authorities() -> Vec<AuthorityId> { vec![1] }
///         fn execute_block(block: Block) {
///             //comment
///             let block = call_arbitrary_code(block);
///             execute(block);
///         }
///     }
///
///     impl TaggedTransactionQueue<Block> for Runtime {
///			fn validate_transaction(tx: <Block as BlockT>::Extrinsic) -> TransactionValidity {
///				unimplemented!()
///			}
///     }
/// }
///
/// fn main() {}
/// ```
#[macro_export]
macro_rules! impl_apis {
	(
		impl $trait_name:ident $( < $( $generic:ident ),* > )* for $runtime:ident {
			$(
				fn $fn_name:ident ( $( $arg_name:ident : $arg_ty:ty ),* ) $( -> $return_ty:ty )* {
					$( $impl:tt )*
				}
			)*
		}
		$( $rest:tt )*
	) => {
		impl $trait_name $( < $( $generic ),* > )* for $runtime {
			$(
				fn $fn_name ( $( $arg_name : $arg_ty ),* ) $( -> $return_ty )* {
					$( $impl )*
				}
			)*
		}
		impl_apis! {
			$runtime;
			$( $fn_name ( $( $arg_name: $arg_ty ),* ); )*;
			$( $rest )*
		}
	};
	(
		$runtime:ident;
		$( $fn_name_parsed:ident ( $( $arg_name_parsed:ident : $arg_ty_parsed:ty ),* ); )*;
		impl $trait_name:ident $( < $( $generic:ident ),* > )* for $runtime_ignore:ident {
			$(
				fn $fn_name:ident ( $( $arg_name:ident : $arg_ty:ty ),* ) $( -> $return_ty:ty )* {
					$( $impl:tt )*
				}
			)*
		}
		$( $rest:tt )*
	) => {
		impl $trait_name $( < $( $generic ),* > )* for $runtime {
			$(
				fn $fn_name ( $( $arg_name : $arg_ty ),* ) $( -> $return_ty )* {
					$( $impl )*
				}
			)*
		}
		impl_apis! {
			$runtime;
			$( $fn_name_parsed ( $( $arg_name_parsed: $arg_ty_parsed ),* ); )*
			$( $fn_name ( $( $arg_name: $arg_ty ),* ); )*;
			$( $rest )*
		}
	};
	(
		$runtime:ident;
		$( $fn_name:ident ( $( $arg_name:ident : $arg_ty:ty ),* ); )*;
	) => {
		pub mod api {
			use super::*;

			#[cfg(feature = "std")]
			pub fn dispatch(method: &str, mut data: &[u8]) -> Option<Vec<u8>> {
				match method {
					$(
						stringify!($fn_name) => {
							Some({impl_apis! {
								@GENERATE_IMPL_CALL
								$runtime;
								$fn_name;
								$( $arg_name : $arg_ty ),*;
								data;
							}})
						}
					)*
						_ => None,
				}
			}

			$(
				#[cfg(not(feature = "std"))]
				#[no_mangle]
				pub fn $fn_name(input_data: *mut u8, input_len: usize) -> u64 {
					let mut input = if input_len == 0 {
						&[0u8; 0]
					} else {
						unsafe {
							$crate::slice::from_raw_parts(input_data, input_len)
						}
					};

					let output = { impl_apis! {
						@GENERATE_IMPL_CALL
						$runtime;
						$fn_name;
						$( $arg_name : $arg_ty ),*;
						input;
					} };
					let res = output.as_ptr() as u64 + ((output.len() as u64) << 32);

					// Leak the output vector to avoid it being freed.
					// This is fine in a WASM context since the heap
					// will be discarded after the call.
					::core::mem::forget(output);
					res
				}
			)*
		}
	};
	(@GENERATE_IMPL_CALL
		$runtime:ident;
		$fn_name:ident;
		$arg_name:ident : $arg_ty:ty;
		$input:ident;
	) => {
		let $arg_name : $arg_ty = match $crate::codec::Decode::decode(&mut $input) {
			Some(input) => input,
			None => panic!("Bad input data provided to {}", stringify!($fn_name)),
		};

		let output = $runtime::$fn_name($arg_name);
		$crate::codec::Encode::encode(&output)
	};
	(@GENERATE_IMPL_CALL
		$runtime:ident;
		$fn_name:ident;
		$( $arg_name:ident : $arg_ty:ty ),*;
		$input:ident;
	) => {
		let ( $( $arg_name ),* ) : ($( $arg_ty ),*) = match $crate::codec::Decode::decode(&mut $input) {
			Some(input) => input,
			None => panic!("Bad input data provided to {}", stringify!($fn_name)),
		};

		let output = $runtime::$fn_name($( $arg_name ),*);
		$crate::codec::Encode::encode(&output)
	};
}

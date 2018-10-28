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

//! Transaction pool errors.

use sr_primitives::transaction_validity::TransactionPriority as Priority;

error_chain! {
	errors {
		/// Transaction is not verifiable yet, but might be in the future.
		UnknownTransactionValidity {
			description("Runtime cannot determine validity of the transaction yet."),
			display("Unkown Transaction Validity"),
		}
		/// Transaction is invalid
		InvalidTransaction {
			description("Runtime check for the transaction failed."),
			display("Invalid Transaction"),
		}
		/// The transaction is temporarily baned
		TemporarilyBanned {
			description("Transaction is temporarily banned from importing to the pool."),
			display("Temporarily Banned"),
		}
		/// The transaction is already in the pool.
		AlreadyImported {
			description("Transaction is already in the pool."),
			display("Already imported"),
		}
		/// The transaction cannot be imported cause it's a replacement and has too low priority.
		TooLowPriority(old: Priority, new: Priority) {
			description("The priority is too low to replace transactions already in the pool."),
			display("Too low priority ({} > {})", old, new)
		}
		/// Deps cycle detected and we couldn't import transaction.
		CycleDetected {
			description("Transaction was not imported because of detected cycle."),
			display("Cycle Detected"),
		}
	}
}

/// Transaction pool error conversion.
pub trait IntoPoolError: ::std::error::Error + Send + Sized {
	/// Try to extract original `Error`
	///
	/// This implementation is optional and used only to
	/// provide more descriptive error messages for end users
	/// of RPC API.
	fn into_pool_error(self) -> ::std::result::Result<Error, Self> { Err(self) }
}

impl IntoPoolError for Error {
	fn into_pool_error(self) -> ::std::result::Result<Error, Self> { Ok(self) }
}

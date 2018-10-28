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

/// Unwraps the trailing parameter or falls back with the closure result.
pub fn unwrap_or_else<F, H, E>(or_else: F, optional: ::jsonrpc_macros::Trailing<H>) -> Result<H, E> where
	F: FnOnce() -> Result<H, E>,
{
	match optional.into() {
		None => or_else(),
		Some(x) => Ok(x),
	}
}

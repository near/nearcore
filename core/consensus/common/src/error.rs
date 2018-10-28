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

//! Error types in Consensus
use runtime_version::RuntimeVersion;

error_chain! {
	errors {
		/// Missing state at block with given descriptor.
		StateUnavailable(b: String) {
			description("State missing at given block."),
			display("State unavailable at block {}", b),
		}

		/// I/O terminated unexpectedly
		IoTerminated {
			description("I/O terminated unexpectedly."),
			display("I/O terminated unexpectedly."),
		}

		/// Unable to schedule wakeup.
		FaultyTimer(e: ::tokio::timer::Error) {
			description("Timer error"),
			display("Timer error: {}", e),
		}

		/// Unable to propose a block.
		CannotPropose {
			description("Unable to create block proposal."),
			display("Unable to create block proposal."),
		}

		/// Error checking signature
		InvalidSignature(s: ::primitives::ed25519::Signature, a: ::primitives::AuthorityId) {
			description("Message signature is invalid"),
			display("Message signature {:?} by {:?} is invalid.", s, a),
		}

		/// Account is not an authority.
		InvalidAuthority(a: ::primitives::AuthorityId) {
			description("Message sender is not a valid authority"),
			display("Message sender {:?} is not a valid authority.", a),
		}

		/// Authoring interface does not match the runtime.
		IncompatibleAuthoringRuntime(native: RuntimeVersion, on_chain: RuntimeVersion) {
			description("Authoring for current runtime is not supported"),
			display("Authoring for current runtime is not supported. Native ({}) cannot author for on-chain ({}).", native, on_chain),
		}

		/// Authoring interface does not match the runtime.
		RuntimeVersionMissing {
			description("Current runtime has no version"),
			display("Authoring for current runtime is not supported since it has no version."),
		}

		/// Authoring interface does not match the runtime.
		NativeRuntimeMissing {
			description("This build has no native runtime"),
			display("Authoring in current build is not supported since it has no runtime."),
		}

		/// Justification requirements not met.
		InvalidJustification {
			description("Invalid justification"),
			display("Invalid justification."),
		}

		/// Some other error.
		Other(e: Box<::std::error::Error + Send>) {
			description("Other error")
			display("Other error: {}", e.description())
		}
	}
}

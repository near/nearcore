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

#![warn(unused_extern_crates)]
#![warn(missing_docs)]

// tag::description[]
//! Substrate-specific P2P networking: synchronizing blocks, propagating BFT messages.
//! Allows attachment of an optional subprotocol for chain-specific requests.
// end::description[]

extern crate linked_hash_map;
extern crate parking_lot;
extern crate substrate_primitives as primitives;
extern crate substrate_client as client;
extern crate sr_primitives as runtime_primitives;
extern crate substrate_network_libp2p as network_libp2p;
extern crate substrate_consensus_common as consensus;
extern crate parity_codec as codec;
extern crate futures;
extern crate rustc_hex;
extern crate rand;
extern crate tokio;
#[macro_use] extern crate log;
#[macro_use] extern crate bitflags;
#[macro_use] extern crate error_chain;
#[macro_use] extern crate parity_codec_derive;

#[cfg(test)]
extern crate env_logger;

#[cfg(any(test, feature = "test-helpers"))]
extern crate substrate_keyring as keyring;

#[cfg(any(test, feature = "test-helpers"))]
extern crate substrate_test_client as test_client;

mod service;
mod sync;
#[macro_use]
mod protocol;
mod io;
mod config;
mod chain;
mod blocks;
mod on_demand;
pub mod import_queue;
pub mod consensus_gossip;
pub mod error;
pub mod message;
pub mod specialization;

#[cfg(any(test, feature = "test-helpers"))]
pub mod test;

pub use chain::Client as ClientHandle;
pub use service::{Service, FetchFuture, TransactionPool, Params, ManageNetwork, SyncProvider};
pub use protocol::{ProtocolStatus, PeerInfo, Context};
pub use sync::{Status as SyncStatus, SyncState};
pub use network_libp2p::{NonReservedPeerMode, NetworkConfiguration, NodeIndex, ProtocolId, Severity, Protocol};
pub use message::{generic as generic_message, RequestId, Status as StatusMessage};
pub use error::Error;
pub use config::{Roles, ProtocolConfig};
pub use on_demand::{OnDemand, OnDemandService, RemoteResponse};
#[doc(hidden)]
pub use runtime_primitives::traits::Block as BlockT;

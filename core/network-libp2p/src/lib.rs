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
//! TODO: Missing doc
// end::description[]

#![recursion_limit="128"]
#![type_length_limit = "268435456"]

extern crate parking_lot;
extern crate fnv;
extern crate futures;
extern crate tokio;
extern crate tokio_io;
extern crate tokio_timer;
extern crate libc;
#[macro_use]
extern crate libp2p;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate bytes;
extern crate unsigned_varint;

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[cfg(test)] #[macro_use]
extern crate assert_matches;

mod custom_proto;
mod error;
mod node_handler;
mod secret;
mod service_task;
mod swarm;
mod topology;
mod traits;
mod transport;

pub use custom_proto::RegisteredProtocol;
pub use error::{Error, ErrorKind, DisconnectReason};
pub use libp2p::{Multiaddr, multiaddr::Protocol, PeerId};
pub use service_task::{start_service, Service, ServiceEvent};
pub use traits::*;	// TODO: expand to actual items

/// Check if node url is valid
pub fn validate_node_url(url: &str) -> Result<(), Error> {
	match url.parse::<Multiaddr>() {
		Ok(_) => Ok(()),
		Err(_) => Err(ErrorKind::InvalidNodeId.into()),
	}
}

/// Parses a string address and returns the component, if valid.
pub fn parse_str_addr(addr_str: &str) -> Result<(PeerId, Multiaddr), Error> {
	let mut addr: Multiaddr = addr_str.parse().map_err(|_| ErrorKind::AddressParse)?;
	let who = match addr.pop() {
		Some(Protocol::P2p(key)) =>
			PeerId::from_multihash(key).map_err(|_| ErrorKind::AddressParse)?,
		_ => return Err(ErrorKind::AddressParse.into()),
	};
	Ok((who, addr))
}

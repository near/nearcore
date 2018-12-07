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

use parking_lot::Mutex;
use std::sync::Arc;
use substrate_network_libp2p::{NodeIndex, PeerId, ProtocolId, Service, Severity};

/// Wraps the network service. IO interface for the syncing handler.
/// Provides peer connection management and an interface to the BlockChain client.
pub struct NetSyncIo {
    network: Arc<Mutex<Service>>,
    protocol: ProtocolId,
}

impl NetSyncIo {
    /// Creates a new instance.
    pub fn new(network: Arc<Mutex<Service>>, protocol: ProtocolId) -> NetSyncIo {
        NetSyncIo { network, protocol }
    }

    /// Report a peer for misbehaviour.
    pub fn report_peer(&mut self, who: NodeIndex, reason: Severity) {
        info!("Purposefully dropping {} ; reason: {:?}", who, reason);
        match reason {
            Severity::Bad(_) => self.network.lock().ban_node(who),
            Severity::Useless(_) => self.network.lock().drop_node(who),
            Severity::Timeout => self.network.lock().drop_node(who),
        }
    }

    /// Send a packet to a peer.
    pub fn send(&mut self, who: NodeIndex, data: Vec<u8>) {
        self.network.lock().send_custom_message(who, self.protocol, data)
    }

    /// Returns information on p2p session
    pub fn peer_id(&self, who: NodeIndex) -> Option<PeerId> {
        self.network.lock().peer_id_of_node(who).cloned()
    }

    /// Returns peer identifier string
    pub fn peer_debug_info(&self, who: NodeIndex) -> String {
        let net = self.network.lock();
        if let (Some(peer_id), Some(addr)) = (net.peer_id_of_node(who), net.node_endpoint(who)) {
            format!("{:?} through {:?}", peer_id, addr)
        } else {
            "unknown".to_string()
        }
    }
}

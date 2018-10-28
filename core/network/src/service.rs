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

use std::collections::HashMap;
use std::sync::Arc;
use std::{io, thread};
use std::time::Duration;
use futures::{self, Future, Stream, stream, sync::oneshot};
use parking_lot::Mutex;
use network_libp2p::{ProtocolId, PeerId, NetworkConfiguration, ErrorKind};
use network_libp2p::{start_service, Service as NetworkService, ServiceEvent as NetworkServiceEvent};
use network_libp2p::{RegisteredProtocol, parse_str_addr, Protocol as Libp2pProtocol};
use io::NetSyncIo;
use protocol::{self, Protocol, ProtocolContext, Context, ProtocolStatus};
use config::{ProtocolConfig};
use error::Error;
use chain::Client;
use specialization::Specialization;
use on_demand::OnDemandService;
use import_queue::ImportQueue;
use runtime_primitives::traits::{Block as BlockT};
use tokio::{runtime::Runtime, timer::Interval};

/// Type that represents fetch completion future.
pub type FetchFuture = oneshot::Receiver<Vec<u8>>;

const TICK_TIMEOUT: Duration = Duration::from_millis(1000);
const PROPAGATE_TIMEOUT: Duration = Duration::from_millis(5000);

bitflags! {
	/// Node roles bitmask.
	pub struct Roles: u8 {
		/// No network.
		const NONE = 0b00000000;
		/// Full node, does not participate in consensus.
		const FULL = 0b00000001;
		/// Light client node.
		const LIGHT = 0b00000010;
		/// Act as an authority
		const AUTHORITY = 0b00000100;
	}
}

impl ::codec::Encode for Roles {
	fn encode_to<T: ::codec::Output>(&self, dest: &mut T) {
		dest.push_byte(self.bits())
	}
}

impl ::codec::Decode for Roles {
	fn decode<I: ::codec::Input>(input: &mut I) -> Option<Self> {
		Self::from_bits(input.read_byte()?)
	}
}

/// Sync status
pub trait SyncProvider<B: BlockT>: Send + Sync {
	/// Get sync status
	fn status(&self) -> ProtocolStatus<B>;
	/// Get this node id if available.
	fn node_id(&self) -> Option<String>;
}

pub trait ExHashT: ::std::hash::Hash + Eq + ::std::fmt::Debug + Clone + Send + Sync + 'static {}
impl<T> ExHashT for T where T: ::std::hash::Hash + Eq + ::std::fmt::Debug + Clone + Send + Sync + 'static {}

/// Transaction pool interface
pub trait TransactionPool<H: ExHashT, B: BlockT>: Send + Sync {
	/// Get transactions from the pool that are ready to be propagated.
	fn transactions(&self) -> Vec<(H, B::Extrinsic)>;
	/// Import a transaction into the pool.
	fn import(&self, transaction: &B::Extrinsic) -> Option<H>;
	/// Notify the pool about transactions broadcast.
	fn on_broadcasted(&self, propagations: HashMap<H, Vec<String>>);
}

/// Service able to execute closure in the network context.
pub trait ExecuteInContext<B: BlockT>: Send + Sync {
	/// Execute closure in network context.
	fn execute_in_context<F: Fn(&mut Context<B>)>(&self, closure: F);
}

/// Service initialization parameters.
pub struct Params<B: BlockT, S, H: ExHashT> {
	/// Configuration.
	pub config: ProtocolConfig,
	/// Network layer configuration.
	pub network_config: NetworkConfiguration,
	/// Substrate relay chain access point.
	pub chain: Arc<Client<B>>,
	/// On-demand service reference.
	pub on_demand: Option<Arc<OnDemandService<B>>>,
	/// Transaction pool.
	pub transaction_pool: Arc<TransactionPool<H, B>>,
	/// Protocol specialization.
	pub specialization: S,
}

/// Substrate network service. Handles network IO and manages connectivity.
pub struct Service<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> {
	/// Network service
	network: Arc<Mutex<NetworkService>>,
	/// Protocol handler
	handler: Arc<Protocol<B, S, H>>,
	/// Protocol ID.
	protocol_id: ProtocolId,
	/// Sender for messages to the backgound service task, and handle for the background thread.
	/// Dropping the sender should close the task and the thread.
	/// This is an `Option` because we need to extract it in the destructor.
	bg_thread: Option<(oneshot::Sender<()>, thread::JoinHandle<()>)>,
}

impl<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> Service<B, S, H> {
	/// Creates and register protocol with the network service
	pub fn new<I: 'static + ImportQueue<B>>(
		params: Params<B, S, H>,
		protocol_id: ProtocolId,
		import_queue: I,
	) -> Result<Arc<Service<B, S, H>>, Error> {
		let chain = params.chain.clone();
		let import_queue = Arc::new(import_queue);
		let handler = Arc::new(Protocol::new(
			params.config,
			params.chain,
			import_queue.clone(),
			params.on_demand,
			params.transaction_pool,
			params.specialization,
		)?);
		let versions = [(protocol::CURRENT_VERSION as u8)];
		let registered = RegisteredProtocol::new(protocol_id, &versions[..]);
		let (thread, network) = start_thread(params.network_config, handler.clone(), registered)?;

		let sync = Arc::new(Service {
			network,
			protocol_id,
			handler,
			bg_thread: Some(thread),
		});

		import_queue.start(
			Arc::downgrade(sync.handler.sync()),
			Arc::downgrade(&sync),
			Arc::downgrade(&chain)
		)?;

		Ok(sync)
	}

	/// Called when a new block is imported by the client.
	pub fn on_block_imported(&self, hash: B::Hash, header: &B::Header) {
		self.handler.on_block_imported(&mut NetSyncIo::new(&self.network, self.protocol_id), hash, header)
	}

	/// Called when new transactons are imported by the client.
	pub fn trigger_repropagate(&self) {
		self.handler.propagate_extrinsics(&mut NetSyncIo::new(&self.network, self.protocol_id));
	}

	/// Execute a closure with the chain-specific network specialization.
	pub fn with_spec<F, U>(&self, f: F) -> U
		where F: FnOnce(&mut S, &mut Context<B>) -> U
	{
		self.handler.with_spec(&mut NetSyncIo::new(&self.network, self.protocol_id), f)
	}
}

impl<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> ::consensus::SyncOracle for Service<B, S, H> {
	fn is_major_syncing(&self) -> bool {
		self.handler.sync().read().status().is_major_syncing()
	}
}

impl<B: BlockT + 'static, S: Specialization<B>, H:ExHashT> Drop for Service<B, S, H> {
	fn drop(&mut self) {
		self.handler.stop();
		if let Some((sender, join)) = self.bg_thread.take() {
			let _ = sender.send(());
			if let Err(e) = join.join() {
				error!("Error while waiting on background thread: {:?}", e);
			}
		}
	}
}

impl<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> ExecuteInContext<B> for Service<B, S, H> {
	fn execute_in_context<F: Fn(&mut ::protocol::Context<B>)>(&self, closure: F) {
		closure(&mut ProtocolContext::new(self.handler.context_data(), &mut NetSyncIo::new(&self.network, self.protocol_id)))
	}
}

impl<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> SyncProvider<B> for Service<B, S, H> {
	/// Get sync status
	fn status(&self) -> ProtocolStatus<B> {
		self.handler.status()
	}

	fn node_id(&self) -> Option<String> {
		let network = self.network.lock();
		let ret = network
			.listeners()
			.next()
			.map(|addr| {
				let mut addr = addr.clone();
				addr.append(Libp2pProtocol::P2p(network.peer_id().clone().into()));
				addr.to_string()
			});
		ret
	}
}

/// Trait for managing network
pub trait ManageNetwork: Send + Sync {
	/// Set to allow unreserved peers to connect
	fn accept_unreserved_peers(&self);
	/// Set to deny unreserved peers to connect
	fn deny_unreserved_peers(&self);
	/// Remove reservation for the peer
	fn remove_reserved_peer(&self, peer: PeerId);
	/// Add reserved peer
	fn add_reserved_peer(&self, peer: String) -> Result<(), String>;
}

impl<B: BlockT + 'static, S: Specialization<B>, H: ExHashT> ManageNetwork for Service<B, S, H> {
	fn accept_unreserved_peers(&self) {
		self.network.lock().accept_unreserved_peers();
	}

	fn deny_unreserved_peers(&self) {
		// This method can disconnect nodes, in which case we have to properly close them in the
		// protocol.
		let disconnected = self.network.lock().deny_unreserved_peers();
		let mut net_sync = NetSyncIo::new(&self.network, self.protocol_id);
		for node_index in disconnected {
			self.handler.on_peer_disconnected(&mut net_sync, node_index)
		}
	}

	fn remove_reserved_peer(&self, peer: PeerId) {
		// This method can disconnect a node, in which case we have to properly close it in the
		// protocol.
		let disconnected = self.network.lock().remove_reserved_peer(peer);
		if let Some(node_index) = disconnected {
			let mut net_sync = NetSyncIo::new(&self.network, self.protocol_id);
			self.handler.on_peer_disconnected(&mut net_sync, node_index)
		}
	}

	fn add_reserved_peer(&self, peer: String) -> Result<(), String> {
		let (addr, peer_id) = parse_str_addr(&peer).map_err(|e| format!("{:?}", e))?;
		self.network.lock().add_reserved_peer(addr, peer_id);
		Ok(())
	}
}

/// Starts the background thread that handles the networking.
fn start_thread<B: BlockT + 'static, S: Specialization<B>, H: ExHashT>(
	config: NetworkConfiguration,
	protocol: Arc<Protocol<B, S, H>>,
	registered: RegisteredProtocol,
) -> Result<((oneshot::Sender<()>, thread::JoinHandle<()>), Arc<Mutex<NetworkService>>), Error> {
	let protocol_id = registered.id();

	// Start the main service.
	let service = match start_service(config, Some(registered)) {
		Ok(service) => Arc::new(Mutex::new(service)),
		Err(err) => {
			match err.kind() {
				ErrorKind::Io(ref e) if e.kind() == io::ErrorKind::AddrInUse =>
					warn!("Network port is already in use, make sure that another instance of Substrate client is not running or change the port using the --port option."),
				_ => warn!("Error starting network: {}", err),
			};
			return Err(err.into())
		},
	};

	let (close_tx, close_rx) = oneshot::channel();
	let service_clone = service.clone();
	let mut runtime = Runtime::new()?;
	let thread = thread::Builder::new().name("network".to_string()).spawn(move || {
		let fut = run_thread(service_clone, protocol, protocol_id)
			.select(close_rx.then(|_| Ok(())))
			.map(|(val, _)| val)
			.map_err(|(err,_ )| err);

		// Note that we use `block_on` and not `block_on_all` because we want to kill the thread
		// instantly if `close_rx` receives something.
		match runtime.block_on(fut) {
			Ok(()) => debug!(target: "sub-libp2p", "Networking thread finished"),
			Err(err) => error!(target: "sub-libp2p", "Error while running libp2p: {:?}", err),
		};
	})?;

	Ok(((close_tx, thread), service))
}

/// Runs the background thread that handles the networking.
fn run_thread<B: BlockT + 'static, S: Specialization<B>, H: ExHashT>(
	network_service: Arc<Mutex<NetworkService>>,
	protocol: Arc<Protocol<B, S, H>>,
	protocol_id: ProtocolId,
) -> impl Future<Item = (), Error = io::Error> {
	// Interval for performing maintenance on the protocol handler.
	let tick = Interval::new_interval(TICK_TIMEOUT)
		.for_each({
			let protocol = protocol.clone();
			let network_service = network_service.clone();
			move |_| {
				protocol.tick(&mut NetSyncIo::new(&network_service, protocol_id));
				Ok(())
			}
		})
		.then(|res| {
			match res {
				Ok(()) => (),
				Err(err) => error!("Error in the propagation timer: {:?}", err),
			};
			Ok(())
		});

	// Interval at which we gossip extrinsics over the network.
	let propagate = Interval::new_interval(PROPAGATE_TIMEOUT)
		.for_each({
			let protocol = protocol.clone();
			let network_service = network_service.clone();
			move |_| {
				protocol.propagate_extrinsics(&mut NetSyncIo::new(&network_service, protocol_id));
				Ok(())
			}
		})
		.then(|res| {
			match res {
				Ok(()) => (),
				Err(err) => error!("Error in the propagation timer: {:?}", err),
			};
			Ok(())
		});

	// The network service produces events about what happens on the network. Let's process them.
	let network_service2 = network_service.clone();
	let network = stream::poll_fn(move || network_service2.lock().poll()).for_each(move |event| {
		let mut net_sync = NetSyncIo::new(&network_service, protocol_id);

		match event {
			NetworkServiceEvent::NodeClosed { node_index, closed_custom_protocols } => {
				if !closed_custom_protocols.is_empty() {
					debug_assert_eq!(closed_custom_protocols, &[protocol_id]);
					protocol.on_peer_disconnected(&mut net_sync, node_index);
				}
			}
			NetworkServiceEvent::ClosedCustomProtocols { node_index, protocols } => {
				if !protocols.is_empty() {
					debug_assert_eq!(protocols, &[protocol_id]);
					protocol.on_peer_disconnected(&mut net_sync, node_index);
				}
			}
			NetworkServiceEvent::OpenedCustomProtocol { node_index, version, .. } => {
				debug_assert_eq!(version, protocol::CURRENT_VERSION as u8);
				protocol.on_peer_connected(&mut net_sync, node_index);
			}
			NetworkServiceEvent::ClosedCustomProtocol { node_index, .. } => {
				protocol.on_peer_disconnected(&mut net_sync, node_index);
			}
			NetworkServiceEvent::CustomMessage { node_index, data, .. } => {
				protocol.handle_packet(&mut net_sync, node_index, &data);
			}
		};

		Ok(())
	});

	// Merge all futures into one.
	let futures: Vec<Box<Future<Item = (), Error = io::Error> + Send>> = vec![
		Box::new(tick) as Box<_>,
		Box::new(propagate) as Box<_>,
		Box::new(network) as Box<_>
	];

	futures::select_all(futures)
		.and_then(move |_| {
			debug!("Networking ended");
			Ok(())
		})
		.map_err(|(r, _, _)| r)
}

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

//! Substrate service components.

use std::sync::Arc;
use std::marker::PhantomData;
use std::ops::Deref;
use serde::{Serialize, de::DeserializeOwned};
use tokio::runtime::TaskExecutor;
use chain_spec::ChainSpec;
use client_db;
use client::{self, Client};
use {error, Service};
use network::{self, OnDemand, import_queue::ImportQueue};
use substrate_executor::{NativeExecutor, NativeExecutionDispatch};
use transaction_pool::txpool::{self, Options as TransactionPoolOptions, Pool as TransactionPool};
use runtime_primitives::{traits::Block as BlockT, traits::Header as HeaderT, BuildStorage};
use config::Configuration;
use primitives::{Blake2Hasher};

// Type aliases.
// These exist mainly to avoid typing `<F as Factory>::Foo` all over the code.
/// Network service type for a factory.
pub type NetworkService<F> = network::Service<
	<F as ServiceFactory>::Block,
	<F as ServiceFactory>::NetworkProtocol,
	<<F as ServiceFactory>::Block as BlockT>::Hash,
>;

/// Code executor type for a factory.
pub type CodeExecutor<F> = NativeExecutor<<F as ServiceFactory>::RuntimeDispatch>;

/// Full client backend type for a factory.
pub type FullBackend<F> = client_db::Backend<<F as ServiceFactory>::Block>;

/// Full client executor type for a factory.
pub type FullExecutor<F> = client::LocalCallExecutor<
	client_db::Backend<<F as ServiceFactory>::Block>,
	CodeExecutor<F>,
>;

/// Light client backend type for a factory.
pub type LightBackend<F> = client::light::backend::Backend<
	client_db::light::LightStorage<<F as ServiceFactory>::Block>,
	network::OnDemand<<F as ServiceFactory>::Block, NetworkService<F>>,
>;

/// Light client executor type for a factory.
pub type LightExecutor<F> = client::light::call_executor::RemoteCallExecutor<
	client::light::blockchain::Blockchain<
		client_db::light::LightStorage<<F as ServiceFactory>::Block>,
		network::OnDemand<<F as ServiceFactory>::Block, NetworkService<F>>
	>,
	network::OnDemand<<F as ServiceFactory>::Block, NetworkService<F>>,
	Blake2Hasher,
>;

/// Full client type for a factory.
pub type FullClient<F> = Client<FullBackend<F>, FullExecutor<F>, <F as ServiceFactory>::Block>;

/// Light client type for a factory.
pub type LightClient<F> = Client<LightBackend<F>, LightExecutor<F>, <F as ServiceFactory>::Block>;

/// `ChainSpec` specialization for a factory.
pub type FactoryChainSpec<F> = ChainSpec<<F as ServiceFactory>::Genesis>;

/// `Genesis` specialization for a factory.
pub type FactoryGenesis<F> = <F as ServiceFactory>::Genesis;

/// `Block` type for a factory.
pub type FactoryBlock<F> = <F as ServiceFactory>::Block;

/// `Extrinsic` type for a factory.
pub type FactoryExtrinsic<F> = <<F as ServiceFactory>::Block as BlockT>::Extrinsic;

/// `Number` type for a factory.
pub type FactoryBlockNumber<F> = <<FactoryBlock<F> as BlockT>::Header as HeaderT>::Number;

/// Full `Configuration` type for a factory.
pub type FactoryFullConfiguration<F> = Configuration<<F as ServiceFactory>::Configuration, FactoryGenesis<F>>;

/// Client type for `Components`.
pub type ComponentClient<C> = Client<
	<C as Components>::Backend,
	<C as Components>::Executor,
	FactoryBlock<<C as Components>::Factory>
>;

/// Block type for `Components`
pub type ComponentBlock<C> = <<C as Components>::Factory as ServiceFactory>::Block;

/// Extrinsic hash type for `Components`
pub type ComponentExHash<C> = <<C as Components>::TransactionPoolApi as txpool::ChainApi>::Hash;

/// Extrinsic type.
pub type ComponentExtrinsic<C> = <ComponentBlock<C> as BlockT>::Extrinsic;

/// Extrinsic pool API type for `Components`.
pub type PoolApi<C> = <C as Components>::TransactionPoolApi;

/// A set of traits for the runtime genesis config.
pub trait RuntimeGenesis: Serialize + DeserializeOwned + BuildStorage {}
impl<T: Serialize + DeserializeOwned + BuildStorage> RuntimeGenesis for T {}

/// A collection of types and methods to build a service on top of the substrate service.
pub trait ServiceFactory: 'static + Sized {
	/// Block type.
	type Block: BlockT;
	/// Network protocol extensions.
	type NetworkProtocol: network::specialization::Specialization<Self::Block>;
	/// Chain runtime.
	type RuntimeDispatch: NativeExecutionDispatch + Send + Sync + 'static;
	/// Extrinsic pool backend type for the full client.
	type FullTransactionPoolApi: txpool::ChainApi<Hash = <Self::Block as BlockT>::Hash, Block = Self::Block> + Send + 'static;
	/// Extrinsic pool backend type for the light client.
	type LightTransactionPoolApi: txpool::ChainApi<Hash = <Self::Block as BlockT>::Hash, Block = Self::Block> + 'static;
	/// Genesis configuration for the runtime.
	type Genesis: RuntimeGenesis;
	/// Other configuration for service members.
	type Configuration: Default;
	/// Extended full service type.
	type FullService: Deref<Target = Service<FullComponents<Self>>> + Send + Sync + 'static;
	/// Extended light service type.
	type LightService: Deref<Target = Service<LightComponents<Self>>> + Send + Sync + 'static;
	/// ImportQueue for full client
	type FullImportQueue: network::import_queue::ImportQueue<Self::Block> + 'static;
	/// ImportQueue for light clients
	type LightImportQueue: network::import_queue::ImportQueue<Self::Block> + 'static;

	//TODO: replace these with a constructor trait. that TransactionPool implements.
	/// Extrinsic pool constructor for the full client.
	fn build_full_transaction_pool(config: TransactionPoolOptions, client: Arc<FullClient<Self>>)
		-> Result<TransactionPool<Self::FullTransactionPoolApi>, error::Error>;
	/// Extrinsic pool constructor for the light client.
	fn build_light_transaction_pool(config: TransactionPoolOptions, client: Arc<LightClient<Self>>)
		-> Result<TransactionPool<Self::LightTransactionPoolApi>, error::Error>;

	/// Build network protocol.
	fn build_network_protocol(config: &FactoryFullConfiguration<Self>)
		-> Result<Self::NetworkProtocol, error::Error>;

	/// Build full service.
	fn new_full(config: FactoryFullConfiguration<Self>, executor: TaskExecutor)
		-> Result<Self::FullService, error::Error>;
	/// Build light service.
	fn new_light(config: FactoryFullConfiguration<Self>, executor: TaskExecutor)
		-> Result<Self::LightService, error::Error>;

	/// ImportQueue for a full client
	fn build_full_import_queue(
		config: &FactoryFullConfiguration<Self>,
		_client: Arc<FullClient<Self>>
	) -> Result<Self::FullImportQueue, error::Error> {
		if let Some(name) = config.chain_spec.consensus_engine() {
			match name {
				_ => Err(format!("Chain Specification defines unknown consensus engine '{}'", name).into())
			}

		} else {
			Err("Chain Specification doesn't contain any consensus_engine name".into())
		}
	}

	/// ImportQueue for a light client
	fn build_light_import_queue(
		config: &FactoryFullConfiguration<Self>,
		_client: Arc<LightClient<Self>>
	) -> Result<Self::LightImportQueue, error::Error> {
		if let Some(name) = config.chain_spec.consensus_engine() {
			match name {
				_ => Err(format!("Chain Specification defines unknown consensus engine '{}'", name).into())
			}

		} else {
			Err("Chain Specification doesn't contain any consensus_engine name".into())
		}
	}
}

/// A collection of types and function to generalise over full / light client type.
pub trait Components: 'static {
	/// Associated service factory.
	type Factory: ServiceFactory;
	/// Client backend.
	type Backend: 'static + client::backend::Backend<FactoryBlock<Self::Factory>, Blake2Hasher>;
	/// Client executor.
	type Executor: 'static + client::CallExecutor<FactoryBlock<Self::Factory>, Blake2Hasher> + Send + Sync + Clone;
	/// Extrinsic pool type.
	type TransactionPoolApi: 'static + txpool::ChainApi<
		Hash = <<Self::Factory as ServiceFactory>::Block as BlockT>::Hash,
		Block = FactoryBlock<Self::Factory>
	>;

	/// Our Import Queue
	type ImportQueue: ImportQueue<FactoryBlock<Self::Factory>> + 'static;

	/// Create client.
	fn build_client(
		config: &FactoryFullConfiguration<Self::Factory>,
		executor: CodeExecutor<Self::Factory>,
	)
		-> Result<(
			Arc<ComponentClient<Self>>,
			Option<Arc<OnDemand<FactoryBlock<Self::Factory>, NetworkService<Self::Factory>>>>
		), error::Error>;

	/// Create extrinsic pool.
	fn build_transaction_pool(config: TransactionPoolOptions, client: Arc<ComponentClient<Self>>)
		-> Result<TransactionPool<Self::TransactionPoolApi>, error::Error>;

	/// instance of import queue for clients
	fn build_import_queue(
		config: &FactoryFullConfiguration<Self::Factory>,
		client: Arc<ComponentClient<Self>>
	) -> Result<Self::ImportQueue, error::Error>;
}

/// A struct that implement `Components` for the full client.
pub struct FullComponents<Factory: ServiceFactory> {
	_factory: PhantomData<Factory>,
}

impl<Factory: ServiceFactory> Components for FullComponents<Factory> {
	type Factory = Factory;
	type Executor = FullExecutor<Factory>;
	type Backend = FullBackend<Factory>;
	type TransactionPoolApi = <Factory as ServiceFactory>::FullTransactionPoolApi;
	type ImportQueue = Factory::FullImportQueue;

	fn build_client(
		config: &FactoryFullConfiguration<Factory>,
		executor: CodeExecutor<Self::Factory>,
	)
		-> Result<(
			Arc<ComponentClient<Self>>,
			Option<Arc<OnDemand<FactoryBlock<Self::Factory>, NetworkService<Self::Factory>>>>
		), error::Error>
	{
		let db_settings = client_db::DatabaseSettings {
			cache_size: None,
			path: config.database_path.as_str().into(),
			pruning: config.pruning.clone(),
		};
		Ok((Arc::new(client_db::new_client(
			db_settings,
			executor,
			&config.chain_spec,
			config.block_execution_strategy,
			config.api_execution_strategy,
		)?), None))
	}

	fn build_transaction_pool(config: TransactionPoolOptions, client: Arc<ComponentClient<Self>>)
		-> Result<TransactionPool<Self::TransactionPoolApi>, error::Error>
	{
		Factory::build_full_transaction_pool(config, client)
	}

	fn build_import_queue(
		config: &FactoryFullConfiguration<Self::Factory>,
		client: Arc<ComponentClient<Self>>
	) -> Result<Self::ImportQueue, error::Error> {
		Factory::build_full_import_queue(config, client)
	}
}

/// A struct that implement `Components` for the light client.
pub struct LightComponents<Factory: ServiceFactory> {
	_factory: PhantomData<Factory>,
}

impl<Factory: ServiceFactory> Components for LightComponents<Factory> {
	type Factory = Factory;
	type Executor = LightExecutor<Factory>;
	type Backend = LightBackend<Factory>;
	type TransactionPoolApi = <Factory as ServiceFactory>::LightTransactionPoolApi;
	type ImportQueue = <Factory as ServiceFactory>::LightImportQueue;

	fn build_client(
		config: &FactoryFullConfiguration<Factory>,
		executor: CodeExecutor<Self::Factory>,
	)
		-> Result<(
			Arc<ComponentClient<Self>>,
			Option<Arc<OnDemand<FactoryBlock<Self::Factory>,
			NetworkService<Self::Factory>>>>
		), error::Error>
	{
		let db_settings = client_db::DatabaseSettings {
			cache_size: None,
			path: config.database_path.as_str().into(),
			pruning: config.pruning.clone(),
		};
		let db_storage = client_db::light::LightStorage::new(db_settings)?;
		let light_blockchain = client::light::new_light_blockchain(db_storage);
		let fetch_checker = Arc::new(client::light::new_fetch_checker::<_, Blake2Hasher>(executor));
		let fetcher = Arc::new(network::OnDemand::new(fetch_checker));
		let client_backend = client::light::new_light_backend(light_blockchain, fetcher.clone());
		let client = client::light::new_light(client_backend, fetcher.clone(), &config.chain_spec)?;
		Ok((Arc::new(client), Some(fetcher)))
	}

	fn build_transaction_pool(config: TransactionPoolOptions, client: Arc<ComponentClient<Self>>)
		-> Result<TransactionPool<Self::TransactionPoolApi>, error::Error>
	{
		Factory::build_light_transaction_pool(config, client)
	}

	fn build_import_queue(
		config: &FactoryFullConfiguration<Self::Factory>,
		client: Arc<ComponentClient<Self>>
	) -> Result<Self::ImportQueue, error::Error> {
		Factory::build_light_import_queue(config, client)
	}
}

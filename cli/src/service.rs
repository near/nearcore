//! Service and ServiceFactory implementation. Specialized wrapper over Substrate service.

#![warn(unused_extern_crates)]

use std::sync::Arc;
use transaction_pool::{self, txpool::{Pool as TransactionPool}};
use neard_runtime::{self, GenesisConfig, opaque::Block};
use substrate_service::{
	FactoryFullConfiguration, LightComponents, FullComponents, FullBackend,
	FullClient, LightClient, LightBackend, FullExecutor, LightExecutor,
	Roles, TaskExecutor,
};
use consensus::{import_queue, start_aura, Config as AuraConfig, AuraImportQueue};

pub use substrate_executor::NativeExecutor;
// Our native executor instance.
native_executor_instance!(
	pub Executor,
	neard_runtime::api::dispatch,
	neard_runtime::native_version,
	include_bytes!("../../runtime/wasm/target/wasm32-unknown-unknown/release/neard_runtime.compact.wasm")
);

const AURA_SLOT_DURATION: u64 = 6;

construct_simple_protocol! {
	/// Demo protocol attachment for substrate.
	pub struct NodeProtocol where Block = Block { }
}

construct_simple_service!(Service);

construct_service_factory! {
	struct Factory {
		Block = Block,
		NetworkProtocol = NodeProtocol { |config| Ok(NodeProtocol::new()) },
		RuntimeDispatch = Executor,
		FullTransactionPoolApi = transaction_pool::ChainApi<FullBackend<Self>, FullExecutor<Self>, Block>
			{ |config, client| Ok(TransactionPool::new(config, transaction_pool::ChainApi::new(client))) },
		LightTransactionPoolApi = transaction_pool::ChainApi<LightBackend<Self>, LightExecutor<Self>, Block>
			{ |config, client| Ok(TransactionPool::new(config, transaction_pool::ChainApi::new(client))) },
		Genesis = GenesisConfig,
		Configuration = (),
		FullService = Service<FullComponents<Self>>
			{ |config: FactoryFullConfiguration<Self>, executor: TaskExecutor| {
				let is_auth = config.roles == Roles::AUTHORITY;
				Service::<FullComponents<Factory>>::new(config, executor.clone()).map(move |service|{
					if is_auth {
						if let Ok(Some(Ok(key))) = service.keystore().contents()
							.map(|keys| keys.get(0).map(|k| service.keystore().load(k, "")))
						{
							info!("Using authority key {}", key.public());
							let task = start_aura(
								AuraConfig {
									local_key:  Some(Arc::new(key)),
									slot_duration: AURA_SLOT_DURATION,
								},
								service.client(),
								service.proposer(),
								service.network(),
							);

							executor.spawn(task);
						}
					}

					service
				})
			}
		},
		LightService = Service<LightComponents<Self>>
			{ |config, executor| Service::<LightComponents<Factory>>::new(config, executor) },
		FullImportQueue = AuraImportQueue<Self::Block, FullClient<Self>>
			{ |config, client| Ok(import_queue(AuraConfig {
						local_key: None,
						slot_duration: 5
					}, client)) },
		LightImportQueue = AuraImportQueue<Self::Block, LightClient<Self>>
			{ |config, client| Ok(import_queue(AuraConfig {
						local_key: None,
						slot_duration: 5
					}, client)) },
	}
}

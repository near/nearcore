use super::drop_condition::{DropCondition, TestLoopChunksStorage};
use super::mock_pma::delayed_senders::NETWORK_DELAY;
use super::mock_pma::{TestLoopNetworkSharedState, TestLoopPeerManagerActor};
use super::testloop_transport::registry::TestLoopNodeRegistry;
use super::testloop_transport::shared_state::TestLoopNetworkSharedStateV2;
use near_async::messaging::IntoMultiSender;
use near_async::test_loop::data::TestLoopDataHandle;
use near_async::test_loop::sender::TestLoopSender;
use near_chain::resharding::resharding_actor::ReshardingActor;
use near_chain::spice_core_writer_actor::SpiceCoreWriterActor;
use near_chain_configs::{ClientConfig, Genesis};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::archive::cloud_archival_writer::CloudArchivalWriterHandle;
use near_client::archive::cold_store_actor::ColdStoreActor;
use near_client::client_actor::ClientActor;
use near_client::spice_data_distributor_actor::SpiceDataDistributorActor;
use near_client::{
    ChunkEndorsementHandlerActor, PartialWitnessActor, RpcHandlerActor, StateRequestActor,
    ViewClientActor,
};
use near_jsonrpc::ViewClientSenderForRpc;
use near_jsonrpc::client::{JsonRpcClient, RpcTransport};
use near_jsonrpc::sharded_rpc::ShardedRpcPool;
use near_network::NetworkState;
use near_network::types::PeerInfo;
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, Nonce};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::archive::cloud_storage::bucket_config::BucketConfig;
use near_store::test_utils::TestNodeStorage;
use nearcore::state_sync::StateSyncDumpHandle;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tempfile::TempDir;

/// This is the state associate with the test loop environment.
/// This state is shared across all nodes and none of it belongs to a specific node.
pub struct SharedState {
    pub genesis: Genesis,
    /// Directory of the current test. This is automatically deleted once tempdir goes out of scope.
    pub tempdir: TempDir,
    pub epoch_config_store: EpochConfigStore,
    pub runtime_config_store: Option<RuntimeConfigStore>,
    /// Shared state for the legacy mock PMA path. Maps AccountId,
    /// PeerId, and the route back CryptoHash, so mock network actors
    /// can do routing. Removed in T6 alongside the mock.
    pub network_shared_state: TestLoopNetworkSharedState,
    /// Filters + delays for the real-PMA `TestLoopTransport`. Empty
    /// by default; tests register filters via this handle.
    pub transport_shared_state: TestLoopNetworkSharedStateV2,
    /// Cross-node lookup of `Arc<TestLoopTransport>` for the real-PMA
    /// path. Populated as nodes are registered in `setup_client`.
    pub registry: TestLoopNodeRegistry,
    /// Mirrors `TestLoopBuilder::use_legacy_mock_pma` so `restart_node`
    /// / `add_node` use the same PMA path as the original build.
    /// Removed in T6 alongside the flag.
    pub use_legacy_mock_pma: bool,
    pub upgrade_schedule: ProtocolUpgradeVotingSchedule,
    /// Stores all chunks ever observed on chain. Used by drop conditions to simulate network drops.
    pub chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    /// List of drop conditions that apply to all nodes in the network.
    pub drop_conditions: Vec<DropCondition>,
    pub load_memtries_for_tracked_shards: bool,
    /// Flag to indicate if warmup is pending. This is used to ensure that warmup is only done once.
    pub warmup_pending: Arc<AtomicBool>,
    /// Archive-wide config for cloud archival nodes. Defaults to
    /// `BucketConfig::canonical()`; tests may override.
    pub bucket_config: BucketConfig,
}

/// This is the state associated with each node in the test loop environment before being built.
/// The setup_client function will be called for each node to build the node and return TestData
pub struct NodeSetupState {
    pub account_id: AccountId,
    pub client_config: ClientConfig,
    pub storage: TestNodeStorage,
    pub validator_signer: Option<Arc<ValidatorSigner>>,
}

/// This is the state associated with each node in the test loop environment after being built.
/// This state is specific to each node and is not shared across nodes.
/// We can access each of the individual actors and senders from this state.
#[derive(Clone)]
pub struct NodeExecutionData {
    pub identifier: String,
    pub account_id: AccountId,
    pub peer_id: PeerId,
    pub client_sender: TestLoopSender<ClientActor>,
    pub view_client_sender: TestLoopSender<ViewClientActor>,
    pub state_request_sender: TestLoopSender<StateRequestActor>,
    pub rpc_handler_sender: TestLoopSender<RpcHandlerActor>,
    pub chunk_endorsement_handler_sender: TestLoopSender<ChunkEndorsementHandlerActor>,
    pub shards_manager_sender: TestLoopSender<ShardsManagerActor>,
    pub partial_witness_sender: TestLoopSender<PartialWitnessActor>,
    /// Handle to the legacy mock `TestLoopPeerManagerActor`. Populated
    /// only when the builder opts into the mock via
    /// `.use_legacy_mock_pma()`. Tests that exercise the real PMA path
    /// leave this `None`.
    pub legacy_mock_pma_sender: Option<TestLoopSender<TestLoopPeerManagerActor>>,
    /// The real PMA's `NetworkState`. `None` on the legacy mock path.
    /// Used by `populate_full_mesh` at end-of-build to seed peer_store,
    /// account_announcements, and `state.peers` bidirectionally.
    pub network_state: Option<Arc<NetworkState>>,
    /// Mirror of `client_config.archive`. Needed by `populate_full_mesh`
    /// to set `archival` on each peer's `PeerConnectionInfo`.
    pub is_archival: bool,
    pub resharding_sender: TestLoopSender<ReshardingActor>,
    pub state_sync_dumper_handle: TestLoopDataHandle<Arc<StateSyncDumpHandle>>,
    pub spice_data_distributor_sender: TestLoopSender<SpiceDataDistributorActor>,
    pub spice_core_writer_sender: TestLoopSender<SpiceCoreWriterActor>,
    pub cold_store_sender: Option<TestLoopSender<ColdStoreActor>>,
    pub cloud_storage_sender: TestLoopDataHandle<Option<Arc<CloudStorage>>>,
    pub cloud_archival_writer_handle: TestLoopDataHandle<Option<CloudArchivalWriterHandle>>,
    pub jsonrpc_transport: Arc<dyn RpcTransport>,
    pub sharded_rpc_pool: Arc<RwLock<ShardedRpcPool>>,
    /// Extra blocks of delay between consensus head and execution head.
    /// Set by delay_endorsements_propagation to account for certification delay in timeouts.
    /// It is Arc<_> so updates are visible through clones.
    pub(super) expected_execution_delay: Arc<AtomicU64>,
    /// Tracks the next nonce to use per account across multiple transactions
    /// within the same block, before on-chain nonces are updated.
    pub(crate) pending_nonces: Arc<Mutex<HashMap<AccountId, Nonce>>>,
}

impl NodeExecutionData {
    pub fn expected_execution_delay(&self) -> u64 {
        self.expected_execution_delay.load(Ordering::Relaxed)
    }

    pub fn set_expected_execution_delay(&self, delay: u64) {
        self.expected_execution_delay.store(delay, Ordering::Relaxed);
    }

    pub fn jsonrpc_client(&self) -> JsonRpcClient {
        JsonRpcClient::new_with_transport(self.jsonrpc_transport.clone())
    }

    /// Handle to the legacy mock PMA. Panics if the test didn't opt
    /// into the mock via `.use_legacy_mock_pma()` on the builder.
    /// Used by tests that register network handlers via
    /// `register_override_handler`.
    pub fn legacy_pma_handle(
        &self,
    ) -> near_async::test_loop::data::TestLoopDataHandle<TestLoopPeerManagerActor> {
        self.legacy_mock_pma_sender
            .as_ref()
            .expect(
                "test uses register_override_handler — must call .use_legacy_mock_pma() on the builder",
            )
            .actor_handle()
    }

    /// Build a `PeerInfo` for this node. Used by the real-PMA path's
    /// `populate_full_mesh` to seed cross-node `peer_store` entries.
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo { id: self.peer_id.clone(), addr: None, account_id: Some(self.account_id.clone()) }
    }
}

impl From<&NodeExecutionData> for AccountId {
    fn from(data: &NodeExecutionData) -> AccountId {
        data.account_id.clone()
    }
}

impl From<&NodeExecutionData> for PeerId {
    fn from(data: &NodeExecutionData) -> PeerId {
        data.peer_id.clone()
    }
}

impl From<&NodeExecutionData> for ViewClientSenderForRpc {
    fn from(data: &NodeExecutionData) -> ViewClientSenderForRpc {
        data.view_client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl NodeExecutionData {
    pub(crate) fn homedir(tempdir: &TempDir, identifier: &str) -> PathBuf {
        tempdir.path().join(identifier)
    }
}

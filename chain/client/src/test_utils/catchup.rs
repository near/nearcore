use itertools::Itertools;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::swap;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::adapter::{
    AnnounceAccountRequest, BlockApproval, BlockHeadersRequest, BlockHeadersResponse, BlockRequest,
    BlockResponse, ProcessTxResponse, SetNetworkInfo, StateRequestHeader, StateRequestPart,
};
use crate::{start_view_client, Client, ClientActor, SyncStatus, ViewClientActor};
use actix::{Actor, Addr, AsyncContext, Context};
use actix_rt::{Arbiter, System};
use chrono::DateTime;
use chrono::Utc;
use futures::{future, FutureExt};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::messaging::{CanSend, IntoSender, LateBoundSender, Sender};
use near_async::time;
use near_chain::chain::{do_apply_chunks, BlockCatchUpRequest};
use near_chain::resharding::StateSplitRequest;
use near_chain::state_snapshot_actor::MakeSnapshotCallback;
use near_chain::test_utils::{
    wait_for_all_blocks_in_processing, wait_for_block_in_processing, KeyValueRuntime,
    MockEpochManager, ValidatorSchedule,
};
use near_chain::types::{ChainConfig, RuntimeAdapter};
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode, Provenance};
use near_chain_configs::{ClientConfig, GenesisConfig};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::shards_manager_actor::start_shards_manager;
use near_chunks::test_utils::{MockClientAdapterForShardsManager, SynchronousShardsManagerAdapter};
use near_chunks::ShardsManager;
use near_client_primitives::types::Error;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::{
    AccountOrPeerIdOrHash, HighestHeightPeerInfo, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerInfo, PeerType,
};
use near_network::types::{BlockInfo, PeerChainInfo};
use near_network::types::{
    ConnectedPeerInfo, FullPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_network::types::{
    NetworkInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, SetChainInfo,
};
use near_o11y::testonly::TracingCapture;
use near_o11y::WithSpanContextExt;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::block::{ApprovalInner, Block, GenesisId};
use near_primitives::epoch_manager::RngSeed;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::{merklize, MerklePath, PartialMerkleTree};
use near_primitives::network::PeerId;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{EncodedShardChunk, PartialEncodedChunk, ReedSolomonWrapper};
use near_primitives::static_clock::StaticClock;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochId, NumBlocks, NumSeats, NumShards,
    ShardId,
};
use near_primitives::utils::MaybeValidated;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, QueryRequest, QueryResponseKind, StateItem,
};
use near_store::test_utils::create_test_store;
use near_store::{NodeStorage, Store};
use near_telemetry::TelemetryActor;
use num_rational::Ratio;
use once_cell::sync::OnceCell;
use rand::{thread_rng, Rng};
use tracing::info;


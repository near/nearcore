# NEAR RPC Architecture

This document describes the internals of the RPC layer in nearcore: how requests are received, routed, processed, and responded to. It covers the JSON-RPC server, the Rosetta API server, and light client support.

## Overview

The nearcore RPC layer consists of three main components:

1. **JSON-RPC Server** (`chain/jsonrpc/`) - The primary interface for interacting with a NEAR node. Listens on port 3030 by default.
2. **Rosetta RPC Server** (`chain/rosetta-rpc/`) - A [Rosetta API](https://www.rosetta-api.org/) compatible server for exchanges and blockchain integrators. Listens on port 3040 by default.
3. **Light Client endpoints** - A set of JSON-RPC methods that provide Merkle proofs for trustless verification of on-chain data.

All three share the same backend: they communicate with internal actors (`ClientActor`, `ViewClientActor`, `RpcHandlerActor`) via async message passing to access blockchain state.

---

## JSON-RPC Server

### Web Framework and Server Startup

The JSON-RPC server uses **Axum** (Rust async web framework built on top of Tokio and Tower).

Startup happens in `start_http()` in `chain/jsonrpc/src/lib.rs`. The function:

1. Creates an Axum `Router` via `create_jsonrpc_app()` with all routes and middleware.
2. Binds a TCP listener on the configured address (default `0.0.0.0:3030`).
3. Spawns the server asynchronously via `FutureSpawner`.
4. Optionally starts a separate Prometheus-only HTTP server if `prometheus_addr` is configured to a different address.

The socket is bound eagerly (before spawning), so callers can be sure the server is reachable once `start_http()` returns.

### Routes

The `create_jsonrpc_app()` function registers these HTTP routes:

| Route | Method | Handler |
|---|---|---|
| `/` | POST | Main JSON-RPC endpoint |
| `/status` | GET, HEAD | Node status |
| `/health` | GET, HEAD | Health check |
| `/network_info` | GET | Network peer information |
| `/metrics` | GET | Prometheus metrics |
| `/openapi.json` | GET | OpenAPI specification |

When `enable_debug_rpc` is set to true, additional debug routes are registered:

| Route | Method | Handler |
|---|---|---|
| `/debug` | GET | Debug HTML dashboard |
| `/debug/pages/{page}` | GET | Debug pages (HTML/CSS/JS) |
| `/debug/api/entity` | POST | Entity debug handler |
| `/debug/api/block_status` | GET | Block status query |
| `/debug/api/epoch_info/{epoch_id}` | GET | Epoch information |
| `/debug/api/instrumented_threads` | GET | Thread instrumentation |
| `/debug/api/{*api_path}` | GET | Generic debug API |
| `/debug/client_config` | GET | Client configuration |

### Middleware Stack

The Axum router is wrapped with these middleware layers:

- **CORS** - Configurable via `cors_allowed_origins`. When set to `["*"]`, uses `CorsLayer::permissive()`. Otherwise, allows specific origins with GET/POST methods and Authorization/Accept/Content-Type headers. Max age is 3600 seconds.
- **Request body size limit** - Default 10MB (`json_payload_max_size`).

### The JsonRpcHandler

The `JsonRpcHandler` struct is the core of the RPC server. It holds references (as async message senders) to the backend actors:

```
struct JsonRpcHandler {
    client_sender: ClientSenderForRpc,           // → ClientActor
    view_client_sender: ViewClientSenderForRpc,  // → ViewClientActor
    process_tx_sender: ProcessTxSenderForRpc,    // → RpcHandlerActor
    peer_manager_sender: PeerManagerSenderForRpc, // → PeerManagerActor
    polling_config: RpcPollingConfig,
    genesis_config: GenesisConfig,
    enable_debug_rpc: bool,
    debug_pages_src_path: Option<PathBuf>,
    entity_debug_handler: Arc<dyn EntityDebugHandler>,
    block_notification_watcher: Receiver<Option<BlockNotificationMessage>>,
}
```

It is wrapped in an `Arc` and passed as Axum shared state to all route handlers.

### Request Processing Pipeline

When a POST request arrives at `/`, it goes through these steps:

1. **`rpc_handler()`** (Axum handler) - Deserializes the HTTP body into a `Message` (JSON-RPC message envelope). Calls `JsonRpcHandler::process()`.

2. **`process()`** - Checks that the message is a `Request` (not a response or notification). Extracts the `id` and delegates to `process_request()`.

3. **`process_request()`** - A metrics wrapper. Records timing, request count, and error count per method name. Delegates to `process_request_internal()`.

4. **`process_request_internal()`** - The core routing function. It tries handlers in this order:
   - **Adversarial requests** (only with `test_features` cargo feature) - Methods like `adv_disable_header_sync`, `adv_produce_blocks`, etc. Used in tests only.
   - **Basic requests** (`process_basic_requests_internal`) - Matches the method name against known RPC methods (see below).
   - **Query fallback** - If the method name is `"query"`, parses query parameters and dispatches to the query handler with sub-type tracking for metrics.
   - **Unknown method** - Returns `RpcError::method_not_found`.

5. **`process_method_call()`** - A generic helper that:
   - Calls `R::parse(request.params)` to parse the method parameters.
   - Invokes the async handler callback.
   - Serializes the successful result to JSON.
   - Converts errors via `RpcError::from()`.

6. **HTTP Status Code Mapping** - Back in `rpc_handler()`, the JSON-RPC response is mapped to an HTTP status code:
   - `200 OK` - Successful response.
   - `400 Bad Request` - Request validation errors.
   - `408 Request Timeout` - Timeout errors.
   - `422 Unprocessable Entity` - `UNKNOWN_BLOCK` errors when the block height is behind the latest known block.
   - `500 Internal Server Error` - Internal errors.

### JSON-RPC Methods

The following methods are registered in `process_basic_requests_internal()`:

**Core Methods:**

| Method | Handler | Backend Actor |
|---|---|---|
| `block` | `block()` | ViewClientActor (GetBlock) |
| `broadcast_tx_async` | `send_tx_async()` | RpcHandlerActor (ProcessTxRequest, fire-and-forget) |
| `broadcast_tx_commit` | `send_tx_commit()` | RpcHandlerActor + ViewClientActor (submit, then poll) |
| `chunk` | `chunk()` | ViewClientActor (GetChunk) |
| `gas_price` | `gas_price()` | ViewClientActor (GetGasPrice) |
| `health` | `health()` | ClientActor (Status with is_health_check=true) |
| `network_info` | `network_info()` | ClientActor (GetNetworkInfo) |
| `send_tx` | `send_tx()` | RpcHandlerActor + ViewClientActor |
| `status` | `status()` | ClientActor (Status) |
| `tx` | `tx_status_common()` | ViewClientActor (TxStatus) |
| `validators` | `validators()` | ViewClientActor (GetValidatorInfo) |
| `query` | `query()` | ViewClientActor (ClientQuery) |

**Light Client Methods:**

| Method | Handler |
|---|---|
| `next_light_client_block` | `next_light_client_block()` |
| `light_client_proof` | `light_client_execution_outcome_proof()` |

**Other Methods:**

| Method | Handler |
|---|---|
| `block_effects` / `EXPERIMENTAL_changes_in_block` | `changes_in_block()` |
| `changes` / `EXPERIMENTAL_changes` | `changes_in_block_by_type()` |
| `genesis_config` / `EXPERIMENTAL_genesis_config` | Returns `self.genesis_config` directly |
| `maintenance_windows` / `EXPERIMENTAL_maintenance_windows` | `maintenance_windows()` |
| `client_config` | `client_config()` |

**EXPERIMENTAL Methods:**

| Method | Handler |
|---|---|
| `EXPERIMENTAL_view_account` | `view_account()` |
| `EXPERIMENTAL_view_code` | `view_code()` |
| `EXPERIMENTAL_view_state` | `view_state()` |
| `EXPERIMENTAL_view_access_key` | `view_access_key()` |
| `EXPERIMENTAL_view_access_key_list` | `view_access_key_list()` |
| `EXPERIMENTAL_call_function` | `call_function()` |
| `EXPERIMENTAL_congestion_level` | `congestion_level()` |
| `EXPERIMENTAL_light_client_proof` | `light_client_execution_outcome_proof()` (alias) |
| `EXPERIMENTAL_light_client_block_proof` | `light_client_block_proof()` |
| `EXPERIMENTAL_protocol_config` | `protocol_config()` |
| `EXPERIMENTAL_receipt` | `receipt()` |
| `EXPERIMENTAL_tx_status` | `tx_status_common()` (with fetch_receipt=true) |
| `EXPERIMENTAL_validators_ordered` | `validators_ordered()` |
| `EXPERIMENTAL_split_storage_info` | `split_storage_info()` |

**Sandbox Methods** (only with `sandbox` cargo feature):

| Method | Handler |
|---|---|
| `sandbox_patch_state` | `sandbox_patch_state()` |
| `sandbox_fast_forward` | `sandbox_fast_forward()` |

### Parameter Parsing

Parameter parsing is handled by the `Params<T>` struct in `chain/jsonrpc/src/api/mod.rs`. Each RPC method type implements the `RpcRequest` trait:

```rust
pub trait RpcRequest: Sized {
    fn parse(value: Value) -> Result<Self, RpcParseError>;
}
```

The `Params<T>` struct provides chained parsing methods for backward compatibility with legacy parameter formats:

- `try_singleton()` - Try parsing as a single-element JSON array (legacy format).
- `try_pair()` - Try parsing as a two-element JSON array (legacy format).
- `unwrap_or_parse()` - Fall back to direct deserialization as a JSON object (modern format).

For example, the `query` method supports both:
- **Legacy path-based format**: `["account/alice.near"]` (single string with path components)
- **Modern object format**: `{"request_type": "view_account", "account_id": "alice.near", "finality": "final"}`

The legacy path format is parsed in `chain/jsonrpc/src/api/query.rs` and supports paths like:
- `account/{account_id}` → ViewAccount
- `access_key/{account_id}` → ViewAccessKeyList
- `access_key/{account_id}/{public_key}` → ViewAccessKey
- `code/{account_id}` → ViewCode
- `contract/{account_id}` → ViewState
- `call/{account_id}/{method_name}` → CallFunction

### The Query Method in Detail

The `query` method is special in the routing logic. While other methods are matched in `process_basic_requests_internal()`, `query` has its own branch in `process_request_internal()` because it needs sub-type metrics tracking. The flow is:

1. Parse the `RpcQueryRequest` (supports both legacy path and modern object format).
2. Determine the query sub-type for metrics (e.g., `query_view_account`, `query_call_function`).
3. Send a `ClientQuery` message to `ViewClientActor` with the `block_reference` and `request` fields.
4. The response goes through `process_query_response()` which provides backward-compatible error formatting for `ContractExecutionError` and `UnknownAccessKey` errors (returning a JSON object with `error`, `logs`, `block_height`, `block_hash` fields instead of a structured error).

Query types supported:
- `ViewAccount` - Account info
- `ViewCode` - Contract code (WASM)
- `ViewState` - Contract state (with optional Merkle proof)
- `ViewAccessKey` - Single access key
- `ViewAccessKeyList` - All access keys for an account
- `CallFunction` - Call a view function on a contract
- `ViewGasKeyNonces` - Gas key nonces
- `ViewGlobalContractCode` - Global contract code
- `ViewGlobalContractCodeByAccountId` - Global contract code by account ID

---

## Actor Architecture

The RPC layer communicates with the blockchain backend through four actors using async message passing. Each actor runs on its own thread pool.

### Message Sender Types

The JSON-RPC handler holds typed message senders, defined using the `near_async` `MultiSend` macro system:

- **`ClientSenderForRpc`** - Sends messages to `ClientActor`. Handles: `Status`, `GetNetworkInfo`, `GetClientConfig`, `DebugStatus`.
- **`ViewClientSenderForRpc`** - Sends messages to `ViewClientActor`. Handles: `GetBlock`, `GetChunk`, `ClientQuery`, `TxStatus`, `GetGasPrice`, `GetReceipt`, `GetProtocolConfig`, `GetValidatorInfo`, `GetValidatorOrdered`, `GetExecutionOutcome`, `GetNextLightClientBlock`, `GetBlockProof`, `GetStateChanges`, `GetStateChangesInBlock`, `GetMaintenanceWindows`, `GetSplitStorageInfo`.
- **`ProcessTxSenderForRpc`** - Sends messages to `RpcHandlerActor`. Handles: `ProcessTxRequest`. Has both async (returns `ProcessTxResponse`) and fire-and-forget variants.
- **`PeerManagerSenderForRpc`** - Sends messages to `PeerManagerActor`. Handles: `GetDebugStatus`.

All messages sent to `ClientActor` are wrapped with `SpanWrapped` for distributed tracing.

The generic `client_send()`, `view_client_send()`, and `peer_manager_send()` methods on `JsonRpcHandler` handle the send-and-await-response pattern, mapping `AsyncSendError` (actor unavailable) to appropriate RPC errors.

### ViewClientActor

`ViewClientActor` (defined in `chain/client/src/view_client_actor.rs`) is the primary read-only actor that services most RPC queries. It holds:

- A `Chain` instance for accessing blockchain state.
- An `EpochManagerAdapter` for epoch-related queries.
- A `ShardTracker` for tracking which shards this node follows.
- A `RuntimeAdapter` for executing view calls.

It implements `Handler<M, R>` for each message type. For example, the `GetBlock` handler:
1. Resolves the `BlockReference` (by height, hash, or finality) to an actual `Block`.
2. Looks up the block producer from the epoch manager.
3. Converts to a `BlockView` and returns it.

The `ClientQuery` handler processes query requests by delegating to the runtime adapter, which executes the appropriate state read (account lookup, view function call, state access, etc.) against the Trie at the specified block height.

### RpcHandlerActor

`RpcHandlerActor` (defined in `chain/client/src/rpc_handler.rs`) is a multithreaded actor dedicated to transaction pre-processing. It runs on a configurable number of threads (`handler_threads` in config).

It holds:
- A `ShardedTransactionPool` (shared via `Arc<Mutex<...>>`) for buffering transactions.
- `ChainStoreAdapter` for reading chain state.
- `EpochManagerAdapter` for shard assignment and validator lookups.
- `ShardTracker` for knowing which shards this node tracks.
- `RuntimeAdapter` for transaction validation.
- `PeerManagerAdapter` for forwarding transactions to validators.

When `process_tx()` is called with a `SignedTransaction`, it:

1. **Validates the transaction validity period** - Checks that the transaction's reference block hash is within the validity window.
2. **Validates the transaction** - Calls `runtime.validate_tx()` for signature and basic validity checks.
3. **Determines the signer's shard** - Uses the shard layout to find which shard handles this transaction.
4. **If this node tracks the shard:**
   - Performs deeper validation via `runtime.can_verify_and_charge_tx()` (balance, nonce, etc.).
   - If `check_only` is true, returns `ValidTx` without inserting.
   - If this node is a chunk producer for the shard, inserts into the transaction pool.
   - If this node is an active validator, optionally forwards to next epoch validators.
   - If not an active validator, forwards to current epoch validators.
5. **If this node doesn't track the shard:**
   - Forwards the transaction to a validator that does track it.

### ClientActor

`ClientActor` handles node-level operations like status queries and network info. The RPC layer sends it `Status` messages (for `/status` and `/health` endpoints) and `GetNetworkInfo` messages.

### Wiring at Node Startup

In `nearcore/src/lib.rs`, the actors are created in this order:
1. `ViewClientActor` - spawned as a multithread actor.
2. `StateRequestActor`, `PartialWitnessActor`, and other background actors.
3. `ClientActor` - spawned via `start_client()`.
4. `RpcHandlerActor` - spawned as a multithread actor.
5. `PeerManagerActor` - spawned for network communication.
6. `start_http()` is called, receiving all four actor handles converted to their `*SenderForRpc` types via `.into_multi_sender()`.

The Rosetta RPC server is started similarly, receiving `ClientActor`, `ViewClientActor`, and `RpcHandlerActor` handles.

---

## Transaction Processing

Transaction submission and status tracking is one of the more complex flows in the RPC layer.

### send_tx_async (broadcast_tx_async)

The simplest path. `send_tx_async()`:
1. Extracts the `SignedTransaction` from the request.
2. Computes the transaction hash.
3. Fires a `ProcessTxRequest` message to `RpcHandlerActor` via the fire-and-forget `Sender` (not the async sender). Does not wait for a response.
4. Returns the transaction hash immediately.

### send_tx

The `send_tx` method accepts a `wait_until` parameter that controls how long to wait:

- `TxExecutionStatus::None` - Same as `send_tx_async`, returns immediately.
- `TxExecutionStatus::Included` - Wait until the transaction is included in a block.
- `TxExecutionStatus::IncludedFinal` - Wait until the transaction is included in a final block.
- `TxExecutionStatus::ExecutedOptimistic` - Wait until the transaction's execution outcome is available.
- `TxExecutionStatus::Executed` - Wait until execution outcome is in a final block.
- `TxExecutionStatus::Final` - Wait until fully final.

The flow for non-None wait levels:
1. Call `send_tx_internal()` which sends `ProcessTxRequest` to `RpcHandlerActor` via the async sender and waits for validation.
2. If `ProcessTxResponse::ValidTx` or `RequestRouted` is returned, proceed to poll.
3. Call `tx_status_fetch()` which polls `ViewClientActor` for the transaction status.

### send_tx_commit (broadcast_tx_commit)

Equivalent to `send_tx` with `wait_until: ExecutedOptimistic`.

### Transaction Status Polling

`tx_status_fetch()` uses a block notification watch channel for efficient polling:

1. Creates a clone of the `block_notification_watcher` (a `tokio::sync::watch::Receiver`).
2. In a loop:
   - Sends a `TxStatus` message to `ViewClientActor`.
   - If the returned status meets the requested finality level, returns success.
   - If `UnknownTransaction` and the caller provided the signed transaction, validates it again via `send_tx_internal(check_only=true)` to detect `InvalidTransaction` early.
   - Otherwise, waits on `block_notification_watcher.changed()` for a new block to arrive.
3. The entire loop is wrapped in a `timeout()` (default 10 seconds via `polling_config.polling_timeout`).

### Transaction Idempotency

`send_tx_internal()` provides idempotent resubmission. If `RpcHandlerActor` returns `InvalidNonce`, the handler checks via `tx_exists()` whether the transaction was already processed on chain. If it was, it returns `ValidTx` instead of an error.

---

## Error Handling

### RPC Error Structure

All JSON-RPC errors are represented as `RpcError` (in `chain/jsonrpc-primitives/src/errors.rs`):

```
RpcError {
    code: i64,              // JSON-RPC error code
    message: String,        // Human-readable message
    data: Option<Box<Value>>,    // Additional error data
    error_struct: Option<RpcErrorKind>,  // Structured error for programmatic handling
}
```

`RpcErrorKind` has three variants:
- `RequestValidationError` - Bad parameters, unknown method, parse errors.
- `HandlerError` - Application-level errors (unknown block, unknown transaction, etc.).
- `InternalError` - Server-side failures.

Standard JSON-RPC error codes used:
- `-32700` - Parse error
- `-32601` - Method not found
- `-32602` - Invalid params
- `-32000` - Server error (covers internal errors, handler errors, and timeouts)

### Per-Method Error Types

Each RPC method defines its own error enum that maps to `RpcError`. For example:
- `RpcQueryError` - NoSyncedBlocks, UnavailableShard, UnknownBlock, UnknownAccount, ContractExecutionError, etc.
- `RpcBlockError` - UnknownBlock, NotSyncedYet, InternalError.
- `RpcTransactionError` - InvalidTransaction, TimeoutError, UnknownTransaction, DoesNotTrackShard, InternalError.

Error conversion between internal types and RPC types is handled by the `RpcFrom` / `RpcInto` traits, which are custom conversion traits similar to `From`/`Into` but in the RPC domain.

### Metrics

The RPC layer collects several types of metrics:
- `HTTP_RPC_REQUEST_COUNT` - Counter per method name.
- `RPC_PROCESSING_TIME` - Histogram per method name.
- `RPC_ERROR_COUNT` - Counter per method name and error code.
- `RPC_TIMEOUT_TOTAL` - Counter for timeout errors.

The `query` method provides finer-grained metrics by using sub-type names (e.g., `query_view_account`, `query_call_function`) instead of just `query`.

---

## Rosetta RPC Server

### What Is Rosetta

Rosetta is a standardized blockchain API specification created by Coinbase. Its purpose is to provide a uniform interface so that exchanges, wallets, and other integrators can interact with any blockchain using the same API shape. The nearcore Rosetta implementation follows Rosetta API spec version 1.4.4.

### Server Setup

The Rosetta server is also built on **Axum** and is started via `start_rosetta_rpc()` in `chain/rosetta-rpc/src/lib.rs`. It receives the same actor handles as the JSON-RPC server (ClientActor, ViewClientActor, RpcHandlerActor) and communicates with them via async message passing.

Middleware includes CORS, request body size limiting, and HTTP tracing. A Swagger UI is served at `/swagger-ui` and the OpenAPI spec at `/api/openapi.json`.

### Shared State

```
struct RosettaAppState {
    genesis: Arc<GenesisWithIdentifier>,
    client_addr: TokioRuntimeHandle<ClientActor>,
    view_client_addr: MultithreadRuntimeHandle<ViewClientActor>,
    tx_handler_addr: MultithreadRuntimeHandle<RpcHandlerActor>,
    currencies: Option<Vec<models::Currency>>,
}
```

The `currencies` field supports NEP-141 fungible tokens. Each configured currency has a symbol, decimals, and contract address.

### Endpoints

**Data API (read-only):**

| Route | Purpose |
|---|---|
| `POST /network/list` | List available networks (returns the NEAR network this node is on) |
| `POST /network/status` | Node sync status, peers, genesis/current block identifiers |
| `POST /network/options` | API version, supported operation types, error types |
| `POST /block` | Get a block with all balance-changing operations |
| `POST /block/transaction` | Get a specific transaction from a block |
| `POST /account/balance` | Get account balance (liquid, locked, liquid_for_storage) |
| `POST /mempool` | Not implemented (NEAR mempool is short-lived) |
| `POST /mempool/transaction` | Not implemented |

**Construction API (transaction lifecycle):**

| Route | Purpose | Online/Offline |
|---|---|---|
| `POST /construction/derive` | Derive account ID from public key (implicit accounts) | Offline |
| `POST /construction/preprocess` | Prepare a metadata request from operations | Offline |
| `POST /construction/metadata` | Get nonce and recent block hash for transaction construction | Online |
| `POST /construction/payloads` | Generate unsigned transaction bytes from operations | Offline |
| `POST /construction/combine` | Attach signature to unsigned transaction | Offline |
| `POST /construction/parse` | Parse a signed or unsigned transaction into operations | Offline |
| `POST /construction/hash` | Compute transaction hash | Offline |
| `POST /construction/submit` | Submit signed transaction to the network | Online |

### How Rosetta Interacts with Backend Actors

Rosetta uses the same actor infrastructure as JSON-RPC:

- **ClientActor** - Used for `Status` queries (network status, sync info, peer list).
- **ViewClientActor** - Used for block fetching (`GetBlock`), state change queries (`GetStateChanges`), chunk retrieval (`GetChunk`), execution outcomes, and account state queries.
- **RpcHandlerActor** - Used for transaction submission (`ProcessTxRequest`).

The Rosetta server does **not** go through the JSON-RPC server. Both servers talk directly to the same actor instances.

### NEAR to Rosetta Concept Mapping

The adapters in `chain/rosetta-rpc/src/adapters/` handle the bidirectional conversion between NEAR Protocol concepts and Rosetta API concepts.

**Accounts:**
- NEAR account IDs map directly to Rosetta `AccountIdentifier.address`.
- NEAR's three balance types map to Rosetta sub-accounts:
  - `liquid` - Main available balance.
  - `locked` - Staked balance (represented as a `SubAccount`).
  - `liquid_for_storage` - Balance reserved for storage (represented as a `SubAccount`).

**Operations:**
NEAR `Action`s are converted to one or more Rosetta `Operation`s. A single NEAR action can expand into 1-3 operations:

| NEAR Action | Rosetta Operations |
|---|---|
| `Transfer` | Sender debit + Receiver credit (2 operations) |
| `CreateAccount` | Initiate + Create (2 operations) |
| `DeleteAccount` | Initiate + Delete + RefundDeleteAccount (3 operations) |
| `AddKey` | Initiate + AddKey (2 operations) |
| `DeleteKey` | Initiate + DeleteKey (2 operations) |
| `Stake` | 1 operation |
| `DeployContract` | Initiate + Deploy (2 operations) |
| `FunctionCall` | Up to 3 operations (Transfer if deposit > 0, Initiate, FunctionCall) |
| `DelegateAction` | 4+ operations (nested structure) |

This conversion is **bijective** - operations can be converted back to NEAR actions, which is what the Construction API does.

**Blocks:**
- Block identifier is a combination of height and hash.
- The genesis block gets special handling: all genesis account balances are represented as synthetic `Transfer` operations.
- Regular blocks: the Rosetta server queries `GetStateChanges` to find all accounts whose balances changed, then constructs operations showing the balance differences.

**Currencies:**
- Native NEAR token uses yoctoNEAR (10^-24) precision, symbol "NEAR".
- NEP-141 fungible tokens are supported via configuration, with events extracted from execution logs.

### Network Identifier Validation

Every Rosetta request includes a `NetworkIdentifier` with `blockchain` and `network` fields. The server validates that `blockchain` equals `"nearprotocol"` and that `network` matches the chain ID returned by the node's status (e.g., `"mainnet"`, `"testnet"`). Sub-network identifiers are rejected.

---

## Light Client Support

The JSON-RPC server provides three endpoints for light client verification. These allow external light clients to trustlessly verify on-chain data using Merkle proofs without running a full node.

### Methods

**`next_light_client_block`**

Given a `last_block_hash` that the light client has already verified, returns the next `LightClientBlockView` the client should verify.

- If the last known block is in the same epoch as the head, returns a light client block constructed from the head header.
- If the last known block is in a different epoch, returns the epoch's stored light client block.

The `LightClientBlockView` contains:
```
LightClientBlockView {
    prev_block_hash: CryptoHash,
    next_block_inner_hash: CryptoHash,
    inner_lite: BlockHeaderInnerLiteView,   // height, epoch_id, state root, outcome root, etc.
    inner_rest_hash: CryptoHash,
    next_bps: Option<Vec<ValidatorStakeView>>,  // Next epoch's block producers (at epoch boundary)
    approvals_after_next: Vec<Option<Box<Signature>>>,  // BFT approvals
}
```

This is enough data for a light client to verify the block header's authenticity via the approval signatures and to track validator set changes across epochs.

**`light_client_proof` / `EXPERIMENTAL_light_client_proof`**

Given a transaction or receipt ID and a light client head block hash, returns a Merkle proof that the execution outcome is included in the specified block's state. The response contains:

```
RpcLightClientExecutionProofResponse {
    outcome_proof: ExecutionOutcomeWithIdView,  // The execution outcome with Merkle path
    outcome_root_proof: MerklePath,             // Proof linking outcome to outcome root
    block_header_lite: LightClientBlockLiteView, // Lightweight block header
    block_proof: MerklePath,                    // Proof linking block to the light client head's merkle tree
}
```

A light client can use these proofs to verify:
1. The execution outcome is included in a specific block (via `outcome_proof` and `outcome_root_proof`).
2. That block is included in the chain at the light client's known head (via `block_proof`).

**`EXPERIMENTAL_light_client_block_proof`**

Given a block hash and a light client head block hash, returns a Merkle proof that the specified block is an ancestor of the light client head. The response contains a `LightClientBlockLiteView` and a `MerklePath`.

### Merkle Proof System

The proof system is implemented in `core/store/src/merkle_proof.rs`. The core algorithm is `compute_past_block_proof_in_merkle_tree_of_later_block()`:

1. Gets the leaf index of the block to prove in the block merkle tree.
2. Gets the tree size at the head block.
3. Validates that the target block is not ahead of the head.
4. Walks the merkle tree to build a path of sibling hashes with left/right directions.

The `MerklePath` is a vector of `MerklePathItem`, each containing a hash and a direction (Left or Right). A verifier can reconstruct the root hash by hashing along the path and compare it to the known block merkle root from the light client's verified header.

### ViewClientActor Handlers for Light Client

- `Handler<GetNextLightClientBlock>` - Looks up the next light client block, handling same-epoch vs cross-epoch cases.
- `Handler<GetExecutionOutcome>` - Retrieves an execution outcome with its Merkle proof. Validates shard configuration consistency.
- `Handler<GetBlockProof>` - Computes a Merkle proof for a past block within a later block's merkle tree.

### Error Types

- `RpcLightClientProofError` - UnknownBlock, InconsistentState, NotConfirmed, UnknownTransactionOrReceipt, UnavailableShard, InternalError.
- `RpcLightClientNextBlockError` - InternalError, UnknownBlock, EpochOutOfBounds.

---

## Configuration

### JSON-RPC Configuration

Configured via the `rpc` section in `config.json`:

- `addr` - Listen address (default: `0.0.0.0:3030`).
- `prometheus_addr` - Optional separate Prometheus metrics address.
- `cors_allowed_origins` - List of allowed CORS origins (default: `["*"]`).
- `polling_config.polling_interval` - Interval for polling loops (default: 200ms).
- `polling_config.polling_timeout` - Timeout for polling operations like `broadcast_tx_commit` (default: 10s).
- `limits_config.json_payload_max_size` - Max request body size (default: 10MB).
- `enable_debug_rpc` - Whether to enable debug endpoints.

### Rosetta RPC Configuration

Configured via the `rosetta_rpc` section in `config.json`:

- `addr` - Listen address (default: `0.0.0.0:3040`).
- `cors_allowed_origins` - CORS origins.
- `limits.input_payload_max_size` - Max request body size (default: 10MB).
- `currencies` - List of NEP-141 fungible tokens to track, each with symbol, decimals, and contract address metadata.

---

## Key Source Files

| File | Description |
|---|---|
| `chain/jsonrpc/src/lib.rs` | Main JSON-RPC server: handler struct, routing, method implementations, server startup |
| `chain/jsonrpc/src/api/mod.rs` | Parameter parsing infrastructure (`Params`, `RpcRequest` trait, `process_method_call`) |
| `chain/jsonrpc/src/api/query.rs` | Query request parsing (including legacy path format) |
| `chain/jsonrpc/src/api/transactions.rs` | Transaction request parsing |
| `chain/jsonrpc-primitives/src/errors.rs` | RPC error types and conversion |
| `chain/jsonrpc-primitives/src/types/` | Request/response types for each RPC method |
| `chain/client/src/view_client_actor.rs` | ViewClientActor: read-only blockchain state access |
| `chain/client/src/rpc_handler.rs` | RpcHandlerActor: transaction pre-processing and routing |
| `chain/rosetta-rpc/src/lib.rs` | Rosetta RPC server: all endpoint handlers and server startup |
| `chain/rosetta-rpc/src/adapters/mod.rs` | NEAR to Rosetta concept conversion |
| `chain/rosetta-rpc/src/adapters/transactions.rs` | Block and transaction conversion for Rosetta |
| `chain/rosetta-rpc/src/models.rs` | Rosetta API data structures |
| `chain/chain/src/lightclient.rs` | Light client block creation |
| `core/store/src/merkle_proof.rs` | Merkle proof computation for light client |
| `nearcore/src/lib.rs` | Node startup: actor creation and RPC server wiring |

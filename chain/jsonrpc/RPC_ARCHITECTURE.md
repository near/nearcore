# NEAR RPC Architecture

This document describes the internals of the RPC layer in nearcore: how requests are received, routed, processed, and responded to.

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

## Gotchas and Conventions

- **The `query` method is routed separately.** It has its own branch in `process_request_internal()` (not in `process_basic_requests_internal()`) because it needs sub-type metrics tracking. Don't add query sub-types to `process_basic_requests_internal`.
- **Legacy parameter formats must be preserved.** Many methods accept both a legacy JSON array format and a modern object format. The `Params<T>` builder chain (`try_singleton()` / `try_pair()` / `unwrap_or_parse()`) handles this. Don't remove the legacy parsing paths.
- **Error types use `RpcFrom`/`RpcInto`, not `From`/`Into`.** Each RPC method has its own error enum in `chain/jsonrpc-primitives/src/types/`. These must implement `RpcFrom` to convert to `RpcError`. Forgetting this will compile but produce bad error responses.
- **`process_query_response()` has intentional backward-compatible error formatting.** For `ContractExecutionError` and `UnknownAccessKey`, it returns a flat JSON object with `error`, `logs`, `block_height`, `block_hash` fields instead of a structured RPC error. This is deliberate.
- **Rosetta and JSON-RPC are independent servers.** Both talk directly to the same actor instances. Rosetta does not go through the JSON-RPC server.
- **`send_tx_internal()` has idempotency logic.** If `RpcHandlerActor` returns `InvalidNonce`, it checks via `tx_exists()` whether the transaction was already processed on chain, and returns `ValidTx` instead of an error.

## Testing

Integration tests live in `chain/jsonrpc/jsonrpc-tests/tests/`:
- `rpc_query.rs` - Tests for query, block, chunk, validators, and other read methods.
- `rpc_transactions.rs` - Tests for transaction submission and status polling.
- `http_query.rs` - Tests for HTTP-level behavior (status, health endpoints).

Run with:

```
cargo test -p near-jsonrpc-tests --features test_features
```

## Don't Forget

When modifying the RPC layer:

- **Metrics** - If adding a new method, add its name to the metrics tracking in `process_request()`. The `query` method uses sub-type names (e.g., `query_view_account`) for finer granularity.
- **OpenAPI spec** - The spec is served at `/openapi.json`. Update it if you change method signatures (see the `openapi-spec` recipe in the `Justfile`).
- **HTTP status codes** - `rpc_handler()` maps RPC errors to HTTP status codes. `UNKNOWN_BLOCK` gets `422` when the block height is behind the latest known block; this is intentional.

---

## Overview

The RPC layer consists of two servers:

1. **JSON-RPC Server** (`chain/jsonrpc/`) - Primary node interface. Port 3030 by default. Includes light client endpoints (`next_light_client_block`, `light_client_proof`, etc.) which provide Merkle proofs for trustless verification.
2. **Rosetta RPC Server** (`chain/rosetta-rpc/`) - Rosetta API compatible server for exchanges. Port 3040 by default.

Both communicate with internal actors (`ClientActor`, `ViewClientActor`, `RpcHandlerActor`) via async message passing.

---

## JSON-RPC Server

### Server Startup

The JSON-RPC server uses **Axum**. Startup happens in `start_http()` in `chain/jsonrpc/src/lib.rs`:

1. Creates an Axum `Router` via `create_jsonrpc_app()` with all routes and middleware.
2. Binds a TCP listener on the configured address (default `0.0.0.0:3030`).
3. Spawns the server asynchronously via `FutureSpawner`.

The socket is bound eagerly (before spawning), so callers can be sure the server is reachable once `start_http()` returns.

### Routes

`create_jsonrpc_app()` registers these HTTP routes:

| Route | Method | Handler |
|---|---|---|
| `/` | POST | Main JSON-RPC endpoint |
| `/status` | GET, HEAD | Node status |
| `/health` | GET, HEAD | Health check |
| `/network_info` | GET | Network peer information |
| `/metrics` | GET | Prometheus metrics |
| `/openapi.json` | GET | OpenAPI specification |

When `enable_debug_rpc` is true, additional routes under `/debug` and `/debug/api/` are registered.

Middleware: CORS (configurable via `cors_allowed_origins`) and request body size limit (default 10MB).

### The JsonRpcHandler

`JsonRpcHandler` (in `lib.rs`) is the core of the RPC server. It holds async message senders to the four backend actors (ClientActor, ViewClientActor, RpcHandlerActor, PeerManagerActor), polling configuration, the genesis config, and a `block_notification_watcher` (a `tokio::sync::watch::Receiver` that gets notified each time a new block is accepted by the client). It is wrapped in an `Arc` and passed as Axum shared state to all route handlers.

### Request Processing Pipeline

When a POST request arrives at `/`:

1. **`rpc_handler()`** - Deserializes the body into a JSON-RPC `Message`. Calls `JsonRpcHandler::process()`.
2. **`process()`** - Validates it's a `Request`, extracts `id`, delegates to `process_request()`.
3. **`process_request()`** - Metrics wrapper (timing, request count, error count per method). Delegates to `process_request_internal()`.
4. **`process_request_internal()`** - Core routing. Tries in order:
   - Adversarial requests (only with `test_features` cargo feature).
   - `process_basic_requests_internal()` - matches method name against known RPC methods.
   - Special `"query"` branch with sub-type metrics tracking.
   - Returns `method_not_found` if no match.
5. **`process_method_call()`** - Generic helper: parses params via `R::parse()`, invokes handler, serializes result, converts errors.
6. **HTTP status code mapping** in `rpc_handler()`: 200 (success), 400 (validation), 408 (timeout), 422 (UNKNOWN_BLOCK behind head), 500 (internal).

### JSON-RPC Methods

Methods registered in `process_basic_requests_internal()`:

**Core Methods:**

| Method | Handler | Backend Actor |
|---|---|---|
| `block` | `block()` | ViewClientActor (GetBlock) |
| `broadcast_tx_async` | `send_tx_async()` | RpcHandlerActor (fire-and-forget) |
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

**Light Client:** `next_light_client_block`, `light_client_proof`

**Other:** `block_effects`/`EXPERIMENTAL_changes_in_block`, `changes`/`EXPERIMENTAL_changes`, `genesis_config`/`EXPERIMENTAL_genesis_config`, `maintenance_windows`/`EXPERIMENTAL_maintenance_windows`, `client_config`

**EXPERIMENTAL:** ~14 methods for direct queries, protocol config, receipts, tx status, validators, congestion level, light client block proofs, etc. All follow the same `process_method_call` pattern. See `process_basic_requests_internal()` for the full list.

**Sandbox** (only with `sandbox` feature): `sandbox_patch_state`, `sandbox_fast_forward`

### Parameter Parsing

Each RPC method type implements `RpcRequest::parse()` in `chain/jsonrpc/src/api/`. The `Params<T>` struct provides chained parsing for backward compatibility:

- `try_singleton()` - Single-element JSON array (legacy).
- `try_pair()` - Two-element JSON array (legacy).
- `unwrap_or_parse()` - Direct JSON object deserialization (modern).

The `query` method supports both a legacy path format (`["account/alice.near"]`) and a modern object format (`{"request_type": "view_account", "account_id": "alice.near", "finality": "final"}`). The legacy paths are parsed in `chain/jsonrpc/src/api/query.rs`.

### The Query Method

The `query` method has special routing (see Gotchas above). The flow:

1. Parse `RpcQueryRequest` (supports both legacy path and modern object format).
2. Determine the query sub-type for metrics (e.g., `query_view_account`).
3. Send `ClientQuery` to `ViewClientActor`.
4. Response goes through `process_query_response()` for backward-compatible error formatting.

Query types: `ViewAccount`, `ViewCode`, `ViewState`, `ViewAccessKey`, `ViewAccessKeyList`, `CallFunction`, `ViewGasKeyNonces`, `ViewGlobalContractCode`, `ViewGlobalContractCodeByAccountId`.

---

## Actor Architecture

The RPC layer communicates with the blockchain backend through four actors via async message passing.

### Message Sender Types

Defined using the `near_async` `MultiSend` macro system in `lib.rs`. Each sender type wraps `AsyncSender` channels for specific message types:

- **`ClientSenderForRpc`** -> `ClientActor`. A few messages: `Status`, `GetNetworkInfo`, `GetClientConfig`, `DebugStatus`.
- **`ViewClientSenderForRpc`** -> `ViewClientActor`. The workhorse — handles ~16 message types for all read-only queries (blocks, chunks, queries, tx status, gas price, validators, light client, state changes, etc.).
- **`ProcessTxSenderForRpc`** -> `RpcHandlerActor`. Only `ProcessTxRequest`, with both async (returns `ProcessTxResponse`) and fire-and-forget variants.
- **`PeerManagerSenderForRpc`** -> `PeerManagerActor`. Only `GetDebugStatus`.

The `client_send()`, `view_client_send()`, and `peer_manager_send()` helper methods on `JsonRpcHandler` handle the send-and-await pattern, mapping `AsyncSendError` to RPC errors. All messages to `ClientActor` are wrapped with `SpanWrapped` for distributed tracing.

### ViewClientActor

Defined in `chain/client/src/view_client_actor.rs`. The primary read-only actor, spawned as a **multithread actor**. Holds a `Chain`, `EpochManagerAdapter`, `ShardTracker`, and `RuntimeAdapter`. Implements `Handler<M, R>` for each message type.

The `ClientQuery` handler delegates to the runtime adapter, which executes state reads (account lookup, view function call, state access, etc.) against the Trie at the specified block height.

### RpcHandlerActor

Defined in `chain/client/src/rpc_handler.rs`. A **separate multithreaded actor** dedicated to transaction pre-processing. It exists to keep transaction validation work (signature checks, nonce lookups, balance verification) off the consensus-critical `ClientActor`. Thread count is configurable via `handler_threads`.

When `process_tx()` is called with a `SignedTransaction`:

1. Validates transaction validity period and basic validity (signature, etc.).
2. Determines the signer's shard via shard layout.
3. **If this node tracks the shard:** deeper validation (balance, nonce), then inserts into transaction pool or forwards to validators.
4. **If not:** forwards to a validator that tracks the shard.

### ClientActor

Handles node-level operations: `Status` (for `/status` and `/health`) and `GetNetworkInfo`. Runs on a **Tokio runtime handle** (not multithread actor), since these operations are infrequent and lightweight.

### Wiring at Node Startup

In `nearcore/src/lib.rs`: `ViewClientActor` -> `ClientActor` -> `RpcHandlerActor` -> `PeerManagerActor` -> `start_http()` receives all four handles converted to `*SenderForRpc` types via `.into_multi_sender()`.

---

## Transaction Processing

### send_tx_async (broadcast_tx_async)

Fires `ProcessTxRequest` to `RpcHandlerActor` (fire-and-forget). Returns the transaction hash immediately.

### send_tx

Accepts a `wait_until` parameter: `None` (immediate), `Included`, `IncludedFinal`, `ExecutedOptimistic`, `Executed`, `Final`.

For non-None: sends `ProcessTxRequest` and waits for validation, then polls `ViewClientActor` via `tx_status_fetch()`.

### send_tx_commit (broadcast_tx_commit)

Equivalent to `send_tx` with `wait_until: ExecutedOptimistic`.

### Transaction Status Polling

`tx_status_fetch()` uses a `tokio::sync::watch` channel (`block_notification_watcher`) for efficient polling:

- Sends `TxStatus` to `ViewClientActor` each time a new block arrives.
- If `UnknownTransaction`, re-validates via `send_tx_internal(check_only=true)` to detect `InvalidTransaction` early.
- Wrapped in a `timeout()` (default 10s via `polling_config.polling_timeout`).

---

## Error Handling

All JSON-RPC errors use `RpcError` (in `chain/jsonrpc-primitives/src/errors.rs`) with `RpcErrorKind` variants: `RequestValidationError`, `HandlerError`, `InternalError`.

Standard JSON-RPC error codes: `-32700` (parse), `-32601` (method not found), `-32602` (invalid params), `-32000` (server error).

Each RPC method defines its own error enum (e.g., `RpcQueryError`, `RpcBlockError`, `RpcTransactionError`) that converts to `RpcError` via `RpcFrom`.

### Metrics

- `HTTP_RPC_REQUEST_COUNT` - Counter per method name.
- `RPC_PROCESSING_TIME` - Histogram per method name.
- `RPC_ERROR_COUNT` - Counter per method name and error code.
- `RPC_TIMEOUT_TOTAL` - Counter for timeout errors.

---

## Rosetta RPC Server

Built on **Axum**, started via `start_rosetta_rpc()` in `chain/rosetta-rpc/src/lib.rs`. Uses the same actor handles as JSON-RPC. Follows Rosetta API spec version 1.4.4.

**Data API:** `/network/list`, `/network/status`, `/network/options`, `/block`, `/block/transaction`, `/account/balance`. Mempool endpoints are not implemented.

**Construction API:** `/construction/derive`, `/construction/preprocess`, `/construction/metadata`, `/construction/payloads`, `/construction/combine`, `/construction/parse`, `/construction/hash`, `/construction/submit`.

The adapters in `chain/rosetta-rpc/src/adapters/` handle bidirectional conversion between NEAR and Rosetta concepts. NEAR actions expand into one or more Rosetta operations (e.g., `Transfer` becomes sender debit + receiver credit). This conversion is bijective.

Every request is validated against the node's network identifier (`blockchain: "nearprotocol"`, `network` must match chain ID).

---

## Light Client Support

Three endpoints for trustless verification via Merkle proofs:

- **`next_light_client_block`** - Returns the next `LightClientBlockView` (header, approvals, optional next-epoch block producers) for light client chain following.
- **`light_client_proof` / `EXPERIMENTAL_light_client_proof`** - Returns a Merkle proof that an execution outcome is included in a given block.
- **`EXPERIMENTAL_light_client_block_proof`** - Returns a Merkle proof that a block is an ancestor of a given head block.

Proof computation is in `core/store/src/merkle_proof.rs`. ViewClientActor handlers: `GetNextLightClientBlock`, `GetExecutionOutcome`, `GetBlockProof`.

---

## Configuration

**JSON-RPC** (via `rpc` section in `config.json`):

- `addr` - Listen address (default: `0.0.0.0:3030`).
- `cors_allowed_origins` - CORS origins (default: `["*"]`).
- `polling_config.polling_timeout` - Timeout for `broadcast_tx_commit` etc. (default: 10s).
- `limits_config.json_payload_max_size` - Max request body (default: 10MB).
- `enable_debug_rpc` - Enable debug endpoints.

**Rosetta RPC** (via `rosetta_rpc` section in `config.json`):

- `addr` - Listen address (default: `0.0.0.0:3040`).
- `currencies` - NEP-141 fungible tokens to track.

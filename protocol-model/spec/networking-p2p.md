# Networking / P2P

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `chain/network/src/network_protocol/mod.rs`, `chain/network/src/network_protocol/network.proto`, `chain/network/src/network_protocol/proto_conv/{peer_message,handshake,util}.rs`, `chain/network/src/network_protocol/state_sync.rs`, `chain/network/src/peer/peer_actor.rs`, `chain/network/src/peer/stream.rs`, `chain/network/src/recv_permit.rs`, `chain/network/src/concurrency/outgoing_queue_limiter.rs`, `chain/network/src/peer_manager/peer_manager_actor.rs`, `chain/network/src/peer_manager/network_state/{mod,routing,tier1}.rs`, `chain/network/src/peer_manager/{connection/mod,network_transport,tcp_transport,peer_store/mod}.rs`, `chain/network/src/routing/{bfs,route_back_cache}.rs`, `chain/network/src/routing/graph/mod.rs`, `chain/network/src/routing/routing_table_view/mod.rs`, `chain/network/src/rate_limits/messages_limits.rs`, `chain/network/src/tcp.rs`, `chain/network/src/config.rs`, `chain/network/src/config_json.rs`, `chain/network/src/types.rs`

## Role

The P2P layer is the transport for everything else in the protocol: it establishes and maintains TCP connections to peers, negotiates a wire protocol on connect, gossips blocks/transactions, and routes targeted messages (chunk parts, approvals, witnesses, endorsements, state-part requests) to specific validators or peers — possibly over multiple hops. It exposes two delivery models: **broadcast** (send to every directly-connected TIER2 peer) and **routed** (deliver to a single `AccountId`/`PeerId`, finding a path via a routing graph). It feeds raw messages up into [chain-block-processing](chain-block-processing.md), [sync](sync.md), [sharding-chunks](sharding-chunks.md), [stateless-validation](stateless-validation.md), and [consensus-finality](consensus-finality.md), and carries those components' outbound messages back onto the wire. The semantics of payloads belong to those specs; this spec covers only how the bytes travel. Peer discovery for the validator overlay (TIER1) is bootstrapped over the general gossip overlay (TIER2).

2.14.0 is largely a **network-hardening** release: the layer now bounds *memory* explicitly (incoming and outgoing byte semaphores), bounds *per-message-type size*, enforces *per-message-kind rate limits by default*, and rejects several pre-authentication decode-time amplification vectors. §9 documents these; none of it is protocol-version gated.

## Key data structures

- **`PeerMessage`** — `chain/network/src/network_protocol/mod.rs:418` — the top-level wire enum. Variants split into TCP-protocol traffic (`Tier1Handshake`/`Tier2Handshake`/`Tier3Handshake`, `HandshakeFailure`, `LastEdge`, `SyncRoutingTable`, `RequestUpdateNonce`, `SyncAccountsData`, `PeersRequest`/`PeersResponse`, `Disconnect`), direct payloads (`Block`, `OptimisticBlock`, `BlockHeaders`, `Transaction`, `BlockRequest`, `BlockHeadersRequest`, `StateRequestHeader`/`StateRequestPart`/`VersionedStateResponse`, `EpochSyncRequest`/`EpochSyncResponse`, `SyncSnapshotHosts`, `Challenge`), and the catch-all `Routed(Box<RoutedMessage>)`. Serialized as protobuf (`PeerMessage::serialize`, `mod.rs:473`), not borsh; a tracing span context is injected into every message.
- **`PeerMessage::max_size`** — `chain/network/src/network_protocol/mod.rs:501` — new in 2.14.0. Maps every `PeerMessage` variant (and, for `Routed`, every `T1MessageBody`/`T2MessageBody` variant) to one of four class limits: `MAX_SMALL_MESSAGE_SIZE = 512 KiB` (`mod.rs:83`), `MAX_MEDIUM_MESSAGE_SIZE = 32 MiB` (`mod.rs:84`), `MAX_LARGE_MESSAGE_SIZE = 128 MiB` (`mod.rs:85`), `MAX_HUGE_MESSAGE_SIZE = 512 MiB` (`mod.rs:86`).
- **`Handshake`** — `chain/network/src/network_protocol/mod.rs:355` — `protocol_version`, `oldest_supported_version`, `sender_peer_id`, `target_peer_id`, `sender_listen_port`, `sender_chain_info: PeerChainInfoV2` (genesis id, height, tracked shards, archival flag, built in `send_handshake`, `chain/network/src/peer/peer_actor.rs:470`), `partial_edge_info: PartialEdgeInfo` (a nonce + half-signature used to build the connection's `Edge`), and an optional `owned_account: SignedOwnedAccount` (validator-key ownership proof).
- **`HandshakeFailureReason`** — `chain/network/src/network_protocol/mod.rs:375` — `ProtocolVersionMismatch { version, oldest_supported_version }`, `GenesisMismatch(GenesisId)`, `InvalidTarget`.
- **`TieredMessageBody`** — `chain/network/src/network_protocol/mod.rs:582` — `T1(Box<T1MessageBody>)` or `T2(Box<T2MessageBody>)`. The tier of a routed payload is a property of the body type, not negotiated. `from_routed` (`mod.rs:647`) maps a flat `RoutedMessageBody` into the tiered form; `From<TieredMessageBody> for RoutedMessageBody` maps back.
- **`T1MessageBody`** — `chain/network/src/network_protocol/mod.rs:764` — consensus/production-critical routed payloads: `BlockApproval`, `VersionedPartialEncodedChunk`, `PartialEncodedChunkForward`, `PartialEncodedStateWitness(+Forward)`, `VersionedPartialEncodedStateWitness(+Forward)`, `VersionedChunkEndorsement`, `ChunkContractAccesses`, `ContractCodeRequest`/`ContractCodeResponse`, plus Spice variants (the Spice request variant was renamed `SpicePartialDataRequest` → `SpiceDataRequest` in 2.14.0, keeping borsh discriminant 11 / `RoutedMessageBody` discriminant 36). `message_resend_count` (`mod.rs:785`) returns `IMPORTANT_MESSAGE_RESENT_COUNT = 3` (`mod.rs:80`) for `BlockApproval`/`VersionedPartialEncodedChunk`, else 1.
- **`T2MessageBody`** — `chain/network/src/network_protocol/mod.rs:820` — multi-hop gossip-overlay payloads: `ForwardTx`, `TxStatusRequest`/`TxStatusResponse`, `PartialEncodedChunkRequest`/`Response`, `Ping`/`Pong`, `ChunkStateWitnessAck`, `StatePartRequest`, `PartialEncodedContractDeploys`, `StateHeaderRequest`, `StateRequestAck`.
- **`RoutedResponseKind`** — `chain/network/src/network_protocol/mod.rs:840` — new in 2.14.0. `Pong | TxStatusResponse | PartialEncodedChunkResponse`. `T2MessageBody::requested_response_kind` (`mod.rs:866`) says which reply a request asks for; `response_kind` (`mod.rs:878`) says which reply a body *is*; `must_arrive_on_route_back` (`mod.rs:861`) is true only for `TxStatusResponse`.
- **`RoutedMessageBody`** — `chain/network/src/network_protocol/mod.rs:902` — the flat body (explicit borsh discriminants) used for V1/V2 wire encoding and for hashing.
- **`RoutedMessage`** — `chain/network/src/network_protocol/mod.rs:1284` — `V1(RoutedMessageV1)` (`mod.rs:1172`), `V2(RoutedMessageV2)` (`mod.rs:1189`), `V3(RoutedMessageV3)` (`mod.rs:1221`). V1 = `{target: PeerIdOrHash, author, signature, ttl, body: RoutedMessageBody}`; V2 adds `created_at`, `num_hops`; V3 carries `body: TieredMessageBody` and a now-**optional** `signature`. All accessors (`target`, `author`, `body`, `verify`, `decrease_ttl`, `hash`, …) dispatch over the variant; `upgrade_to_v3` (`mod.rs:1442`) normalizes V1/V2 to V3 in memory. In-memory the node always builds V3 (`RawRoutedMessage::sign`, `mod.rs:1683`), but the wire encoding is always V1 borsh: outbound serialization calls `msg_v1()` (`mod.rs:1313`) and writes it into the `proto::RoutedMessage.borsh` field (`chain/network/src/network_protocol/proto_conv/peer_message.rs:297`), with `created_at`/`num_hops` carried as separate proto fields (`peer_message.rs:298`, `:306`). A `proto::RoutedMessageV3` parse path exists on receive (`peer_message.rs:504`) but nothing emits it today (TODO #13709, `mod.rs:1312`).
- **`PeerIdOrHash`** — `chain/network/src/network_protocol/mod.rs:1515` — a routed message's target: `PeerId(..)` (forward toward a peer) or `Hash(..)` (route the response *back* along the path the request took; the hash identifies the route-back cache entry).
- **`Edge`** — `chain/network/src/network_protocol/edge.rs` — a signed, nonce-stamped assertion that two peers are connected. Edges are the adjacency proofs exchanged via `SyncRoutingTable(RoutingTableUpdate)` (`mod.rs:334`) and fed into the routing graph.
- **`routing::Graph`** — `chain/network/src/routing/graph/mod.rs:358` — the edge store + routing-table owner. Restructured in 2.14.0 (#15656): the mutable state now lives in a plain `parking_lot::Mutex<Inner>` (`graph/mod.rs:362`) with an `ArcSwap<GraphSnapshot>` read path, so `Graph::update` (`graph/mod.rs:407`) is a **synchronous** function rather than the previous `async` `ArcMutex`-based one. The persistent edge map is `imbl::HashMap` (`graph/mod.rs:47`, `:61`) after the `im` → `imbl` swap (#16208); the connection `Pool` snapshot uses `imbl` too (`chain/network/src/peer_manager/connection/mod.rs:167`).
- **`bfs::Graph`** — `chain/network/src/routing/bfs.rs:10` — the undirected connectivity graph used for BFS routing. Peers are interned to `u32` ids (`p2id`/`id2p`); `adjacency` stores both directions; `max_peers` caps the number of distinct peers (`add_edge` refuses to grow past it, `bfs.rs:109`).
- **`RouteBackCache`** — `chain/network/src/routing/route_back_cache.rs:50` — maps a request's message hash → a `Record { inserted_at, target, expected_response }` (`route_back_cache.rs:81`), so responses (`PeerIdOrHash::Hash`) can be sent back along the same path. `expected_response: Option<ExpectedResponse>` (`route_back_cache.rs:75`) is new in 2.14.0 and records `{responder, kind}` for entries the node created for its *own* requests. Capacity-bounded with anti-poisoning eviction (`DEFAULT_CAPACITY = 100_000`, `route_back_cache.rs:9`; `DEFAULT_CACHE_EVICT_TIMEOUT = 120s`, `:11`; `DEFAULT_REMOVE_BATCH_SIZE = 100`, `:13`).
- **`RoutingTableView`** — `chain/network/src/routing/routing_table_view/mod.rs:15` (inner `Inner`) — holds the computed `next_hops` (target → set of first-hop neighbors) and `distance` tables, plus a `last_routed` LRU (`:29`, `LAST_ROUTED_CACHE_SIZE = 10_000`, `:11`) used to load-balance across equal-cost next hops.
- **`SnapshotHostInfo` / `SyncSnapshotHosts`** — `chain/network/src/network_protocol/state_sync.rs:28`, `:97` — a peer's signed advertisement of which shards it can serve state for (`peer_id`, `sync_hash`, `epoch_height`, `shards`, `signature`).
- **`AccountData` / `SignedAccountData`** — `chain/network/src/network_protocol/mod.rs:140`, `:266` — a validator's broadcast of its `peer_id` and TIER1 `proxies`; the global, bounded state that lets TIER1 nodes find each other (`MAX_ACCOUNT_DATA_SIZE_BYTES = 10000`, `mod.rs:176`, enforced in `VersionedAccountData::sign`, `mod.rs:219`).
- **`tcp::Tier`** — `chain/network/src/tcp.rs:22` — `T1` (`:25`, validator consensus overlay), `T2` (`:29`, general gossip P2P), `T3` (`:34`, ad-hoc direct connections for large transfers, e.g. state parts).
- **`RateLimits`** — `chain/network/src/rate_limits/messages_limits.rs:13` — per-connection `EnumMap<RateLimitedPeerMessageKey, Option<TokenBucket>>`, used to throttle inbound messages.
- **`RecvMessagePermit`** — `chain/network/src/recv_permit.rs:8` — an `OwnedSemaphorePermit` held for as long as an inbound message is alive anywhere in the node, bounding total incoming-message memory. `RecvMessagePermit::none()` (`recv_permit.rs:18`) is used for locally-generated messages.
- **`OutgoingQueueLimiter` / `OutgoingPermit`** — `chain/network/src/concurrency/outgoing_queue_limiter.rs:6`, `:29` — a byte-counting semaphore covering everything sitting in outgoing send queues across *all* connections. `try_acquire` (`:17`) is non-blocking and returns `None` when there is no headroom; `shrink_to` (`:48`) releases surplus when a pre-reserved worst-case estimate turns out larger than the real serialized size.

## Behavior

### 1. Tiers (T1 / T2 / T3)

`tcp::Tier` (`chain/network/src/tcp.rs:22`) defines three overlays:
- **T1** (`tcp.rs:25`) — connections between BFT consensus participants (or their proxies), reserved for production-critical messages. Established only by/for validators; bootstrapped via `AccountData` learned over T2 (`chain/network/src/peer_manager/network_state/tier1.rs:203`).
- **T2** (`tcp.rs:29`) — the general P2P gossip mesh used for everything else and for discovering T1 peers; this is the default overlay where BFS routing happens and the only one with broadcast semantics (`NetworkTransport::broadcast_message`, `chain/network/src/peer_manager/network_transport.rs:47`; `TcpTransport` broadcasts on `tier2` only, `chain/network/src/peer_manager/tcp_transport.rs:201`).
- **T3** (`tcp.rs:34`) — created ad hoc to ship one large message (state parts/headers). The *request* travels over T2; the responder dials a direct T3 connection back to send the response, so the bulk transfer does not block other traffic. `TIER3_IDLE_TIMEOUT = 15s` (`chain/network/src/peer_manager/peer_manager_actor.rs:103`).

Admission by tier is enforced in both directions: `Tier::is_allowed_receive` (`chain/network/src/peer_manager/connection/mod.rs:31`) pins each handshake variant to its own tier, allows `HandshakeFailure`/`LastEdge`/`Disconnect`/`OptimisticBlock` on any tier, allows `VersionedStateResponse` on T2/T3, delegates `Routed` to `is_allowed_receive_routed` (`:63`, T1 body anywhere, T2 body only on T2) and restricts every remaining variant to T2. `Tier::is_allowed_send_routed` (`:70`) mirrors this on send. A disallowed inbound message closes the connection with `ClosingReason::DisallowedMessage` (`peer_actor.rs:1707`).

### 2. Handshake

A handshake is a single `PeerMessage::{Tier1,Tier2,Tier3}Handshake(Handshake)` exchange; the variant fixes the tier (the tier is **not** negotiable). The outbound side (`send_handshake`, `peer_actor.rs:470`) sends a `Handshake` carrying `protocol_version = spec.protocol_version`, `oldest_supported_version = MIN_SUPPORTED_PROTOCOL_VERSION` (`peer_actor.rs:479`), its peer id/listen port/chain info, a `partial_edge_info` (nonce + half-signature), and — if a validator — a `SignedOwnedAccount`.

`PeerActor::process_handshake` (`peer_actor.rs:526`) validates, in order:

1. **Outbound responder checks** (`peer_actor.rs:533`): for a connection we initiated, the reply's `protocol_version`, `genesis_id`, `sender_peer_id`, `tier` and `partial_edge_info.nonce` must each match what we proposed, else `stop(HandshakeFailed)` (the tier check at `peer_actor.rs:552`, the nonce check at `:557`).
2. **Inbound version gate** (`peer_actor.rs:564`): if `protocol_version < MIN_SUPPORTED_PROTOCOL_VERSION` or `> PROTOCOL_VERSION`, reply `HandshakeFailure(ProtocolVersionMismatch { version: PROTOCOL_VERSION, oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION })` (`peer_actor.rs:573`).
3. **Genesis gate** (`peer_actor.rs:580`): mismatched `genesis_id` → `HandshakeFailure(GenesisMismatch(..))` (`peer_actor.rs:583`).
4. **Target check** (`peer_actor.rs:589`): if `target_peer_id != my id` → `HandshakeFailure(InvalidTarget)` (`peer_actor.rs:591`).
5. **Nonce sanity** (`peer_actor.rs:599`): `verify_nonce` (timestamp-derived bound); on a nonce not greater than an existing local edge's nonce, reply `LastEdge(..)` as evidence (`peer_actor.rs:610`) instead of forming a new edge.
6. **Owned-account proof** (`peer_actor.rs:619`): if present, the signature must verify (else `Ban(InvalidSignature)`, `peer_actor.rs:621`), `owned_account.peer_id` must equal `sender_peer_id` (else `OwnedAccountMismatch`, `peer_actor.rs:625`), and the timestamp must be within `MAX_CLOCK_SKEW = 30min` (`peer_actor.rs:61`) (else `TooLargeClockSkew`, `peer_actor.rs:628`).

Before any of this, the handshake is parsed, and parsing itself is bounded: `PeerChainInfoV2::try_from` rejects a handshake advertising more than `MAX_TRACKED_SHARDS_PER_PEER = 128` tracked shards (`chain/network/src/network_protocol/mod.rs:200`, enforced at `chain/network/src/network_protocol/proto_conv/handshake.rs:63`) *before* the `Vec<ShardId>` is allocated — see §9.

On success it forms the connection's signed `Edge` from both half-signatures (`Edge::new`, `peer_actor.rs:644`), builds a `connection::Connection`, and registers it with the `PeerManager` (which may still reject for connection-count/duplicate reasons). When the initiator receives a `ProtocolVersionMismatch` failure it computes `common_version = min(peer.version, PROTOCOL_VERSION)` and retries the handshake at that version, but only if `common_version` is ≥ both the peer's advertised `oldest_supported_version` and its own `MIN_SUPPORTED_PROTOCOL_VERSION`; otherwise it gives up (`peer_actor.rs:913`).

The negotiated `protocol_version` is the **wire** protocol version (the same `PROTOCOL_VERSION` integer that gates on-chain features) and determines which `RoutedMessage` variant and message encodings are mutually understood; it is enforced per-connection, independent of the chain's current epoch protocol version.

### 3. Routed messaging: signing, target selection, forwarding

**Construction & signing.** A `RawRoutedMessage { target, body }` (`mod.rs:1675`) is signed by `RawRoutedMessage::sign` (`mod.rs:1683`) → always a `RoutedMessage::V3` with `ttl = routed_message_ttl` (default `ROUTED_MESSAGE_TTL = 100`, `chain/network/src/types.rs:50`), `created_at = now`, `num_hops = 0`, and a signature over `RoutedMessage::build_hash(target, author, body)` (the borsh hash of `RoutedMessageNoSignature{target, author, body}`, `mod.rs:1303`). `NetworkState::sign_message` (`chain/network/src/peer_manager/network_state/mod.rs:736`) wraps this with the node key.

**Send to an `AccountId`** — `NetworkState::send_message_to_account` (`network_state/mod.rs:835`):
1. If the target account is *this* node's validator account, deliver locally via `receive_routed_message` instead of the wire (`network_state/mod.rs:853`), passing `RecvMessagePermit::none()` (`network_state/mod.rs:861`); a `debug_assert!(msg.allow_sending_to_self())` (`network_state/mod.rs:845`) documents the expectation that only self-deliverable bodies (witnesses, endorsements — `mod.rs:794`) reach this path.
2. If `Tier::T1.is_allowed_send_routed(body)` (i.e. a T1 body, `network_state/mod.rs:869`), look up the account's `SignedAccountData`, pick a TIER1 peer via `get_tier1_proxy` (`tier1.rs:348`) — a direct T1 connection if present, else one of the validator's advertised proxies — and send directly over T1, once. No BFS, single hop.
3. Otherwise resolve the account → `PeerId`: prefer the peer id from `accounts_data` (`network_state/mod.rs:893`), fall back to the (being-deprecated) `account_announcements` table (`network_state/mod.rs:907`). If neither resolves, drop with `MessageDropped::UnknownAccount` (`network_state/mod.rs:912`).
4. Sign once and `send_message_to_peer` over T2 `message_resend_count()` times (3× for important bodies, else 1×, `network_state/mod.rs:925`).

**Send to a `PeerId`/`Hash`** — `NetworkState::send_message_to_peer` (`network_state/mod.rs:746`), per tier:
- **T1**: if `target` is a `Hash`, pop the responder from `tier1_route_back` (`network_state/mod.rs:768`); if a `PeerId`, send directly. T1 does not BFS.
- **T2**: `tier2_find_route` (`routing.rs:143`) — for a `PeerId`, ask `RoutingTableView::find_next_hop_for_target`; for a `Hash`, pop the next hop from `tier2_route_back` (`routing.rs:152`). If we are the author and the message `expect_response()`, insert our own id into `tier2_route_back` under `msg.hash()` first, **together with** the `ExpectedResponse` computed by `expected_response_for_own_request` (`network_state/mod.rs:664`, inserted at `:788`) so the eventual response can be matched against the request. A `FindRouteError` drops the message (`MessageDropped::NoRouteFound`, `network_state/mod.rs:803`).
- **T3**: direct `PeerId` only (a `Hash` target trips a `debug_assert!` and returns false, `network_state/mod.rs:816`).

**Receiving / forwarding** — `PeerActor` Routed arm (`peer_actor.rs:1319`) then `NetworkState::process_incoming_routed` (`network_state/mod.rs:1158`):
1. Drop messages duplicated within `DROP_DUPLICATED_MESSAGES_PERIOD = 50ms` (`peer_actor.rs:70`) keyed by `(author, target, signature)` (`peer_actor.rs:1327`).
2. For `ForwardTx`, drop if `txns_since_last_block > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE = 1000` (`peer_actor.rs:66`, `:1350`), else increment the counter (`peer_actor.rs:1354`). The counter is reset to 0 whenever a `PeerMessage::Block` arrives (`peer_actor.rs:1019`).
3. `msg.verify()` (signature check) — failure ⇒ `Ban(InvalidSignature)` (`peer_actor.rs:1360`).
4. `process_incoming_routed`: compute `message_for_me(target)` (`network_state/mod.rs:693`) — a `PeerId` match against our id, or for a `Hash`, a `compare_route_back` lookup (`routing.rs:192`). **New in 2.14.0**: if the message is for me and `body.must_arrive_on_route_back()` (only `TxStatusResponse`), `unsolicited_response_reason` (`network_state/mod.rs:676`) must return `None` or the message is dropped — see §9.
5. If for me, a network-wide dedup over `recent_routed_messages` (`RECENT_ROUTED_MESSAGES_CACHE_SIZE = 10000`, `network_state/mod.rs:74`) records whether this copy was the fastest (`network_state/mod.rs:1186`). **Then always** call `add_route_back` (`network_state/mod.rs:1191`, `routing.rs:163`): if the body `expect_response()` (Ping/TxStatusRequest/PartialEncodedChunkRequest via `requested_response_kind`, `mod.rs:866`), record `from` under `msg.hash()` in the tier's route-back cache with `expected_response = None` (we are forwarding someone else's request, so there is nothing of ours to bind the reply to, `routing.rs:176`).
6. If for me ⇒ `RoutedAction::ForMe` (`network_state/mod.rs:1194`); Ping/Pong are handled inline in the peer actor (`peer_actor.rs:1374`), everything else goes to `receive_routed_message` dispatch.
7. Else `decrease_ttl()` (`mod.rs:1262`): if still > 0, increment `num_hops` and return `RoutedAction::Forward` (re-enter `send_message_to_peer` to the next hop); if it hit 0, drop (`ROUTED_MESSAGE_DROPPED`, `network_state/mod.rs:1204`).

`receive_routed_message` (`network_state/mod.rs:932`) is the demux that hands each `T1*`/`T2*` body to the right adapter (client, shards manager, partial-witness actor, spice distributor). Since 2.14.0 every such hand-off also carries the message's `RecvMessagePermit` so the inbound memory reservation is released only when the downstream actor is done with the payload (§9).

### 4. BFS routing over the connectivity graph

`bfs::Graph::calculate_next_hops_and_distance` (`bfs.rs:148`) runs a single BFS from `source` over the undirected adjacency. For each reachable node it records `distance` and a `u128` bitset `routes` of which **direct neighbors of source** lie on *some* shortest path (one bit per source-neighbor; only the first `MAX_TIER2_PEERS = 128` neighbors get bits, `bfs.rs:167`, `peer_manager_actor.rs:90`). `compute_result` (`bfs.rs:203`) expands each bitset into a `Vec<PeerId>` of candidate first hops. `unreliable_peers` are excluded as *transit* nodes: they are not enqueued (so paths never traverse them, `bfs.rs:168`), but a direct edge to them is still kept so messages addressed *to* them work. The result feeds `RoutingTableView::update` (`routing_table_view/mod.rs:72`). `find_next_hop` (`routing_table_view/mod.rs:35`) picks, among equal-cost next hops, the one least-recently routed through (`last_routed` LRU, `:39`) — spreading load across paths.

The graph is built from `Edge`s exchanged via `SyncRoutingTable(RoutingTableUpdate)` (§6) plus the node's own local edges; `set_unreliable_peers` (`routing.rs:197`) marks peers to avoid as transit. `Inner::update` (`graph/mod.rs:318`) prunes expired edges, recomputes BFS, refreshes `peer_reachable_at`, prunes unreachable peers, and rebuilds the `GraphSnapshot`; `Graph::update` (`graph/mod.rs:407`) drives it under the `Mutex` and publishes the snapshot via `ArcSwap`.

**`peer_reachable_at` accounting (#16373).** `prune_unreachable_peers` (`graph/mod.rs:162`) now first retains only entries with `reachable_at >= unreachable_since` (`graph/mod.rs:163`) and then treats *any* peer absent from `peer_reachable_at` as unreachable (`graph/mod.rs:168`). Previously the map was consulted with a `< unreachable_since` comparison and pruned separately *only for the peers selected in this round*, which left stale entries behind for peers that had no edges at selection time. The observable effect is that stale/unreachable peers and their adjacent edges are now dropped from the in-memory graph promptly and consistently.

### 5. Route-back cache

Responses to a request are addressed `PeerIdOrHash::Hash(request_hash)` rather than a peer id, because the requester may be many hops away and the responder need not know a forward route. `RouteBackCache` (`route_back_cache.rs:50`) stores `hash → Record { inserted_at, target, expected_response }`. Every `insert` (`route_back_cache.rs:239`) first calls `remove_evicted` (`route_back_cache.rs:158`); when the cache is full it runs `remove_frequent` (`:115`) **first** — dropping the oldest batch from the peer holding the most entries — and **then** drops any entries older than `evict_timeout` — bounding a single abuser to roughly `capacity / active_connections` slots (asserted by the `poison_attack` test, `route_back_cache.rs:450`). `tier2_find_route` `remove`s (consumes) the entry when routing a response back (`route_back_cache.rs:207`).

`expected_response` (`route_back_cache.rs:203`) is populated only for entries the node created for its own outbound requests, and is what §9's unsolicited-response check reads.

### 6. Routing-table / peer-info exchange

Peers periodically gossip `PeerMessage::SyncRoutingTable(RoutingTableUpdate { edges, accounts })` (`mod.rs:334`). On receipt, `PeerActor::handle_sync_routing_table` (`peer_actor.rs:1431`), in order:

1. **Edge ingress cap** — if `edges.len() > routing_graph_max_edges_per_message` (default `DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_MESSAGE = 50_000`, `chain/network/src/config.rs:33`), the whole edge set is dropped with `EDGE_DROPPED` before dedup/verification (`peer_actor.rs:1440`); accounts are still processed. Otherwise `NetworkState::add_edges` (`routing.rs:99`) aggregates through a demux and calls `Graph::update`; an invalid edge (bad signature, or a self-loop from a remote peer) makes `add_edges` return a ban reason and the connection is stopped (`peer_actor.rs:1461`).
2. **Account ingress cap (new, #16132)** — if `accounts.len() > routing_graph_max_accounts_per_message` (default `DEFAULT_ROUTING_GRAPH_MAX_ACCOUNTS_PER_MESSAGE = 10_000`, `config.rs:39`; configurable via `NetworkConfigOverrides`, `chain/network/src/config_json.rs:352`, and required `> 0` by `NetworkConfig::verify`, `config.rs:631`), the accounts are dropped wholesale, `ACCOUNT_ANNOUNCEMENT_DROPPED` is incremented and the handler returns (`peer_actor.rs:1467`). Each `AnnounceAccount` is signature-verified downstream, so an unbounded list was the same DoS shape as the edge list.
3. `AnnounceAccount`s that pass the cap are forwarded to the client for signature verification and staleness filtering before being adopted (`peer_actor.rs:1494`).

Edge admission inside the graph (`Inner::add_edges`, `graph/mod.rs:192`) applies, in order: dedup; drop edges already known at ≥ nonce; drop edges older than `prune_edges_after`; **drop edges whose nonce timestamp is more than `EDGE_NONCE_FUTURE_TOLERANCE = 5min` in the future, or whose nonce does not map to a valid timestamp at all** (`graph/mod.rs:31`, `:228` — new in #16132; this closes a hole where a far-future nonce made an edge permanently un-prunable); a cheap pre-check against `max_total_edges` / `max_edges_per_source` *before* paying for signature verification (`graph/mod.rs:268`, `:274`); then signature verification, stopping at the first invalid edge. Limit-triggered drops are non-punitive and do not set the ban flag (`graph/mod.rs:184`).

New local edges are broadcast (`broadcast_routing_table_update`, `routing.rs:20`). `PeersRequest`/`PeersResponse` (`mod.rs:391`, `:400`) exchange `PeerInfo`s (and direct peers) for discovery; validator discovery rides on `SyncAccountsData` (`mod.rs:383`), which propagates `SignedAccountData` (validator ↔ peer-id ↔ proxies) so `tier1_connect` (`tier1.rs:203`) can dial the right T1 peers/proxies. `SyncSnapshotHosts` propagates state-snapshot availability (§10).

### 7. Peer store, discovery, scoring, eviction

The `PeerStore` (`peer_store/mod.rs`) persists known peers with a `KnownPeerStatus` (`Unknown`/`NotConnected`/`Connected`/`Banned(reason, time)`). `PeerManagerActor::monitor_peers_trigger` (`peer_manager_actor.rs:669`) drives discovery: it first runs `PeerStore::update`, then if `is_outbound_bootstrap_needed` (`peer_manager_actor.rs:462`) — i.e. `total_connections < ideal_connections_lo`, or (`total_connections < max_num_peers` and `potential_outbound_connections < minimum_outbound_peers`), and `outbound_disabled` is false (`peer_manager_actor.rs:474`) — it picks an `unconnected_peer` (`peer_store/mod.rs:439`) — preferring previously-connected, address-bearing peers, with probability `PREFER_PREVIOUSLY_CONNECTED_PEER = 0.6` (`peer_manager_actor.rs:95`, `:684`) — and dials it. Reconnect retries up to `MAX_RECONNECT_ATTEMPTS = 6` (`peer_manager_actor.rs:71`, `:258`).

When connections exceed `ideal_connections_hi`, `maybe_stop_active_connection` (`peer_manager_actor.rs:543`) builds a `safe_set` of connections to keep (whitelisted, outbound, archival, then earliest-established up to `safe_set_size`, `peer_manager_actor.rs:550`–`613`) and closes the rest. `PeerStore::update` (`peer_store/mod.rs:281`) periodically unbans peers whose `ban_window` has elapsed (`:245`), refreshes `last_seen` for connected peers (`:261`), and removes expired non-connected entries. The T2 mesh size is capped at `MAX_TIER2_PEERS = 128` (`peer_manager_actor.rs:90`).

### 8. Rate limiting & banning

Per-connection `RateLimits` (`rate_limits/messages_limits.rs:13`) maintains a `TokenBucket` per message kind; `is_allowed` (`:49`) acquires `cost` tokens and returns false (drop the message, bump `PEER_MESSAGE_RATE_LIMITED_BY_TYPE_TOTAL`) when the bucket is empty. It is evaluated per inbound frame in `PeerActor`'s `IncomingFrame` handler (`peer_actor.rs:1691`) — *after* protobuf decode and the per-type size check, *before* the tier check and before any handler runs. Buckets are configured per message type with `maximum_size` (burst), `refill_rate` (tokens/s) and optional `initial_size` (`messages_limits.rs:62`).

**Default limits are now non-trivial (#16165, #16132).** Prior to 2.14.0 `Config::standard_preset` (`messages_limits.rs:105`) configured only `EpochSyncRequest`/`EpochSyncResponse`. It now also configures, per connection (burst = rate unless noted):

| Message kind(s) | Burst / refill |
|---|---|
| `EpochSyncRequest`, `EpochSyncResponse` | 1 / (1 per 30s) (`messages_limits.rs:113`) |
| `SyncRoutingTable` | 10 / 1 per s (`messages_limits.rs:125`) |
| `Transaction`, `ForwardTx`, `TxStatusRequest`, `TxStatusResponse` | 30 000/s (`messages_limits.rs:135`) |
| `StatePartRequest`, `StateHeaderRequest`, `StateRequestAck`, `VersionedStateResponse`, and the legacy `StateRequestHeader`/`StateRequestPart`/`StateResponse` | 100/s (`messages_limits.rs:141`) |
| `BlockApproval` | 2 000/s (`messages_limits.rs:152`) |
| `ChunkEndorsement`, `SpiceChunkEndorsement` | 20 000/s (`messages_limits.rs:154`) |
| `PartialEncodedChunkRequest`/`Response`, `VersionedPartialEncodedChunk` | 5 000/s (`messages_limits.rs:158`) |
| `PartialEncodedChunkForward` | 1 000/s (`messages_limits.rs:163`) |
| `Block`, `OptimisticBlock`, `BlockRequest` | 10/s (`messages_limits.rs:165`) |
| `BlockHeaders`, `BlockHeadersRequest` | 100/s (`messages_limits.rs:170`) |
| `ChunkStateWitnessAck`, `PartialEncodedStateWitness(+Forward)` | 1 000/s (`messages_limits.rs:174`) |
| contract distribution (`ChunkContractAccesses`, `ContractCodeRequest`/`Response`, `PartialEncodedContractDeploys`, spice equivalents) | 1 000/s (`messages_limits.rs:179`) |
| spice data (`SpicePartialData`, `SpiceDataRequest`) | 1 000/s (`messages_limits.rs:188`) |
| `PeersRequest`, `PeersResponse`, `SyncAccountsData`, `SyncSnapshotHosts` | 10/s (`messages_limits.rs:192`) |
| `RequestUpdateNonce` | 100/s (`messages_limits.rs:198`) |

`standard_preset` is the default for a real node (`config.rs:458`); `NetworkConfig::from_seed` (test-only) uses `Config::default()`, i.e. no limits (`config.rs:549`). Operators may override or disable individual entries via `OverrideConfig` (`messages_limits.rs:84`, applied at `config.rs:293`); the config is validated at startup (`config.rs:612`).

Misbehavior bans are issued via `PeerStore::peer_ban` (`peer_store/mod.rs:419`, status → `Banned(reason, now)`, `:430`) and by `PeerActor` stopping with `ClosingReason::Ban(ReasonForBan::...)`: invalid routed signatures (`peer_actor.rs:1360`), invalid owned-account proof (`peer_actor.rs:619`), oversized `BlockHeadersRequest` (`> MAX_BLOCK_HEADER_HASHES = 20` ⇒ `Abusive`, `config.rs:53`, `network_state/mod.rs:1252`), bad edges (`peer_actor.rs:1461`), and abusive `PeersResponse` (below).

**`PeersResponse` ban now actually stops processing (#16138).** `handle_msg_ready` bans a peer that sends more than `PEERS_RESPONSE_MAX_PEERS = 512` peers (`config.rs:50`) or more than `MAX_TIER2_PEERS = 128` direct peers, and **returns immediately** after each `self.stop(...)` (`peer_actor.rs:1134`, `:1139`). Before the fix, `stop()` only recorded the closing reason and execution fell through, so the abusive peer list was still ingested into the peer store on the way out.

### 9. Memory and decode-time hardening (new in 2.14.0)

This is the bulk of the release's changes to this component. All of it is ungated `neard` behavior.

**9.1 Incoming memory semaphore (#16166).** `NetworkState::incoming_message_semaphore` is a `tokio::sync::Semaphore` with `INCOMING_SEMAPHORE_PERMITS = 1_000_000_000` (1 GB, `network_state/mod.rs:92`, constructed at `:359`) shared by every connection. In `FramedStream::run_recv_loop` (`chain/network/src/peer/stream.rs:201`) the length prefix is read first; then, **before** the body is read into memory, the loop awaits `acquire_many_owned(n)` for exactly `n` bytes (`stream.rs:235`) and wraps the permit in a `RecvMessagePermit`. The permit travels with the message as `IncomingFrame { data, recv_permit }` (`stream.rs:75`) into `PeerActor` (`peer_actor.rs:1635`), through `handle_msg_ready` (`peer_actor.rs:1068`) — where long-running handlers explicitly capture it for the duration (`peer_actor.rs:1179`, `:1229`, `:1277`, `:1303`) — and on into `receive_routed_message`/`handle_peer_message`, which hand it to the downstream actor message (`ShardsManagerRequestFromNetwork`, `PartialEncodedStateWitnessMessage`, `Tier3Request`, …). It is released when the last holder drops it. Consequences: total in-flight inbound message bytes across the node are bounded at 1 GB; a connection whose message cannot be admitted simply stalls its read loop (backpressure, not a drop).
- The whole acquire-then-read step is wrapped in a `READ_TIMEOUT = WRITE_TIMEOUT = 2min` (`stream.rs:24`, `:27`); on expiry the loop returns `RecvError::IO(TimedOut)` and the connection is closed. This also bounds how long a peer can pin permits by dribbling a declared-large message.
- If the semaphore is closed (shutdown) the loop returns `RecvError::IncomingSemaphoreClosed` (`stream.rs:47`), which is classified as an expected, non-punitive close (`peer_actor.rs:1608`).
- The pre-existing frame cap `NETWORK_MESSAGE_MAX_SIZE_BYTES = 512 MiB` (`stream.rs:20`) is still checked on the length prefix *before* the permit is taken (`stream.rs:224`). Violating it is the one framing error that is punitive: `RecvError::MessageTooLarge` is classified as abusive and the connection is closed with `Ban(ReasonForBan::Abusive)` (`peer_actor.rs:1602`).

**9.2 Outgoing memory semaphore (#16172).** `NetworkState::outgoing_queue_limiter` is an `OutgoingQueueLimiter` sized `outgoing_queue_limiter_capacity_bytes`, default `DEFAULT_OUTGOING_QUEUE_LIMITER_CAPACITY_BYTES = 3 GiB` (`config.rs:42`, wired at `network_state/mod.rs:349`, configurable in `config.json` via `outgoing_queue_limiter_capacity_bytes`, `config_json.rs:218`). Every outbound message passes through `PeerActor::send_message_inner` (`peer_actor.rs:404`), which serializes the message and then either shrinks a caller-supplied reservation to the real size (`peer_actor.rs:439`) or does a **non-blocking** `try_acquire(bytes_len)`; on failure the message is **dropped** with `MessageDropped::OutgoingQueueLimitExceeded` and a warning (`peer_actor.rs:442`–`:455`). The permit rides in the `Frame` (`stream.rs:50`) and is released as the frame drains to the socket, explicitly dropped before the next frame is pulled (`stream.rs:306`).
  - Large responses reserve capacity *before* being generated, so the node does not build a multi-hundred-MB response it cannot send: `EpochSyncRequest` handling reserves `EPOCH_SYNC_RESPONSE_BYTES = 300 MiB` (`network_state/mod.rs:95`, `:1321`) and drops the request if it cannot; `Tier3Request` handling reserves `STATE_SYNC_RESPONSE_BYTES = 30 MiB` (`network_state/mod.rs:98`, `peer_manager_actor.rs:1571`) and, if it cannot, answers `StateRequestAckBody::Busy` instead of producing a state header/part response. The reservation is carried through `SendMessage.reserved_permit` (`chain/network/src/private_messages.rs:28`), `Connection::send_message_with_permit` (`connection/mod.rs:151`), `Pool::send_message_with_permit` (`connection/mod.rs:392`), `NetworkTransport::send_message_with_permit` (`network_transport.rs:31`, TCP impl at `tcp_transport.rs:187`) and `NetworkRequestWithPermit` (`types.rs:451`, handled at `peer_manager_actor.rs:1552`).
- Per-connection the write buffer is separately capped by `max_write_buffer_capacity_bytes`, default `DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES = 700 MiB` (`config.rs:44`; previously a hard-coded 1 GiB in `stream.rs`). Exceeding it raises `SendError::QueueOverflow` (`stream.rs:185`), which closes the connection but is classified as expected/non-punitive (`peer_actor.rs:1606`).

**9.3 Per-message-type size limit (#16173).** After protobuf decode and before anything else, `PeerActor`'s `IncomingFrame` handler compares the *encoded frame length* against `peer_msg.max_size()` (`peer_actor.rs:1666`–`:1679`). A frame larger than its type's class limit is dropped (not banned) with `MessageDropped::TooLargeForType`. The classes (`mod.rs:501`): 512 KiB for requests/handshakes/approvals/endorsements/pings/state-part requests; 32 MiB for `PeersResponse`, `Transaction`, `SyncRoutingTable`, `SyncAccountsData`, `SyncSnapshotHosts`, `Block`/`BlockHeaders`/`OptimisticBlock`, `ForwardTx`, `TxStatusResponse`, chunk request/response, `VersionedPartialEncodedChunk`, contract-accesses/code-request, `SpiceDataRequest`; 128 MiB for partial state witnesses, `ContractCodeResponse`, `PartialEncodedContractDeploys`, spice partial data, `VersionedStateResponse`; 512 MiB for `EpochSyncResponse`.

**9.4 Pre-auth shard-id list caps (#16129).** Both are `repeated uint64` fields reachable on the very first frame of a raw TCP connection, before authentication or rate limiting:
- `Handshake.sender_chain_info.tracked_shards` is rejected above `MAX_TRACKED_SHARDS_PER_PEER = 128` (`mod.rs:200`) in `TryFrom<&proto::PeerChainInfo>` (`proto_conv/handshake.rs:63`), i.e. *before* the `Vec<ShardId>` allocation; the send side carries a matching `debug_assert!` (`proto_conv/handshake.rs:44`).
- `SnapshotHostInfo.shards` is rejected above `MAX_SHARDS_PER_SNAPSHOT_HOST_INFO = 512` (`mod.rs:189`) in `TryFrom<&proto::SnapshotHostInfo>` (`proto_conv/peer_message.rs:149`). The cap already existed but was only checked in `SnapshotHostInfo::verify` (`state_sync.rs:71`), which runs *after* decoding and therefore after the allocation.
Both are receive-side caps and are safe to tighten unilaterally; loosening them later needs the usual two-release receive-then-send skew handling.

**9.5 Decode-size limits for borsh sub-frames (#16131).** `try_from_slice_with_limit` (`proto_conv/util.rs:20`) rejects a borsh blob larger than a limit *before* deserializing it. It is applied to `proto::SignedTransaction.borsh` with `MAX_TRANSACTION_SIZE_BYTES = 16 MiB` (`proto_conv/peer_message.rs:28`, `:476`) and to routed-message bodies with `MAX_ROUTED_MESSAGE_SIZE_BYTES = 64 MiB` (`peer_message.rs:34`, `:482` for the V1 path, `:592` for the V3 path). This blocks a `Transaction`/`ForwardTx` whose single `Delegate` action declares a huge nested-action list from inflating ~1 wire byte into ~96 bytes of heap during decode. The load-bearing half of that fix lives outside this component, in `validate_delegate_action` (`runtime/runtime/src/action_validation.rs`), which now checks the nested-action count against `max_actions_per_receipt` before cloning the list — see [runtime-execution](runtime-execution.md). The runtime change is consensus-neutral (same `TotalNumberOfActionsExceeded` error for the same inputs).

**9.6 Drop unsolicited `TxStatusResponse` (#16385).** `TxStatusResponse` is the only routed reply whose payload *is* the answer — the receiver has no committed data to check it against (unlike a `PartialEncodedChunkResponse`, whose parts are verified against the locally stored chunk header) — so `must_arrive_on_route_back` is true only for it (`mod.rs:861`). When such a message is for us, `unsolicited_response_reason` (`network_state/mod.rs:676`) rejects it with one of three reasons and `process_incoming_routed` drops it, bumping `ROUTED_MESSAGE_DROPPED` (`network_state/mod.rs:1168`):
1. it is addressed to our `PeerId` rather than a route-back hash;
2. no route-back entry with an `ExpectedResponse` exists for the hash;
3. `body.response_kind()` differs from the recorded `kind`, or `msg.author()` differs from the recorded `responder`.
The route-back hash alone is weak evidence, because `build_hash` covers only `{target, author, body}` — any peer able to guess those can address a message to it, and one hash shape serves every request kind. Binding the entry to the responder and the reply kind at request time (§3, `expected_response_for_own_request`) is what makes the check meaningful.

**9.7 `network.proto` buf lint pass (#16259).** Enum *value names* were renamed to the `ENUM_NAME_VALUE` convention (`HandshakeFailure.Reason`: `UNKNOWN`→`REASON_UNSPECIFIED`, `ProtocolVersionMismatch`→`REASON_PROTOCOL_VERSION_MISMATCH`, `GenesisMismatch`→`REASON_GENESIS_MISMATCH`, `InvalidTarget`→`REASON_INVALID_TARGET`; `TraceContext.SamplingPriority` similarly), and `reserved` lists were reformatted. **No field number, field type, enum number, or message shape changed** — the rename is source-level only and the wire encoding is byte-identical. The corresponding Rust match arms were updated in `proto_conv/handshake.rs:147`–`:199`.

**9.8 What each bound does when it is violated.** Every limit in this section fails in exactly one of four ways — backpressure (the read loop stalls), drop (the message is discarded, the connection survives), close (the connection is torn down without a ban record), or ban (the peer is recorded `Banned` in the peer store and refused).

| Limit | Value | Enforcement site | On violation |
|---|---|---|---|
| `NETWORK_MESSAGE_MAX_SIZE_BYTES` | 512 MiB frame | `stream.rs:224` | **ban** — `Ban(Abusive)` (`peer_actor.rs:1602`) |
| `INCOMING_SEMAPHORE_PERMITS` | 1 GB node-wide (10^9 bytes) | `stream.rs:235` | **backpressure** — the recv loop awaits the permit before reading the body |
| `READ_TIMEOUT` | 2 min, wrapping acquire+read | `stream.rs:231`, `stream.rs:24`/`:27` | **close** — `RecvError::IO(TimedOut)`, an expected close |
| `PeerMessage::max_size` | 512 KiB / 32 MiB / 128 MiB / 512 MiB by type | `peer_actor.rs:1668` | **drop** — `MessageDropped::TooLargeForType`, explicitly not a ban |
| per-kind `RateLimits` | `standard_preset` (§8 table) | `peer_actor.rs:1691` | **drop** — `PEER_MESSAGE_RATE_LIMITED_BY_TYPE_TOTAL` |
| tier admission | `is_allowed_receive` | `peer_actor.rs:1707` | **close** — `ClosingReason::DisallowedMessage` (a TODO notes it arguably deserves a ban) |
| `outgoing_queue_limiter_capacity_bytes` | 3 GiB node-wide | `peer_actor.rs:442` | **drop** — non-blocking `try_acquire`, `MessageDropped::OutgoingQueueLimitExceeded` |
| `max_write_buffer_capacity_bytes` | 700 MiB per connection | `stream.rs:183` | **close** — `SendError::QueueOverflow`, expected/non-punitive; the frame is still queued |
| `MAX_TRACKED_SHARDS_PER_PEER` | 128 | `proto_conv/handshake.rs:63` | **drop** — parse error, so the whole handshake frame is discarded pre-auth |
| `MAX_SHARDS_PER_SNAPSHOT_HOST_INFO` | 512 | `proto_conv/peer_message.rs:149` | **drop** — parse error on the frame |
| `MAX_TRANSACTION_SIZE_BYTES` | 16 MiB borsh | `proto_conv/peer_message.rs:476` | **drop** — parse error on the frame |
| `MAX_ROUTED_MESSAGE_SIZE_BYTES` | 64 MiB borsh | `proto_conv/peer_message.rs:482`, `:592` | **drop** — parse error on the frame |
| `routing_graph_max_edges_per_message` | 50 000 | `peer_actor.rs:1440` | **drop** — edges only; accounts in the same message are still processed |
| `routing_graph_max_accounts_per_message` | 10 000 | `peer_actor.rs:1467` | **drop** — accounts only, handler returns |
| `EDGE_NONCE_FUTURE_TOLERANCE` | 5 min | `graph/mod.rs:228` | **drop** — edge filtered out, explicitly non-punitive (`graph/mod.rs:184`) |
| `max_total_edges` / `max_edges_per_source` | 1 000 000 / 50 000 | `graph/mod.rs:268`, `:274` | **drop** — pre-signature-check, non-punitive |
| invalid edge signature / remote self-loop | — | `peer_actor.rs:1461` | **ban** — `add_edges` returns a `ReasonForBan` |
| `PEERS_RESPONSE_MAX_PEERS` / `MAX_TIER2_PEERS` in `PeersResponse` | 512 / 128 | `peer_actor.rs:1134`, `:1139` | **ban** — `Ban(Abusive)`, followed by `return` |
| `MAX_BLOCK_HEADER_HASHES` | 20 | `network_state/mod.rs:1252` | **ban** — `Ban(Abusive)` |
| routed-message signature | — | `peer_actor.rs:1360` | **ban** — `Ban(InvalidSignature)` |
| `MAX_TRANSACTIONS_PER_BLOCK_MESSAGE` | 1000 per inter-block window | `peer_actor.rs:1350` | **drop** |
| `ROUTED_MESSAGE_TTL` | 100 hops | `network_state/mod.rs:1204` | **drop** |
| unsolicited `TxStatusResponse` | — | `network_state/mod.rs:1168` | **drop** |
| `MAX_ACCOUNT_DATA_SIZE_BYTES` | 10 000 | `mod.rs:219` | **send-side** — `sign` refuses to produce the payload |

### 10. Block / chunk / tx / approval / witness propagation

`PeerManagerActor::handle_msg_network_requests` (`peer_manager_actor.rs:838`) dispatches outbound `NetworkRequests` (it now takes an optional pre-acquired `OutgoingPermit`, used only by the `EpochSyncResponse` arm, `peer_manager_actor.rs:1336`):
- **Block** — `broadcast_message(PeerMessage::Block)` to all ready T2 peers (`peer_manager_actor.rs:849`). Block requests/headers are direct `BlockRequest`/`BlockHeadersRequest` to a peer (`peer_manager_actor.rs:876`, `:887`).
- **Transactions** — `ForwardTx(account_id, tx)` is routed to the responsible validator as a T2 routed message (`peer_manager_actor.rs:1219`); a node may also receive a raw `PeerMessage::Transaction` and feed it to the client.
- **Approvals** — `T1MessageBody::BlockApproval` sent via `send_message_to_account` (`peer_manager_actor.rs:867`); 3× resend on the T2 fallback, prefers T1 overlay. See [consensus-finality](consensus-finality.md).
- **Chunk parts** — `VersionedPartialEncodedChunk` (T1, 3× resend) and `PartialEncodedChunkForward` (T1) routed to the target; `PartialEncodedChunkRequest`/`Response` are T2 (request/route-back). See [sharding-chunks](sharding-chunks.md).
- **Endorsements / witnesses** — `VersionedChunkEndorsement` (`peer_manager_actor.rs:1252`), `PartialEncodedStateWitness(+Forward)` (`:1261`, `:1293`) and their `Versioned*` forms are T1 routed (and `allow_sending_to_self`). See [stateless-validation](stateless-validation.md).
- **OptimisticBlock** — sent over T1 as a direct `PeerMessage::OptimisticBlock` to each chunk producer/proxy (`peer_manager_actor.rs:853`).
- **State sync** — `StateHeaderRequest`/`StatePartRequest` are T2 routed (`peer_manager_actor.rs:898`, `:942`); the responder unconditionally returns a `StateRequestAck` over T2 and, if it accepted, opens a fresh T3 connection for the `VersionedStateResponse` (`peer_manager_actor.rs:1560`–`1683`). Part identifiers are now the `StatePartIndex` alias rather than bare `u64` throughout (`network_protocol/state_sync.rs:151`, `types.rs:598`), and `PartIdOrHeader` was renamed `PartOrHeader` — a rename only, no wire change. Availability is advertised via `SyncSnapshotHosts`/`SnapshotHostInfo` (`peer_manager_actor.rs:1089`). The removal of centralized (external-storage) state sync (#16009) did **not** touch `chain/network`: no `PeerMessage` or routed-body variant was removed, and the deprecated `PeerMessage::StateRequestHeader`/`StateRequestPart` variants still exist (`mod.rs:449`). See [sync](sync.md).

## Interactions

- **Consumes**: TCP byte streams (`tcp.rs`, `tcp_transport.rs`); `NetworkRequests` / `NetworkRequestWithPermit` from the client/shards-manager/partial-witness actors (mapped to `PeerMessage`s in `peer_manager_actor.rs`); `SignedAccountData` and `AnnounceAccount` for validator discovery.
- **Produces**: decoded `PeerMessage`/routed bodies delivered to the right actor by `receive_routed_message` (`network_state/mod.rs:932`) and the direct-payload handlers, each carrying a `RecvMessagePermit` that the receiving actor must hold for as long as it retains the payload.
- **Touches**: [sync](sync.md) (block/header/state-part/epoch-sync transport and `SnapshotHostInfo` availability), [chain-block-processing](chain-block-processing.md) (block/header broadcast & ingest), [sharding-chunks](sharding-chunks.md) (partial chunk parts/forwards/requests), [stateless-validation](stateless-validation.md) (partial witnesses, endorsements, contract-code distribution), [consensus-finality](consensus-finality.md) (block approvals), [runtime-execution](runtime-execution.md) (the delegate-action validation half of #16131), [data-structures-serialization](data-structures-serialization.md) (borsh/proto encoding of payloads). Payload meaning lives in those specs; this layer only transports them.

## Protocol-version-gated behavior

**Nothing in this component is `ProtocolFeature`-gated: the string `ProtocolFeature` does not appear anywhere under `chain/network/src/`.** None of the 2.14.0 network-hardening work is gated either. The semaphores, per-type size limits, rate-limit preset, admission caps, shard-id caps, decode-size limits, the unsolicited-`TxStatusResponse` drop, the `peer_reachable_at` fix, the `Graph`-as-`Mutex` refactor, the `im`→`imbl` swap and the `network.proto` lint rename are all unconditional `neard` behavior at every protocol version, changing only what a node accepts/emits locally. Several are deliberately *receive-side only* caps (§9.4) precisely so they need no version gate.

The P2P **wire** protocol (which `RoutedMessage` variant, which `PeerMessage` fields) is gated by the handshake-negotiated `protocol_version` against `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`core/primitives-core/src/version.rs:652` — raised from 83 in 2.13.0) and `PROTOCOL_VERSION = 87` (`version.rs:692`, from `STABLE_PROTOCOL_VERSION`, `version.rs:680`), **not** by `ProtocolFeature` flags. `RoutedMessageV3` (tiered body, optional T1 signature) is the in-memory form produced by `sign` today (`mod.rs:1683`), but every outbound routed message is still serialized as V1 borsh via `msg_v1()` (`mod.rs:1313`, `proto_conv/peer_message.rs:297`); the V3 proto field is only parsed on receive (`peer_message.rs:504`), never emitted (TODO #13709, `mod.rs:1312`). Incoming V1/V2 are upgraded to V3 in memory (`mod.rs:1442`).

`ProtocolFeature`s touching message *payloads* carried by this layer (verified against `core/primitives-core/src/version.rs` at PV 87):
- **`PostQuantumSignatures`** — activates at **v85** (`version.rs:609`). Adds FIPS 204 ML-DSA-65 as a third signature scheme for transactions/`AddKey`; changes the contents of `Transaction`/`ForwardTx` payloads but not the transport. Semantics in [accounts-keys](accounts-keys.md).
- **`SignedContractCodeResponse`** — activates at **v85** (`version.rs:614`). Senders emit `ContractCodeResponseV2` (signed inner payload) and receivers require a verifiable signature before processing; affects the `ContractCodeResponse` T1 routed payload. Semantics in [stateless-validation](stateless-validation.md).
- **`ExecutionMetadataV4`** — activates at **v85** (`version.rs:613`). Changes borsh wire format of execution metadata carried in chunk-related payloads; requires coordinated network cutover. Semantics in [runtime-execution](runtime-execution.md).
- **`ContinuousEpochSync`** — activates at **v85** (`version.rs:605`). Changes the epoch-sync proof payload carried by `PeerMessage::EpochSyncRequest`/`EpochSyncResponse` (the 512 KiB / 512 MiB size classes of §9.3 and the 1-per-30s rate limit of §8 apply regardless). Semantics in [sync](sync.md).
- **`DynamicResharding`** — activates at **v85** (`version.rs:606`). Changes how the shard set evolves, which is what populates `Handshake.sender_chain_info.tracked_shards` and `SnapshotHostInfo.shards`; the receive-side caps of §9.4 (128 / 512) are sized to leave headroom above any realistic shard count and are not version-gated. Semantics in [sharding-chunks](sharding-chunks.md).
- **`RejectDelegateV2`** — activates at **v87** (`version.rs:621`). Rejects `Action::DelegateV2` in action validation; it narrows what a `Transaction`/`ForwardTx` payload may legally contain but does not change the transport or the encoding. Semantics in [runtime-execution](runtime-execution.md).

No `ProtocolFeature` at v86 or v87 alters the handshake, tiering, BFS routing, or route-back behavior themselves. Note that `MIN_SUPPORTED_PROTOCOL_VERSION` moving 83 → 84 is itself a networking-visible change: a peer that handshakes with `protocol_version < 84` is now rejected with `ProtocolVersionMismatch` (`peer_actor.rs:564`).

## Invariants & failure modes

- **Routed signatures are verified before forwarding/delivery** — `msg.verify()` failing ⇒ ban the previous sender (`peer_actor.rs:1360`). V3 makes the signature optional (`mod.rs:1221`); `RoutedMessageV3::verify` returns false when the signature is absent (`mod.rs:1251`), and `msg_v1()` logs an error and substitutes `Signature::default()` if it ever has to serialize an unsigned V3 (`mod.rs:1322`).
- **TTL bounds hops** — `decrease_ttl` saturates at 0 and a 0-TTL message is dropped (`mod.rs:1262`, `network_state/mod.rs:1204`), preventing routing loops; default TTL 100 (`types.rs:50`).
- **Tier is fixed, not negotiable** — a tier mismatch in a handshake reply is treated as malicious and the connection is dropped (`peer_actor.rs:552`); a message not allowed on the connection's tier closes it with `DisallowedMessage` (`connection/mod.rs:31`, `peer_actor.rs:1707`).
- **Self-addressed messages** — dropped by default; only bodies with `allow_sending_to_self()` (witnesses/endorsements) are looped back locally (`network_state/mod.rs:845`, `mod.rs:794`); a routed message targeting our own `PeerId` is dropped with `CONNECTED_TO_MYSELF` (`network_state/mod.rs:756`).
- **In-flight inbound bytes ≤ 1 GB node-wide** — enforced by acquiring the permit before reading the body (`stream.rs:235`); over-subscription manifests as read backpressure plus the 2-minute `READ_TIMEOUT`, not as a drop.
- **Queued outbound bytes ≤ 3 GiB node-wide, ≤ 700 MiB per connection** — the global limiter drops the message (`peer_actor.rs:442`); the per-connection write-buffer cap closes the connection (`stream.rs:185`).
- **A message never exceeds its type's size class** — over-sized frames are dropped after decode (`peer_actor.rs:1668`); note the check is on the *encoded* length, and it is a drop, not a ban.
- **Nothing unbounded is allocated before authentication** — frame ≤ 512 MiB (`stream.rs:224`), tracked-shard list ≤ 128 (`proto_conv/handshake.rs:63`), snapshot-host shard list ≤ 512 (`proto_conv/peer_message.rs:149`), transaction borsh ≤ 16 MiB and routed borsh ≤ 64 MiB (`proto_conv/peer_message.rs:476`, `:482`).
- **Caches and gossip inputs are abuse-resistant** — `RouteBackCache` evicts the heaviest peer's oldest entries first (`route_back_cache.rs:115`); `AccountData` payloads are size-capped at 10 kB, though only on the *send* side, in `VersionedAccountData::sign` (`mod.rs:219`) — inbound `SyncAccountsData` is bounded instead by the 32 MiB size class and the 10/s rate limit; `SyncRoutingTable` edge and account counts are capped per message (`peer_actor.rs:1440`, `:1467`) and rate-limited to 10-burst/1-per-second (`messages_limits.rs:125`); edges with far-future nonces are rejected (`graph/mod.rs:228`); transactions per inter-block window are capped (`peer_actor.rs:1350`).
- **An unsolicited `TxStatusResponse` is never delivered to the client** — it must arrive on a route-back hash bound to a request this node sent, to the same responder, of the same kind (`network_state/mod.rs:676`).
- **Banning an abusive peer stops processing its message** — every `stop(Ban(..))` in `handle_msg_ready`'s `PeersResponse` arm is followed by `return` (`peer_actor.rs:1134`, `:1139`).
- **Dropped (not error) on no route** — a missing forward route or route-back entry silently drops the message with a metric (`MessageDropped::NoRouteFound`, `FindRouteError::RouteBackNotFound`, `routing.rs:152`); there is no retransmit beyond `message_resend_count`.
- **Nonce monotonicity** — connection edges use increasing, timestamp-bounded nonces; a stale nonce is answered with `LastEdge` evidence rather than accepted (`peer_actor.rs:610`).
- **Graph size caps** — `Inner::update_edge` enforces `max_total_edges` (default 1 000 000, `config.rs:36`) and per-source `max_edges_per_source` (default 50 000, `config.rs:34`) (`graph/mod.rs:87`, `:94`); `bfs::Graph::add_edge` refuses edges that would exceed `max_graph_peers` (default 100 000, `config.rs:35`) (`bfs.rs:109`); BFS only uses the first 128 source-neighbors for the route bitset (`bfs.rs:167`). Limit-triggered drops are non-punitive (`graph/mod.rs:184`).

## Code anchors

| Location | Symbol | What happens here |
|----------|--------|-------------------|
| `chain/network/src/network_protocol/mod.rs:418` | `PeerMessage` | Top-level wire message enum |
| `chain/network/src/network_protocol/mod.rs:501` | `PeerMessage::max_size` | Per-message-type size class (512 KiB / 32 MiB / 128 MiB / 512 MiB) |
| `chain/network/src/network_protocol/mod.rs:355` | `Handshake` | Handshake payload (version/genesis/peer-id/edge/owned-account) |
| `chain/network/src/network_protocol/mod.rs:375` | `HandshakeFailureReason` | ProtocolVersionMismatch / GenesisMismatch / InvalidTarget |
| `chain/network/src/network_protocol/mod.rs:200` | `MAX_TRACKED_SHARDS_PER_PEER` | Pre-auth cap on handshake `tracked_shards` = 128 |
| `chain/network/src/network_protocol/mod.rs:189` | `MAX_SHARDS_PER_SNAPSHOT_HOST_INFO` | Cap on `SnapshotHostInfo.shards` = 512 |
| `chain/network/src/network_protocol/mod.rs:582` | `TieredMessageBody` | T1/T2 split of routed bodies |
| `chain/network/src/network_protocol/mod.rs:764` | `T1MessageBody` | Consensus-critical routed payloads (3× resend for some) |
| `chain/network/src/network_protocol/mod.rs:820` | `T2MessageBody` | Multi-hop gossip routed payloads |
| `chain/network/src/network_protocol/mod.rs:840` | `RoutedResponseKind` | Pong / TxStatusResponse / PartialEncodedChunkResponse |
| `chain/network/src/network_protocol/mod.rs:861` | `T2MessageBody::must_arrive_on_route_back` | True only for `TxStatusResponse` |
| `chain/network/src/network_protocol/mod.rs:866` | `T2MessageBody::requested_response_kind` | Which reply a request asks for |
| `chain/network/src/network_protocol/mod.rs:1221` | `RoutedMessageV3` | Current routed in-memory form: tiered body, optional signature, ttl, num_hops |
| `chain/network/src/network_protocol/mod.rs:1303` | `RoutedMessage::build_hash` | Borsh hash of {target, author, body} — signed & route-back key |
| `chain/network/src/network_protocol/mod.rs:1313` | `RoutedMessage::msg_v1` | Down-converts to V1 for wire serialization |
| `chain/network/src/network_protocol/mod.rs:1683` | `RawRoutedMessage::sign` | Produces a signed V3 with default TTL/created_at |
| `chain/network/src/network_protocol/mod.rs:1515` | `PeerIdOrHash` | Forward (PeerId) vs. route-back (Hash) target |
| `chain/network/src/network_protocol/proto_conv/peer_message.rs:297` | `RoutedMessage` proto conv | Serializes V1 borsh into `proto::RoutedMessage.borsh` |
| `chain/network/src/network_protocol/proto_conv/peer_message.rs:28` | `MAX_TRANSACTION_SIZE_BYTES` | Pre-decode cap on `Transaction` borsh = 16 MiB |
| `chain/network/src/network_protocol/proto_conv/peer_message.rs:34` | `MAX_ROUTED_MESSAGE_SIZE_BYTES` | Pre-decode cap on routed borsh = 64 MiB |
| `chain/network/src/network_protocol/proto_conv/peer_message.rs:149` | `TryFrom<&proto::SnapshotHostInfo>` | Shard-list cap before `Vec<ShardId>` allocation |
| `chain/network/src/network_protocol/proto_conv/handshake.rs:63` | `TryFrom<&proto::PeerChainInfo>` | `tracked_shards` cap before allocation |
| `chain/network/src/network_protocol/proto_conv/util.rs:20` | `try_from_slice_with_limit` | Size-bounded borsh deserialization helper |
| `chain/network/src/types.rs:50` | `ROUTED_MESSAGE_TTL` | Default TTL = 100 |
| `chain/network/src/types.rs:451` | `NetworkRequestWithPermit` | `NetworkRequests` + pre-acquired outgoing permit |
| `chain/network/src/tcp.rs:22` | `Tier` | T1/T2/T3 overlay definitions |
| `chain/network/src/peer_manager/connection/mod.rs:31` | `Tier::is_allowed_receive` | Which message types each tier accepts |
| `chain/network/src/peer_manager/connection/mod.rs:70` | `Tier::is_allowed_send_routed` | T2 body only over T2; T1 body anywhere |
| `chain/network/src/peer/peer_actor.rs:470` | `PeerActor::send_handshake` | Builds & sends the outbound Handshake |
| `chain/network/src/peer/peer_actor.rs:526` | `PeerActor::process_handshake` | Handshake validation & edge creation |
| `chain/network/src/peer/peer_actor.rs:404` | `PeerActor::send_message_inner` | Outgoing-queue permit acquire/shrink; drop on saturation |
| `chain/network/src/peer/peer_actor.rs:1134` | `PeersResponse` arm | Ban + `return` on abusive peer lists |
| `chain/network/src/peer/peer_actor.rs:1319` | `PeerActor` Routed arm | Dedup, tx cap, verify, dispatch ForMe/Forward |
| `chain/network/src/peer/peer_actor.rs:1431` | `handle_sync_routing_table` | Edge cap, edge merge, account cap, account merge |
| `chain/network/src/peer/peer_actor.rs:1634` | `Handler<IncomingFrame>` | Decode → size class check → rate limit → tier check → handle |
| `chain/network/src/peer/stream.rs:201` | `FramedStream::run_recv_loop` | Frame cap, incoming semaphore acquire, read timeout |
| `chain/network/src/peer/stream.rs:173` | `FramedStream::send` | Per-connection write-buffer cap |
| `chain/network/src/recv_permit.rs:8` | `RecvMessagePermit` | Inbound memory reservation held for a message's lifetime |
| `chain/network/src/concurrency/outgoing_queue_limiter.rs:17` | `OutgoingQueueLimiter::try_acquire` | Non-blocking outbound byte reservation |
| `chain/network/src/peer_manager/network_state/mod.rs:92` | `INCOMING_SEMAPHORE_PERMITS` | Node-wide inbound memory bound = 1 GB |
| `chain/network/src/peer_manager/network_state/mod.rs:95` | `EPOCH_SYNC_RESPONSE_BYTES` | Pre-reservation for an epoch-sync response = 300 MiB |
| `chain/network/src/peer_manager/network_state/mod.rs:98` | `STATE_SYNC_RESPONSE_BYTES` | Pre-reservation for a state-sync response = 30 MiB |
| `chain/network/src/peer_manager/network_state/mod.rs:664` | `expected_response_for_own_request` | Binds a route-back entry to responder + reply kind |
| `chain/network/src/peer_manager/network_state/mod.rs:676` | `unsolicited_response_reason` | Why a routed reply answers no request of ours |
| `chain/network/src/peer_manager/network_state/mod.rs:746` | `send_message_to_peer` | Per-tier next-hop selection & route-back insert |
| `chain/network/src/peer_manager/network_state/mod.rs:835` | `send_message_to_account` | Account→peer resolution, T1 fast path, resend |
| `chain/network/src/peer_manager/network_state/mod.rs:932` | `receive_routed_message` | Body demux to actors, carrying the recv permit |
| `chain/network/src/peer_manager/network_state/mod.rs:1158` | `process_incoming_routed` | Unsolicited-reply drop, for-me check, add_route_back, TTL/forward |
| `chain/network/src/peer_manager/network_state/routing.rs:143` | `tier2_find_route` | RoutingTableView next-hop or route-back pop |
| `chain/network/src/peer_manager/network_state/routing.rs:163` | `add_route_back` | Records prev-hop for response-bearing requests (unbound) |
| `chain/network/src/peer_manager/network_state/tier1.rs:203` | `tier1_connect` | Establishes/prunes T1 connections from AccountData |
| `chain/network/src/peer_manager/network_state/tier1.rs:348` | `get_tier1_proxy` | Picks a direct T1 peer or advertised proxy |
| `chain/network/src/routing/graph/mod.rs:192` | `Inner::add_edges` | Dedup, prune window, future-nonce reject, cap pre-check, verify |
| `chain/network/src/routing/graph/mod.rs:31` | `EDGE_NONCE_FUTURE_TOLERANCE` | Edge nonce future bound = 5 min |
| `chain/network/src/routing/graph/mod.rs:162` | `prune_unreachable_peers` | Fixed `peer_reachable_at` retention/selection |
| `chain/network/src/routing/graph/mod.rs:407` | `Graph::update` | Synchronous Mutex-guarded edge add + snapshot publish |
| `chain/network/src/routing/bfs.rs:148` | `bfs::Graph::calculate_next_hops_and_distance` | BFS shortest-path next-hop computation |
| `chain/network/src/routing/route_back_cache.rs:115` | `RouteBackCache::remove_frequent` | Anti-poisoning eviction |
| `chain/network/src/routing/route_back_cache.rs:239` | `RouteBackCache::insert` | Stores prev-hop + optional `ExpectedResponse` |
| `chain/network/src/routing/routing_table_view/mod.rs:35` | `find_next_hop` | Load-balanced next-hop pick over equal-cost paths |
| `chain/network/src/peer_manager/peer_store/mod.rs:419` | `PeerStore::peer_ban` | Marks a peer Banned(reason, now) |
| `chain/network/src/peer_manager/peer_store/mod.rs:439` | `unconnected_peer` | Candidate selection for outbound dials |
| `chain/network/src/peer_manager/peer_manager_actor.rs:90` | `MAX_TIER2_PEERS` | T2 mesh cap = 128 |
| `chain/network/src/peer_manager/peer_manager_actor.rs:462` | `is_outbound_bootstrap_needed` | Whether to dial more outbound peers |
| `chain/network/src/peer_manager/peer_manager_actor.rs:543` | `maybe_stop_active_connection` | safe_set computation + evict excess |
| `chain/network/src/peer_manager/peer_manager_actor.rs:838` | `handle_msg_network_requests` | Outbound NetworkRequests → PeerMessages |
| `chain/network/src/peer_manager/peer_manager_actor.rs:1571` | `Handler<Tier3Request>` | Reserves 30 MiB before producing a state response; Busy if it can't |
| `chain/network/src/rate_limits/messages_limits.rs:49` | `RateLimits::is_allowed` | Per-kind token-bucket throttle |
| `chain/network/src/rate_limits/messages_limits.rs:105` | `Config::standard_preset` | Default per-kind rate limits (now covers ~35 message kinds) |
| `chain/network/src/network_protocol/state_sync.rs:28` | `SnapshotHostInfo` | Signed state-snapshot availability advertisement |
| `chain/network/src/network_protocol/mod.rs:140` | `AccountData` | Validator peer-id + proxies for T1 discovery |
| `chain/network/src/config.rs:33` | `DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_MESSAGE` | Per-message edge ingress cap = 50 000 |
| `chain/network/src/config.rs:39` | `DEFAULT_ROUTING_GRAPH_MAX_ACCOUNTS_PER_MESSAGE` | Per-message account ingress cap = 10 000 |
| `chain/network/src/config.rs:42` | `DEFAULT_OUTGOING_QUEUE_LIMITER_CAPACITY_BYTES` | Node-wide outbound queue bound = 3 GiB |
| `chain/network/src/config.rs:44` | `DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES` | Per-connection write-buffer bound = 700 MiB |
| `chain/network/src/config.rs:50` | `PEERS_RESPONSE_MAX_PEERS` | `PeersResponse` abuse threshold = 512 |
| `core/primitives-core/src/version.rs:652` | `MIN_SUPPORTED_PROTOCOL_VERSION` | Wire-version floor = 84 (was 83 at 2.13.0) |

## Open questions

- The `account_announcements` (`AnnounceAccount`) account→peer table is explicitly being deprecated in favor of `accounts_data` (comment at `network_state/mod.rs:903`); the exact removal version / protocol-compat plan is still not encoded in code.
- At v87 *every* peer emits V1 borsh on the wire — outbound serialization unconditionally calls `msg_v1()` (`proto_conv/peer_message.rs:297`) — so the V3 proto variant remains decode-only. TODO #13709 (`mod.rs:1312`) tracks removing V1 support after a forward-compatible release that starts emitting V3; the cutover version is still not fixed in code.
- `RoutedMessageV3.signature` is `Option`, but nothing currently produces an unsigned V3: `sign` always sets it and `msg_v1()` logs "signature is missing, this should not yet happen" and substitutes a default signature if it ever sees `None` (`mod.rs:1322`). Which T1 bodies are intended to eventually go unsigned, and what would then authenticate them, is not determinable from the code.
- The per-message-type size check (§9.3) compares the *protobuf frame* length against a limit whose classes were chosen per *decoded* body; for `Routed` messages the frame also carries the trace context and proto framing. Whether any legitimate payload sits close enough to a class boundary for that overhead to matter (notably 512 KiB for `VersionedChunkEndorsement`) was not determinable from code alone and is not covered by an assertion in production code.
- `EPOCH_SYNC_RESPONSE_BYTES` (300 MiB) and `STATE_SYNC_RESPONSE_BYTES` (30 MiB) are fixed worst-case estimates, not derived from any configured limit; if a real response ever exceeds the reservation the surplus is simply acquired late in `send_message_inner` (or the message is dropped there). No code enforces that a produced response fits its reservation.
- `docs/architecture/network.md`, `docs/NetworkSpec/*`, and `docs/advanced_configuration/networking.md` were not re-read line-by-line for this pass; the spec is derived from source. They predate the T1/T2/T3 tier split, `RoutedMessageV3`, and all of the 2.14.0 hardening work, so cross-check before relying on them.

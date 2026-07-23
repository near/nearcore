# Networking / P2P

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/network/src/network_protocol/mod.rs`, `chain/network/src/network_protocol/network.proto`, `chain/network/src/network_protocol/proto_conv/peer_message.rs`, `chain/network/src/peer/peer_actor.rs`, `chain/network/src/peer_manager/peer_manager_actor.rs`, `chain/network/src/peer_manager/network_state/{mod,routing,tier1}.rs`, `chain/network/src/peer_manager/connection/mod.rs`, `chain/network/src/peer_manager/peer_store/mod.rs`, `chain/network/src/routing/{bfs,route_back_cache}.rs`, `chain/network/src/routing/routing_table_view/mod.rs`, `chain/network/src/rate_limits/messages_limits.rs`, `chain/network/src/network_protocol/state_sync.rs`, `chain/network/src/tcp.rs`, `chain/network/src/config.rs`, `chain/network/src/types.rs`

## Role

The P2P layer is the transport for everything else in the protocol: it establishes and maintains TCP connections to peers, negotiates a wire protocol on connect, gossips blocks/transactions, and routes targeted messages (chunk parts, approvals, witnesses, endorsements, state-part requests) to specific validators or peers — possibly over multiple hops. It exposes two delivery models: **broadcast** (send to every directly-connected peer) and **routed** (deliver to a single `AccountId`/`PeerId`, finding a path via a routing graph). It feeds raw messages up into [chain-block-processing](chain-block-processing.md), [sync](sync.md), [sharding-chunks](sharding-chunks.md), [stateless-validation](stateless-validation.md), and [consensus-finality](consensus-finality.md), and carries those components' outbound messages back onto the wire. The semantics of payloads belong to those specs; this spec covers only how the bytes travel. Peer discovery for the validator overlay (TIER1) is bootstrapped over the general gossip overlay (TIER2).

## Key data structures

- **`PeerMessage`** — `chain/network/src/network_protocol/mod.rs:399` — the top-level wire enum. Variants split into TCP-protocol traffic (`Tier1Handshake`/`Tier2Handshake`/`Tier3Handshake`, `HandshakeFailure`, `LastEdge`, `SyncRoutingTable`, `RequestUpdateNonce`, `SyncAccountsData`, `PeersRequest`/`PeersResponse`, `Disconnect`), direct payloads (`Block`, `OptimisticBlock`, `BlockHeaders`, `Transaction`, `BlockRequest`, `BlockHeadersRequest`, state-sync requests/responses, `EpochSyncRequest`/`Response`, `SyncSnapshotHosts`), and the catch-all `Routed(Box<RoutedMessage>)`. Serialized as protobuf (`PeerMessage::serialize`, `chain/network/src/network_protocol/mod.rs:454`), not borsh; a tracing span context is injected into every message.
- **`Handshake`** — `chain/network/src/network_protocol/mod.rs:336` — `protocol_version`, `oldest_supported_version`, `sender_peer_id`, `target_peer_id`, `sender_listen_port`, `sender_chain_info: PeerChainInfoV2` (genesis id, height, tracked shards, archival flag, built in `send_handshake`, `chain/network/src/peer/peer_actor.rs:443`), `partial_edge_info: PartialEdgeInfo` (a nonce + half-signature used to build the connection's `Edge`), and an optional `owned_account: SignedOwnedAccount` (validator-key ownership proof).
- **`HandshakeFailureReason`** — `chain/network/src/network_protocol/mod.rs:356` — `ProtocolVersionMismatch { version, oldest_supported_version }`, `GenesisMismatch(GenesisId)`, `InvalidTarget`.
- **`TieredMessageBody`** — `chain/network/src/network_protocol/mod.rs:487` — `T1(Box<T1MessageBody>)` or `T2(Box<T2MessageBody>)`. The tier of a routed payload is a property of the body type, not negotiated. `from_routed` (`mod.rs:531`) maps a flat `RoutedMessageBody` into the tiered form; `From<TieredMessageBody> for RoutedMessageBody` (`mod.rs:910`) maps back.
- **`T1MessageBody`** — `chain/network/src/network_protocol/mod.rs:648` — consensus/production-critical routed payloads: `BlockApproval`, `VersionedPartialEncodedChunk`, `PartialEncodedChunkForward`, `PartialEncodedStateWitness(+Forward)`, `VersionedPartialEncodedStateWitness(+Forward)`, `VersionedChunkEndorsement`, `ChunkContractAccesses`, `ContractCodeRequest`/`ContractCodeResponse`, plus Spice variants. `message_resend_count` (`mod.rs:669`) returns `IMPORTANT_MESSAGE_RESENT_COUNT = 3` (`mod.rs:78`) for `BlockApproval`/`VersionedPartialEncodedChunk`, else 1.
- **`T2MessageBody`** — `chain/network/src/network_protocol/mod.rs:704` — multi-hop gossip-overlay payloads: `ForwardTx`, `TxStatusRequest`/`TxStatusResponse`, `PartialEncodedChunkRequest`/`Response`, `Ping`/`Pong`, `ChunkStateWitnessAck`, `StatePartRequest`, `PartialEncodedContractDeploys`, `StateHeaderRequest`, `StateRequestAck`.
- **`RoutedMessageBody`** — `chain/network/src/network_protocol/mod.rs:744` — the flat body (explicit borsh discriminants) used for V1/V2 wire encoding and for hashing.
- **`RoutedMessage`** — `chain/network/src/network_protocol/mod.rs:1135` — `V1(RoutedMessageV1)`, `V2(RoutedMessageV2)`, `V3(RoutedMessageV3)`. V1 = `{target: PeerIdOrHash, author, signature, ttl, body: RoutedMessageBody}` (`mod.rs:1014`); V2 adds `created_at`, `num_hops` (`mod.rs:1031`); V3 (`mod.rs:1063`) carries `body: TieredMessageBody` and a now-**optional** `signature`. All accessors (`target`, `author`, `body`, `verify`, `decrease_ttl`, `hash`, …) dispatch over the variant; `upgrade_to_v3` (`mod.rs:1293`) normalizes V1/V2 to V3 in memory. In-memory the node always builds V3 (`RawRoutedMessage::sign`, `mod.rs:1534`), but the wire encoding is always V1 borsh: outbound serialization calls `msg_v1()` (`mod.rs:1164`) and writes it into the `proto::RoutedMessage.borsh` field (`chain/network/src/network_protocol/proto_conv/peer_message.rs:272`), with `created_at`/`num_hops` carried as separate proto fields (`peer_message.rs:273`, `:281`). A `proto::RoutedMessageV3` parse path exists on receive but nothing emits it today (TODO #13709, `mod.rs:1163`).
- **`PeerIdOrHash`** — `chain/network/src/network_protocol/mod.rs:1366` — a routed message's target: `PeerId(..)` (forward toward a peer) or `Hash(..)` (route the response *back* along the path the request took; the hash identifies the route-back cache entry).
- **`Edge`** — `chain/network/src/network_protocol/edge.rs` — a signed, nonce-stamped assertion that two peers are connected. Edges are the adjacency proofs exchanged via `SyncRoutingTable(RoutingTableUpdate)` (`mod.rs:315`) and fed into the routing `Graph`.
- **`Graph`** — `chain/network/src/routing/bfs.rs:10` — the undirected connectivity graph used for BFS routing. Peers are interned to `u32` ids (`p2id`/`id2p`); `adjacency` stores both directions; `max_peers` caps the number of distinct peers (`add_edge` refuses to grow past it, `bfs.rs:116`).
- **`RouteBackCache`** — `chain/network/src/routing/route_back_cache.rs:49` — maps a request's message hash → the peer it arrived from, so responses (`PeerIdOrHash::Hash`) can be sent back along the same path. Capacity-bounded with anti-poisoning eviction (default `DEFAULT_CAPACITY = 100_000`, `route_back_cache.rs:8`; `evict_timeout` field at `:53`).
- **`RoutingTableView`** — `chain/network/src/routing/routing_table_view/mod.rs` (inner `Inner` at `:15`) — holds the computed `next_hops` (target → set of first-hop neighbors) and `distance` tables, plus a `last_routed` LRU (`:29`) used to load-balance across equal-cost next hops.
- **`SnapshotHostInfo` / `SyncSnapshotHosts`** — `chain/network/src/network_protocol/state_sync.rs:27` — a peer's signed advertisement of which shards it can serve state for (`peer_id`, `sync_hash`, `epoch_height`, `shards`, `signature` over `(sync_hash, epoch_height, shards)`, `state_sync.rs:36`).
- **`AccountData` / `SignedAccountData`** — `chain/network/src/network_protocol/mod.rs:132`, `:247` — a validator's broadcast of its `peer_id` and TIER1 `proxies`; the global, bounded state that lets TIER1 nodes find each other (`MAX_ACCOUNT_DATA_SIZE_BYTES = 10000`, `mod.rs:168`, enforced in `AccountData::sign`, `mod.rs:200`).
- **`tcp::Tier`** — `chain/network/src/tcp.rs:22` — `T1` (`:25`, validator consensus overlay), `T2` (`:29`, general gossip P2P), `T3` (`:34`, ad-hoc direct connections for large transfers, e.g. state parts).
- **`RateLimits`** — `chain/network/src/rate_limits/messages_limits.rs:13` — per-connection map of `TokenBucket` keyed by message kind, used to throttle inbound messages.

## Behavior

### 1. Tiers (T1 / T2 / T3)

`tcp::Tier` (`chain/network/src/tcp.rs:22`) defines three overlays:
- **T1** (`tcp.rs:25`) — connections between BFT consensus participants (or their proxies), reserved for production-critical messages. Established only by/for validators; bootstrapped via `AccountData` learned over T2 (`chain/network/src/peer_manager/network_state/tier1.rs`).
- **T2** (`tcp.rs:29`) — the general P2P gossip mesh used for everything else and for discovering T1 peers; this is the default overlay where BFS routing happens.
- **T3** (`tcp.rs:34`) — created ad hoc to ship one large message (state parts). The *request* travels over T2; the responder dials a direct T3 connection back to send the response, so the bulk transfer does not block other traffic. `TIER3_IDLE_TIMEOUT = 15s` (`chain/network/src/peer_manager/peer_manager_actor.rs:102`).

A routed payload's tier is fixed by its body type: `Tier::is_allowed_send_routed` (`chain/network/src/peer_manager/connection/mod.rs:69`) allows any `T1` body on any tier but allows a `T2` body only over a T2 connection. T1 traffic thus prefers the T1 overlay when available and falls back to T2 routing.

### 2. Handshake

A handshake is a single `PeerMessage::{Tier1,Tier2,Tier3}Handshake(Handshake)` exchange; the variant fixes the tier (the tier is **not** negotiable — `chain/network/src/peer/peer_actor.rs:518`). The outbound side (`send_handshake`, `peer_actor.rs:436`) sends a `Handshake` carrying `protocol_version = spec.protocol_version`, `oldest_supported_version = MIN_SUPPORTED_PROTOCOL_VERSION` (`peer_actor.rs:445`), its peer id/listen port/chain info, a `partial_edge_info` (nonce + half-signature), and — if a validator — a `SignedOwnedAccount`.

`PeerActor::process_handshake` (`peer_actor.rs:492`) validates, in order:

1. **Outbound responder check** (`peer_actor.rs:499`): for a connection we initiated, the reply's `protocol_version`, `genesis_id`, `sender_peer_id`, and `tier` must each match what we proposed, else `stop(HandshakeFailed)`.
2. **Inbound version gate** (`peer_actor.rs:530`): if `protocol_version < MIN_SUPPORTED_PROTOCOL_VERSION` or `> PROTOCOL_VERSION`, reply `HandshakeFailure(ProtocolVersionMismatch { version: PROTOCOL_VERSION, oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION })` (`peer_actor.rs:539`).
3. **Genesis gate** (`peer_actor.rs:547`): mismatched `genesis_id` → `HandshakeFailure(GenesisMismatch(..))` (`peer_actor.rs:551`).
4. **Target check** (`peer_actor.rs:555`): if `target_peer_id != my id` → `HandshakeFailure(InvalidTarget)` (`peer_actor.rs:559`).
5. **Nonce sanity** (`peer_actor.rs:565`): `verify_nonce` (timestamp-derived bound); on a too-low nonce vs. an existing local edge, reply `LastEdge(..)` as evidence (`peer_actor.rs:577`) instead of a new edge.
6. **Owned-account proof** (`peer_actor.rs:585`): if present, the signature must verify (else `Ban(InvalidSignature)`, `peer_actor.rs:587`), `owned_account.peer_id` must equal `sender_peer_id` (else `OwnedAccountMismatch`, `peer_actor.rs:591`), and the timestamp must be within `MAX_CLOCK_SKEW = 30min` (`peer_actor.rs:58`) (else `TooLargeClockSkew`, `peer_actor.rs:595`).

On success it forms the connection's signed `Edge` from both half-signatures (`Edge::new`, `peer_actor.rs:610`), builds a `connection::Connection`, and registers it with the `PeerManager` (which may still reject for connection-count/duplicate reasons). When the initiator receives a `ProtocolVersionMismatch` failure it computes `common_version = min(peer.version, PROTOCOL_VERSION)` and retries the handshake at that version, but only if `common_version` is ≥ both the peer's advertised `oldest_supported_version` and its own `MIN_SUPPORTED_PROTOCOL_VERSION`; otherwise it gives up (`peer_actor.rs:879`).

The negotiated `protocol_version` is the **wire** protocol version (the same `PROTOCOL_VERSION` integer that gates on-chain features) and determines which `RoutedMessage` variant and message encodings are mutually understood; it is enforced per-connection, independent of the chain's current epoch protocol version.

### 3. Routed messaging: signing, target selection, forwarding

**Construction & signing.** A `RawRoutedMessage { target, body }` (`mod.rs:1526`) is signed by `RawRoutedMessage::sign` (`mod.rs:1534`) → always a `RoutedMessage::V3` with `ttl = routed_message_ttl` (default `ROUTED_MESSAGE_TTL = 100`, `chain/network/src/types.rs:47`), `created_at = now`, `num_hops = 0`, and a signature over `RoutedMessage::build_hash(target, author, body)` (the borsh hash of `RoutedMessageNoSignature{target, author, body}`, `mod.rs:1154`). `NetworkState::sign_message` (`chain/network/src/peer_manager/network_state/mod.rs:677`) wraps this with the node key.

**Send to an `AccountId`** — `NetworkState::send_message_to_account` (`network_state/mod.rs:770`):
1. If the target account is *this* node's validator account, deliver locally via `receive_routed_message` instead of the wire (`network_state/mod.rs:790`); a `debug_assert!(msg.allow_sending_to_self())` (`network_state/mod.rs:780`) documents the expectation that only self-deliverable bodies (witnesses, endorsements — `mod.rs:678`) reach this path.
2. If `Tier::T1.is_allowed_send_routed(body)` (i.e. a T1 body, `network_state/mod.rs:803`), look up the account's `SignedAccountData`, pick a TIER1 peer via `get_tier1_proxy` (`tier1.rs:351`) — a direct T1 connection if present, else one of the validator's advertised proxies — and send directly over T1. No BFS, single hop.
3. Otherwise resolve the account → `PeerId`: prefer the peer id from `accounts_data` (`network_state/mod.rs:827`), fall back to the (being-deprecated) `account_announcements` table (`network_state/mod.rs:841`). If neither resolves, drop with `MessageDropped::UnknownAccount` (`network_state/mod.rs:846`).
4. Sign once and `send_message_to_peer` over T2 `message_resend_count()` times (3× for important bodies, else 1×, `network_state/mod.rs:861`).

**Send to a `PeerId`/`Hash`** — `NetworkState::send_message_to_peer` (`network_state/mod.rs:687`), per tier:
- **T1**: if `target` is a `Hash`, pop the responder from `tier1_route_back` (`network_state/mod.rs:710`); if a `PeerId`, send directly. T1 does not BFS.
- **T2**: `tier2_find_route` (`routing.rs:143`) — for a `PeerId`, ask `RoutingTableView::find_next_hop_for_target`; for a `Hash`, pop the next hop from `tier2_route_back` (`routing.rs:156`). If we are the author and the message `expect_response()`, insert our own id into `tier2_route_back` under `msg.hash()` first (`network_state/mod.rs:728`) so the eventual response can be routed back to us. A `FindRouteError` drops the message (`MessageDropped::NoRouteFound`, `network_state/mod.rs:738`).
- **T3**: direct `PeerId` only (a `Hash` target is a bug).

**Receiving / forwarding** — `PeerActor` Routed arm (`peer_actor.rs:1263`) then `NetworkState::process_incoming_routed` (`network_state/mod.rs:1071`):
1. Drop messages duplicated within `DROP_DUPLICATED_MESSAGES_PERIOD = 50ms` (`peer_actor.rs:67`) keyed by `(author, target, signature)` (`peer_actor.rs:1270`).
2. For `ForwardTx`, drop if `txns_since_last_block > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE = 1000` (`peer_actor.rs:63`, `:1294`), else increment the counter (`peer_actor.rs:1298`). The counter is reset to 0 on each new block (`peer_actor.rs:980`).
3. `msg.verify()` (signature check) — failure ⇒ `Ban(InvalidSignature)` (`peer_actor.rs:1304`).
4. `process_incoming_routed`: compute `message_for_me(target)` (`network_state/mod.rs:634`) — a `PeerId` match against our id, or for a `Hash`, a `compare_route_back` lookup (`network_state/mod.rs:639`). If for me, a network-wide dedup over `recent_routed_messages` records whether this copy was the fastest (`network_state/mod.rs:1083`). **Then always** call `add_route_back` (`network_state/mod.rs:1087`, `routing.rs:163`): if the body `expect_response()` (Ping/TxStatusRequest/PartialEncodedChunkRequest, `mod.rs:1099`), record `from` under `msg.hash()` in the tier's route-back cache.
5. If for me ⇒ `RoutedAction::ForMe` (`network_state/mod.rs:1090`); Ping/Pong are handled inline in the peer actor (`peer_actor.rs:1336`), everything else goes to `receive_routed_message` dispatch.
6. Else `decrease_ttl()` (`mod.rs:1113`): if still > 0, increment `num_hops` via `num_hops_mut` and return `RoutedAction::Forward` (re-enter `send_message_to_peer` to the next hop); if it hit 0, drop (`ROUTED_MESSAGE_DROPPED`, `network_state/mod.rs:1100`).

`receive_routed_message` (`network_state/mod.rs:866`) is the demux that hands each `T1*`/`T2*` body to the right adapter (client, shards manager, partial-witness actor, etc.).

### 4. BFS routing over the connectivity graph

`Graph::calculate_next_hops_and_distance` (`bfs.rs:148`) runs a single BFS from `source` over the undirected adjacency. For each reachable node it records `distance` and a `u128` bitset `routes` of which **direct neighbors of source** lie on *some* shortest path (one bit per source-neighbor; only the first `MAX_TIER2_PEERS = 128` neighbors get bits, `bfs.rs:167`, `peer_manager_actor.rs:89`). `compute_result` (`bfs.rs:203`) expands each bitset into a `Vec<PeerId>` of candidate first hops. `unreliable_peers` are excluded as *transit* nodes: they are not enqueued (so paths never traverse them, `bfs.rs:168`), but a direct edge to them is still kept so messages addressed *to* them work (asserted by `graph_distance4_with_unreliable_nodes`, `bfs.rs:427`). The result feeds `RoutingTableView::update` (`routing_table_view/mod.rs:72`). `find_next_hop` (`routing_table_view/mod.rs:35`) picks, among equal-cost next hops, the one least-recently routed through (`last_routed` LRU, `:39`) — spreading load across paths.

The graph is built from `Edge`s exchanged via `SyncRoutingTable(RoutingTableUpdate)` (§6) plus the node's own local edges; `set_unreliable_peers` (`routing.rs:191`) marks peers to avoid as transit.

### 5. Route-back cache

Responses to a request are addressed `PeerIdOrHash::Hash(request_hash)` rather than a peer id, because the requester may be many hops away and the responder need not know a forward route. `RouteBackCache` (`route_back_cache.rs`) stores `hash → (arrival_time, prev_hop)`. Every `insert` (`route_back_cache.rs:218`) first calls `remove_evicted` (`route_back_cache.rs:141`); when the cache is full it runs `remove_frequent` (`:98`) **first** — dropping the oldest batch from the peer holding the most entries — and **then** drops any entries older than `evict_timeout` (`:146`) — bounding a single abuser to roughly `capacity / active_connections` slots (asserted by the `poison_attack` test, `:421`). `tier2_find_route` `remove`s (consumes) the entry when routing a response back.

### 6. Routing-table / peer-info exchange

Peers periodically gossip `PeerMessage::SyncRoutingTable(RoutingTableUpdate { edges, accounts })` (`mod.rs:315`). On receipt, `PeerActor::handle_sync_routing_table` (`peer_actor.rs:1371`): an ingress cap of `routing_graph_max_edges_per_message` (default `DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_MESSAGE = 50_000`, `chain/network/src/config.rs:32`) drops the edge set wholesale if exceeded (`peer_actor.rs:1380`, still processing accounts); otherwise `NetworkState::add_edges` (`routing.rs:99`) validates and merges them into the `Graph` (a bad edge ⇒ ban the source). `AnnounceAccount`s are forwarded to the client for signature verification and staleness filtering before being adopted (`peer_actor.rs:1411`). New local edges are themselves broadcast (`broadcast_routing_table_update`, `routing.rs:20`). `PeersRequest`/`PeersResponse` (`mod.rs:372`, `:381`) exchange `PeerInfo`s (and direct peers) for discovery.

Validator discovery rides on top: `SyncAccountsData` (`mod.rs:364`) propagates `SignedAccountData` (validator ↔ peer-id ↔ proxies) so `tier1_connect` (`tier1.rs:206`) can dial the right T1 peers/proxies; `SyncSnapshotHosts` propagates state-snapshot availability (§9).

### 7. Peer store, discovery, scoring, eviction

The `PeerStore` (`peer_store/mod.rs`) persists known peers with a `KnownPeerStatus` (`Unknown`/`NotConnected`/`Connected`/`Banned(reason, time)`). `PeerManagerActor::monitor_peers_trigger` (`peer_manager_actor.rs:668`) drives discovery: it first runs `PeerStore::update`, then if `is_outbound_bootstrap_needed` (`peer_manager_actor.rs:461`) — i.e. `total_connections < ideal_connections_lo`, or (`total_connections < max_num_peers` and `potential_outbound_connections < minimum_outbound_peers`), and `outbound_disabled` is false (`peer_manager_actor.rs:469`) — it picks an `unconnected_peer` (`peer_store/mod.rs:439`) — preferring previously-connected, address-bearing peers, with probability `PREFER_PREVIOUSLY_CONNECTED_PEER = 0.6` (`peer_manager_actor.rs:94`, `:683`) — and dials it. Reconnect retries up to `MAX_RECONNECT_ATTEMPTS = 6` (`peer_manager_actor.rs:70`, `network_state/mod.rs:601`).

When connections exceed `ideal_connections_hi`, `maybe_stop_active_connection` (`peer_manager_actor.rs:542`) builds a `safe_set` of connections to keep (earliest-established first up to `safe_set_size`, plus whitelisted/outbound/archival, `peer_manager_actor.rs:549`–`607`) and closes the rest. `PeerStore::update` (`peer_store/mod.rs:281`) periodically unbans peers whose `ban_window` has elapsed (`:245`), refreshes `last_seen` for connected peers (`:261`), and removes expired non-connected entries. The T2 mesh size is capped at `MAX_TIER2_PEERS = 128` (`peer_manager_actor.rs:89`).

### 8. Rate limiting & banning

Per-connection `RateLimits` (`rate_limits/messages_limits.rs:13`) maintains a `TokenBucket` per message kind; `is_allowed` (`:49`) acquires `cost` tokens and returns false (throttle/drop) when the bucket is empty. Buckets are configured per message type with `maximum_size`, `refill_rate`, optional `initial_size` (`messages_limits.rs:24`). Misbehavior bans are issued via `PeerStore::peer_ban` (`peer_store/mod.rs:419`, status → `Banned(reason, now)`, `:430`) and by `PeerActor` stopping with `ClosingReason::Ban(ReasonForBan::...)`: invalid routed signatures (`peer_actor.rs:1304`), invalid owned-account proof (`peer_actor.rs:587`), oversized `BlockHeadersRequest` (`> MAX_BLOCK_HEADER_HASHES = 20` ⇒ `Abusive`, `config.rs:44`, `network_state/mod.rs:1146`), bad edges (`peer_actor.rs:1140`), etc.

### 9. Block / chunk / tx / approval / witness propagation

`PeerManagerActor::handle_msg_network_requests` (`peer_manager_actor.rs:837`) dispatches outbound `NetworkRequests`:
- **Block** — `broadcast_message(PeerMessage::Block)` to all ready peers (`peer_manager_actor.rs:845`). Block requests/headers are direct `BlockRequest`/`BlockHeadersRequest` to a peer (`peer_manager_actor.rs:875`, `:886`).
- **Transactions** — `ForwardTx(account_id, tx)` is routed to the responsible validator as a T2 routed message (`peer_manager_actor.rs:1214`); a node may also receive a raw `PeerMessage::Transaction` and feed it to the client.
- **Approvals** — `T1MessageBody::BlockApproval` sent via `send_message_to_account` (`peer_manager_actor.rs:863`); 3× resend, prefers T1 overlay. See [consensus-finality](consensus-finality.md) for approval semantics.
- **Chunk parts** — `VersionedPartialEncodedChunk` (T1, 3× resend) and `PartialEncodedChunkForward` (T1) routed to the target; `PartialEncodedChunkRequest`/`Response` are T2 (request/route-back). See [sharding-chunks](sharding-chunks.md).
- **Endorsements / witnesses** — `VersionedChunkEndorsement`, `PartialEncodedStateWitness(+Forward)` and their `Versioned*` forms are T1 routed (and `allow_sending_to_self`). See [stateless-validation](stateless-validation.md).
- **OptimisticBlock** — sent over T1 as a direct `PeerMessage::OptimisticBlock` to each chunk producer/proxy (`peer_manager_actor.rs:848`).
- **State sync** — `StatePartRequest`/`StateHeaderRequest` are T2 routed; the response comes back over a fresh T3 connection. Availability is advertised via `SyncSnapshotHosts`/`SnapshotHostInfo` (`peer_manager_actor.rs:1084`, `network_protocol/state_sync.rs`). See [sync](sync.md).

## Interactions

- **Consumes**: TCP byte streams (`tcp.rs`, `tcp_transport.rs`); `NetworkRequests` from the client/shards-manager/partial-witness actors (mapped to `PeerMessage`s in `peer_manager_actor.rs`); `SignedAccountData` and `AnnounceAccount` for validator discovery.
- **Produces**: decoded `PeerMessage`/routed bodies delivered to the right actor by `receive_routed_message` (`network_state/mod.rs:866`) and the direct-payload handlers.
- **Touches**: [sync](sync.md) (block/header/state-part/epoch-sync transport and `SnapshotHostInfo` availability), [chain-block-processing](chain-block-processing.md) (block/header broadcast & ingest), [sharding-chunks](sharding-chunks.md) (partial chunk parts/forwards/requests), [stateless-validation](stateless-validation.md) (partial witnesses, endorsements, contract-code distribution), [consensus-finality](consensus-finality.md) (block approvals), [data-structures-serialization](data-structures-serialization.md) (borsh/proto encoding of payloads). Payload meaning lives in those specs; this layer only transports them.

## Protocol-version-gated behavior

The P2P **wire** protocol (which `RoutedMessage` variant, which `PeerMessage` fields) is gated by the handshake-negotiated `protocol_version` against `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`core/primitives-core/src/version.rs:600`) and `PROTOCOL_VERSION` (`version.rs:640`), **not** by `ProtocolFeature` flags. `RoutedMessageV3` (tiered body, optional T1 signature) is the in-memory form produced by `sign` today (`mod.rs:1534`), but every outbound routed message is still serialized as V1 borsh via `msg_v1()` (`mod.rs:1164`, `proto_conv/peer_message.rs:272`); the V3 proto field is only parsed on receive, never emitted (TODO #13709, `mod.rs:1163`). Incoming V1/V2 are upgraded to V3 in memory (`mod.rs:1293`).

`ProtocolFeature`s touching message *payloads* carried by this layer (verified against `core/primitives-core/src/version.rs` in this 2.13.0 tree, PV 86):
- **`PostQuantumSignatures`** — activates at **v85** (`version.rs:567`). Adds FIPS 204 ML-DSA-65 as a third signature scheme for transactions/`AddKey`; changes the contents of `Transaction`/`ForwardTx` payloads but not the transport. Semantics in [accounts-keys](accounts-keys.md).
- **`SignedContractCodeResponse`** — activates at **v85** (`version.rs:572`). Senders emit `ContractCodeResponseV2` (signed inner payload) and receivers require a verifiable signature before processing; affects the `ContractCodeResponse` T1 routed payload. Semantics in [stateless-validation](stateless-validation.md).
- **`ExecutionMetadataV4`** — activates at **v85** (`version.rs:571`). Changes borsh wire format of execution metadata carried in chunk-related payloads; requires coordinated network cutover. Semantics in [runtime-execution](runtime-execution.md).

No `ProtocolFeature` at v86 (the sole PV-86 feature is `EnforcePerReceiptStorageProofLimit`, `version.rs:576` — a stateless-validation storage-proof limit, not a networking change) alters the handshake, tiering, BFS routing, or route-back behavior themselves.

## Invariants & failure modes

- **Routed signatures are verified before forwarding/delivery** — `msg.verify()` failing ⇒ ban the previous sender (`peer_actor.rs:1304`). V3 makes the signature optional for T1 bodies (`mod.rs:1075`); `RoutedMessageV3::verify` returns false when the signature is absent (`mod.rs:1093`).
- **TTL bounds hops** — `decrease_ttl` saturates at 0 and a 0-TTL message is dropped (`mod.rs:1113`, `network_state/mod.rs:1100`), preventing routing loops; default TTL 100 (`types.rs:47`).
- **Tier is fixed, not negotiable** — a tier mismatch in a handshake reply is treated as malicious and the connection is dropped (`peer_actor.rs:518`); a T2 body may not be sent over a T1/T3 connection (`connection/mod.rs:72`).
- **Self-addressed messages** — dropped by default; only bodies with `allow_sending_to_self()` (witnesses/endorsements) are looped back locally (`network_state/mod.rs:780`, `mod.rs:678`).
- **Caches are abuse-resistant** — `RouteBackCache` evicts the heaviest peer's oldest entries (`route_back_cache.rs:98`); `AccountData` is size-capped (`MAX_ACCOUNT_DATA_SIZE_BYTES`, `mod.rs:200`); `SyncRoutingTable` edge counts are capped per message (`peer_actor.rs:1380`); transactions per inter-block window are capped (`peer_actor.rs:1294`).
- **Dropped (not error) on no route** — a missing forward route or route-back entry silently drops the message with a metric (`MessageDropped::NoRouteFound`, `FindRouteError::RouteBackNotFound`, `routing.rs:156`); there is no retransmit beyond `message_resend_count`.
- **Nonce monotonicity** — connection edges use increasing, timestamp-bounded nonces; a stale nonce is answered with `LastEdge` evidence rather than accepted (`peer_actor.rs:577`).
- **Graph size caps** — `Graph::add_edge` refuses edges that would exceed `max_peers` (`bfs.rs:116`); BFS only uses the first 128 source-neighbors for the route bitset (`bfs.rs:167`).

## Code anchors

| Location | Symbol | What happens here |
|----------|--------|-------------------|
| `chain/network/src/network_protocol/mod.rs:399` | `PeerMessage` | Top-level wire message enum |
| `chain/network/src/network_protocol/mod.rs:336` | `Handshake` | Handshake payload (version/genesis/peer-id/edge/owned-account) |
| `chain/network/src/network_protocol/mod.rs:356` | `HandshakeFailureReason` | ProtocolVersionMismatch / GenesisMismatch / InvalidTarget |
| `chain/network/src/network_protocol/mod.rs:487` | `TieredMessageBody` | T1/T2 split of routed bodies |
| `chain/network/src/network_protocol/mod.rs:648` | `T1MessageBody` | Consensus-critical routed payloads (3× resend for some) |
| `chain/network/src/network_protocol/mod.rs:704` | `T2MessageBody` | Multi-hop gossip routed payloads |
| `chain/network/src/network_protocol/mod.rs:1063` | `RoutedMessageV3` | Current routed in-memory form: tiered body, optional T1 signature, ttl, num_hops |
| `chain/network/src/network_protocol/mod.rs:1154` | `RoutedMessage::build_hash` | Borsh hash of {target, author, body} — signed & route-back key |
| `chain/network/src/network_protocol/mod.rs:1164` | `RoutedMessage::msg_v1` | Down-converts to V1 for wire serialization |
| `chain/network/src/network_protocol/mod.rs:1534` | `RawRoutedMessage::sign` | Produces a signed V3 with default TTL/created_at |
| `chain/network/src/network_protocol/mod.rs:1366` | `PeerIdOrHash` | Forward (PeerId) vs. route-back (Hash) target |
| `chain/network/src/network_protocol/proto_conv/peer_message.rs:272` | `RoutedMessage` proto conv | Serializes V1 borsh into `proto::RoutedMessage.borsh` |
| `chain/network/src/types.rs:47` | `ROUTED_MESSAGE_TTL` | Default TTL = 100 |
| `chain/network/src/tcp.rs:22` | `Tier` | T1/T2/T3 overlay definitions |
| `chain/network/src/peer_manager/connection/mod.rs:69` | `Tier::is_allowed_send_routed` | T2 body only over T2; T1 body anywhere |
| `chain/network/src/peer/peer_actor.rs:436` | `PeerActor::send_handshake` | Builds & sends the outbound Handshake |
| `chain/network/src/peer/peer_actor.rs:492` | `PeerActor::process_handshake` | Handshake validation & edge creation |
| `chain/network/src/peer/peer_actor.rs:1263` | `PeerActor` Routed arm | Dedup, tx cap, verify, dispatch ForMe/Forward |
| `chain/network/src/peer/peer_actor.rs:1371` | `handle_sync_routing_table` | Edge ingress cap, edge/account merge |
| `chain/network/src/peer_manager/network_state/mod.rs:687` | `send_message_to_peer` | Per-tier next-hop selection & route-back insert |
| `chain/network/src/peer_manager/network_state/mod.rs:770` | `send_message_to_account` | Account→peer resolution, T1 fast path, resend |
| `chain/network/src/peer_manager/network_state/mod.rs:866` | `receive_routed_message` | Body demux to actors |
| `chain/network/src/peer_manager/network_state/mod.rs:1071` | `process_incoming_routed` | for-me check, add_route_back, TTL/forward |
| `chain/network/src/peer_manager/network_state/routing.rs:143` | `tier2_find_route` | RoutingTableView next-hop or route-back pop |
| `chain/network/src/peer_manager/network_state/routing.rs:163` | `add_route_back` | Records prev-hop for response-bearing requests |
| `chain/network/src/peer_manager/network_state/tier1.rs:206` | `tier1_connect` | Establishes/prunes T1 connections from AccountData |
| `chain/network/src/peer_manager/network_state/tier1.rs:351` | `get_tier1_proxy` | Picks a direct T1 peer or advertised proxy |
| `chain/network/src/routing/bfs.rs:148` | `Graph::calculate_next_hops_and_distance` | BFS shortest-path next-hop computation |
| `chain/network/src/routing/route_back_cache.rs:98` | `RouteBackCache::remove_frequent` | Anti-poisoning eviction |
| `chain/network/src/routing/routing_table_view/mod.rs:35` | `find_next_hop` | Load-balanced next-hop pick over equal-cost paths |
| `chain/network/src/peer_manager/peer_store/mod.rs:419` | `PeerStore::peer_ban` | Marks a peer Banned(reason, now) |
| `chain/network/src/peer_manager/peer_store/mod.rs:439` | `unconnected_peer` | Candidate selection for outbound dials |
| `chain/network/src/peer_manager/peer_manager_actor.rs:89` | `MAX_TIER2_PEERS` | T2 mesh cap = 128 |
| `chain/network/src/peer_manager/peer_manager_actor.rs:461` | `is_outbound_bootstrap_needed` | Whether to dial more outbound peers |
| `chain/network/src/peer_manager/peer_manager_actor.rs:542` | `maybe_stop_active_connection` | safe_set computation + evict excess |
| `chain/network/src/peer_manager/peer_manager_actor.rs:837` | `handle_msg_network_requests` | Outbound NetworkRequests → PeerMessages |
| `chain/network/src/rate_limits/messages_limits.rs:49` | `RateLimits::is_allowed` | Per-kind token-bucket throttle |
| `chain/network/src/network_protocol/state_sync.rs:27` | `SnapshotHostInfo` | Signed state-snapshot availability advertisement |
| `chain/network/src/network_protocol/mod.rs:132` | `AccountData` | Validator peer-id + proxies for T1 discovery |
| `chain/network/src/config.rs:32` | `DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_MESSAGE` | Per-message edge ingress cap = 50_000 |
| `core/primitives-core/src/version.rs:600` | `MIN_SUPPORTED_PROTOCOL_VERSION` | Wire-version floor = 83 |

## Open questions

- The `account_announcements` (`AnnounceAccount`) account→peer table is explicitly being deprecated in favor of `accounts_data` (comment near `network_state/mod.rs:835`); the exact removal version / protocol-compat plan is not encoded in code read here.
- At v86 *every* peer emits V1 borsh on the wire — outbound serialization unconditionally calls `msg_v1()` (`proto_conv/peer_message.rs:272`) — so the V3 proto variant is decode-only. TODO #13709 (`mod.rs:1163`) tracks removing V1 support after a forward-compatible release that starts emitting V3; the cutover version is not fixed in code.
- `docs/architecture/network.md`, `docs/NetworkSpec/*`, and `docs/advanced_configuration/networking.md` were not re-read line-by-line for this pass; the spec is derived from source. They predate the T1/T2/T3 tier split and `RoutedMessageV3`, so cross-check before relying on them.

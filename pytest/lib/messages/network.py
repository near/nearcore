from messages.crypto import Signature, PublicKey, MerklePath, ShardProof
from messages.tx import SignedTransaction, Receipt
from messages.block import Block, Approval, PartialEncodedChunk, PartialEncodedChunkV1, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PartialEncodedChunkForwardMsg, BlockHeader, ShardChunk, ShardChunkHeader, ShardChunkHeaderV1
from messages.shard import StateRootNode


class SocketAddr:
    pass


class PeerMessage:
    pass


class Handshake:
    pass


class HandshakeV2:
    pass


class HandshakeFailureReason:
    pass


class ProtocolVersionMismatch:
    pass


class PeerInfo:
    pass


class PeerChainInfo:
    pass


class PeerChainInfoV2:
    pass


class EdgeInfo:
    pass


class GenesisId:
    pass


class Edge:
    pass


class SyncData:
    pass


class AnnounceAccount:
    pass


class RoutedMessage:
    pass


class PeerIdOrHash:
    pass


class RoutedMessageBody:
    pass


class PingPong:
    pass


class StateResponseInfo:
    pass


class StateResponseInfoV1:
    pass


class StateResponseInfoV2:
    pass


class ShardStateSyncResponse:
    pass


class ShardStateSyncResponseV1:
    pass


class ShardStateSyncResponseV2:
    pass


class ShardStateSyncResponseHeader:
    pass


class ShardStateSyncResponseHeaderV1:
    pass


class ShardStateSyncResponseHeaderV2:
    pass


class RoutingTableSyncV2:
    pass


class IbfElem:
    pass


class RoutingVersion2:
    pass


class RoutingState:
    pass


class PartialSync:
    pass


network_schema = [
    [
        SocketAddr, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V4', ([4], 'u16')], ['V6', ([16], 'u16')]]
        }
    ],
    [
        PeerMessage,
        {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['Handshake', Handshake],
                ['HandshakeFailure', (PeerInfo, HandshakeFailureReason)],
                ['LastEdge', Edge],
                ['Sync', SyncData],
                ['RequestUpdateNonce', EdgeInfo],
                ['ResponseUpdateNonce', Edge],
                ['PeersRequest', ()],
                ['PeersResponse', [PeerInfo]],
                ['BlockHeadersRequest', [[32]]],
                ['BlockHeaders', [BlockHeader]],
                ['BlockRequest', [32]],
                ['Block', Block],
                ['Transaction', SignedTransaction],
                ['Routed', RoutedMessage],
                ['Disconnect'],
                ['Challenge', None],  # TODO
                ['HandshakeV2', HandshakeV2],
                ['EpochSyncRequest', None],  # TODO
                ['EpochSyncResponse', None],  # TODO
                ['EpochSyncFinalizationRequest', None],  # TODO
                ['EpochSyncFinalizationResponse', None],  # TODO
                ['RoutingTableSyncV2', RoutingTableSyncV2],
            ]
        }
    ],
    [
        Handshake, {
            'kind':
                'struct',
            'fields': [
                ['version', 'u32'],
                ['oldest_supported_version', 'u32'],
                ['peer_id', PublicKey],
                ['target_peer_id', PublicKey],
                ['listen_port', {
                    'kind': 'option',
                    'type': 'u16'
                }],
                ['chain_info', PeerChainInfoV2],
                ['edge_info', EdgeInfo],
            ]
        }
    ],
    [
        HandshakeV2, {
            'kind':
                'struct',
            'fields': [
                ['version', 'u32'],
                ['oldest_supported_version', 'u32'],
                ['peer_id', PublicKey],
                ['target_peer_id', PublicKey],
                ['listen_port', {
                    'kind': 'option',
                    'type': 'u16'
                }],
                ['chain_info', PeerChainInfo],
                ['edge_info', EdgeInfo],
            ]
        }
    ],
    [
        HandshakeFailureReason, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['ProtocolVersionMismatch', ProtocolVersionMismatch],
                ['GenesisMismatch', GenesisId],
                ['InvalidTarget', ()],
            ]
        }
    ],
    [
        ProtocolVersionMismatch, {
            'kind': 'struct',
            'fields': [
                ['version', 'u32'],
                ['oldest_supported_version', 'u32'],
            ]
        }
    ],
    [
        PeerInfo, {
            'kind':
                'struct',
            'fields': [['id', PublicKey],
                       ['addr', {
                           'kind': 'option',
                           'type': SocketAddr
                       }], ['account_id', {
                           'kind': 'option',
                           'type': 'string'
                       }]]
        }
    ],
    [
        PeerChainInfo, {
            'kind':
                'struct',
            'fields': [['genesis_id', GenesisId], ['height', 'u64'],
                       ['tracked_shards', ['u64']]]
        }
    ],
    [
        PeerChainInfoV2, {
            'kind':
                'struct',
            'fields': [['genesis_id', GenesisId], ['height', 'u64'],
                       ['tracked_shards', ['u64']], ['archival', 'bool']]
        }
    ],
    [
        Edge, {
            'kind':
                'struct',
            'fields': [
                ['peer0', PublicKey],
                ['peer1', PublicKey],
                ['nonce', 'u64'],
                ['signature0', Signature],
                ['signature1', Signature],
                ['removal_info', {
                    'kind': 'option',
                    'type': ('u8', Signature)
                }],
            ]
        }
    ],
    [
        SyncData, {
            'kind': 'struct',
            'fields': [
                ['edges', [Edge]],
                ['accounts', [AnnounceAccount]],
            ]
        }
    ],
    [
        EdgeInfo, {
            'kind': 'struct',
            'fields': [
                ['nonce', 'u64'],
                ['signature', Signature],
            ]
        }
    ],
    [
        GenesisId, {
            'kind': 'struct',
            'fields': [
                ['chain_id', 'string'],
                ['hash', [32]],
            ]
        }
    ],
    [
        AnnounceAccount, {
            'kind':
                'struct',
            'fields': [
                ['account_id', 'string'],
                ['peer_id', PublicKey],
                ['epoch_id', [32]],
                ['signature', Signature],
            ]
        }
    ],
    [
        RoutedMessage, {
            'kind':
                'struct',
            'fields': [
                ['target', PeerIdOrHash],
                ['author', PublicKey],
                ['signature', Signature],
                ['ttl', 'u8'],
                ['body', RoutedMessageBody],
            ]
        }
    ],
    [
        PeerIdOrHash, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['PeerId', PublicKey],
                ['Hash', [32]],
            ]
        }
    ],
    [
        RoutedMessageBody,
        {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['BlockApproval', Approval],
                ['ForwardTx', SignedTransaction],
                ['TxStatusRequest', ('string', [32])],
                ['TxStatusResponse', None],  # TODO
                ['QueryRequest', None],  # TODO
                ['QueryResponse', None],  # TODO
                ['ReceiptOutcomeRequest', [32]],
                ['ReceiptOutcomeResponse', None],  # TODO
                ['StateRequestHeader', ('u64', [32])],
                ['StateRequestPart', ('u64', [32], 'u64')],
                ['StateResponseInfo', StateResponseInfoV1],
                ['PartialEncodedChunkRequest', PartialEncodedChunkRequestMsg],
                ['PartialEncodedChunkResponse', PartialEncodedChunkResponseMsg],
                ['PartialEncodedChunk', PartialEncodedChunkV1],
                ['Ping', PingPong],
                ['Pong', PingPong],
                ['VersionedPartialEncodedChunk', PartialEncodedChunk],
                ['VersionedStateResponse', StateResponseInfo],
                ['PartialEncodedChunkForward', PartialEncodedChunkForwardMsg]
            ]
        }
    ],
    [
        PingPong, {
            'kind': 'struct',
            'fields': [['nonce', 'u64'], ['source', PublicKey]]
        }
    ],
    [
        StateResponseInfo, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V1', StateResponseInfoV1], ['V2', StateResponseInfoV2]]
        }
    ],
    [
        StateResponseInfoV1, {
            'kind':
                'struct',
            'fields': [['shard_id', 'u64'], ['sync_hash', [32]],
                       ['state_response', ShardStateSyncResponseV1]]
        }
    ],
    [
        StateResponseInfoV2, {
            'kind':
                'struct',
            'fields': [['shard_id', 'u64'], ['sync_hash', [32]],
                       ['state_response', ShardStateSyncResponse]]
        }
    ],
    [
        ShardStateSyncResponse, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['V1', ShardStateSyncResponseV1],
                       ['V2', ShardStateSyncResponseV2]]
        }
    ],
    [
        ShardStateSyncResponseV1, {
            'kind':
                'struct',
            'fields': [[
                'header', {
                    'kind': 'option',
                    'type': ShardStateSyncResponseHeaderV1
                }
            ], ['part', {
                'kind': 'option',
                'type': ('u64', ['u8'])
            }]]
        }
    ],
    [
        ShardStateSyncResponseV2, {
            'kind':
                'struct',
            'fields': [[
                'header', {
                    'kind': 'option',
                    'type': ShardStateSyncResponseHeaderV2
                }
            ], ['part', {
                'kind': 'option',
                'type': ('u64', ['u8'])
            }]]
        }
    ],
    [
        ShardStateSyncResponseHeader, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['V1', ShardStateSyncResponseHeaderV1],
                       ['V2', ShardStateSyncResponseHeaderV2]]
        }
    ],
    [
        ShardStateSyncResponseHeaderV1, {
            'kind':
                'struct',
            'fields': [['chunk', ShardChunk], ['chunk_proof', MerklePath],
                       [
                           'prev_chunk_header', {
                               'kind': 'option',
                               'type': ShardChunkHeaderV1
                           }
                       ],
                       [
                           'prev_chunk_proof', {
                               'kind': 'option',
                               'type': MerklePath
                           }
                       ],
                       [
                           'incoming_receipts_proofs',
                           [([32], [([Receipt], ShardProof)])]
                       ], ['root_proofs', [[([32], MerklePath)]]],
                       ['state_root_node', StateRootNode]]
        }
    ],
    [
        ShardStateSyncResponseHeaderV2, {
            'kind':
                'struct',
            'fields': [['chunk', ShardChunk], ['chunk_proof', MerklePath],
                       [
                           'prev_chunk_header', {
                               'kind': 'option',
                               'type': ShardChunkHeader
                           }
                       ],
                       [
                           'prev_chunk_proof', {
                               'kind': 'option',
                               'type': MerklePath
                           }
                       ],
                       [
                           'incoming_receipts_proofs',
                           [([32], [([Receipt], ShardProof)])]
                       ], ['root_proofs', [[([32], MerklePath)]]],
                       ['state_root_node', StateRootNode]]
        }
    ],
    [
        RoutingTableSyncV2, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['Version2', RoutingVersion2]]
        }
    ],
    [
        IbfElem, {
            'kind': 'struct',
            'fields': [
                ['xor_elem', 'u64'],
                ['xor_hash', 'u64'],
            ]
        }
    ],
    [
        RoutingVersion2, {
            'kind':
                'struct',
            'fields': [
                ['known_edges', 'u64'],
                ['seed', 'u64'],
                ['edges', [Edge]],
                ['routing_state', RoutingState],
            ]
        }
    ],
    [
        RoutingState, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['PartialSync', PartialSync],
                ['RequestAllEdges', ()],
                ['Done', ()],
                ['RequestMissingEdges', ['u64']],
                ['InitializeIbf', ()],
            ]
        }
    ],
    [
        PartialSync, {
            'kind': 'struct',
            'fields': [
                ['ibf_level', 'u64'],
                ['ibf', [IbfElem]],
            ]
        }
    ]
]

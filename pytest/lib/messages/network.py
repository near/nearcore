from messages.crypto import Signature, PublicKey
from messages.tx import SignedTransaction
from messages.block import Block, Approval, PartialEncodedChunk, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, BlockHeader


class SocketAddr:
    pass


class PeerMessage:
    pass


class Handshake:
    pass


class HandshakeFailureReason:
    pass


class PeerInfo:
    pass


class PeerChainInfo:
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


network_schema = [
    [
        SocketAddr, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['V4', ([4], 'u16')], ['V6', ([16], 'u16')]]
        }
    ],
    [
        PeerMessage, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [['Handshake', Handshake],
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
                       ['Disconnect', ()],
                       ['Challenge', None]] # TODO
        }
    ],
    [
        Handshake, {
            'kind':
                'struct',
            'fields': [
                ['version', 'u32'],
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
                ['ProtocolVersionMismatch', 'u32'],
                ['GenesisMismatch', GenesisId],
                ['InvalidTarget', ()],
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
        Edge, {
            'kind': 'struct',
            'fields': [
                ['peer0', PublicKey],
                ['peer1', PublicKey],
                ['nonce', 'u64'],
                ['signature0', Signature],
                ['signature1', Signature],
                ['removal_info', {'kind': 'option', 'type': ('u8', Signature)}],
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
            'kind': 'struct',
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
            'kind': 'struct',
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
        RoutedMessageBody, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['BlockApproval', Approval],
                ['ForwardTx', SignedTransaction],
                ['TxStatusRequest', ('string', [32])],
                ['TxStatusResponse', None], # TODO
                ['QueryRequest', None], # TODO
                ['QueryResponse', None], # TODO
                ['ReceiptOutcomeRequest', [32]],
                ['ReceiptOutcomeResponse', None], # TODO
                ['StateRequestHeader', ('u64', [32])],
                ['StateRequestPart', ('u64', [32], 'u64')],
                ['StateResponseInfo', None], # TODO
                ['PartialEncodedChunkRequest', PartialEncodedChunkRequestMsg],
                ['PartialEncodedChunkResponse', PartialEncodedChunkResponseMsg],
                ['PartialEncodedChunk', PartialEncodedChunk],
                ['Ping', PingPong],
                ['Pong', PingPong],
            ]
        }
    ],
    [
        PingPong, {
            'kind': 'struct',
            'fields': [['nonce', 'u64'], ['source', PublicKey]]
        }
    ],
]

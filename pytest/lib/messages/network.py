from messages.crypto import Signature, PublicKey


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
                       ['HandshakeFailure', (PeerInfo, HandshakeFailureReason)]]
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
    ]
]

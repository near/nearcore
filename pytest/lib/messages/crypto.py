class Signature:
    pass


class PublicKey:
    pass


class AccessKey:
    pass


class AccessKeyPermission:
    pass


class FunctionCallPermission:
    pass


class FullAccessPermission:
    pass


crypto_schema = [
    [
        Signature, {
            'kind': 'struct',
            'fields': [['keyType', 'u8'], ['data', [64]]]
        }
    ],
    [
        PublicKey, {
            'kind': 'struct',
            'fields': [['keyType', 'u8'], ['data', [32]]]
        }
    ],
    [
        AccessKey, {
            'kind': 'struct',
            'fields': [
                ['nonce', 'u64'],
                ['permission', AccessKeyPermission],
            ]
        }
    ],
    [
        AccessKeyPermission, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['functionCall', FunctionCallPermission],
                ['fullAccess', FullAccessPermission],
            ]
        }
    ],
    [
        FunctionCallPermission, {
            'kind':
                'struct',
            'fields': [
                ['allowance', {
                    'kind': 'option',
                    type: 'u128'
                }],
                ['receiverId', 'string'],
                ['methodNames', ['string']],
            ]
        }
    ],
    [FullAccessPermission, {
        'kind': 'struct',
        'fields': []
    }],
]

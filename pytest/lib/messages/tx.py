from messages.crypto import Signature, PublicKey, AccessKey


class SignedTransaction:
    pass


class Transaction:
    pass


class Action:
    pass


class CreateAccount:
    pass


class DeployContract:
    pass


class FunctionCall:
    pass


class Transfer:
    pass


class Stake:
    pass


class AddKey:
    pass


class DeleteKey:
    pass


class DeleteAccount:
    pass


tx_schema = [
    [
        SignedTransaction, {
            'kind': 'struct',
            'fields': [['transaction', Transaction], ['signature', Signature]]
        }
    ],
    [
        Transaction, {
            'kind':
                'struct',
            'fields': [['signerId', 'string'], ['publicKey', PublicKey],
                       ['nonce', 'u64'], ['receiverId', 'string'],
                       ['blockHash', [32]], ['actions', [Action]]]
        }
    ],
    [
        Action, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['createAccount', CreateAccount],
                ['deployContract', DeployContract],
                ['functionCall', FunctionCall],
                ['transfer', Transfer],
                ['stake', Stake],
                ['addKey', AddKey],
                ['deleteKey', DeleteKey],
                ['deleteAccount', DeleteAccount],
            ]
        }
    ],
    [CreateAccount, {
        'kind': 'struct',
        'fields': []
    }],
    [DeployContract, {
        'kind': 'struct',
        'fields': [['code', ['u8']]]
    }],
    [
        FunctionCall, {
            'kind':
                'struct',
            'fields': [['methodName', 'string'], ['args', ['u8']],
                       ['gas', 'u64'], ['deposit', 'u128']]
        }
    ],
    [Transfer, {
        'kind': 'struct',
        'fields': [['deposit', 'u128']]
    }],
    [
        Stake, {
            'kind': 'struct',
            'fields': [['stake', 'u128'], ['publicKey', PublicKey]]
        }
    ],
    [
        AddKey, {
            'kind': 'struct',
            'fields': [['publicKey', PublicKey], ['accessKey', AccessKey]]
        }
    ],
    [DeleteKey, {
        'kind': 'struct',
        'fields': [['publicKey', PublicKey]]
    }],
    [
        DeleteAccount, {
            'kind': 'struct',
            'fields': [['beneficiaryId', 'string']]
        }
    ],
]

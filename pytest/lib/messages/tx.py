from messages.crypto import Signature, PublicKey, AccessKey


class SignedTransaction:
    pass


class Transaction:
    pass


class TransactionV1:
    pass


class TransactionNonce:
    pass


class GasKeyNonceData:
    pass


class Action:
    pass


class CreateAccount:
    pass


class DeployContract:
    pass


class DeployGlobalContract:
    pass


class UseGlobalContract:
    pass


class GlobalContractIdentifier:
    pass


class GlobalContractDeployMode:
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


class DeterministicStateInit:
    pass


class TransferToGasKey:
    pass


class WithdrawFromGasKey:
    pass


class SignedDelegate:
    pass


class DelegateAction:
    pass


class Receipt:
    pass


class ReceiptEnum:
    pass


class ActionReceipt:
    pass


class DataReceipt:
    pass


class DataReceiver:
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
                ['delegate', SignedDelegate],
                ['deployGlobalContract', DeployGlobalContract],
                ['useGlobalContract', UseGlobalContract],
                ['deterministicStateInit', DeterministicStateInit],
                ['transferToGasKey', TransferToGasKey],
                ['withdrawFromGasKey', WithdrawFromGasKey],
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
        DeployGlobalContract, {
            'kind':
                'struct',
            'fields': [['code', ['u8']],
                       ['deployMode', GlobalContractDeployMode]]
        }
    ],
    [
        GlobalContractDeployMode, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['codeHash', ()],
                ['accountId', ()],
            ]
        }
    ],
    [
        UseGlobalContract, {
            'kind': 'struct',
            'fields': [['contractIdentifier', GlobalContractIdentifier]]
        }
    ],
    [
        GlobalContractIdentifier, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['codeHash', [32]],
                ['accountId', 'string'],
            ]
        }
    ],
    [
        FunctionCall, {
            'kind':
                'struct',
            'fields': [['methodName', 'string'], ['args', ['u8']],
                       ['gas', 'u64'], ['deposit', 'u128']]
        }
    ],
    [
        SignedDelegate, {
            'kind':
                'struct',
            'fields': [['delegateAction', DelegateAction],
                       ['signature', Signature]]
        }
    ],
    [
        DelegateAction, {
            'kind':
                'struct',
            'fields': [['senderId', 'string'], ['receiverId', 'string'],
                       ['actions', [Action]], ['nonce', 'u64'],
                       ['maxBlockHeight', 'u64'], ['publicKey', PublicKey]]
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
    # DeterministicStateInit fields are not implemented for pytest yet.
    # The entry must remain in the Action enum to preserve borsh variant indices.
    [DeterministicStateInit, {
        'kind': 'struct',
        'fields': []
    }],
    [
        TransferToGasKey, {
            'kind': 'struct',
            'fields': [['publicKey', PublicKey], ['deposit', 'u128']]
        }
    ],
    [
        WithdrawFromGasKey, {
            'kind': 'struct',
            'fields': [['publicKey', PublicKey], ['amount', 'u128']]
        }
    ],
    [
        TransactionV1, {
            'kind':
                'struct',
            'fields': [['signerId', 'string'], ['publicKey', PublicKey],
                       ['nonce', TransactionNonce], ['receiverId', 'string'],
                       ['blockHash', [32]], ['actions', [Action]]]
        }
    ],
    [
        TransactionNonce, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['nonce', 'u64'],
                ['gasKeyNonce', GasKeyNonceData],
            ]
        }
    ],
    [
        GasKeyNonceData, {
            'kind': 'struct',
            'fields': [['nonce', 'u64'], ['nonceIndex', 'u16']]
        }
    ],
    [
        Receipt, {
            'kind':
                'struct',
            'fields': [
                ['predecessor_id', 'string'],
                ['receiver_id', 'string'],
                ['receipt_id', [32]],
                ['receipt', ReceiptEnum],
            ]
        }
    ],
    [
        ReceiptEnum, {
            'kind': 'enum',
            'field': 'enum',
            'values': [
                ['Action', ActionReceipt],
                ['Data', DataReceipt],
            ]
        }
    ],
    [
        ActionReceipt, {
            'kind':
                'struct',
            'fields': [
                ['signer_id', 'string'],
                ['signer_public_key', PublicKey],
                ['gas_price', 'u128'],
                ['output_data_receivers', [DataReceiver]],
                ['input_data_ids', [[32]]],
                ['actions', [Action]],
            ],
        }
    ],
    [
        DataReceipt, {
            'kind':
                'struct',
            'fields': [
                ['data_id', [32]],
                ['data', {
                    'kind': 'option',
                    'type': ['u8']
                }],
            ]
        }
    ],
    [
        DataReceiver, {
            'kind': 'struct',
            'fields': [
                ['data_id', [32]],
                ['receiver_id', 'string'],
            ]
        }
    ],
]

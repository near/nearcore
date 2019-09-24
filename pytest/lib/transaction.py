from serializer import BinarySerializer
import hashlib
from ed25519 import SigningKey


class Signature:
    pass

class SignedTransaction:
    pass

class Transaction:
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

tx_schema = dict([[Signature, { 'kind': 'struct', 'fields': [
            ['keyType', 'u8'],
            ['data', [64]]
        ] }],
[SignedTransaction, { 'kind': 'struct', 'fields': [
            ['transaction', Transaction],
            ['signature', Signature]
        ] }],
[Transaction, { 'kind': 'struct', 'fields': [
            ['signerId', 'string'],
            ['publicKey', PublicKey],
            ['nonce', 'u64'],
            ['receiverId', 'string'],
            ['blockHash', [32]],
            ['actions', [Action]]
        ] }],
[PublicKey, { 'kind': 'struct', 'fields': [
            ['keyType', 'u8'],
            ['data', [32]]
        ] }],
[AccessKey, { 'kind': 'struct', 'fields': [
            ['nonce', 'u64'],
            ['permission', AccessKeyPermission],
        ] }],
[AccessKeyPermission, { 'kind': 'enum', 'field': 'enum', 'values': [
            ['functionCall', FunctionCallPermission],
            ['fullAccess', FullAccessPermission],
        ] }],
[FunctionCallPermission, { 'kind': 'struct', 'fields': [
            ['allowance', { 'kind': 'option', type: 'u128' }],
            ['receiverId', 'string'],
            ['methodNames', ['string']],
        ] }],
[FullAccessPermission, { 'kind': 'struct', 'fields': [] }],
[Action, { 'kind': 'enum', 'field': 'enum', 'values': [
            ['createAccount', CreateAccount],
            ['deployContract', DeployContract],
            ['functionCall', FunctionCall],
            ['transfer', Transfer],
            ['stake', Stake],
            ['addKey', AddKey],
            ['deleteKey', DeleteKey],
            ['deleteAccount', DeleteAccount],
        ] }],
[CreateAccount, { 'kind': 'struct', 'fields': [] }],
[DeployContract, { 'kind': 'struct', 'fields': [
            ['code', ['u8']]
        ] }],
[FunctionCall, { 'kind': 'struct', 'fields': [
            ['methodName', 'string'],
            ['args', ['u8']],
            ['gas', 'u64'],
            ['deposit', 'u128']
        ] }],
[Transfer, { 'kind': 'struct', 'fields': [
            ['deposit', 'u128']
        ] }],
[Stake, { 'kind': 'struct', 'fields': [
            ['stake', 'u128'],
            ['publicKey', PublicKey]
        ] }],
[AddKey, { 'kind': 'struct', 'fields': [
            ['publicKey', PublicKey],
            ['accessKey', AccessKey]
        ] }],
[DeleteKey, { 'kind': 'struct', 'fields': [
            ['publicKey', PublicKey]
        ] }],
[DeleteAccount, { 'kind': 'struct', 'fields': [
            ['beneficiaryId', 'string']
        ] }],
])

def sign_and_serialize_transaction(receiverId, nonce, actions, blockHash, accountId, pk, sk):
    tx = Transaction()
    tx.signerId = accountId
    tx.publicKey = PublicKey()
    tx.publicKey.keyType = 0
    tx.publicKey.data = pk
    tx.nonce = nonce
    tx.receiverId = receiverId
    tx.actions = actions
    tx.blockHash = blockHash

    msg = BinarySerializer(tx_schema).serialize(tx)
    hash_ = hashlib.sha256(msg).digest()

    signature = Signature()
    signature.keyType = 0
    signature.data = SigningKey(sk).sign(hash_)

    signedTx = SignedTransaction()
    signedTx.transaction = tx
    signedTx.signature = signature

    return BinarySerializer(tx_schema).serialize(signedTx)

def create_payment_action(amount):
    transfer = Transfer()
    transfer.deposit = amount
    action = Action()
    action.enum = 'transfer'
    action.transfer = transfer
    return action

def sign_payment_tx(account, to, amount, nonce, blockHash):
    action = create_payment_action(amount)
    return sign_and_serialize_transaction(to, nonce, [action], blockHash, account.account_id, account.decoded_pk(), account.decoded_sk())



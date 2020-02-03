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

def create_staking_action(amount, pk):
    stake = Stake()
    stake.stake = amount
    stake.publicKey = PublicKey()
    stake.publicKey.keyType = 0
    stake.publicKey.data = pk
    action = Action()
    action.enum = 'stake'
    action.stake = stake
    return action

def create_deploy_contract_action(code):
    deployContract = DeployContract()
    deployContract.code = code
    action = Action()
    action.enum = 'deployContract'
    action.deployContract = deployContract
    return action

def create_function_call_action(methodName, args, gas, deposit):
    functionCall = FunctionCall()
    functionCall.methodName = methodName
    functionCall.args = args
    functionCall.gas = gas
    functionCall.deposit = deposit
    action = Action()
    action.enum = 'functionCall'
    action.functionCall = functionCall
    return action

def create_create_account_action():
    action = Action()
    action.enum = 'createAccount'
    action.createAccount = CreateAccount()
    return action

def create_add_key_action(pk):
    addKey = AddKey()
    addKey.publicKey = PublicKey()
    addKey.publicKey.keyType = 0
    addKey.publicKey.data = pk
    addKey.accessKey = AccessKey()
    addKey.accessKey.nonce = 0
    addKey.accessKey.permission = AccessKeyPermission()
    addKey.accessKey.permission.enum = 'fullAccess'
    addKey.accessKey.permission.fullAccess = FullAccessPermission()
    action = Action()
    action.enum = 'addKey'
    action.addKey = addKey
    return action

def sign_payment_tx(key, to, amount, nonce, blockHash):
    action = create_payment_action(amount)
    return sign_and_serialize_transaction(to, nonce, [action], blockHash, key.account_id, key.decoded_pk(), key.decoded_sk())

def sign_staking_tx(signer_key, validator_key, amount, nonce, blockHash):
    action = create_staking_action(amount, validator_key.decoded_pk())
    return sign_and_serialize_transaction(signer_key.account_id, nonce, [action], blockHash, signer_key.account_id, signer_key.decoded_pk(), signer_key.decoded_sk())

def sign_deploy_contract_tx(signer_key, code, nonce, blockHash, contract_account=None):
    action = create_deploy_contract_action(code)
    if contract_account is None:
        contract_account = signer_key.account_id
    return sign_and_serialize_transaction(contract_account, nonce, [action], blockHash, contract_account, signer_key.decoded_pk(), signer_key.decoded_sk())

def sign_function_call_tx(signer_key, methodName, args, gas, deposit, nonce, blockHash, contractName=None):
    if contractName is None:
        contractName = signer_key.account_id
    action = create_function_call_action(methodName, args, gas, deposit)
    return sign_and_serialize_transaction(contractName, nonce, [action], blockHash, contractName, signer_key.decoded_pk(), signer_key.decoded_sk())

def create_account_tx(signer_key, account_name, initial_balance, nonce, blockHash):
    create_account_action = create_create_account_action()
    transfer_action = create_payment_action(initial_balance)
    add_key_action = create_add_key_action(signer_key.decoded_pk())
    return sign_and_serialize_transaction(account_name, nonce, [create_account_action, transfer_action, add_key_action],
                                          blockHash, signer_key.account_id, signer_key.decoded_pk(), signer_key.decoded_sk())

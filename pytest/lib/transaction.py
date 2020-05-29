from serializer import BinarySerializer
import hashlib
from ed25519 import SigningKey

from messages.tx import *
from messages.crypto import *

schema = dict(tx_schema + crypto_schema)


def sign_and_serialize_transaction(receiverId, nonce, actions, blockHash,
                                   accountId, pk, sk):
    tx = Transaction()
    tx.signerId = accountId
    tx.publicKey = PublicKey()
    tx.publicKey.keyType = 0
    tx.publicKey.data = pk
    tx.nonce = nonce
    tx.receiverId = receiverId
    tx.actions = actions
    tx.blockHash = blockHash

    msg = BinarySerializer(schema).serialize(tx)
    hash_ = hashlib.sha256(msg).digest()

    signature = Signature()
    signature.keyType = 0
    signature.data = SigningKey(sk).sign(hash_)

    signedTx = SignedTransaction()
    signedTx.transaction = tx
    signedTx.signature = signature

    return BinarySerializer(schema).serialize(signedTx)


def create_create_account_action():
    createAccount = CreateAccount()
    action = Action()
    action.enum = 'createAccount'
    action.createAccount = createAccount
    return action


def create_full_access_key_action(pk):
    permission = AccessKeyPermission()
    permission.enum = 'fullAccess'
    permission.fullAccess = FullAccessPermission()
    accessKey = AccessKey()
    accessKey.nonce = 0
    accessKey.permission = permission
    publicKey = PublicKey()
    publicKey.keyType = 0
    publicKey.data = pk
    addKey = AddKey()
    addKey.accessKey = accessKey
    addKey.publicKey = publicKey
    action = Action()
    action.enum = 'addKey'
    action.addKey = addKey
    return action


def create_delete_access_key_action(pk):
    publicKey = PublicKey()
    publicKey.keyType = 0
    publicKey.data = pk
    deleteKey = DeleteKey()
    deleteKey.publicKey = publicKey
    action = Action()
    action.enum = 'deleteKey'
    action.deleteKey = deleteKey
    return action


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


def sign_create_account_tx(creator_key, new_account_id, nonce, block_hash):
    action = create_create_account_action()
    return sign_and_serialize_transaction(new_account_id, nonce, [action],
                                          block_hash, creator_key.account_id,
                                          creator_key.decoded_pk(),
                                          creator_key.decoded_sk())


def sign_create_account_with_full_access_key_and_balance_tx(
        creator_key, new_account_id, new_key, balance, nonce, block_hash):
    create_account_action = create_create_account_action()
    full_access_key_action = create_full_access_key_action(new_key.decoded_pk())
    payment_action = create_payment_action(balance)
    actions = [create_account_action, full_access_key_action, payment_action]
    return sign_and_serialize_transaction(new_account_id, nonce, actions,
                                          block_hash, creator_key.account_id,
                                          creator_key.decoded_pk(),
                                          creator_key.decoded_sk())


def sign_delete_access_key_tx(signer_key, target_account_id, key_for_deletion,
                              nonce, block_hash):
    action = create_delete_access_key_action(key_for_deletion.decoded_pk())
    return sign_and_serialize_transaction(target_account_id, nonce, [action],
                                          block_hash, signer_key.account_id,
                                          signer_key.decoded_pk(),
                                          signer_key.decoded_sk())


def sign_payment_tx(key, to, amount, nonce, blockHash):
    action = create_payment_action(amount)
    return sign_and_serialize_transaction(to, nonce, [action], blockHash,
                                          key.account_id, key.decoded_pk(),
                                          key.decoded_sk())


def sign_staking_tx(signer_key, validator_key, amount, nonce, blockHash):
    action = create_staking_action(amount, validator_key.decoded_pk())
    return sign_and_serialize_transaction(signer_key.account_id, nonce,
                                          [action], blockHash,
                                          signer_key.account_id,
                                          signer_key.decoded_pk(),
                                          signer_key.decoded_sk())


def sign_deploy_contract_tx(signer_key, code, nonce, blockHash):
    action = create_deploy_contract_action(code)
    return sign_and_serialize_transaction(signer_key.account_id, nonce,
                                          [action], blockHash,
                                          signer_key.account_id,
                                          signer_key.decoded_pk(),
                                          signer_key.decoded_sk())


def sign_function_call_tx(signer_key, contract_id, methodName, args, gas,
                          deposit, nonce, blockHash):
    action = create_function_call_action(methodName, args, gas, deposit)
    return sign_and_serialize_transaction(contract_id, nonce, [action],
                                          blockHash, signer_key.account_id,
                                          signer_key.decoded_pk(),
                                          signer_key.decoded_sk())

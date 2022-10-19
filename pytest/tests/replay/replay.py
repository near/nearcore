# TODO(#7132): Convert this test to a nightly regression test.

from account import Account
from collections import OrderedDict
from key import Key
from messages.tx import *
from messages.crypto import AccessKey, crypto_schema, PublicKey, Signature
from messages.bridge import bridge_schema
from serializer import BinarySerializer
import mocknet_helpers

import argparse
import base58
import base64
import hashlib
import json
import os

LOCALHOST = '127.0.0.1'


def generate_new_key():
    return Key.implicit_account()


def save_genesis_with_new_key_pair(genesis_path, key_pair, output_path):
    node0_dir = os.path.join(output_path, 'node0/')
    if not os.path.exists(node0_dir):
        os.makedirs(node0_dir)
    with open(genesis_path) as fin:
        genesis = json.load(fin)

    new_key = key_pair.pk.split(':')[1] if ':' in key_pair.pk else key_pair.pk
    # TODO(jc-near): Use different key pairs for different accounts.
    for i in range(len(genesis['validators'])):
        genesis['validators'][i]['public_key'] = new_key
    for record in genesis['records']:
        if 'AccessKey' in record:
            record['AccessKey']['public_key'] = new_key
    with open(os.path.join(node0_dir, 'genesis.json'), 'w') as fout:
        json.dump(genesis, fout, indent=2)

    key_pair.account_id = genesis['validators'][0]['account_id']
    key_json = dict()
    key_json['account_id'] = genesis['validators'][0]['account_id']
    key_json['public_key'] = key_pair.pk
    key_json['secret_key'] = key_pair.sk
    with open(os.path.join(node0_dir, 'node_key.json'), 'w') as fout:
        json.dump(key_json, fout, indent=2)
    with open(os.path.join(node0_dir, 'validator_key.json'), 'w') as fout:
        json.dump(key_json, fout, indent=2)


def prompt_to_launch_localnet(hint_dir):
    print('New genesis ready.')
    print('Please launch your localnet node now.')
    print('Hint: nearup run localnet --home %s --num-nodes 1' % hint_dir)


def convert_snack_case_to_camel_case(s):
    words = s.split('_')
    return words[0] + ''.join(w.title() for w in words[1:])


def convert_json_rust_instance_to_py_object(ordered_dict_instance, class_name):
    ans = class_name()
    for attr_key, attr_value in ordered_dict_instance.items():
        setattr(ans, convert_snack_case_to_camel_case(attr_key), attr_value)
    return ans


def convert_transaction_type_string_to_class(name):
    tx_class_by_type = {
        "CreateAccount": CreateAccount,
        "DeleteAccount": DeleteAccount,
        "DeployContract": DeployContract,
        "FunctionCall": FunctionCall,
        "Transfer": Transfer,
        "Stake": Stake,
        "AddKey": AddKey,
        "DeleteKey": DeleteKey,
    }
    if name in tx_class_by_type:
        return tx_class_by_type[name]
    raise ValueError('Unknown tx type: %s' % name)


def convert_json_public_key_to_py_public_key(json_public_key):
    pk_str = json_public_key.split(
        ':')[1] if ':' in json_public_key else json_public_key
    ans = PublicKey()
    ans.keyType = 0
    ans.data = base58.b58decode(pk_str.encode('ascii'))
    return ans


'''
Get tx fields read from json ready to be serialized.
See `pytest/lib/messages/` for tx schemas desired.
'''


def fix_json_fields_by_tx_type(py_tx, tx_type):
    if tx_type == "CreateAccount":
        pass
    elif tx_type == "DeleteAccount":
        pass
    elif tx_type == "DeployContract":
        py_tx.code = base64.b64decode(py_tx.code.encode('ascii'))
    elif tx_type == "FunctionCall":
        py_tx.args = base64.b64decode(py_tx.args.encode('ascii'))
        py_tx.deposit = int(py_tx.deposit)
    elif tx_type == "Transfer":
        py_tx.deposit = int(py_tx.deposit)
    elif tx_type == "Stake":
        py_tx.stake = int(py_tx.stake)
        py_tx.publicKey = convert_json_public_key_to_py_public_key(
            py_tx.publicKey)
    else:
        raise ValueError('Unsupported tx type: %s' % tx_type)
    return py_tx


def convert_json_action_to_py_action(action_dict):
    assert (len(action_dict) == 1)
    for tx_type, value in action_dict.items():
        contents = convert_json_rust_instance_to_py_object(
            value, convert_transaction_type_string_to_class(tx_type))
        contents = fix_json_fields_by_tx_type(contents, tx_type)
        action = Action()
        action.enum = tx_type[0].lower() + tx_type[1:]
        setattr(action, action.enum, contents)
        return action


def send_resigned_transactions(tx_path, home_dir):
    with open(os.path.join(home_dir, 'node0/', 'node_key.json'), 'r') as fin:
        key_pair_json = json.load(fin)
    key_pair = Key(key_pair_json['account_id'], key_pair_json['public_key'],
                   key_pair_json['secret_key'])
    base_block_hash = mocknet_helpers.get_latest_block_hash(addr=LOCALHOST)
    my_account = Account(key_pair,
                         init_nonce=0,
                         base_block_hash=base_block_hash,
                         rpc_infos=[(LOCALHOST, "3030")])

    schema = dict(tx_schema + crypto_schema + bridge_schema)
    with open(tx_path) as fin:
        txs = json.load(fin, object_pairs_hook=OrderedDict)
    for original_signed_tx in txs:
        tx = convert_json_rust_instance_to_py_object(
            original_signed_tx['transaction'], Transaction)
        if hasattr(tx, 'blockHash'):
            tx.blockHash = base_block_hash
        if hasattr(tx, 'actions'):
            try:
                tx.actions = [
                    convert_json_action_to_py_action(action_dict)
                    for action_dict in tx.actions
                ]
            except ValueError:
                continue
        tx.publicKey = PublicKey()
        tx.publicKey.keyType = 0
        tx.publicKey.data = key_pair.decoded_pk()
        msg = BinarySerializer(schema).serialize(tx)
        hash_ = hashlib.sha256(msg).digest()
        signature = Signature()
        signature.keyType = 0
        signature.data = key_pair.sign_bytes(hash_)
        resigned_tx = SignedTransaction()
        resigned_tx.transaction = tx
        resigned_tx.signature = signature
        resigned_tx.hash = hash_
        my_account.send_tx(BinarySerializer(schema).serialize(resigned_tx))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup replay')
    parser.add_argument('operation',
                        type=str,
                        help="choose between [generate/send]")
    parser.add_argument('--tx-json',
                        type=str,
                        required=False,
                        help="Path of tx history json")
    parser.add_argument('--genesis',
                        type=str,
                        required=False,
                        help="Path of genesis")
    parser.add_argument('--home-dir',
                        type=str,
                        required=True,
                        help="Path of the new home directory")
    args = parser.parse_args()

    if args.operation == 'generate':
        if args.genesis:
            key_pair = generate_new_key()
            save_genesis_with_new_key_pair(args.genesis, key_pair,
                                           args.home_dir)
            prompt_to_launch_localnet(args.home_dir)
        else:
            parser.error('Cannot run generate without genesis')
    elif args.operation == 'send':
        if args.tx_json:
            send_resigned_transactions(args.tx_json, args.home_dir)
        else:
            parser.error('Cannot run send without tx history')
    else:
        parser.error(
            'Unsupported positional argument: replay [operation] where operation = %s'
            % args.opeartion)

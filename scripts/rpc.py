#!/usr/bin/env python

import argparse
import hashlib
import json
import os
import subprocess
import sys

try:
    # py2
    from urllib2 import urlopen, Request, HTTPError, URLError
except ImportError:
    # py3
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError

try:
    import bson
except ImportError:
    print("Please install bson: pip install bson")
    exit(1)


# Data is empty string instead of None because method is
# defined by whether or not data is None and cannot be
# specified otherwise in py2
def _post(url, data=""):
    if data != "":
        data = json.dumps(data).encode('utf-8')

    request = Request(url, data=data)

    if data is not None:
        request.add_header('Content-Type', 'application/json')

    connection = urlopen(request)
    return connection


alphabet = b'123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def b58encode_int(i, default_one=True):
    """Encode an integer using Base58"""
    if not i and default_one:
        return alphabet[0:1]

    string = b''
    while i:
        i, idx = divmod(i, 58)
        string = alphabet[idx:idx + 1] + string
    return string


def b58encode(v):
    """Encode a string using Base58"""
    pad_size = len(v)
    v = v.lstrip(b'\0')
    pad_size -= len(v)

    p, acc = 1, 0
    for c in reversed(list(bytearray(v))):
        acc += p * c
        p = p << 8

    result = b58encode_int(acc, default_one=False)

    return alphabet[0:1] * pad_size + result


def _get_account_id(account_alias):
    digest = hashlib.sha256(account_alias.encode('utf-8')).digest()
    return b58encode(digest).decode('utf-8')


def _json_to_bson_bytes(args):
    args = json.loads(args)
    return list(bytearray(bson.dumps(args)))


class NearRPC(object):
    def __init__(
            self,
            server_url,
            keystore_binary=None,
            keystore_path=None,
            public_key=None,
            debug=False,
    ):
        self._server_url = server_url
        self._keystore_binary = keystore_binary
        self._keystore_path = keystore_path
        self._nonces = {}
        self._debug = debug

        # This may be None, use 'self._get_public_key' in order
        # to check against the keystore
        self._public_key = public_key

    def _get_nonce(self, sender):
        if sender not in self._nonces:
            view_result = self.view_account(sender)
            self._nonces[sender] = view_result.get('nonce', 0) + 1

        return self._nonces[sender]

    def _update_nonce(self, sender):
        self._nonces[sender] += 1

    def _call_rpc(self, method_name, params=None):
        data = params
        if self._debug:
            print(data)

        try:
            connection = _post(self._server_url + method_name, data)
            raw = connection.read()
            if self._debug:
                print(raw)
            return json.loads(raw)
        except HTTPError as e:
            if e.code == 400:
                print(e.fp.read())
                exit(1)
            raise
        except URLError:
            error = "Connection to {} refused. " \
                    "To start RPC server at http://127.0.0.1:3030, run:\n" \
                    "cargo run -p devnet"
            print(error.format(self._server_url))
            exit(1)

    def _sign_transaction_body(self, body):
        if self._keystore_binary is not None:
            args = [self._keystore_binary]
        else:
            args = 'cargo run -p keystore --'.split()

        args += [
            'sign_transaction',
            '--data',
            json.dumps(body),
            '--keystore-path',
            self._keystore_path,
        ]

        if self._public_key is not None:
            args += ['--public-key', self._public_key]

        null = open(os.devnull, 'w')
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=null)
        stdout = process.communicate()[0].decode('utf-8')
        if process.returncode != 0:
            sys.stdout.write(stdout)
            exit(1)

        return json.loads(stdout)

    def _handle_prepared_transaction_body_response(self, response):
        signed_transaction = self._sign_transaction_body(response['body'])
        return self._call_rpc('submit_transaction', signed_transaction)

    def _get_public_key(self):
        if self._public_key is None:
            if self._keystore_binary is not None:
                args = [self._keystore_binary]
            else:
                args = 'cargo run -p keystore --'.split()

            args += ['get_public_key', '--keystore-path', self._keystore_path]

            null = open(os.devnull, 'w')
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=null)
            stdout = process.communicate()[0].decode('utf-8')
            if process.returncode != 0:
                sys.stdout.write(stdout)

                if process.returncode == 3:
                    _help = "To create, run:\ncargo run -p keystore " \
                           "-- keygen -p {keystore_path}"
                    print(_help.format(keystore_path=self._keystore_path))

                exit(1)

            self._public_key = stdout
        return self._public_key

    def deploy_contract(self, sender, contract_name, wasm_file):
        with open(wasm_file, 'rb') as f:
            wasm_byte_array = list(bytearray(f.read()))

        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender_account_id': _get_account_id(sender),
            'contract_account_id': _get_account_id(contract_name),
            'wasm_byte_array': wasm_byte_array,
            'public_key': self._get_public_key(),
        }
        self._update_nonce(sender)
        response = self._call_rpc('deploy_contract', params)
        return self._handle_prepared_transaction_body_response(response)

    def send_money(self, sender, receiver, amount):
        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender_account_id': _get_account_id(sender),
            'receiver_account_id': _get_account_id(receiver),
            'amount': amount,
        }
        self._update_nonce(sender)
        response = self._call_rpc('send_money', params)
        return self._handle_prepared_transaction_body_response(response)

    def stake(self, sender, amount):
        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'staker_account_id': sender,
            'amount': amount,
        }
        self._update_nonce(sender)
        response = self._call_rpc('stake', params)
        return self._handle_prepared_transaction_body_response(response)

    def schedule_function_call(
        self,
        sender,
        contract_name,
        method_name,
        amount,
        args=None,
    ):
        if args is None:
            args = "{}"
        args = _json_to_bson_bytes(args)

        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'originator_account_id': _get_account_id(sender),
            'contract_account_id': _get_account_id(contract_name),
            'method_name': method_name,
            'args': args,
            'amount': amount,
        }
        self._update_nonce(sender)
        response = self._call_rpc('schedule_function_call', params)
        return self._handle_prepared_transaction_body_response(response)

    def view_state(self, contract_name):
        params = {'contract_account_id': _get_account_id(contract_name)}
        return self._call_rpc('view_state', params)

    def view_latest_beacon_block(self):
        return self._call_rpc('view_latest_beacon_block')

    def get_beacon_block_by_hash(self, _hash):
        params = {'hash': _hash}
        return self._call_rpc('get_beacon_block_by_hash', params)

    def view_latest_shard_block(self):
        return self._call_rpc('view_latest_shard_block')

    def get_shard_block_by_hash(self, _hash):
        params = {'hash': _hash}
        return self._call_rpc('get_shard_block_by_hash', params)

    def create_account(
        self,
        sender,
        account_alias,
        amount,
        account_public_key,
    ):
        if not account_public_key:
            account_public_key = self._get_public_key()

        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender': _get_account_id(sender),
            'new_account_id': _get_account_id(account_alias),
            'amount': amount,
            'public_key': account_public_key,
        }
        self._update_nonce(sender)
        response = self._call_rpc('create_account', params)
        return self._handle_prepared_transaction_body_response(response)

    def swap_key(
        self,
        account,
        current_key,
        new_key,
    ):
        nonce = self._get_nonce(account)
        params = {
            'nonce': nonce,
            'account': _get_account_id(account),
            'current_key': current_key,
            'new_key': new_key,
        }
        self._update_nonce(account)
        response = self._call_rpc('swap_key', params)
        return self._handle_prepared_transaction_body_response(response)

    def view_account(self, account_alias):
        params = {
            'account_id': _get_account_id(account_alias),
        }
        return self._call_rpc('view_account', params)

    def call_view_function(
        self,
        originator,
        contract_name,
        function_name,
        args=None,
    ):
        if args is None:
            args = "{}"
        args = _json_to_bson_bytes(args)

        params = {
            'originator_id': _get_account_id(originator),
            'contract_account_id': _get_account_id(contract_name),
            'method_name': function_name,
            'args': args,
        }
        return self._call_rpc('call_view_function', params)


class MultiCommandParser(object):
    def __init__(self):
        parser = argparse.ArgumentParser(
            usage="""python rpc.py <command> [<args>]

Commands:
call_view_function        {}
deploy                    {}
send_money                {}
schedule_function_call    {}
view_account              {}
view_state                {}
stake                     {}
create_account            {}
swap_key                  {}
view_latest_beacon_block  {}
get_beacon_block_by_hash  {}
view_latest_shard_block   {}
get_beacon_block_by_hash  {}
            """.format(
                self.call_view_function.__doc__,
                self.deploy.__doc__,
                self.send_money.__doc__,
                self.schedule_function_call.__doc__,
                self.view_account.__doc__,
                self.view_state.__doc__,
                self.stake.__doc__,
                self.create_account.__doc__,
                self.swap_key.__doc__,
                self.view_latest_beacon_block.__doc__,
                self.get_beacon_block_by_hash.__doc__,
                self.view_latest_shard_block.__doc__,
                self.get_shard_block_by_hash.__doc__,
            )
        )
        parser.add_argument('command', help='Command to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command: {}\n".format(args.command))
            parser.print_help()
            exit(1)
        else:
            response = getattr(self, args.command)()
            print(json.dumps(response))

    @staticmethod
    def _get_command_parser(description):
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            '-u',
            '--server-url',
            type=str,
            default='http://127.0.0.1:3030/',
            help='url of RPC server',
        )
        parser.add_argument(
            '--debug',
            action="store_true",
            default=False,
            help='set to emit debug logs',
        )
        return parser

    @staticmethod
    def _add_transaction_args(parser):
        parser.add_argument(
            '-s',
            '--sender',
            type=str,
            default='alice',
            help='account alias of sender',
        )
        parser.add_argument(
            '-b',
            '--keystore-binary',
            type=str,
            help='location of keystore binary',
        )
        parser.add_argument(
            '-d',
            '--keystore-path',
            type=str,
            default='keystore/',
            help='location of keystore for signing transactions',
        )
        parser.add_argument(
            '-k',
            '--public-key',
            type=str,
            help='public key for signing transactions',
        )

    @staticmethod
    def _get_command_args(parser):
        # now that we're inside a sub-command, ignore the first
        # two args, ie the command (rpc.py) and the sub-command (send_money)
        return parser.parse_args(sys.argv[2:])

    @staticmethod
    def _get_rpc_client(command_args):
        keystore_binary = getattr(command_args, 'keystore_binary', None)
        keystore_path = getattr(command_args, 'keystore_path', None)
        public_key = getattr(command_args, 'public_key', None)
        return NearRPC(
            command_args.server_url,
            keystore_binary,
            keystore_path,
            public_key,
            command_args.debug,
        )

    def send_money(self):
        """Send money from one account to another"""
        parser = self._get_command_parser(self.send_money.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument(
            '-r',
            '--receiver',
            type=str,
            default='bob',
            help='account alias of receiver',
        )
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money being sent',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.send_money(args.sender, args.receiver, args.amount)

    def deploy(self):
        """Deploy a smart contract"""
        parser = self._get_command_parser(self.deploy.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('wasm_file_location', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.deploy_contract(
            args.sender,
            args.contract_name,
            args.wasm_file_location,
        )

    def create_account(self):
        """Create an account"""
        parser = self._get_command_parser(self.create_account.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('account_alias', type=str)
        parser.add_argument('amount', type=int)
        parser.add_argument('--account_public-key', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.create_account(
            args.sender,
            args.account_alias,
            args.amount,
            args.account_public_key,
        )

    def swap_key(self):
        """Swap key for an account"""
        parser = self._get_command_parser(self.swap_key.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('current_key', type=str)
        parser.add_argument('new_key', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.swap_key(
            args.sender,
            args.current_key,
            args.new_key,
        )

    def schedule_function_call(self):
        """Schedule a function call on a smart contract"""
        parser = self._get_command_parser(self.schedule_function_call.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        parser.add_argument('--args', type=str, default="{}")
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money being sent with the function call',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.schedule_function_call(
            args.sender,
            args.contract_name,
            args.function_name,
            args.amount,
            args.args,
        )

    def call_view_function(self):
        """Call a view function on a smart contract"""
        parser = self._get_command_parser(self.call_view_function.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        parser.add_argument('--args', type=str, default="{}")
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.call_view_function(
            args.sender,
            args.contract_name,
            args.function_name,
            args.args,
        )

    def view_account(self):
        """View an account"""
        parser = self._get_command_parser(self.view_account.__doc__)
        parser.add_argument(
            '-a',
            '--account',
            type=str,
            default='alice',
            help='alias of account to view',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_account(args.account)

    def stake(self):
        """Stake money for validation"""
        parser = self._get_command_parser(self.stake.__doc__)
        self._add_transaction_args(parser)
        parser.add_argument(
            '-a',
            '--amount',
            type=int,
            default=0,
            help='amount of money to stake',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.stake(args.sender, args.amount)

    def view_state(self):
        """View state of the contract."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('contract_name', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_state(args.contract_name)

    def view_latest_beacon_block(self):
        """View latest beacon block."""
        parser = self._get_command_parser(self.view_state.__doc__)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_latest_beacon_block()

    def get_beacon_block_by_hash(self):
        """Get beacon block by hash."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('hash', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.get_beacon_block_by_hash(args.hash)

    def view_latest_shard_block(self):
        """View latest shard block."""
        parser = self._get_command_parser(self.view_state.__doc__)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view_latest_shard_block()

    def get_shard_block_by_hash(self):
        """Get shard block by hash."""
        parser = self._get_command_parser(self.view_state.__doc__)
        parser.add_argument('hash', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.get_shard_block_by_hash(args.hash)


if __name__ == "__main__":
    MultiCommandParser()

#!/usr/bin/env python

import argparse
import hashlib
import sys

try:
    import requests
except ImportError:
    import urllib
    import urllib2
    import json as json_lib


    def _post(url, json):
        """This is alternative to requests.post"""
        handler = urllib2.HTTPHandler()
        opener = urllib2.build_opener(handler)
        request = urllib2.Request(url, data=json_lib.dumps(json))
        request.add_header('Content-Type', 'application/json')
        try:
            connection = opener.open(request)
        except urllib2.HTTPError as e:
            connection = e
        if connection.code == 200:
            data = connection.read()
            return json_lib.loads(data)
        else:
            return {'error': connection}
else:
    import requests


    def _post(url, json):
        response = requests.post(url, json=json).json()
        if 'error' in response:
            raise Exception(response)
        return response

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
    return b58encode(digest)


class NearRPC(object):
    def __init__(self, server_url):
        self.server_url = server_url
        self._nonces = {}

    def _get_nonce(self, sender):
        if sender not in self._nonces:
            view_result = self.view_account(sender)
            self._nonces[sender] = view_result['result'].get('nonce', 0) + 1

        return self._nonces[sender]

    def _update_nonce(self, sender):
        self._nonces[sender] += 1

    def _call_rpc(self, method_name, params):
        data = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': method_name,
            'params': [params],
        }
        return _post(self.server_url, data)

    def deploy_contract(self, sender, contract_name, wasm_file):
        with open(wasm_file, 'rb') as f:
            wasm_byte_array = list(bytearray(f.read()))

        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender_account_id': _get_account_id(sender),
            'contract_account_id': _get_account_id(contract_name),
            'wasm_byte_array': wasm_byte_array,
        }
        self._update_nonce(sender)
        return self._call_rpc('deploy_contract', params)

    def send_money(self, sender, receiver, amount):
        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender_account_id': _get_account_id(sender),
            'receiver_account_id': _get_account_id(receiver),
            'amount': amount,
        }
        self._update_nonce(sender)
        return self._call_rpc('send_money', params)

    def schedule_function_call(
        self,
        sender,
        contract_name,
        method_name,
        args=None,
    ):
        if args is None:
            args = [[]]

        nonce = self._get_nonce(sender)
        params = {
            'nonce': nonce,
            'sender_account_id': _get_account_id(sender),
            'contract_account_id': _get_account_id(contract_name),
            'method_name': method_name,
            'args': args,
        }
        self._update_nonce(sender)
        return self._call_rpc('schedule_function_call', params)

    def view_account(self, account_alias):
        params = {
            'account_id': _get_account_id(account_alias),
        }
        return self._call_rpc('view_account', params)

    def call_view_function(self, contract_name, function_name, args=None):
        if args is None:
            args = [[]]

        params = {
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
call_view_function       {}
deploy                   {}
send_money               {}
schedule_function_call   {}
view_account             {}
            """.format(
                self.call_view_function.__doc__,
                self.deploy.__doc__,
                self.send_money.__doc__,
                self.schedule_function_call.__doc__,
                self.view_account.__doc__,
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
            print(getattr(self, args.command)())

    @staticmethod
    def _get_command_parser(description, include_sender=True):
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            '-u',
            '--server-url',
            type=str,
            default='http://127.0.0.1:3030',
            help='url of RPC server',
        )
        if include_sender:
            parser.add_argument(
                '-s',
                '--sender',
                type=str,
                default='alice',
                help='account alias of sender',
            )

        return parser

    @staticmethod
    def _get_command_args(parser):
        # now that we're inside a sub-command, ignore the first
        # two args, ie the command (rpc.py) and the sub-command (send_money)
        return parser.parse_args(sys.argv[2:])

    @staticmethod
    def _get_rpc_client(command_args):
        return NearRPC(command_args.server_url)

    def send_money(self):
        """Send money from one account to another"""
        parser = self._get_command_parser(self.send_money.__doc__)
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
        parser.add_argument('contract_name', type=str)
        parser.add_argument('wasm_file_location', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.deploy_contract(
            args.sender,
            args.contract_name,
            args.wasm_file_location,
        )

    def schedule_function_call(self):
        """Schedule a function call on a smart contract"""
        parser = self._get_command_parser(self.schedule_function_call.__doc__)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.schedule_function_call(
            args.sender,
            args.contract_name,
            args.function_name,
        )

    def call_view_function(self):
        """Call a view function on a smart contract"""
        parser = self._get_command_parser(self.call_view_function.__doc__)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('function_name', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.call_view_function(
            args.contract_name,
            args.function_name,
        )

    def view_account(self):
        """View an account"""
        parser = self._get_command_parser(self.view_account.__doc__, include_sender=False)
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


if __name__ == "__main__":
    MultiCommandParser()

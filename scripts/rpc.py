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


    def post(url, json):
        """This is alternative to requests.post"""
        handler = urllib2.HTTPHandler()
        opener = urllib2.build_opener(handler)
        request = urllib2.Request(url, data=json_lib.dumps(json))
        request.add_header("Content-Type", "application/json")
        try:
            connection = opener.open(request)
        except urllib2.HTTPError as e:
            connection = e
        if connection.code == 200:
            data = connection.read()
            return json_lib.loads(data)
        else:
            return {"error": connection}
else:
    import requests


    def post(url, json):
        return requests.post(url, json=json).json()


def near_hash(s):
    digest = hashlib.sha256(s.encode('utf-8')).digest()
    return list(bytearray(digest))


class NearRPC(object):
    def __init__(self, server_url):
        self.server_url = server_url
        self.nonce = None

    def view(self, account_id):
        data = {
            "jsonrpc": "2.0", "id": 1, "method": "view",
            "params": [{"account": near_hash(account_id), "method_name": "", "args": []}]
        }
        return post(self.server_url, json=data)

    def _send_transaction(self, sender, receiver, amount, method_name, args):
        if self.nonce is None:
            view_result = self.view(sender)
            self.nonce = view_result['result'].get('nonce', 0) + 1

        data = {
            "jsonrpc": "2.0", "id": 1, "method": "receive_transaction",
            "params": [
                {
                    "nonce": self.nonce,
                    "sender": near_hash(sender),
                    "receiver": near_hash(receiver),
                    "amount": amount,
                    "method_name": method_name,
                    "args": args
                }
            ]
        }
        self.nonce += 1
        return post(self.server_url, json=data)

    def send_money(self, sender, receiver, amount):
        return self._send_transaction(sender, receiver, amount, '', [])

    def call_function(self, contract_name, method_name, args):
        return self._send_transaction(
            contract_name,
            contract_name,
            0,
            method_name,
            args,
        )

    def deploy_contract(self, contract_name, wasm_file):
        with open(wasm_file, 'rb') as f:
            content = list(bytearray(f.read()))
        return self._send_transaction(
            contract_name,
            contract_name,
            0,
            'deploy',
            [content],
        )


class MultiCommandParser(object):
    def __init__(self):
        parser = argparse.ArgumentParser(
            usage="""python rpc.py <command> [<args>]

Commands:
call_method     {}
deploy          {}
send_money      {}
view            {}
            """.format(
                self.call_method.__doc__,
                self.deploy.__doc__,
                self.send_money.__doc__,
                self.view.__doc__,
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
    def _get_command_parser(description):
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
            '-s',
            '--sender',
            type=str,
            default='alice',
            help='account alias of issuer',
        )
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
            args.contract_name,
            args.wasm_file_location,
        )

    def call_method(self):
        """Call a method on a smart contract"""
        parser = self._get_command_parser(self.call_method.__doc__)
        parser.add_argument('contract_name', type=str)
        parser.add_argument('method_name', type=str)
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.call_function(
            args.contract_name,
            args.method_name,
            args=[],
        )

    def view(self):
        """View an account"""
        parser = self._get_command_parser(self.view.__doc__)
        parser.add_argument(
            '-a',
            '--account',
            type=str,
            default='alice',
            help='alias of account to view',
        )
        args = self._get_command_args(parser)
        client = self._get_rpc_client(args)
        return client.view(args.account)


if __name__ == "__main__":
    MultiCommandParser()

#!/usr/bin/python

import argparse

try:
    import requests

    post = lambda url, json: requests.post(url, json=json).json()
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


class NearRPC(object):

    def __init__(self, server_url, sender):
        self.server_url = server_url
        self.sender = sender
        view_result = self.view(sender)
        self.nonce = view_result['result'].get('nonce', 1)

    def view(self, account_id):
        data = {"jsonrpc": "2.0", "id": 1, "method": "view", "params": [{"account": account_id}]}
        return post(self.server_url, json=data)

    def send_transaction(self, receiver, amount, method_name, args):
        data = {
            "jsonrpc": "2.0", "id": 1, "method": "receive_transaction",
            "params": [
                {
                    "nonce": self.nonce,
                    "sender": self.sender,
                    "receiver": receiver,
                    "amount": amount,
                    "method_name": method_name,
                    "args": args
                }
            ]
        }
        self.nonce += 1
        return post(self.server_url, json=data)

    def send_money(self, receiver, amount):
        return self.send_transaction(receiver, amount, '', [])

    def call_function(self, receiver, method_name, args):
        return self.call_function(receiver, 0, method_name, args)

    def deploy_contract(self, receiver, wasm_file):
        with open(wasm_file, 'rb') as f:
            content = f.read()
        wasm_bytes = [ord(x) for x in content]
        return self.send_transaction(receiver, 0, 'deploy', [wasm_bytes])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--server_url', type=str, default='http://127.0.0.1:3030')
    parser.add_argument('--account', type=int, default=1)
    parser.add_argument('--view', type=int, default=0)
    parser.add_argument('--send_money', action='store_true', default=False)
    parser.add_argument('--receiver', type=int, default=0)
    parser.add_argument('--amount', type=int, default=0)
    parser.add_argument('--method_name', type=str, default='')
    parser.add_argument('--deploy', action='store_true', default=False)
    parser.add_argument('--wasm', type=str, default='')
    args, _ = parser.parse_known_args()

    rpc = NearRPC(args.server_url, args.account)
    if args.view != 0:
        print(rpc.view(args.view))
    elif args.send_money:
        print(rpc.send_money(args.receiver, args.amount))
    elif args.deploy or args.method_name == 'deploy':
        print(rpc.deploy_contract(args.receiver, args.wasm))
    elif args.method_name != '':
        print(rpc.call_function(args.receiver, args.method_name, []))

import requests
import time
import json

nonce = 0
while True:
    args = {"jsonrpc":"2.0", "id": 1, "method": "receive_transaction", "params": [{"nonce":nonce, "sender":1,"receiver":2,"amount":0,"method_name": "run_test", "args":[[]]}]}
    r = requests.post("http://127.0.0.1:3030", json=args)
    print(r.json())
    nonce += 1
    time.sleep(1)
    args = {"jsonrpc":"2.0", "id": 1, "method": "receive_transaction", "params": [{"nonce":nonce, "sender":1,"receiver":2,"amount":1,"method_name": "", "args":[[]]}]}
    r = requests.post("http://127.0.0.1:3030", json=args)
    print(r.json())
    nonce += 1
    time.sleep(1)
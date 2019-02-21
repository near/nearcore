#!/usr/bin/env python
import os
import random
import json
from time import sleep
from subprocess import call

def setup(account_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(os.path.join(dir_path, '..'))
    # Deploying a contract
    call(("./scripts/rpc.py", "create_account", "test_contract", "0"))
    sleep(3.1)
    call(("./scripts/rpc.py", "deploy", "test_contract", "tests/hello.wasm"))
    sleep(3.1)
    call(("./scripts/rpc.py", "create_account", account_id, "1"))
    sleep(3.1)


NUM_KEYS = 100

def random_int():
    return random.randint(0, 1<<16)

def stress_one(account_id):
    # Starting spamming transactions
    total_n = 0;
    while True:
        total_n += NUM_KEYS;
        offset = random.randint(0, total_n);
        os.system(" ".join(("./scripts/rpc.py", "schedule_function_call", "-s", account_id, "test_contract", "store_many", "--args='{\"offset\":%d,\"n\":%d}'" % (offset, NUM_KEYS))))
        sleep(0.1)
        os.system(" ".join(("./scripts/rpc.py", "call_view_function", "-s", account_id, "test_contract", "read_many", "--args='{\"offset\":%d,\"n\":%d}'" % (offset, NUM_KEYS))))
        sleep(0.1)

def stress_two(account_id):
    # Starting spamming transactions
    while True:
        keys = ["%d" % random_int() for _ in range(NUM_KEYS)]
        values = ["%d" % random_int() for _ in range(NUM_KEYS)]
        write_args = json.dumps({
            "keys": keys,
            "values": values,
        })
        os.system(" ".join(("./scripts/rpc.py", "schedule_function_call", "-s", account_id, "test_contract", "store_many_strs", "--args='%s'" % (write_args,))))
        sleep(0.1)
        read_args = json.dumps({
            "keys": keys,
        })
        os.system(" ".join(("./scripts/rpc.py", "call_view_function", "-s", account_id, "test_contract", "read_many_strs", "--args='%s'" % (read_args,))))
        sleep(0.1)

def main():
    account_id = "acc_%d" % random_int()
    setup(account_id)
    stress_two(account_id)

if __name__ == "__main__":
    main()
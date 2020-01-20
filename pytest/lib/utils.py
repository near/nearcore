from transaction import sign_payment_tx
import random, base58
from retry import retry
from cluster import LocalNode, GCloudNode
from rc import run
import os
import tempfile
class TxContext:
    def __init__(self, act_to_val, nodes):
        self.next_nonce = 2
        self.num_nodes = len(nodes)
        self.nodes = nodes
        self.act_to_val = act_to_val
        print(f'get_balances')
        self.expected_balances = self.get_balances()
        print(f'get_balances done')
        assert len(act_to_val) == self.num_nodes
        assert self.num_nodes >= 2

    @retry(tries=10, backoff=1.2)
    def get_balance(self, whose):
        print(f'get_balance {whose}')
        r = self.nodes[self.act_to_val[whose]].get_account("test%s" % whose)
        assert 'result' in r, r
        return int(r['result']['amount']) + int(r['result']['locked'])
        print(f'get_balance {whose} done')

    def get_balances(self):
        return [
            self.get_balance(i)
            for i in range(self.num_nodes)
        ]

    def send_moar_txs(self, last_block_hash, num, use_routing):
        last_balances = [x for x in self.expected_balances]
        for i in range(num):
            while True:
                from_ = random.randint(0, self.num_nodes - 1)
                if self.nodes[from_] is not None:
                    break
            to = random.randint(0, self.num_nodes - 2)
            if to >= from_:
                to += 1
            amt = random.randint(0, 500)
            if self.expected_balances[from_] >= amt:
                print("Sending a tx from %s to %s for %s" % (from_, to, amt));
                tx = sign_payment_tx(self.nodes[from_].signer_key, 'test%s' % to, amt, self.next_nonce, base58.b58decode(last_block_hash.encode('utf8')))
                if use_routing:
                    self.nodes[0].send_tx(tx)
                else:
                    self.nodes[self.act_to_val[from_]].send_tx(tx)
                self.expected_balances[from_] -= amt
                self.expected_balances[to] += amt
                self.next_nonce += 1


# opens up a log file, scrolls to the end. then allows to check if
# a particular line appeared (or didn't) between the last time it was
# checked and now
class LogTracker:
    def __init__(self, node):
        self.node = node
        if type(node) is LocalNode:
            self.fname = node.stderr_name
            with open(self.fname) as f:
                f.seek(0, 2)
                self.offset = f.tell()
        elif type(node) is GCloudNode:
            self.offset = int(node.machine.run("python3", input='''
with open('/tmp/python-rc.log') as f:
    f.seek(0, 2)
    print(f.tell())
''').stdout)
        else:
            # the above method should works for other cloud, if it has node.machine but untested
            raise NotImplementedError()

    # Check whether there is at least on occurrence of pattern in new logs
    def check(self, pattern):
        if type(self.node) is LocalNode:
            with open(self.fname) as f:
                f.seek(self.offset)
                ret = pattern in f.read()
                self.offset = f.tell()
            return ret
        elif type(self.node) is GCloudNode:
            ret, offset = node.machine.run("python3", input=f'''
pattern={pattern}
with open('/tmp/python-rc.log') as f:
    f.seek({self.offset})
    print(s in f.read())
    print(f.tell())
''').stdout.strip().split('\n')
            self.offset = int(offset)
            return ret == "True"
        else:
            raise NotImplementedError()

    def reset(self):
        self.offset = 0

    # Count number of occurrences of pattern in new logs
    def count(self, pattern):
        if type(self.node) is LocalNode:
            with open(self.fname) as f:
                f.seek(self.offset)
                ret = f.read().count(pattern)
                self.offset = f.tell()
            return ret
        elif type(self.node) == GCloudNode:
            ret, offset = node.machine.run("python3", input=f'''
with open('/tmp/python-rc.log') as f:
    f.seek({self.offset})
    print(f.read().count({pattern})
    print(f.tell())
''').stdout.strip().split('\n')
            ret = int(ret)
            self.offset = int(offset)
            return ret
        else:
            raise NotImplementedError()

def load_binary_file(filepath):
    with open(filepath, "rb") as binaryfile:
        return bytearray(binaryfile.read())

def compile_rust_contract(content):
    empty_contract_rs = os.path.join(os.path.dirname(__file__), '../empty-contract-rs')
    run('mkdir -p /tmp/near')
    tmp_contract = tempfile.TemporaryDirectory(dir='/tmp/near').name
    p = run(f'cp -r {empty_contract_rs} {tmp_contract}')
    if p.returncode != 0:
        raise Exception(p.stderr)
    
    with open(f'{tmp_contract}/src/lib.rs', 'a') as f:
        f.write(content)

    p = run('bash', input=f'''
cd {tmp_contract}
./build.sh
''')
    if p.returncode != 0:
        raise Exception(p.stderr)
    return f'{tmp_contract}/target/release/empty_contract_rs.wasm'
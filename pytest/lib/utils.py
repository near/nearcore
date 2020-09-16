from transaction import sign_payment_tx
import random, base58
from retry import retry
from cluster import LocalNode, GCloudNode, CONFIG_ENV_VAR
import sys
from rc import run, gcloud
import os
import tempfile
import json
import hashlib
import time


class TxContext:

    def __init__(self, act_to_val, nodes):
        self.next_nonce = 2
        self.num_nodes = len(nodes)
        self.nodes = nodes
        self.act_to_val = act_to_val
        self.expected_balances = self.get_balances()
        assert len(act_to_val) == self.num_nodes
        assert self.num_nodes >= 2

    @retry(tries=10, backoff=1.2)
    def get_balance(self, whose):
        r = self.nodes[self.act_to_val[whose]].get_account("test%s" % whose)
        assert 'result' in r, r
        return int(r['result']['amount']) + int(r['result']['locked'])

    def get_balances(self):
        return [self.get_balance(i) for i in range(self.num_nodes)]

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
                print("Sending a tx from %s to %s for %s" % (from_, to, amt))
                tx = sign_payment_tx(
                    self.nodes[from_].signer_key, 'test%s' % to, amt,
                    self.next_nonce,
                    base58.b58decode(last_block_hash.encode('utf8')))
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
            self.offset = int(
                node.machine.run("python3",
                                 input='''
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
            ret, offset = map(
                int,
                node.machine.run("python3",
                                 input=f'''
pattern={pattern}
with open('/tmp/python-rc.log') as f:
    f.seek({self.offset})
    print(s in f.read())
    print(f.tell())
''').stdout.strip().split('\n'))
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
            ret, offset = node.machine.run("python3",
                                           input=f'''
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


def chain_query(node, block_handler, *, block_hash=None, max_blocks=-1):
    """
    Query chain block approvals and chunks preceding of block of block_hash.
    If block_hash is None, it query latest block hash
    It query at most max_blocks, or if it's -1, all blocks back to genesis
    """
    if block_hash is None:
        status = node.get_status()
        block_hash = status['sync_info']['latest_block_hash']

    initial_validators = node.validators()

    if max_blocks == -1:
        while True:
            validators = node.validators()
            if validators != initial_validators:
                print(
                    f'Fatal: validator set of node {node} changes, from {initial_validators} to {validators}'
                )
                sys.exit(1)
            block = node.get_block(block_hash)['result']
            block_handler(block)
            block_hash = block['header']['prev_hash']
            block_height = block['header']['height']
            if block_height == 0:
                break
    else:
        for _ in range(max_blocks):
            validators = node.validators()
            if validators != initial_validators:
                print(
                    f'Fatal: validator set of node {node} changes, from {initial_validators} to {validators}'
                )
                sys.exit(1)
            block = node.get_block(block_hash)['result']
            block_handler(block)
            block_hash = block['header']['prev_hash']
            block_height = block['header']['height']
            if block_height == 0:
                break


def load_binary_file(filepath):
    with open(filepath, "rb") as binaryfile:
        return bytearray(binaryfile.read())


def compile_rust_contract(content):
    empty_contract_rs = os.path.join(os.path.dirname(__file__),
                                     '../empty-contract-rs')
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


def user_name():
    username = os.getlogin()
    if username == 'root':  # digitalocean
        username = gcloud.list()[0].username.replace('_nearprotocol_com', '')
    return username


# from https://stackoverflow.com/questions/107705/disable-output-buffering
# this class allows making print always flush by executing
#
#     sys.stdout = Unbuffered(sys.stdout)
class Unbuffered(object):

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


def collect_gcloud_config(num_nodes):
    import pathlib
    keys = []
    for i in range(num_nodes):
        if not os.path.exists(f'/tmp/near/node{i}'):
            # TODO: avoid hardcoding the username
            print(f'downloading node{i} config from gcloud')
            pathlib.Path(f'/tmp/near/node{i}').mkdir(parents=True,
                                                     exist_ok=True)
            gcloud.get(f'pytest-node-{user_name()}-{i}').download(
                '/home/bowen_nearprotocol_com/.near/config.json',
                f'/tmp/near/node{i}/')
            gcloud.get(f'pytest-node-{user_name()}-{i}').download(
                '/home/bowen_nearprotocol_com/.near/signer0_key.json',
                f'/tmp/near/node{i}/')
            gcloud.get(f'pytest-node-{user_name()}-{i}').download(
                '/home/bowen_nearprotocol_com/.near/validator_key.json',
                f'/tmp/near/node{i}/')
            gcloud.get(f'pytest-node-{user_name()}-{i}').download(
                '/home/bowen_nearprotocol_com/.near/node_key.json',
                f'/tmp/near/node{i}/')
        with open(f'/tmp/near/node{i}/signer0_key.json') as f:
            key = json.load(f)
        keys.append(key)
    with open('/tmp/near/node0/config.json') as f:
        config = json.load(f)
    ip_addresses = map(lambda x: x.split('@')[-1],
                       config['network']['boot_nodes'].split(','))
    res = {
        'nodes':
            list(
                map(lambda x: {
                    'ip': x.split(':')[0],
                    'port': 3030
                }, ip_addresses)),
        'accounts':
            keys
    }
    outfile = '/tmp/near/gcloud_config.json'
    with open(outfile, 'w+') as f:
        json.dump(res, f)
    os.environ[CONFIG_ENV_VAR] = outfile


def obj_to_string(obj, extra='    ', full=False):
    if type(obj) in [tuple, list]:
        return "tuple" + '\n' + '\n'.join(
            (extra + obj_to_string(x, extra + '    '))
            for x in obj
        )
    elif hasattr(obj, "__dict__"):
        return str(obj.__class__) + '\n' + '\n'.join(
            extra + (str(item) + ' = ' +
                     obj_to_string(obj.__dict__[item], extra + '    '))
        for item in sorted(obj.__dict__))
    elif isinstance(obj, bytes):
        if not full:
            if len(obj) > 10:
                obj = obj[:7] + b"..."
        return str(obj)
    else:
        return str(obj)


def combine_hash(hash1, hash2):
    return hashlib.sha256(hash1 + hash2).digest()


def compute_merkle_root_from_path(path, leaf_hash):
    res = base58.b58decode(leaf_hash) if type(leaf_hash) is str else leaf_hash
    for node in path:
        if node['direction'] == 'Left':
            res = combine_hash(base58.b58decode(node['hash']), res)
        else:
            res = combine_hash(res, base58.b58decode(node['hash']))
    return res


def wait_for_blocks_or_timeout(node, num_blocks, timeout, callback=None, check_sec=1):
    status = node.get_status()
    start_height = status['sync_info']['latest_block_height']
    max_height = 0
    started = time.time()
    while max_height < start_height + num_blocks:
        assert time.time() - started < timeout
        status = node.get_status()
        max_height = status['sync_info']['latest_block_height']
        if callback is not None:
            if callback():
                break
        time.sleep(check_sec)

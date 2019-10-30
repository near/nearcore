import threading, subprocess, json, os, atexit, shutil, requests, time, base58, base64
import retry

def atexit_cleanup(node):
    print("Cleaning up node %s:%s on script exit" % node.addr())
    try:
        node.cleanup()
    except:
        print("Cleaning failed!")
        pass

class Key(object):
    def __init__(self, account_id, pk, sk):
        super(Key, self).__init__()
        self.account_id = account_id
        self.pk = pk
        self.sk = sk

    def decoded_pk(self):
        key = self.pk.split(':')[1] if ':' in self.pk else self.pk
        return base58.b58decode(key.encode('ascii'))

    def decoded_sk(self):
        key = self.sk.split(':')[1] if ':' in self.sk else self.sk
        return base58.b58decode(key.encode('ascii'))

    @classmethod
    def from_json(self, j):
        return Key(j['account_id'], j['public_key'], j['secret_key'])

    @classmethod
    def from_json_file(self, jf):
        with open(jf) as f:
            return Key.from_json(json.loads(f.read()))


class BaseNode(object):
    def _get_command_line(self, near_root, node_dir, boot_key, boot_node_addr):
        if boot_key is None:
            assert boot_node_addr is None
            return [os.path.join(near_root, 'near'), "--verbose", "", "--home", node_dir, "run"]
        else:
            assert boot_node_addr is not None
            boot_key = boot_key.split(':')[1]
            return [os.path.join(near_root, 'near'), "--verbose", "", "--home", node_dir, "run", '--boot-nodes', "%s@%s:%s" % (boot_key, boot_node_addr[0], boot_node_addr[1])]

    def wait_for_rpc(self):
        retry.retry(lambda: self.get_status(), timeout=1)

    def json_rpc(self, method, params):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post("http://%s:%s" % self.rpc_addr(), json=j)
        r.raise_for_status()
        return json.loads(r.content)

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async', [base64.b64encode(signed_tx).decode('utf8')])

    def get_status(self):
        r = requests.get("http://%s:%s/status" % self.rpc_addr())
        r.raise_for_status()
        return json.loads(r.content)

    def get_account(self, acc):
        return self.json_rpc('query', ["account/%s" % acc, ""])


class LocalNode(BaseNode):
    def __init__(self, port, rpc_port, near_root, node_dir):
        super(LocalNode, self).__init__()
        self.port = port
        self.rpc_port = rpc_port
        self.near_root = near_root
        self.node_dir = node_dir
        with open(os.path.join(node_dir, "config.json")) as f:
            config_json = json.loads(f.read())
        assert config_json['network']['addr'] == '0.0.0.0:24567', config_json['network']['addr']
        assert config_json['rpc']['addr'] == '0.0.0.0:3030', config_json['rpc']['addr']
        # just a sanity assert that the setting name didn't change
        assert 1 <= config_json['consensus']['min_num_peers'] <= 3
        config_json['network']['addr'] = '0.0.0.0:%s' % port
        config_json['rpc']['addr'] = '0.0.0.0:%s' % rpc_port
        config_json['consensus']['min_num_peers'] = 1
        with open(os.path.join(node_dir, "config.json"), 'w') as f:
            f.write(json.dumps(config_json, indent=2))

        self.validator_key = Key.from_json_file(os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(os.path.join(node_dir, "validator_key.json"))

        self.handle = None

        atexit.register(atexit_cleanup, self)


    def addr(self):
        return ("0.0.0.0", self.port)

    def rpc_addr(self):
        return ("0.0.0.0", self.rpc_port)

    def start(self, boot_key, boot_node_addr):
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"

        self.stdout = open(os.path.join(self.node_dir, 'stdout'), 'a')
        self.stderr = open(os.path.join(self.node_dir, 'stderr'), 'a')
        cmd = self._get_command_line(self.near_root, self.node_dir, boot_key, boot_node_addr)
        self.handle = subprocess.Popen(cmd, stdout=self.stdout, stderr=self.stderr, env=env)
        self.wait_for_rpc()

    def kill(self):
        if self.handle is not None:
            self.handle.kill()
            self.handle = None

    def cleanup(self):
        self.kill()
        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)


class BotoNode(BaseNode):
    pass


def spin_up_node(config, near_root, node_dir, ordinal, boot_key, boot_addr):
    is_local = config['local']

    print("Starting node %s %s" % (ordinal, ("as BOOT NODE" if boot_addr is None else ("with boot=%s@%s:%s" % (boot_key, boot_addr[0], boot_addr[1])))))
    if is_local:
        node = LocalNode(24567 + 10 + ordinal, 3030 + 10 + ordinal, near_root, node_dir)
    else:
        node = BotoNode()

    node.start(boot_key, boot_addr)

    return node


def init_cluster(num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes):
    is_local = config['local']
    near_root = config['near_root']

    print("Starting %s cluster with %s nodes" % ("LOCAL" if is_local else "REMOTE", num_nodes));

    process = subprocess.Popen([near_root + "near", "testnet", "--v", str(num_nodes), "--shards", str(num_shards), "--n", str(num_observers), "--prefix", "test"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    assert 0 == process.returncode, err

    node_dirs = [line.split()[-1] for line in err.decode('utf8').split('\n') if '/test' in line]
    assert len(node_dirs) == num_nodes + num_observers

    # apply config changes
    for i, node_dir in enumerate(node_dirs):
        # apply genesis_config.json changes
        fname = os.path.join(node_dir, 'genesis.json')
        with open(fname) as f:
            genesis_config = json.loads(f.read())
        for change in genesis_config_changes:
            cur = genesis_config
            for s in change[:-2]:
                cur = cur[s]
            assert change[-2] in cur
            cur[change[-2]] = change[-1]
        with open(fname, 'w') as f:
            f.write(json.dumps(genesis_config, indent=2))

        # apply config.json changes
        fname = os.path.join(node_dir, 'config.json')
        with open(fname) as f:
            config_json = json.loads(f.read())

        if i in client_config_changes:
            for k, v in client_config_changes[i].items():
                assert k in config_json
                config_json[k] = v

        with open(fname, 'w') as f:
            f.write(json.dumps(config_json, indent=2))

    return near_root, node_dirs


def start_cluster(num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes):
    near_root, node_dirs = init_cluster(num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes)

    ret = []
    def spin_up_node_and_push(i, boot_key, boot_addr):
        node = spin_up_node(config, near_root, node_dirs[i], i, boot_key, boot_addr)
        while len(ret) < i:
            time.sleep(0.01)
        ret.append(node)
        return node

    boot_node = spin_up_node_and_push(0, None, None)

    handles = []
    for i in range(1, num_nodes + num_observers):
        handle = threading.Thread(target = spin_up_node_and_push, args=(i, boot_node.node_key.pk, boot_node.addr()))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()

    return ret


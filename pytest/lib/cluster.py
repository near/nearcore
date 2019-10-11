import threading
import subprocess
import json
import os
import atexit
import shutil
import requests
import time
import base58
import base64
import retry
from gcloud import compute, create_instance, ssh, copy_to_instance
import uuid


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
        return base58.b58decode(self.pk.split(':')[1] if ':' in self.pk else self.pk)

    def decoded_sk(self):
        return base58.b58decode(self.sk.split(':')[1] if ':' in self.sk else self.sk)

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
            return [os.path.join(near_root, 'near'), "--verbose", "--home", node_dir, "run"]
        else:
            assert boot_node_addr is not None
            boot_key = boot_key.split(':')[1]
            return [os.path.join(near_root, 'near'), "--verbose", "--home", node_dir, "run", '--boot-nodes', "%s@%s:%s" % (boot_key, boot_node_addr[0], boot_node_addr[1])]

    def wait_for_rpc(self, timeout=1):
        retry.retry(lambda: self.get_status(), timeout=timeout)

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

        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "signer_key.json"))

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
        cmd = self._get_command_line(
            self.near_root, self.node_dir, boot_key, boot_node_addr)
        self.handle = subprocess.Popen(
            cmd, stdout=self.stdout, stderr=self.stderr, env=env)
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


class GCloudNode(BaseNode):
    def __init__(self, zone, instance_name):
        self.zone = zone
        self.instance_name = instance_name
        self.port = 24567
        self.rpc_port = 3000
        self.resource = compute().instances().get(project='near-core', zone=zone,
                                                instance=instance_name).execute()
        self.ip = self.resource['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    def addr(self):
        return (self.ip, self.port)

    def rpc_addr(self):
        return (self.ip, self.rpc_port)
    
    def exec(self, command):
        return ssh(self.zone, self.instance_name, command)

    def start(self, boot_key, boot_node_addr, zone='us-west2-a'):
        self.exec("""export RUSTUP_HOME=/opt/rustup
export CARGO_HOME=/opt/cargo
tmux new -s node -d
tmux send-keys -t node '{cmd}' C-m
""".format(cmd=" ".join(self._get_command_line('/opt/nearcore/target/debug/', '/opt/near', boot_key, boot_node_addr)))
        )
        self.wait_for_rpc(timeout=10)
    
    def kill(self):
        ssh(self.zone, self.instance_name, "tmux send-keys -t node C-c")

    def cleanup(self):
        ssh(self.zone, )


def spin_up_node(config, near_root, node_dir, ordinal, boot_key, boot_addr):
    is_local = config['local']

    print("Starting node %s %s" % (ordinal, ("as BOOT NODE" if boot_addr is None else (
        "with boot=%s@%s:%s" % (boot_key, boot_addr[0], boot_addr[1])))))
    if is_local:
        node = LocalNode(24567 + 10 + ordinal, 3030 +
                         10 + ordinal, near_root, node_dir)
    else:
        zone = config['remote']['zones'][ordinal % len(config['remote']['zones'])]
        instance_name = config['remote'].get(
            'instance_name', 'near-pytest-{}'.format(ordinal))
        node = GCloudNode(zone, instance_name)

    node.start(boot_key, boot_addr)

    return node


def init_cluster(num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes):
    """
    Create cluster configuration, if it's remote cluster, also launch instance and build near
    """
    is_local = config['local']
    near_root = config['near_root']

    print("Creating %s cluster configuration with %s nodes" %
          ("LOCAL" if is_local else "REMOTE", num_nodes + num_observers))

    process = subprocess.Popen([near_root + "near", "testnet", "--v", str(num_nodes), "--shards", str(
        num_shards), "--n", str(num_observers), "--prefix", "test"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    assert 0 == process.returncode, err

    node_dirs = [line.split()[-1]
                 for line in err.decode('utf8').split('\n') if '/test' in line]
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

    if not is_local:
        _launch_and_deploy_on_remote_instances(config, node_dirs)

    return near_root, node_dirs


def _launch_and_deploy_on_remote_instances(config, node_dirs):
    def instance_ready(zone, instance_name):
        res = compute().instances().get(project='near-core', zone=zone, instance=instance_name).execute()
        if res["status"] != "RUNNING":
            raise "Instance Not Ready"

    def _launch_and_deploy_on_remote_instance(config, i, node_dir):
        zone = config['remote']['zones'][i % len(config['remote']['zones'])]
        instance_name = config['remote'].get(
            'instance_name', 'near-pytest-{}'.format(i))
        res = create_instance(compute(), 'near-core', zone, instance_name, config['remote'].get('machine_type', 'n1-standard-1'))
        retry.retry(lambda: instance_ready(zone, instance_name), 60)
        ssh(zone, instance_name, """sudo chmod 777 /opt
cd /opt
mkdir near
sudo apt update
sudo apt install -y pkg-config libssl-dev build-essential cmake
export RUSTUP_HOME=/opt/rustup
export CARGO_HOME=/opt/cargo
curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-05-22
source $CARGO_HOME/env
git clone --single-branch --branch staging https://github.com/nearprotocol/nearcore.git nearcore
git checkout {}
cd nearcore
cargo build -p near
""".format(config['remote'].get('commit', 'staging'))
        )
        
        copy_to_instance(zone, instance_name, node_dir + "/*", "/opt/.near/")
    
    handles = []
    for i, node_dir in enumerate(node_dirs):
        handle = threading.Thread(target=_launch_and_deploy_on_remote_instance,
                                  args=(config, i, node_dir))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()


def start_cluster(num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes):
    near_root, node_dirs = init_cluster(
        num_nodes, num_observers, num_shards, config, genesis_config_changes, client_config_changes)

    ret = []

    def spin_up_node_and_push(i, boot_key, boot_addr):
        node = spin_up_node(config, near_root,
                            node_dirs[i], i, boot_key, boot_addr)
        while len(ret) < i:
            time.sleep(0.01)
        ret.append(node)
        return node

    boot_node = spin_up_node_and_push(0, None, None)

    handles = []
    for i in range(1, num_nodes + num_observers):
        handle = threading.Thread(target=spin_up_node_and_push, args=(
            i, boot_node.node_key.pk, boot_node.addr()))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()

    return ret

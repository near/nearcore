import multiprocessing
import threading
import subprocess
import json
import os
import sys
import signal
import atexit
import signal
import shutil
import requests
import time
import base58
import base64
from retrying import retry
import rc
from rc import gcloud
import traceback
import uuid
import network
from proxy import NodesProxy
from bridge import GanacheNode, RainbowBridge, alice, bob, carol
from key import Key

os.environ["ADVERSARY_CONSENT"] = "1"

remote_nodes = []
preexisting = None
remote_nodes_lock = threading.Lock()
cleanup_remote_nodes_atexit_registered = False


class DownloadException(Exception):
    pass


def atexit_cleanup(node):
    print("Cleaning up node %s:%s on script exit" % node.addr())
    print("Executed store validity tests: %s" % node.store_tests)
    try:
        node.cleanup()
    except:
        print("Cleaning failed!")
        traceback.print_exc()
        pass


def atexit_cleanup_remote():
    with remote_nodes_lock:
        if remote_nodes:
            rc.pmap(atexit_cleanup, remote_nodes)


# custom retry that is used in wait_for_rpc() and get_status()
def nretry(fn, timeout):
    started = time.time()
    delay = 0.05
    while True:
        try:
            return fn()
        except:
            if time.time() - started >= timeout:
                raise
            time.sleep(delay)
            delay *= 1.2


class BaseNode(object):
    def __init__(self):
        self._start_proxy = None
        self._proxy_local_stopped = None
        self.proxy = None
        self.store_tests = 0
        self.is_check_store = True


    def _get_command_line(self,
                          near_root,
                          node_dir,
                          boot_key,
                          boot_node_addr,
                          binary_name='neard'):
        if boot_key is None:
            assert boot_node_addr is None
            return [
                os.path.join(near_root, binary_name), "--verbose", "", "--home",
                node_dir, "run"
            ]
        else:
            assert boot_node_addr is not None
            boot_key = boot_key.split(':')[1]
            return [
                os.path.join(near_root, binary_name), "--verbose", "", "--home",
                node_dir, "run", '--boot-nodes',
                "%s@%s:%s" % (boot_key, boot_node_addr[0], boot_node_addr[1])
            ]

    def wait_for_rpc(self, timeout=1):
        nretry(lambda: self.get_status(), timeout=timeout)

    def json_rpc(self, method, params, timeout=2):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post("http://%s:%s" % self.rpc_addr(),
                          json=j,
                          timeout=timeout)
        r.raise_for_status()
        return json.loads(r.content)

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async',
                             [base64.b64encode(signed_tx).decode('utf8')])

    def send_tx_and_wait(self, signed_tx, timeout):
        return self.json_rpc('broadcast_tx_commit',
                             [base64.b64encode(signed_tx).decode('utf8')],
                             timeout=timeout)

    def get_status(self, check_storage=True, timeout=2):
        r = requests.get("http://%s:%s/status" % self.rpc_addr(), timeout=timeout)
        r.raise_for_status()
        status = json.loads(r.content)
        if check_storage and status['sync_info']['syncing'] == False:
            # Storage is not guaranteed to be in consistent state while syncing
            self.check_store()
        return status

    def get_all_heights(self):
        status = self.get_status()
        hash_ = status['sync_info']['latest_block_hash']
        heights = []

        while True:
            block = self.get_block(hash_)
            if 'error' in block and 'data' in block[
                    'error'] and 'DB Not Found Error: BLOCK:' in block['error']['data']:
                break
            elif 'result' not in block:
                print(block)

            height = block['result']['header']['height']
            if height == 0:
                break
            heights.append(height)
            hash_ = block['result']['header']['prev_hash']

        return list(reversed(heights))

    def get_validators(self):
        return self.json_rpc('validators', [None])

    def get_account(self, acc, finality='optimistic'):
        return self.json_rpc('query', {
            "request_type": "view_account",
            "account_id": acc,
            "finality": finality
        })

    def call_function(self, acc, method, args, finality='optimistic', timeout=2):
        return self.json_rpc('query', {
            "request_type": "call_function",
            "account_id": acc,
            "method_name": method,
            "args_base64": args,
            "finality": finality
        }, timeout=timeout)

    def get_access_key_list(self, acc, finality='optimistic'):
        return self.json_rpc(
            'query', {
                "request_type": "view_access_key_list",
                "account_id": acc,
                "finality": finality
            })

    def get_nonce_for_pk(self, acc, pk, finality='optimistic'):
        for access_key in self.get_access_key_list(acc,
                                                   finality)['result']['keys']:
            if access_key['public_key'] == pk:
                return access_key['access_key']['nonce']
        return None

    def get_block(self, block_id):
        return self.json_rpc('block', [block_id])

    def get_chunk(self, chunk_id):
        return self.json_rpc('chunk', [chunk_id])

    def get_tx(self, tx_hash, tx_recipient_id):
        return self.json_rpc('tx', [tx_hash, tx_recipient_id])

    def get_changes_in_block(self, changes_in_block_request):
        return self.json_rpc('EXPERIMENTAL_changes_in_block',
                             changes_in_block_request)

    def get_changes(self, changes_request):
        return self.json_rpc('EXPERIMENTAL_changes', changes_request)

    def validators(self):
        return set(
            map(lambda v: v['account_id'],
                self.get_status()['validators']))

    def stop_checking_store(self):
        print("WARN: Stopping checking Storage for inconsistency for %s:%s" %
              self.addr())
        self.is_check_store = False

    def check_store(self):
        if self.is_check_store:
            res = self.json_rpc('adv_check_store', [])
            if not 'result' in res:
                # cannot check Storage Consistency for the node, possibly not Adversarial Mode is running
                pass
            else:
                if res['result'] == 0:
                    print(
                        "ERROR: Storage for %s:%s in inconsistent state, stopping"
                        % self.addr())
                    self.kill()
                self.store_tests += res['result']


class RpcNode(BaseNode):
    """ A running node only interact by rpc queries """

    def __init__(self, host, rpc_port):
        super(RpcNode, self).__init__()
        self.host = host
        self.rpc_port = rpc_port

    def rpc_addr(self):
        return (self.host, self.rpc_port)


class LocalNode(BaseNode):

    def __init__(self,
                 port,
                 rpc_port,
                 near_root,
                 node_dir,
                 blacklist,
                 binary_name='neard'):
        super(LocalNode, self).__init__()
        self.port = port
        self.rpc_port = rpc_port
        self.near_root = near_root
        self.node_dir = node_dir
        self.binary_name = binary_name
        self.cleaned = False
        with open(os.path.join(node_dir, "config.json")) as f:
            config_json = json.loads(f.read())
        # assert config_json['network']['addr'] == '0.0.0.0:24567', config_json['network']['addr']
        # assert config_json['rpc']['addr'] == '0.0.0.0:3030', config_json['rpc']['addr']
        # just a sanity assert that the setting name didn't change
        assert 0 <= config_json['consensus']['min_num_peers'] <= 3, config_json[
            'consensus']['min_num_peers']
        config_json['network']['addr'] = '0.0.0.0:%s' % port
        config_json['network']['blacklist'] = blacklist
        config_json['rpc']['addr'] = '0.0.0.0:%s' % rpc_port
        config_json['rpc']['metrics_addr'] = '0.0.0.0:%s' % (rpc_port + 1000)
        config_json['consensus']['min_num_peers'] = 1
        with open(os.path.join(node_dir, "config.json"), 'w') as f:
            f.write(json.dumps(config_json, indent=2))

        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))

        self.pid = multiprocessing.Value('i', 0)

        atexit.register(atexit_cleanup, self)

    def addr(self):
        return ("127.0.0.1", self.port)

    def rpc_addr(self):
        return ("127.0.0.1", self.rpc_port)

    def start_proxy_if_needed(self):
        if self._start_proxy is not None:
            self._proxy_local_stopped = self._start_proxy()


    def start(self, boot_key, boot_node_addr, skip_starting_proxy=False):
        if self._proxy_local_stopped is not None:
            while self._proxy_local_stopped.value != 2:
                print(f'Waiting for previous proxy instance to close')
                time.sleep(1)


        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get("RUST_LOG", "debug")

        self.stdout_name = os.path.join(self.node_dir, 'stdout')
        self.stderr_name = os.path.join(self.node_dir, 'stderr')
        self.stdout = open(self.stdout_name, 'a')
        self.stderr = open(self.stderr_name, 'a')
        cmd = self._get_command_line(self.near_root, self.node_dir, boot_key,
                                     boot_node_addr, self.binary_name)
        self.pid.value = subprocess.Popen(cmd,
                                          stdout=self.stdout,
                                          stderr=self.stderr,
                                          env=env).pid

        if not skip_starting_proxy:
            self.start_proxy_if_needed()

        try:
            self.wait_for_rpc(10)
        except:
            print(
                '=== Error: failed to start node, rpc does not ready in 10 seconds'
            )
            self.stdout.close()
            self.stderr.close()
            if os.environ.get('BUILDKITE'):
                print('=== stdout: ')
                print(open(self.stdout_name).read())
                print('=== stderr: ')
                print(open(self.stderr_name).read())

    def kill(self):
        if self.pid.value != 0:
            os.kill(self.pid.value, signal.SIGKILL)
            self.pid.value = 0

            if self._proxy_local_stopped is not None:
                self._proxy_local_stopped.value = 1

    def reset_data(self):
        shutil.rmtree(os.path.join(self.node_dir, "data"))

    def reset_validator_key(self, new_key):
        self.validator_key = new_key
        with open(os.path.join(self.node_dir, "validator_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)

    def reset_node_key(self, new_key):
        self.node_key = new_key
        with open(os.path.join(self.node_dir, "node_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)

    def cleanup(self):
        if self.cleaned:
            return

        try:
            self.kill()
        except:
            print("Kill failed on cleanup!")
            traceback.print_exc()
            print("\n\n")

        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)
        self.cleaned = True

    def stop_network(self):
        print("Stopping network for process %s" % self.pid.value)
        network.stop(self.pid.value)

    def resume_network(self):
        print("Resuming network for process %s" % self.pid.value)
        network.resume_network(self.pid.value)


class BotoNode(BaseNode):
    pass


class GCloudNode(BaseNode):

    def __init__(self, *args):
        if len(args) == 1:
            # Get existing instance assume it's ready to run
            name = args[0]
            self.instance_name = name
            self.port = 24567
            self.rpc_port = 3030
            self.machine = gcloud.get(name)
            self.ip = self.machine.ip
        elif len(args) == 4:
            # Create new instance from scratch
            instance_name, zone, node_dir, binary = args
            self.instance_name = instance_name
            self.port = 24567
            self.rpc_port = 3030
            self.node_dir = node_dir
            self.machine = gcloud.create(
                name=instance_name,
                machine_type='n1-standard-2',
                disk_size='50G',
                image_project='gce-uefi-images',
                image_family='ubuntu-1804-lts',
                zone=zone,
                firewall_allows=['tcp:3030', 'tcp:24567'],
                min_cpu_platform='Intel Skylake',
                preemptible=False,
            )
            self.ip = self.machine.ip
            self._upload_config_files(node_dir)
            self._download_binary(binary)
            with remote_nodes_lock:
                global cleanup_remote_nodes_atexit_registered
                if not cleanup_remote_nodes_atexit_registered:
                    atexit.register(atexit_cleanup_remote)
                    cleanup_remote_nodes_atexit_registered = True
        else:
            raise Exception()

    def _upload_config_files(self, node_dir):
        self.machine.run('bash', input='mkdir -p ~/.near')
        self.machine.upload(os.path.join(node_dir, '*.json'),
                            f'/home/{self.machine.username}/.near/')
        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))

    @retry(wait_fixed=1000, stop_max_attempt_number=3)
    def _download_binary(self, binary):
        p = self.machine.run('bash',
                             input=f'''
/snap/bin/gsutil cp gs://nearprotocol_nearcore_release/{binary} near
chmod +x near
''')
        if p.returncode != 0:
            raise DownloadException(p.stderr)

    def addr(self):
        return (self.ip, self.port)

    def rpc_addr(self):
        return (self.ip, self.rpc_port)

    def start(self, boot_key, boot_node_addr):
        self.machine.run_detach_tmux("RUST_BACKTRACE=1 " + " ".join(
            self._get_command_line('.', '.near', boot_key, boot_node_addr)).
                                     replace("--verbose", '--verbose ""'))
        self.wait_for_rpc(timeout=30)

    def kill(self):
        self.machine.run('tmux send-keys -t python-rc C-c')
        time.sleep(3)
        self.machine.kill_detach_tmux()

    def destroy_machine(self):
        self.machine.delete()

    def cleanup(self):
        self.kill()
        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)

        # Get log and delete machine
        rc.run(f'mkdir -p /tmp/pytest_remote_log')
        self.machine.download(
            '/tmp/python-rc.log',
            f'/tmp/pytest_remote_log/{self.machine.name}.log')
        self.destroy_machine()

    def json_rpc(self, method, params, timeout=15):
        return super().json_rpc(method, params, timeout=timeout)

    def get_status(self):
        r = nretry(lambda: requests.get(
            "http://%s:%s/status" % self.rpc_addr(), timeout=15),
                           timeout=45)
        r.raise_for_status()
        return json.loads(r.content)

    def stop_network(self):
        rc.run(
            f'gcloud compute firewall-rules create {self.machine.name}-stop --direction=EGRESS --priority=1000 --network=default --action=DENY --rules=all --target-tags={self.machine.name}'
        )

    def resume_network(self):
        rc.run(f'gcloud compute firewall-rules delete {self.machine.name}-stop',
               input='yes\n')

    def reset_validator_key(self, new_key):
        self.validator_key = new_key
        with open(os.path.join(self.node_dir, "validator_key.json"), 'w+') as f:
            json.dump(new_key.to_json(), f)
        self.machine.upload(os.path.join(self.node_dir, 'validator_key.json'),
                            f'/home/{self.machine.username}/.near/')


def github_auth():
    print("Go to the following link in your browser:")
    print()
    print("http://nayduck.eastus.cloudapp.azure.com:3000/local_auth")
    print()
    code = input("Enter verification code: ")
    with open(os.path.expanduser('~/.nayduck'), 'w') as f:
        f.write(code)
    return code

class AzureNode(BaseNode):
        def __init__(self, ip, token, node_dir, release):
            super(AzureNode, self).__init__()
            self.ip = ip
            self.token = token
            if release:
                self.near_root = '/datadrive/testnodes/worker/nearcore/target/release'
            else:
                self.near_root = '/datadrive/testnodes/worker/nearcore/target/debug'
            self.port = 24567
            self.rpc_port = 3030
            self.node_dir = node_dir
            self._upload_config_files(node_dir)

        def _upload_config_files(self, node_dir):
            post = {'ip': self.ip, 'token': self.token}
            res = requests.post('http://40.112.59.229:5000/cleanup', json=post)
            for f in os.listdir(node_dir):
                if f.endswith(".json"):
                    with open(os.path.join(node_dir, f), "r") as fl:
                        cnt = fl.read()
                    post = {'ip': self.ip, 'cnt': cnt, 'fl_name': f, 'token': self.token}
                    res = requests.post('http://40.112.59.229:5000/upload', json=post)
                    json_res = json.loads(res.text)
                    if json_res['stderr'] != '':
                        print(json_res['stderr'])
                        sys.exit()
            self.validator_key = Key.from_json_file(
                os.path.join(node_dir, "validator_key.json"))
            self.node_key = Key.from_json_file(
                os.path.join(node_dir, "node_key.json"))
            self.signer_key = Key.from_json_file(
                os.path.join(node_dir, "validator_key.json"))

        def start(self, boot_key, boot_node_addr, skip_starting_proxy):
            cmd = ('RUST_BACKTRACE=1 ADVERSARY_CONSENT=1 ' + ' '.join(
                self._get_command_line(self.near_root,
                                       '.near', boot_key, boot_node_addr)).
                                     replace("--verbose", '--verbose ""'))
            post = {'ip': self.ip, 'cmd': cmd, 'token': self.token}
            res = requests.post('http://40.112.59.229:5000/run_cmd', json=post)
            json_res = json.loads(res.text)
            if json_res['stderr'] != '':
                print(json_res['stderr'])
                sys.exit()
            self.wait_for_rpc(timeout=30)

        def kill(self):
            cmd = ('killall -9 neard; pkill -9 -e -f companion.py')
            post = {'ip': self.ip, 'cmd': cmd, 'token': self.token}
            res = requests.post('http://40.112.59.229:5000/run_cmd', json=post)
            json_res = json.loads(res.text)
            if json_res['stderr'] != '':
                print(json_res['stderr'])
            sys.exit()

        def addr(self):
            return (self.ip, self.port)

        def rpc_addr(self):
            return (self.ip, self.rpc_port)

        def companion(self, *args):
            post = {'ip': self.ip, 'args': ' '.join(map(str, args)), 'token': self.token}
            res = requests.post('http://40.112.59.229:5000/companion', json=post)
            json_res = json.loads(res.text)
            if json_res['stderr'] != '':
                print(json_res['stderr'])
                sys.exit()
            

class PreexistingCluster():
        
    def __init__(self, num_nodes, node_dirs, release):
        self.already_cleaned_up = False
        if os.path.isfile(os.path.expanduser('~/.nayduck')):
            with open(os.path.expanduser('~/.nayduck'), 'r') as f:
                self.token = f.read().strip()
        else:
            self.token = github_auth().strip()
        self.nodes = []
        sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], universal_newlines=True).strip()
        requester = subprocess.check_output(['git', 'config', 'user.name'], universal_newlines=True).strip()
        post = {'sha': sha, 'requester': requester, 'release': release,
                'num_nodes': num_nodes, 'token': self.token}
        res = requests.post('http://40.112.59.229:5000/request_a_run', json=post)
        json_res = json.loads(res.text)
        if json_res['code'] == -1:
            print(json_res['err'])
            return
        self.request_id = json_res['request_id']
        print("GG request id: %s" % self.request_id)
        self.ips = []
        atexit.register(self.atexit_cleanup_preexist, None)
        signal.signal(signal.SIGTERM, self.atexit_cleanup_preexist)
        signal.signal(signal.SIGINT, self.atexit_cleanup_preexist)
        k = 0
        while True:
            k += 1
            post = {'num_nodes': num_nodes, 'request_id': self.request_id,
                    'token': self.token}
            res = requests.post('http://40.112.59.229:5000/get_instances', json=post)
            json_res = json.loads(res.text)
            self.ips = json_res['ips']
            print('Got %s nodes out of %s asked\r' % (len(self.nodes), num_nodes),  end='\r')
            if requester == "NayDuck" and k == 3:
                print("Postpone test for NayDuck.")
                sys.exit(13)

            if len(self.ips) != num_nodes:
                time.sleep(10)
                continue
            for i in range(0, num_nodes):
                node = AzureNode(json_res['ips'][i], self.token, node_dirs[i], release)
                self.nodes.append(node)
            if len(self.nodes) == num_nodes:
                break

        print()
        print("ips: %s" % self.ips)
        while True:
            status = {'BUILDING': 0, 'READY': 0, 'BUILD FAILED': 0}
            post = {'ips': self.ips, 'token': self.token}
            res = requests.post('http://40.112.59.229:5000/get_instances_status', json=post)
            json_res = json.loads(res.text)
            for k, v in json_res.items():
                if v in status:
                    status[v] += 1
                else:
                    print("Unexpected status %s for %s" % (v, k))
            print('%s nodes are building and %s nodes are ready' % (status['BUILDING'], status['READY']),  end='\r')
            if status['BUILD FAILED'] > 0:
                print('Build failed for at least one instance')
                self.nodes = []
                break
            if status['READY'] == num_nodes:
                break
            time.sleep(10)

    def get_one_node(self, i):
        return self.nodes[i]

    def atexit_cleanup_preexist(self, *args):
        if self.already_cleaned_up:
            sys.exit(0)
        print()
        post = {'request_id': self.request_id, 'token': self.token} 
        print("Starting cleaning up remote instances.")
        res = requests.post('http://40.112.59.229:5000/cancel_the_run', json=post)
        json_res = json.loads(res.text)
        logs = json_res['logs']
        print("Getting logs from remote instances")
        for i in range(len(self.ips)):
            for log in logs:
                if self.ips[i] in log:
                    if 'companion' in log:
                        fl = os.path.expanduser('~/.near/test' + str(i) + "/companion.log")
                    else:
                        fl = os.path.expanduser('~/.near/test' + str(i) + "/remote.log")
                    res = requests.get(log)
                    with open(fl, 'w') as f:
                        f.write(res.text)
                    print(f"Logs are available in {fl}")
        self.already_cleaned_up = True
        sys.exit(0)

def spin_up_node(config,
                 near_root,
                 node_dir,
                 ordinal,
                 boot_key,
                 boot_addr,
                 blacklist=[],
                 proxy=None,
                 skip_starting_proxy=False):
    is_local = config['local']

    print("Starting node %s %s" % (ordinal,
                                   ("as BOOT NODE" if boot_addr is None else
                                    ("with boot=%s@%s:%s" %
                                     (boot_key, boot_addr[0], boot_addr[1])))))
    if is_local:
        blacklist = [
            "127.0.0.1:%s" % (24567 + 10 + bl_ordinal)
            for bl_ordinal in blacklist
        ]
        node = LocalNode(24567 + 10 + ordinal, 3030 + 10 + ordinal, near_root,
                         node_dir, blacklist, config.get('binary_name', 'near'))
    else:
        # TODO: Figure out how to know IP address beforehand for remote deployment.
        assert len(
            blacklist) == 0, "Blacklist is only supported in LOCAL deployment."

        if config['preexist']:
            global preexisting
            node = preexisting.get_one_node(ordinal)
        else:
            instance_name = '{}-{}-{}'.format(
                config['remote'].get('instance_name', 'near-pytest'), ordinal,
                uuid.uuid4())
            zones = config['remote']['zones']
            zone = zones[ordinal % len(zones)]
            node = GCloudNode(instance_name, zone, node_dir,
                            config['remote']['binary'])
            with remote_nodes_lock:
                remote_nodes.append(node)
            print(f"node {ordinal} machine created")

    if proxy is not None:
        proxy.proxify_node(node)

    node.start(boot_key, boot_addr, skip_starting_proxy)
    time.sleep(3)
    print(f"node {ordinal} started")
    return node


def init_cluster(num_nodes, num_observers, num_shards, config,
                 genesis_config_changes, client_config_changes):
    """
    Create cluster configuration
    """
    if 'local' not in config and 'nodes' in config:
        print("Attempt to launch a regular test with a mocknet config",
              file=sys.stderr)
        sys.exit(1)

    is_local = config['local']
    near_root = config['near_root']
    binary_name = config.get('binary_name', 'near')

    print("Creating %s cluster configuration with %s nodes" %
          ("LOCAL" if is_local else "REMOTE", num_nodes + num_observers))

    process = subprocess.Popen([
        os.path.join(near_root, binary_name), "testnet", "--v",
        str(num_nodes), "--shards",
        str(num_shards), "--n",
        str(num_observers), "--prefix", "test"
    ],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    out, err = process.communicate()
    assert 0 == process.returncode, err

    node_dirs = [
        line.split()[-1]
        for line in err.decode('utf8').split('\n')
        if '/test' in line
    ]
    assert len(
        node_dirs
    ) == num_nodes + num_observers, "node dirs: %s num_nodes: %s num_observers: %s" % (
        len(node_dirs), num_nodes, num_observers)

    print("Search for stdout and stderr in %s" % node_dirs)
    # apply config changes
    for i, node_dir in enumerate(node_dirs):
        apply_genesis_changes(node_dir, genesis_config_changes)
        if i in client_config_changes:
            client_config_change = client_config_changes[i]
            apply_config_changes(node_dir, client_config_change)

    if config['preexist']:
        print("Use preexisting cluster.")
        # ips of azure nodes with build neard but not yet started.
        global preexisting
        preexisting = PreexistingCluster(num_nodes + num_observers, node_dirs, config['release'])
    
    return near_root, node_dirs


def apply_genesis_changes(node_dir, genesis_config_changes):
    # apply genesis.json changes
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


def apply_config_changes(node_dir, client_config_change):
    # apply config.json changes
    fname = os.path.join(node_dir, 'config.json')
    with open(fname) as f:
        config_json = json.loads(f.read())

    for k, v in client_config_change.items():
        assert k in config_json
        if isinstance(v, dict):
            for key, value in v.items():
                assert key in config_json[k], key
                config_json[k][key] = value
        else:
            config_json[k] = v

    with open(fname, 'w') as f:
        f.write(json.dumps(config_json, indent=2))


def start_cluster(num_nodes,
                  num_observers,
                  num_shards,
                  config,
                  genesis_config_changes,
                  client_config_changes,
                  message_handler=None):
    if not config:
        config = load_config()

    if not os.path.exists(os.path.expanduser("~/.near/test0")):
        near_root, node_dirs = init_cluster(num_nodes, num_observers,
                                            num_shards, config,
                                            genesis_config_changes,
                                            client_config_changes)
    else:
        near_root = config['near_root']
        node_dirs = subprocess.check_output(
            "find ~/.near/test* -maxdepth 0",
            shell=True).decode('utf-8').strip().split('\n')
        node_dirs = list(
            filter(lambda n: not n.endswith('_finished'), node_dirs))
    ret = []

    proxy = NodesProxy(message_handler) if message_handler is not None else None

    def spin_up_node_and_push(i, boot_key, boot_addr):
        node = spin_up_node(config, near_root, node_dirs[i], i, boot_key,
                            boot_addr, [], proxy, skip_starting_proxy=True)
        while len(ret) < i:
            time.sleep(0.01)
        ret.append(node)
        return node

    boot_node = spin_up_node_and_push(0, None, None)

    handles = []
    for i in range(1, num_nodes + num_observers):
        handle = threading.Thread(target=spin_up_node_and_push,
                                  args=(i, boot_node.node_key.pk,
                                        boot_node.addr()))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()

    for node in ret:
        node.start_proxy_if_needed()

    return ret

def start_bridge(nodes, start_local_ethereum=True, handle_contracts=True, handle_relays=True, config=None):
    if not config:
        config = load_config()

    config['bridge']['bridge_dir'] = os.path.abspath(os.path.expanduser(os.path.expandvars(config['bridge']['bridge_dir'])))
    config['bridge']['config_dir'] = os.path.abspath(os.path.expanduser(os.path.expandvars(config['bridge']['config_dir'])))

    # Run bridge.__init__() here.
    # It will create necessary folders, download repos and install services automatically.
    bridge = RainbowBridge(config['bridge'], nodes[0])

    ganache_node = None
    if start_local_ethereum:
        ganache_node = GanacheNode(config['bridge'])
        ganache_node.start()
        # TODO wait until ganache actually starts
        time.sleep(2)

    # Allow the Bridge to fill the blockchains with initial contracts
    # such as ed25519, erc20, lockers, token factory, etc.
    # If false, contracts initialization should be completed in the test explicitly.
    if handle_contracts:
        # TODO implement initialization to non-Ganache Ethereum node when required
        assert start_local_ethereum
        bridge.init_near_contracts()
        bridge.init_eth_contracts()
        bridge.init_near_token_factory()

    # Initial test ERC20 tokens distribution
    billion_tokens = 1000000000
    bridge.mint_erc20_tokens(alice, billion_tokens)
    bridge.mint_erc20_tokens(bob, billion_tokens)
    bridge.mint_erc20_tokens(carol, billion_tokens)

    # Allow the Bridge to start Relays and handle them in a proper way.
    # If false, Relays handling should be provided in the test explicitly.
    if handle_relays:
        bridge.start_near2eth_block_relay()
        bridge.start_eth2near_block_relay()

    return (bridge, ganache_node)

DEFAULT_CONFIG = {
    'local': True,
    'preexist': False,
    'near_root': '../target/debug/',
    'binary_name': 'neard',
    'release': False,
    'bridge': {
        'bridge_repo': 'https://github.com/near/rainbow-bridge.git',
        'bridge_dir': '~/.rainbow-bridge',
        'config_dir': '~/.rainbow',
        'ganache_dir': 'testing/vendor/ganache',
        'ganache_bin': 'testing/vendor/ganache/node_modules/.bin/ganache-cli',
        'ganache_block_prod_time': 10,
    }
}

CONFIG_ENV_VAR = 'NEAR_PYTEST_CONFIG'


def load_config():
    config = DEFAULT_CONFIG

    config_file = os.environ.get(CONFIG_ENV_VAR, '')
    if config_file:
        try:
            with open(config_file) as f:
                new_config = json.load(f)
                config.update(new_config)
                print(f"Load config from {config_file}, config {config}")
        except FileNotFoundError:
            print(f"Failed to load config file, use default config {config}")
    else:
        print(f"Use default config {config}")
    return config

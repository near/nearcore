import atexit
import base64
import json
import os
import pathlib
import rc
import requests
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import typing
import uuid
from rc import gcloud
from retrying import retry

import base58

import network
from configured_logger import logger
from key import Key
from proxy import NodesProxy

os.environ["ADVERSARY_CONSENT"] = "1"

remote_nodes = []
remote_nodes_lock = threading.Lock()
cleanup_remote_nodes_atexit_registered = False


class DownloadException(Exception):
    pass


def atexit_cleanup(node):
    logger.info("Cleaning up node %s:%s on script exit" % node.addr())
    logger.info("Executed store validity tests: %s" % node.store_tests)
    try:
        node.cleanup()
    except:
        logger.info("Cleaning failed!")
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


BootNode = typing.Union[None, 'BaseNode', typing.Iterable['BaseNode']]


def make_boot_nodes_arg(boot_node: BootNode) -> typing.Tuple[str]:
    """Converts `boot_node` argument to `--boot-nodes` command line argument.

    If the argument is `None` returns an empty tuple.  Otherwise, returns
    a tuple representing arguments to be added to `neard` invocation for setting
    boot nodes according to `boot_node` argument.

    Apart from `None` as described above, `boot_node` can be a [`BaseNode`]
    object, or an iterable (think list) of [`BaseNode`] objects.  The boot node
    address of a BaseNode object is contstructed using [`BaseNode.addr_with_pk`]
    method.

    If iterable of nodes is given, the `neard` is going to be configured with
    multiple boot nodes.

    Args:
        boot_node: Specification of boot node(s).
    Returns:
        A tuple to add to `neard` invocation specifying boot node(s) if any
        specified.
    """
    if not boot_node:
        return ()
    try:
        it = iter(boot_node)
    except TypeError:
        it = iter((boot_node,))
    nodes = ','.join(node.addr_with_pk() for node in it)
    if not nodes:
        return ()
    return ('--boot-nodes', nodes)


class BlockId(typing.NamedTuple):
    """Stores block’s height and hash.

    The values can be accessed either through properties or by structural
    deconstruction, e.g.:

        block_height, block_hash = block_id
        assert block_height == block_id.height
        assert block_hash == block_id.hash

    Attributes:
        height: Block’s height.
        hash: Block’s hash encoding using base58.
        hash_bytes: Block’s hash decoded as raw bytes.  Note that this attribute
            cannot be accessed through aforementioned deconstruction.
    """
    height: int
    hash: str

    @classmethod
    def from_header(cls, header: typing.Dict[str, typing.Any]) -> 'BlockId':
        return cls(height=int(header['height']), hash=header['hash'])

    @property
    def hash_bytes(self) -> bytes:
        return base58.b58decode(self.hash.encode('ascii'))

    def __str__(self) -> str:
        return f'#{self.height} {self.hash}'

    def __eq__(self, rhs) -> bool:
        return (isinstance(rhs, BlockId) and self.height == rhs.height and
                self.hash == rhs.hash)


class BaseNode(object):

    def __init__(self):
        self._start_proxy = None
        self._proxy_local_stopped = None
        self.proxy = None
        self.store_tests = 0
        self.is_check_store = True

    def change_config(self, overrides: typing.Dict[str, typing.Any]) -> None:
        """Change client config.json of a node by applying given overrides.

        Changes to the configuration need to be made while the node is stopped.
        More precisely, while the changes may be made at any point, the node
        reads the time at startup only.

        The overrides are a dictionary specifying new values for configuration
        keys.  Non-dictionary values are applied directly, while dictionaries
        are non-recursively merged.  For example if the original config is:

            {
                'foo': 42,
                'bar': {'a': 1, 'b': 2, 'c': {'A': 3}},
            }

        and overrides are:

            {
                'foo': 24,
                'bar': {'a': -1, 'c': {'D': 3}, 'd': 1},
            }

        then resulting configuration file will be:

            {
                'foo': 24,
                'bar': {'a': -1, 'b': 2, 'c': {'D': 3}, 'd': 1},
            }

        Args:
            overrides: A dictionary of config overrides.  Non-dictionary values
                are set as is, dictionaries are non-recursively merged.
        Raises:
            NotImplementedError: Currently changing the configuration is
                supported on local node only.
        """
        name = type(self).__name__
        raise NotImplementedError('change_config not supported by ' + name)

    def _get_command_line(self,
                          near_root,
                          node_dir,
                          boot_node: BootNode,
                          binary_name='neard'):
        cmd = (os.path.join(near_root, binary_name), '--home', node_dir, 'run')
        return cmd + make_boot_nodes_arg(boot_node)

    def addr_with_pk(self) -> str:
        pk_hash = self.node_key.pk.split(':')[1]
        host, port = self.addr()
        return '{}@{}:{}'.format(pk_hash, host, port)

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

    def get_status(self,
                   check_storage: bool = True,
                   timeout: float = 4,
                   verbose: bool = False):
        r = requests.get("http://%s:%s/status" % self.rpc_addr(),
                         timeout=timeout)
        r.raise_for_status()
        status = json.loads(r.content)
        if verbose:
            logger.info(f'Status: {status}')
        if check_storage and status['sync_info']['syncing'] == False:
            # Storage is not guaranteed to be in consistent state while syncing
            self.check_store()
        if verbose:
            logger.info(status)
        return status

    def get_latest_block(self, **kw) -> BlockId:
        sync_info = self.get_status(**kw)['sync_info']
        return BlockId(height=sync_info['latest_block_height'],
                       hash=sync_info['latest_block_hash'])

    def get_all_heights(self):
        hash_ = self.get_latest_block().hash
        heights = []

        while True:
            block = self.get_block(hash_)
            if 'error' in block and 'data' in block[
                    'error'] and 'DB Not Found Error: BLOCK:' in block['error'][
                        'data']:
                break
            elif 'result' not in block:
                logger.info(block)

            height = block['result']['header']['height']
            if height == 0:
                break
            heights.append(height)
            hash_ = block['result']['header']['prev_hash']

        return reversed(heights)

    def get_validators(self, epoch_id=None):
        if epoch_id is None:
            args = [None]
        else:
            args = {'epoch_id': epoch_id}
        return self.json_rpc('validators', args)

    def get_account(self, acc, finality='optimistic', do_assert=True, **kwargs):
        res = self.json_rpc('query', {
            "request_type": "view_account",
            "account_id": acc,
            "finality": finality
        }, **kwargs)
        if do_assert:
            assert 'error' not in res, res

        return res

    def call_function(self,
                      acc,
                      method,
                      args,
                      finality='optimistic',
                      timeout=2):
        return self.json_rpc('query', {
            "request_type": "call_function",
            "account_id": acc,
            "method_name": method,
            "args_base64": args,
            "finality": finality
        },
                             timeout=timeout)

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

    def get_block(self, block_id, **kwargs):
        return self.json_rpc('block', [block_id], **kwargs)

    def get_block_by_height(self, block_height, **kwargs):
        return self.json_rpc('block', {'block_id': block_height}, **kwargs)

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
        logger.warning("Stopping checking Storage for inconsistency for %s:%s" %
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
                    logger.error(
                        "Storage for %s:%s in inconsistent state, stopping" %
                        self.addr())
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
                 binary_name=None,
                 single_node=False):
        super(LocalNode, self).__init__()
        self.port = port
        self.rpc_port = rpc_port
        self.near_root = str(near_root)
        self.node_dir = node_dir
        self.binary_name = binary_name or 'neard'
        self.cleaned = False
        self.validator_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(
            os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(
            os.path.join(node_dir, "validator_key.json"))
        self._process = None

        self.change_config({
            'network': {
                'addr': f'0.0.0.0:{port}',
                'blacklist': blacklist
            },
            'rpc': {
                'addr': f'0.0.0.0:{rpc_port}',
            },
            'consensus': {
                'min_num_peers': int(not single_node)
            },
        })

        atexit.register(atexit_cleanup, self)

    def change_config(self, overrides: typing.Dict[str, typing.Any]) -> None:
        apply_config_changes(self.node_dir, overrides)

    def addr(self):
        return ("127.0.0.1", self.port)

    def rpc_addr(self):
        return ("127.0.0.1", self.rpc_port)

    def start_proxy_if_needed(self):
        if self._start_proxy is not None:
            self._proxy_local_stopped = self._start_proxy()

    def output_logs(self):
        stdout = pathlib.Path(self.node_dir) / 'stdout'
        stderr = pathlib.Path(self.node_dir) / 'stderr'
        if os.environ.get('BUILDKITE'):
            logger.info('=== stdout: ')
            logger.info(stdout.read_text('utf-8', 'replace'))
            logger.info('=== stderr: ')
            logger.info(stderr.read_text('utf-8', 'replace'))
        else:
            logger.info(f'=== stdout: available at {stdout}')
            logger.info(f'=== stderr: available at {stderr}')

    def start(self,
              *,
              boot_node: BootNode = None,
              skip_starting_proxy=False,
              extra_env: typing.Dict[str, str] = dict()):
        if self._proxy_local_stopped is not None:
            while self._proxy_local_stopped.value != 2:
                logger.info(f'Waiting for previous proxy instance to close')
                time.sleep(1)

        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
            "RUST_LOG", "debug")
        env.update(extra_env)

        cmd = self._get_command_line(
            self.near_root,
            self.node_dir,
            boot_node,
            self.binary_name,
        )
        node_dir = pathlib.Path(self.node_dir)
        self.stdout_name = node_dir / 'stdout'
        self.stderr_name = node_dir / 'stderr'
        with open(self.stdout_name, 'ab') as stdout, \
             open(self.stderr_name, 'ab') as stderr:
            self._process = subprocess.Popen(cmd,
                                             stdin=subprocess.DEVNULL,
                                             stdout=stdout,
                                             stderr=stderr,
                                             env=env)
        self._pid = self._process.pid

        if not skip_starting_proxy:
            self.start_proxy_if_needed()

        try:
            self.wait_for_rpc(10)
        except:
            logger.error(
                '=== failed to start node, rpc does not ready in 10 seconds')

    def kill(self, *, gentle=False):
        """Kills the process.  If `gentle` sends SIGINT before killing."""
        if self._proxy_local_stopped is not None:
            self._proxy_local_stopped.value = 1
        if self._process and gentle:
            self._process.send_signal(signal.SIGINT)
            try:
                self._process.wait(5)
                self._process = None
            except subprocess.TimeoutExpired:
                pass
        if self._process:
            self._process.kill()
            self._process.wait(5)
            self._process = None

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
            logger.critical('Kill failed on cleanup!', exc_info=sys.exc_info())

        # move the node dir to avoid weird interactions with multiple serial test invocations
        target_path = self.node_dir + '_finished'
        if os.path.exists(target_path) and os.path.isdir(target_path):
            shutil.rmtree(target_path)
        os.rename(self.node_dir, target_path)
        self.node_dir = target_path
        self.output_logs()
        self.cleaned = True

    def stop_network(self):
        logger.info(f'Stopping network for process {self._pid}')
        network.stop(self._pid)

    def resume_network(self):
        logger.info(f'Resuming network for process {self._pid}')
        network.resume_network(self._pid)


class GCloudNode(BaseNode):

    def __init__(self, *args, username=None, project=None, ssh_key_path=None):
        if len(args) == 1:
            name = args[0]
            # Get existing instance assume it's ready to run.
            self.instance_name = name
            self.port = 24567
            self.rpc_port = 3030
            self.machine = gcloud.get(name,
                                      username=username,
                                      project=project,
                                      ssh_key_path=ssh_key_path)
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
            # self.ip = self.machine.ip
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
/snap/bin/gsutil cp gs://nearprotocol_nearcore_release/{binary} neard
chmod +x neard
''')
        if p.returncode != 0:
            raise DownloadException(p.stderr)

    def addr(self):
        return (self.ip, self.port)

    def rpc_addr(self):
        return (self.ip, self.rpc_port)

    def start(self,
              *,
              boot_node: BootNode = None,
              extra_env: typing.Dict[str, str] = dict()):
        if "RUST_BACKTRACE" not in extra_env:
            extra_env["RUST_BACKTRACE"] = "1"
        extra_env = [f"{k}={v}" for (k, v) in extra_env]
        extra_env = " ".join(extra_env)
        self.machine.run_detach_tmux(
            extra_env +
            " ".join(self._get_command_line('.', '.near', boot_node)))
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
        r = nretry(lambda: requests.get("http://%s:%s/status" % self.rpc_addr(),
                                        timeout=15),
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


def spin_up_node(config,
                 near_root,
                 node_dir,
                 ordinal,
                 *,
                 boot_node: BootNode = None,
                 blacklist=[],
                 proxy=None,
                 skip_starting_proxy=False,
                 single_node=False):
    is_local = config['local']

    args = make_boot_nodes_arg(boot_node)
    logger.info("Starting node %s %s" %
                (ordinal,
                 ('with ' + '='.join(args) if args else 'as BOOT NODE')))
    if is_local:
        blacklist = [
            "127.0.0.1:%s" % (24567 + 10 + bl_ordinal)
            for bl_ordinal in blacklist
        ]
        node = LocalNode(24567 + 10 + ordinal, 3030 + 10 + ordinal,
                         near_root, node_dir, blacklist,
                         config.get('binary_name'), single_node)
    else:
        # TODO: Figure out how to know IP address beforehand for remote deployment.
        assert len(
            blacklist) == 0, "Blacklist is only supported in LOCAL deployment."

        instance_name = '{}-{}-{}'.format(
            config['remote'].get('instance_name', 'near-pytest'), ordinal,
            uuid.uuid4())
        zones = config['remote']['zones']
        zone = zones[ordinal % len(zones)]
        node = GCloudNode(instance_name, zone, node_dir,
                          config['remote']['binary'])
        with remote_nodes_lock:
            remote_nodes.append(node)
        logger.info(f"node {ordinal} machine created")

    if proxy is not None:
        proxy.proxify_node(node)

    node.start(boot_node=boot_node, skip_starting_proxy=skip_starting_proxy)
    time.sleep(3)
    logger.info(f"node {ordinal} started")
    return node


def init_cluster(num_nodes,
                 num_observers,
                 num_shards,
                 config,
                 genesis_config_changes,
                 client_config_changes,
                 prefix="test"):
    """
    Create cluster configuration
    """
    if 'local' not in config and 'nodes' in config:
        logger.critical(
            "Attempt to launch a regular test with a mocknet config")
        sys.exit(1)

    if not prefix.startswith("test"):
        logger.critical(f"The prefix must begin with 'test'. prefix = {prefix}")
        sys.exit(1)

    is_local = config['local']
    near_root = config['near_root']
    binary_name = config.get('binary_name', 'neard')

    logger.info("Creating %s cluster configuration with %s nodes" %
                ("LOCAL" if is_local else "REMOTE", num_nodes + num_observers))

    binary_path = os.path.join(near_root, binary_name)
    process = subprocess.Popen(
        [
            binary_path,
            "localnet",
            "--validators",
            str(num_nodes),
            "--non-validators",
            str(num_observers),
            "--shards",
            str(num_shards),
            "--tracked-shards",
            "none",
            "--prefix",
            prefix,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
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

    logger.info("Search for stdout and stderr in %s" % node_dirs)
    # apply config changes
    for i, node_dir in enumerate(node_dirs):
        apply_genesis_changes(node_dir, genesis_config_changes)
        overrides = client_config_changes.get(i)
        if overrides:
            apply_config_changes(node_dir, overrides)

    return near_root, node_dirs


def apply_genesis_changes(node_dir, genesis_config_changes):
    # apply genesis.json changes
    fname = os.path.join(node_dir, 'genesis.json')
    with open(fname) as fd:
        genesis_config = json.load(fd)
    for change in genesis_config_changes:
        cur = genesis_config
        for s in change[:-2]:
            cur = cur[s]
        assert change[-2] in cur
        cur[change[-2]] = change[-1]
    with open(fname, 'w') as fd:
        json.dump(genesis_config, fd, indent=2)


def apply_config_changes(node_dir, client_config_change):
    # apply config.json changes
    fname = os.path.join(node_dir, 'config.json')
    with open(fname) as fd:
        config_json = json.load(fd)

    # ClientConfig keys which are valid but may be missing from the config.json
    # file.  Those are often Option<T> types which are not stored in JSON file
    # when None.
    allowed_missing_configs = (
        'archive',
        'max_gas_burnt_view',
        'rosetta_rpc',
        'save_trie_changes',
        'split_storage',
        'state_sync_enabled',
        'store.state_snapshot_enabled',
    )

    for k, v in client_config_change.items():
        if not (k in allowed_missing_configs or k in config_json):
            raise ValueError(f'Unknown configuration option: {k}')
        if k in config_json and isinstance(v, dict):
            config_json[k].update(v)
        else:
            # Support keys in the form of "a.b.c".
            parts = k.split('.')
            current = config_json
            for part in parts[:-1]:
                if part not in current:
                    raise ValueError(
                        f'{part} is not found in config.json. Key={k}, Value={v}'
                    )
                current = current[part]
            current[parts[-1]] = v

    with open(fname, 'w') as fd:
        json.dump(config_json, fd, indent=2)


def get_config_json(node_dir):
    fname = os.path.join(node_dir, 'config.json')
    with open(fname) as fd:
        return json.load(fd)


def set_config_json(node_dir, config_json):
    fname = os.path.join(node_dir, 'config.json')
    with open(fname, 'w') as fd:
        json.dump(config_json, fd, indent=2)


def start_cluster(num_nodes,
                  num_observers,
                  num_shards,
                  config,
                  genesis_config_changes,
                  client_config_changes,
                  message_handler=None):
    if not config:
        config = load_config()

    dot_near = pathlib.Path.home() / '.near'
    if (dot_near / 'test0').exists():
        near_root = config['near_root']
        node_dirs = [
            str(dot_near / name)
            for name in os.listdir(dot_near)
            if name.startswith('test') and not name.endswith('_finished')
        ]
    else:
        near_root, node_dirs = init_cluster(num_nodes, num_observers,
                                            num_shards, config,
                                            genesis_config_changes,
                                            client_config_changes)

    proxy = NodesProxy(message_handler) if message_handler is not None else None
    ret = []

    def spin_up_node_and_push(i, boot_node: BootNode):
        single_node = (num_nodes == 1) and (num_observers == 0)
        node = spin_up_node(config,
                            near_root,
                            node_dirs[i],
                            i,
                            boot_node=boot_node,
                            proxy=proxy,
                            skip_starting_proxy=True,
                            single_node=single_node)
        ret.append((i, node))
        return node

    boot_node = spin_up_node_and_push(0, None)

    handles = []
    for i in range(1, num_nodes + num_observers):
        handle = threading.Thread(target=spin_up_node_and_push,
                                  args=(i, boot_node))
        handle.start()
        handles.append(handle)

    for handle in handles:
        handle.join()

    nodes = [node for _, node in sorted(ret)]
    for node in nodes:
        node.start_proxy_if_needed()

    return nodes


ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]


def get_near_root():
    cargo_target_dir = os.environ.get('CARGO_TARGET_DIR', 'target')
    default_root = (ROOT_DIR / cargo_target_dir / 'debug').resolve()
    return os.environ.get('NEAR_ROOT', str(default_root))


DEFAULT_CONFIG = {
    'local': True,
    'near_root': get_near_root(),
    'binary_name': 'neard',
    'release': False,
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
                logger.info(f"Load config from {config_file}, config {config}")
        except FileNotFoundError:
            logger.info(
                f"Failed to load config file, use default config {config}")
    else:
        logger.info(f"Use default config {config}")
    return config


# Returns the protocol version of the binary.
def get_binary_protocol_version(config) -> typing.Optional[int]:
    binary_name = config.get('binary_name', 'neard')
    near_root = config.get('near_root')
    binary_path = os.path.join(near_root, binary_name)

    # Get the protocol version of the binary
    # The --version output looks like this:
    # neard (release trunk) (build 1.1.0-3884-ge93793a61-modified) (rustc 1.71.0) (protocol 137) (db 37)
    out = subprocess.check_output([binary_path, "--version"], text=True)
    out = out.replace('(', '')
    out = out.replace(')', '')
    tokens = out.split()
    n = len(tokens)
    for i in range(n):
        if tokens[i] == "protocol" and i + 1 < n:
            return int(tokens[i + 1])
    return None
import atexit
import base58
import hashlib
import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import typing
import requests
from prometheus_client.parser import text_string_to_metric_families
from retrying import retry
from rc import gcloud

import cluster
from configured_logger import logger
from transaction import sign_payment_tx


class TxContext:

    def __init__(self, act_to_val, nodes):
        self.next_nonce = 2
        self.num_nodes = len(nodes)
        self.nodes = nodes
        self.act_to_val = act_to_val
        self.expected_balances = self.get_balances()
        assert len(act_to_val) == self.num_nodes
        assert self.num_nodes >= 2

    @retry(stop_max_attempt_number=10, wait_exponential_multiplier=1.2)
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
                logger.info("Sending a tx from %s to %s for %s" %
                            (from_, to, amt))
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


class LogTracker:
    """Opens up a log file, scrolls to the end and allows to check for patterns.

    The tracker works only on local nodes.

    PLEASE AVOID USING THE TRACKER IN NEW TESTS.
    As depending on the exact log wording is making tests very fragile.
    Try depending on a metric instead.
    """

    def __init__(self, node: cluster.BaseNode) -> None:
        """Initialises the tracker for given local node.

        Args:
            node: Node to create tracker for.
        Raises:
            NotImplementedError: If trying to create a tracker for non-local
                node.
        """
        if not isinstance(node, cluster.LocalNode):
            raise NotImplementedError()
        self.fname = node.stderr_name
        with open(self.fname) as f:
            f.seek(0, 2)
            self.offset = f.tell()

    # Pattern matching ANSI escape codes starting with a Control Sequence
    # Introducer (CSI) sequence.  Most notably Select Graphic Rendition (SGR)
    # such as ‘\x1b[35;41m’.
    _CSI_RE = re.compile('\x1b\\[[^\x40-\x7E]*[\x40-\x7E]')

    def _read_file(self) -> str:
        """Returns data from the file starting from the offset."""
        with open(self.fname) as rd:
            rd.seek(self.offset)
            data = rd.read()
            self.offset = rd.tell()
        # Strip ANSI codes
        return self._CSI_RE.sub('', data)

    def check(self, pattern: str) -> bool:
        """Check whether the pattern can be found in the logs."""
        return pattern in self._read_file()

    def reset(self) -> None:
        """Resets log offset to beginning of the file."""
        self.offset = 0

    def count(self, pattern: str) -> int:
        """Count number of occurrences of pattern in new logs."""
        return self._read_file().count(pattern)


class MetricsTracker:
    """Helper class to collect prometheus metrics from the node.
    
    Usage:
        tracker = MetricsTracker(node)
        assert tracker.get_int_metric_value("near-connections") == 2
    """

    def __init__(self, node: cluster.BaseNode) -> None:
        if not isinstance(node, cluster.LocalNode):
            raise NotImplementedError()
        host, port = node.rpc_addr()

        self.addr = f"http://{host}:{port}/metrics"

    def get_all_metrics(self) -> str:
        response = requests.get(self.addr)
        if not response.ok:
            raise RuntimeError(
                f"Could not fetch metrics from {self.addr}: {response}")
        return response.content.decode('utf-8')

    def get_metric_value(
        self,
        metric_name: str,
        labels: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Optional[str]:
        for family in text_string_to_metric_families(self.get_all_metrics()):
            if family.name == metric_name:
                all_samples = [sample for sample in family.samples]
                if not labels:
                    if len(all_samples) > 1:
                        raise AssertionError(
                            f"Too many metric values ({len(all_samples)}) for {metric_name} - please specify a label"
                        )
                    if not all_samples:
                        return None
                    return all_samples[0].value
                for sample in all_samples:
                    if sample.labels == labels:
                        return sample.value
        return None

    def get_int_metric_value(
        self,
        metric_name: str,
        labels: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Optional[int]:
        """Helper function to return the integer value of the metric (as function above returns strings)."""
        value = self.get_metric_value(metric_name, labels)
        if not value:
            return None
        return round(float(value))


def chain_query(node, block_handler, *, block_hash=None, max_blocks=-1):
    """
    Query chain block approvals and chunks preceding of block of block_hash.
    If block_hash is None, it query latest block hash
    It query at most max_blocks, or if it's -1, all blocks back to genesis
    """
    block_hash = block_hash or node.get_latest_block().hash
    initial_validators = node.validators()

    if max_blocks == -1:
        while True:
            validators = node.validators()
            if validators != initial_validators:
                logger.critical(
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
                logger.critical(
                    f'Fatal: validator set of node {node} changes, from {initial_validators} to {validators}'
                )
                sys.exit(1)
            block = node.get_block(block_hash)['result']
            block_handler(block)
            block_hash = block['header']['prev_hash']
            block_height = block['header']['height']
            if block_height == 0:
                break


def get_near_tempdir(subdir=None, *, clean=False):
    tempdir = pathlib.Path(tempfile.gettempdir()) / 'near'
    if subdir:
        tempdir = tempdir / subdir
    if clean and tempdir.exists():
        shutil.rmtree(tempdir)
    tempdir.mkdir(parents=True, exist_ok=True)
    return tempdir


def load_binary_file(filepath):
    with open(filepath, "rb") as binaryfile:
        return bytearray(binaryfile.read())


def load_test_contract(
        filename: str = 'backwards_compatible_rs_contract.wasm') -> bytearray:
    """Loads a WASM file from near-test-contracts package.

    This is just a convenience function around load_binary_file which loads
    files from ../runtime/near-test-contracts/res directory.  By default
    test_contract_rs.wasm is loaded.
    """
    repo_dir = pathlib.Path(__file__).resolve().parents[2]
    path = repo_dir / 'runtime/near-test-contracts/res' / filename
    return load_binary_file(path)


def user_name():
    username = os.getlogin()
    if username == 'root':  # digitalocean
        username = gcloud.list()[0].username.replace('_nearprotocol_com', '')
    return username


def collect_gcloud_config(num_nodes):
    tempdir = get_near_tempdir()
    keys = []
    for i in range(num_nodes):
        node_dir = tempdir / f'node{i}'
        if not node_dir.exists():
            # TODO: avoid hardcoding the username
            logger.info(f'downloading node{i} config from gcloud')
            node_dir.mkdir(parents=True, exist_ok=True)
            host = gcloud.get(f'pytest-node-{user_name()}-{i}')
            for filename in ('config.json', 'signer0_key.json',
                             'validator_key.json', 'node_key.json'):
                host.download(f'/home/bowen_nearprotocol_com/.near/{filename}',
                              str(node_dir))
        with open(node_dir / 'signer0_key.json') as f:
            key = json.load(f)
        keys.append(key)
    with open(tempdir / 'node0' / 'config.json') as f:
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
    outfile = tempdir / 'gcloud_config.json'
    with open(outfile, 'w') as f:
        json.dump(res, f)
    os.environ[cluster.CONFIG_ENV_VAR] = str(outfile)


def obj_to_string(obj, extra='    ', full=False):
    if type(obj) in [tuple, list]:
        return "tuple" + '\n' + '\n'.join(
            (extra + obj_to_string(x, extra + '    ')) for x in obj)
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


def poll_epochs(node: cluster.LocalNode,
                *,
                epoch_length,
                num_blocks_per_year: int = 31536000,
                timeout: float = 300) -> typing.Iterable[int]:
    """Polls a node about the latest epoch and yields it when it changes.

    The function continues yielding epoch heights indefinitely (so long as the node
    continues reporting them) until timeout is reached or the caller stops
    reading yielded values.  Reaching the timeout is considered to be a failure
    condition and thus it results in an `AssertionError`.  The expected usage is
    that caller reads epoch heights until some condition is met at which point it stops
    iterating over the generator.

    Args:
        node: Node to query about its latest epoch.
        timeout: Total timeout from the first status request sent to the node.
        epoch_length: epoch_length genesis config value
        num_blocks_per_year: num_blocks_per_year genesis config value
    Yields:
        An int for each new epoch height reported. Note that there
        is no guarantee that there will be no skipped epochs.
    Raises:
        AssertionError: If more than `timeout` seconds passes from the start of
            the iteration, or the response from the node is not as expected.
    """
    end = time.time() + timeout
    start_height = -1
    epoch_start = -1
    count = 0
    previous = -1

    while time.time() < end:
        response = node.get_validators()
        assert 'error' not in response, response

        latest = response['result']
        height = latest['epoch_height']
        assert isinstance(height, int) and height >= 1, height

        if start_height == -1:
            start_height = height

        if previous != height:
            yield height

            count += 1
            previous = height
            epoch_start = latest['epoch_start_height']
            assert isinstance(epoch_start,
                              int) and epoch_start >= 1, epoch_start

        blocks_left = epoch_start + epoch_length - node.get_latest_block(
        ).height
        seconds_left = blocks_left / (num_blocks_per_year / 31536000)
        time.sleep(max(seconds_left, 2))

    msg = 'Timed out polling epochs from a node\n'
    if count:
        msg += (f'First epoch: {start_height}; last epoch: {previous}\n'
                f'Total epochs returned: {count}')
    else:
        msg += 'No epochs were returned'
    raise AssertionError(msg)


def poll_blocks(node: cluster.LocalNode,
                *,
                timeout: float = 120,
                poll_interval: float = 0.25,
                __target: typing.Optional[int] = None,
                **kw) -> typing.Iterable[cluster.BlockId]:
    """Polls a node about the latest block and yields it when it changes.

    The function continues yielding blocks indefinitely (so long as the node
    continues reporting its status) until timeout is reached or the caller stops
    reading yielded values.  Reaching the timeout is considered to be a failure
    condition and thus it results in an `AssertionError`.  The expected usage is
    that caller reads blocks until some condition is met at which point it stops
    iterating over the generator.

    Args:
        node: Node to query about its latest block.
        timeout: Total timeout from the first status request sent to the node.
        poll_interval: How long to wait in seconds between each status request
            sent to the node.
        kw: Keyword arguments passed to `BaseDone.get_latest_block` method.
    Yields:
        A `cluster.BlockId` object for each each time node’s latest block
        changes including the first block when function starts.  Note that there
        is no guarantee that there will be no skipped blocks.
    Raises:
        AssertionError: If more than `timeout` seconds passes from the start of
            the iteration.
    """
    end = time.monotonic() + timeout
    start_height = -1
    count = 0
    previous = -1

    while time.monotonic() < end:
        latest = node.get_latest_block(**kw)
        if latest.height != previous:
            if __target:
                msg = f'{latest}  (waiting for #{__target})'
            else:
                msg = str(latest)
            logger.info(msg)
            yield latest
            previous = latest.height
            if start_height == -1:
                start_height = latest.height
            count += 1
        time.sleep(poll_interval)

    msg = 'Timed out polling blocks from a node\n'
    if count > 0:
        msg += (f'First block: {start_height}; last block: {previous}\n'
                f'Total blocks returned: {count}')
    else:
        msg += 'No blocks were returned'
    if __target:
        msg += f'\nWaiting for block: {__target}'
    raise AssertionError(msg)


def wait_for_blocks(node: cluster.LocalNode,
                    *,
                    target: typing.Optional[int] = None,
                    count: typing.Optional[int] = None,
                    timeout: typing.Optional[float] = None,
                    **kw) -> cluster.BlockId:
    """Waits until given node reaches expected target block height.

    Exactly one of `target` or `count` arguments must be specified.  Specifying
    `count` is equivalent to setting `target` to node’s current height plus the
    given count.

    Args:
        node: Node to query about its latest block.
        target: Target height of the latest block known by the node.
        count: How many new blocks to wait for.  If this argument is given,
            target is calculated as node’s current block height plus the given
            count.
        timeout: Total timeout from the first status request sent to the node.
            If not specified, the default is to assume that overall each block
            takes no more than five seconds to generate.
        kw: Keyword arguments passed to `poll_blocks`.  `timeout` and
            `poll_interval` are likely of most interest.
    Returns:
        A `cluster.BlockId` of the block at target height.
    Raises:
        AssertionError: If the node does not reach given block height before
            timeout passes.
    """
    if target is None:
        if count is None:
            raise TypeError('Expected `count` or `target` keyword argument')
        target = node.get_latest_block().height + count
    else:
        if count is not None:
            raise TypeError(
                'Expected at most one of `count` or `target` arguments')
        if timeout is None:
            count = max(0, target - node.get_latest_block().height)
    if timeout is None:
        timeout = max(10, count * 5)
    for latest in poll_blocks(node, timeout=timeout, __target=target, **kw):
        if latest.height >= target:
            return latest


def figure_out_sandbox_binary():
    config = {
        'local': True,
        'release': False,
    }
    repo_dir = pathlib.Path(__file__).resolve().parents[2]
    # When run on NayDuck we end up with a binary called neard in target/debug
    # but when run locally the binary might be neard-sandbox or near-sandbox
    # instead.  Try to figure out whichever binary is available and use that.
    for release in ('release', 'debug'):
        root = repo_dir / 'target' / release
        for exe in ('neard-sandbox', 'near-sandbox', 'neard'):
            if (root / exe).exists():
                logger.info(
                    f'Using {(root / exe).relative_to(repo_dir)} binary')
                config['near_root'] = str(root)
                config['binary_name'] = exe
                return config

    assert False, ('Unable to figure out location of neard-sandbox binary; '
                   'Did you forget to run `make sandbox`?')

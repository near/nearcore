#!/usr/bin/env python3
"""Test if the node is backwards compatible with the latest release."""
import dataclasses
import os
import random
import re
import subprocess
import sys
import pathlib
import threading
import time
import traceback

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from configured_logger import logger
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_payment_tx
import utils

_EXECUTABLES = None

TIMEOUT = 10


def get_executables() -> branches.ABExecutables:
    global _EXECUTABLES
    if _EXECUTABLES is None:
        _EXECUTABLES = branches.prepare_ab_test()
        logger.info(f"Latest mainnet release is {_EXECUTABLES.release}")
    return _EXECUTABLES


def get_proto_version(exe: pathlib.Path) -> (int, int):
    line = subprocess.check_output((exe, '--version'), text=True)
    m = re.search(r'\(release (.*?)\) .* \(protocol ([0-9]+)\)', line)
    assert m, (f'Unable to extract protocol version number from {exe};\n'
               f'Got {line.rstrip()} on standard output')
    return m.group(1), int(m.group(2))


class TrafficGenerator(threading.Thread):
    """ A thread which keeps sending transactions to random addresses until stopped. """

    def __init__(self, rpc_node: cluster.LocalNode, **kwargs) -> None:
        super().__init__(**kwargs)
        self._lock = threading.Lock()
        self._rpc_node = rpc_node
        self._stopped = False
        self._paused = False
        self._failed_txs = 0

    def run(self) -> None:
        logger.info("Starting traffic generator")
        random.seed(2025)
        while not self._stopped:
            if self._paused:
                time.sleep(1)
                continue
            try:
                with self._lock:
                    self.send_transfer()
            except Exception:
                traceback.print_exc()
                self._failed_txs += 1
        logger.info("Traffic generator stopped")

    def stop(self) -> None:
        logger.info("Stopping traffic generator")
        self._stopped = True
        # Acquire lock to make sure no tx is awaiting
        self._lock.acquire()
        self._lock.release()

    def pause(self) -> None:
        logger.info("Pausing traffic generator")
        self._paused = True
        # Acquire lock to make sure no tx is awaiting
        self._lock.acquire()
        self._lock.release()

    def resume(self) -> None:
        logger.info("Resuming traffic generator")
        self._paused = False

    def get_latest_block_hash(self) -> bytes:
        return self._rpc_node.get_latest_block().hash_bytes

    def get_next_nonce(self) -> int:
        rpc_response = self._rpc_node.get_access_key(
            account_id=self._rpc_node.signer_key.account_id,
            public_key=self._rpc_node.signer_key.pk,
        )
        return rpc_response['result']['nonce'] + 1

    def send_tx(self, tx: bytes, timeout=10) -> None:
        res = self._rpc_node.send_tx_and_wait(tx, timeout=timeout)
        assert 'error' not in res, res
        assert 'Failure' not in res['result']['status'], res

    def deploy_test_contract(self, config: cluster.Config) -> None:
        tx = sign_deploy_contract_tx(
            self._rpc_node.signer_key,
            utils.load_test_contract(config=config),
            self.get_next_nonce(),
            self.get_latest_block_hash(),
        )
        self.send_tx(tx)

    def call_test_contract(self) -> None:
        tx = sign_function_call_tx(
            signer_key=self._rpc_node.signer_key,
            contract_id=self._rpc_node.signer_key.account_id,
            methodName='write_random_value',
            args=[],
            gas=10**13,
            deposit=0,
            nonce=self.get_next_nonce(),
            blockHash=self.get_latest_block_hash(),
        )
        self.send_tx(tx)

    def send_transfer(self) -> None:
        account_id = random.randbytes(32).hex()
        logger.info(f"Sending transfer to {account_id}")
        amount = 10**25
        tx = sign_payment_tx(
            key=self._rpc_node.signer_key,
            to=account_id,
            amount=amount,
            nonce=self.get_next_nonce(),
            blockHash=self.get_latest_block_hash(),
        )
        self.send_tx(tx)

        hex_account_balance = int(
            self._rpc_node.get_account(account_id)['result']['amount'])
        assert hex_account_balance == amount

    def assert_no_failed_txs(self) -> None:
        assert self._failed_txs == 0, f"{self._failed_txs} transactions failed"


@dataclasses.dataclass
class Protocols:
    stable: int
    current: int

    @classmethod
    def from_executables(
        cls,
        executables: branches.ABExecutables,
    ) -> 'Protocols':
        _, stable = get_proto_version(executables.stable.neard)
        _, current = get_proto_version(executables.current.neard)
        assert current >= stable, "cannot downgrade protocol version"
        return cls(stable, current)


class TestUpgrade:

    def __init__(
        self,
        num_validators: int,
        node_prefix: str,
        epoch_length: int,
    ) -> None:
        self._num_validators = num_validators
        self._node_prefix = node_prefix
        self._epoch_length = epoch_length

        self._executables = get_executables()
        self._protocols = Protocols.from_executables(self._executables)
        node_dirs = self.configure_nodes()
        nodes = self.start_nodes(node_dirs)

        self._stable_nodes = nodes[0:num_validators]
        self._current_node = nodes[-2]
        self._rpc_node = nodes[-1]
        self._metrics_tracker = utils.MetricsTracker(self._rpc_node)

    def run(self) -> None:
        """Test that upgrade from ‘stable’ to ‘current’ binary is possible.

        1. Start a network with 3 `stable` nodes and 1 `new` node.
        2. Start switching `stable` nodes one by one with `new` nodes.
        3. Run for three epochs and observe that current protocol version of the
           network matches `new` nodes.
        """
        self.wait_epoch()  # Skip first epoch, because nodes are starting
        time.sleep(1)
        traffic_generator = TrafficGenerator(self._rpc_node)
        traffic_generator.deploy_test_contract(
            self._executables.current.node_config())
        traffic_generator.call_test_contract()
        traffic_generator.start()

        try:
            self.wait_epoch()
            self.wait_for_no_missed_endorsements()

            traffic_generator.pause()
            self.upgrade_nodes()
            self.wait_epoch()  # Skip this epoch, because nodes are starting
            time.sleep(1)
            traffic_generator.resume()

            # Protocol version should update by one each epoch
            for expected_version in range(
                    self._protocols.stable + 1,
                    self._protocols.current + 1,
            ):
                self.wait_epoch()
                self.wait_for_no_missed_endorsements()
                self.wait_for_protocol_version(expected_version)

            # Run one more epoch with the latest protocol version
            self.wait_epoch()
            self.wait_for_no_missed_endorsements()

        finally:
            traffic_generator.stop()
            traffic_generator.join(timeout=10)

        traffic_generator.assert_no_failed_txs()

    def configure_nodes(self) -> list[str]:
        node_root = utils.get_near_tempdir('upgradable', clean=True)
        cmd = (
            self._executables.stable.neard,
            f'--home={node_root}',
            'localnet',
            f'--validators={self._num_validators}',
            '--non-validators-rpc=1',
            f'--prefix={self._node_prefix}',
        )
        logger.info(' '.join(str(arg) for arg in cmd))
        subprocess.check_call(cmd)
        genesis_config_changes = [
            ("epoch_length", self._epoch_length),
            ("num_block_producer_seats", 10),
            ("num_block_producer_seats_per_shard", [10]),
            ("block_producer_kickout_threshold", 80),
            ("chunk_producer_kickout_threshold", 80),
        ]
        node_dirs = [
            os.path.join(node_root, f'{self._node_prefix}{i}')
            for i in range(self._num_validators + 1)
        ]
        for node_dir in node_dirs:
            cluster.apply_genesis_changes(node_dir, genesis_config_changes)
        for node_dir in node_dirs[:self._num_validators]:
            # Validators should track only assigned shards
            cluster.apply_config_changes(node_dir, {'tracked_shards': []})

        # Dump epoch configs to use mainnet shard layout
        for node_dir in node_dirs[:self._num_validators - 1]:
            self.dump_epoch_configs(node_dir, self._protocols.stable)
        for node_dir in node_dirs[self._num_validators - 1:]:
            self.dump_epoch_configs(node_dir, self._protocols.current)

        return node_dirs

    def dump_epoch_configs(self, node_dir: str, last_protocol_version: int):
        cmd = (
            self._executables.current.neard,
            f'--home={node_dir}',
            'dump-epoch-configs',
            f'--chain-id=mainnet',
            f'--last-version={last_protocol_version}',
        )
        logger.info(' '.join(str(arg) for arg in cmd))
        subprocess.check_call(cmd)

    def start_nodes(
        self,
        node_dirs: list[str],
    ) -> list[cluster.LocalNode]:
        # Start `self._num_validators - 1` stable nodes and one current node.
        stable_config = self._executables.stable.node_config()
        current_config = self._executables.current.node_config()
        stable_root = self._executables.stable.root
        current_root = self._executables.current.root
        nodes = []
        nodes.extend(
            cluster.spin_up_node(
                config=stable_config if i < (self._num_validators - 1) else current_config,
                near_root=stable_root if i < (self._num_validators - 1) else current_root,
                node_dir=node_dirs[i],
                ordinal=i,
                boot_node=nodes[0] if i > 0 else None,
                sleep_after_start=0,
            ) for i in range(self._num_validators + 1)
        )  # yapf: disable
        return nodes

    def upgrade_nodes(self) -> None:
        # Restart stable nodes into new version.
        for node in self._stable_nodes:
            node.kill()
            self.dump_epoch_configs(node.node_dir, self._protocols.current)
            node.near_root = self._executables.current.root
            node.binary_name = self._executables.current.neard
            node.start(
                boot_node=self._stable_nodes[0],
                extra_env={
                    "NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE": "sequential"
                },
            )

    def wait_epoch(self) -> None:
        start_epoch = self._rpc_node.get_epoch_id()
        logger.info(f"Current epoch is {start_epoch}. Waiting for next epoch")
        for height, _ in utils.poll_blocks(self._rpc_node):
            epoch = self._rpc_node.get_epoch_id(block_height=height)
            if epoch != start_epoch:
                break

    def get_missed_endorsements(self) -> dict:
        prev_epoch_id = self._rpc_node.get_prev_epoch_id()
        validators = self._rpc_node.get_validators(prev_epoch_id)['result']['current_validators']  # yapf: disable
        return {
            v['account_id']: v['num_expected_endorsements'] - v['num_produced_endorsements']
            for v in validators
        }  # yapf: disable

    def wait_for_no_missed_endorsements(self) -> None:
        started = time.time()
        while time.time() - started < TIMEOUT:
            missed_endorsements = self.get_missed_endorsements()
            if all(missed == 0 for missed in missed_endorsements):
                break
            time.sleep(1)

        missed_endorsements = self.get_missed_endorsements()
        logger.info(f"Missed endorsements: {missed_endorsements}")
        for i in range(self._num_validators):
            validator_id = f'{self._node_prefix}{i}'
            assert validator_id in missed_endorsements, f'validator {validator_id} not in active validator set'
            num_missed = missed_endorsements[validator_id]
            assert num_missed == 0, f'validator {validator_id} missed {num_missed} endorsements'

    def wait_for_protocol_version(self, expected_version: int) -> None:
        started = time.time()
        while time.time() - started < TIMEOUT:
            all_done = True
            for node in self._stable_nodes:
                if node.get_status()['protocol_version'] != expected_version:
                    all_done = False
            if all_done:
                break
            time.sleep(1)

        for node in self._stable_nodes:
            protocol_version = node.get_status()['protocol_version']
            assert protocol_version == expected_version, \
                f"Wrong protocol version: {protocol_version} expected: {expected_version}"


def test_upgrade() -> None:
    TestUpgrade(
        num_validators=4,
        node_prefix='test',
        epoch_length=50,
    ).run()


def main():
    test_upgrade()


if __name__ == "__main__":
    main()

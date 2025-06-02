#!/usr/bin/env python3
"""Test if the node is backwards compatible with the latest release."""
import json
import os
import random
import re
import subprocess
import sys
import pathlib
import threading
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from configured_logger import logger
from transaction import sign_function_call_tx, sign_payment_tx, sign_deploy_contract_to_new_account_tx
import utils

ONE_NEAR = 1000000000000000000000000
NUM_VALIDATORS = 4
EPOCH_LENGTH = 40
NODE_PREFIX = "test"


def get_proto_version(exe: pathlib.Path) -> int:
    line = subprocess.check_output((exe, '--version'), text=True)
    m = re.search(r'.* \(protocol ([0-9]+)\)', line)
    assert m, (f'Unable to extract protocol version number from {exe};\n'
               f'Got {line.rstrip()} on standard output')
    logger.info(f'Protocol version {m.group(1)} found in {exe}')
    return int(m.group(1))


class TrafficGenerator(threading.Thread):
    """ A thread which keeps sending transactions to random addresses until stopped. """

    def __init__(self, rpc_node: cluster.LocalNode, **kwargs) -> None:
        super().__init__(**kwargs)
        random.seed(2025)
        self._rpc_node = rpc_node
        self._stopped = False
        self._failed = False
        self._acc1 = f"000000.{self._rpc_node.signer_key.account_id}"
        self._acc2 = f"zzzzzz.{self._rpc_node.signer_key.account_id}"
        self._nonce = 1_000_000

    def run(self) -> None:
        logger.info("Starting traffic generator")
        while not self._stopped:
            self._with_retry(self.send_transfer)
            self._with_retry(self.call_test_contracts)
        logger.info("Traffic generator stopped")

    def join(self, timeout):
        assert not self._failed
        return super().join(timeout=timeout)

    def _with_retry(self, fn):
        for i in range(5):
            try:
                return fn()
            except Exception as e:
                logger.error(f"Failed txn try {i}", exc_info=True)
        self._failed = True

    def stop(self) -> None:
        logger.info("Stopping traffic generator")
        self._stopped = True

    def get_latest_block_hash(self) -> bytes:
        return self._rpc_node.get_latest_block().hash_bytes

    def get_next_nonce(self) -> int:
        self._nonce += 1
        return self._nonce

    def send_tx(self, tx: bytes) -> None:
        res = self._rpc_node.send_tx_and_wait(tx, timeout=10)
        assert 'error' not in res, res
        assert 'Failure' not in res['result']['status'], res

    def deploy_test_contracts(self, config: cluster.Config) -> None:
        for acc in (self._acc1, self._acc2):
            tx = sign_deploy_contract_to_new_account_tx(
                self._rpc_node.signer_key,
                acc,
                utils.load_test_contract(config=config),
                100000 * ONE_NEAR,
                self.get_next_nonce(),
                self.get_latest_block_hash(),
            )
            self._rpc_node.send_tx(tx)

    def call_test_contracts(self) -> None:
        # Make the contract deployed at `acc1` call the contract deployed on `acc2`
        # and then make another call to itself. This should generate a postponed receipt,
        # which allows detecting some potential implicit protocol changes.
        logger.info(f"Calling test contracts")
        data = json.dumps([{
            "create": {
                "account_id": self._acc2,
                "method_name": "call_promise",
                "arguments": [],
                "amount": "0",
                "gas": 10**14
            },
            "id": 0
        }, {
            "then": {
                "promise_index": 0,
                "account_id": self._acc1,
                "method_name": "write_random_value",
                "arguments": [],
                "amount": "0",
                "gas": 10**14,
            },
            "id": 1
        }])
        tx = sign_function_call_tx(
            signer_key=self._rpc_node.signer_key,
            contract_id=self._acc1,
            methodName='call_promise',
            args=bytes(data, 'utf-8'),
            gas=3 * (10**14),
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


class Protocols:

    def __init__(self, executables: branches.ABExecutables):
        self.stable = get_proto_version(executables.stable.neard)
        self.current = get_proto_version(executables.current.neard)
        assert self.current >= self.stable, "cannot downgrade protocol version"


class TestUpgrade:

    def __init__(self) -> None:
        self._executables = branches.prepare_ab_test()
        self._protocols = Protocols(self._executables)
        node_dirs = self.configure_nodes()
        nodes = self.start_nodes(node_dirs)
        time.sleep(5)  # Give some time for nodes to start

        self._stable_nodes = nodes[:NUM_VALIDATORS]
        self._rpc_node = nodes[-1]

    def run(self) -> None:
        """Test that upgrade from `stable` to `current` binary is possible.

        1. Start a network with 3 `stable` nodes and 1 `new` node.
        2. Start switching `stable` nodes one by one with `new` nodes.
        3. Run for three epochs and observe that the current protocol version of the
           network matches `new` nodes.
        """
        traffic_generator = TrafficGenerator(self._rpc_node)
        traffic_generator.deploy_test_contracts(
            self._executables.current.node_config())
        traffic_generator.start()

        try:
            self.wait_epoch()
            self.upgrade_nodes()

            start_pv = self._protocols.stable + 1
            end_pv = self._protocols.current
            for pv in range(start_pv, end_pv + 1):
                self.wait_till_protocol_version(pv)
                self.check_validator_stats()

            # Run one more epoch with the latest protocol version
            self.wait_epoch()
            self.check_validator_stats()

        finally:
            traffic_generator.stop()
            traffic_generator.join(timeout=30)

    def configure_nodes(self) -> list[str]:
        node_root = utils.get_near_tempdir('upgradable', clean=True)
        cmd = (
            str(self._executables.stable.neard),
            f'--home={node_root}',
            'localnet',
            f'--validators={NUM_VALIDATORS}',
            '--non-validators-rpc=1',
            f'--prefix={NODE_PREFIX}',
        )
        logger.info(f"Configuring nodes with command: {cmd}")
        subprocess.check_call(cmd)

        genesis_config_changes = [("epoch_length", EPOCH_LENGTH)]
        node_dirs = [
            os.path.join(node_root, f'{NODE_PREFIX}{i}')
            for i in range(NUM_VALIDATORS + 1)
        ]
        for node_dir in node_dirs:
            cluster.apply_genesis_changes(node_dir, genesis_config_changes)
            # Dump epoch configs to use mainnet shard layout
            self.dump_epoch_configs(node_dir, self._protocols.current)

        for node_dir in node_dirs[:NUM_VALIDATORS]:
            # Validators should track only assigned shards
            cluster.apply_config_changes(node_dir, {'tracked_shards': []})

        return node_dirs

    def dump_epoch_configs(self, node_dir: str, last_protocol_version: int):
        cmd = (
            str(self._executables.current.neard),
            f'--home={node_dir}',
            'dump-epoch-configs',
            f'--chain-id=mainnet',
            f'--last-version={last_protocol_version}',
        )
        logger.info(f"Dumping epoch configs with command: {cmd}")
        subprocess.check_call(cmd)

    def start_nodes(self, node_dirs: list[str]) -> list[cluster.LocalNode]:
        nodes = []
        for i in range(len(node_dirs)):
            executable = (self._executables.stable if i < NUM_VALIDATORS -
                          1 else self._executables.current)
            node = cluster.spin_up_node(
                config=executable.node_config(),
                near_root=executable.root,
                node_dir=node_dirs[i],
                ordinal=i,
                boot_node=nodes[0] if i > 0 else None,
                sleep_after_start=0,
            )
            nodes.append(node)
        return nodes

    def upgrade_nodes(self) -> None:
        # Restart stable nodes into the new version.
        logger.info(f"Restarting nodes with new binary")
        for node in self._stable_nodes:
            node.kill()
            node.near_root = self._executables.current.root
            node.binary_name = self._executables.current.neard
            node.start(
                boot_node=self._stable_nodes[0],
                extra_env={
                    "NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE": "sequential"
                },
            )

    def wait_epoch(self) -> None:
        """Wait until the next epoch starts."""
        start_epoch = self._rpc_node.get_epoch_id()
        logger.info(f"Current epoch is {start_epoch}. Waiting for next epoch")
        condition = lambda h: self._rpc_node.get_epoch_id(block_height=h
                                                         ) != start_epoch
        self._poll_block_till_condition(condition)

    def wait_till_protocol_version(self, version: int):
        """Wait until the protocol version of the node matches the given version."""
        logger.info(f"Waiting for protocol version {version}")
        condition = lambda _: self._rpc_node.get_status()["protocol_version"
                                                         ] == version
        self._poll_block_till_condition(condition)

    def _poll_block_till_condition(self, condition):
        """Wait until the condition is met."""
        for height, _ in utils.poll_blocks(self._rpc_node):
            if condition(height):
                break

    def check_validator_stats(self):
        """Check that all validators are producing blocks"""
        epoch_id = self._rpc_node.get_epoch_id()
        logger.info(f"Checking validators for epoch {epoch_id}")
        validators = self._rpc_node.get_validators(
            epoch_id)["result"]["current_validators"]
        if len(validators) != NUM_VALIDATORS:
            prev_epoch_id = self._rpc_node.get_prev_epoch_id()
            prev_validators = self._rpc_node.get_validators(
                prev_epoch_id)["result"]
            logger.error(
                f"Expected {NUM_VALIDATORS}, got {len(validators)} validators")
            logger.error(f"{epoch_id} validators: {validators}")
            logger.error(f"{prev_epoch_id} validators: {prev_validators}")
            assert False


def main():
    TestUpgrade().run()


if __name__ == "__main__":
    main()

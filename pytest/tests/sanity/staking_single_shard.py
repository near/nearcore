#!/usr/bin/env python3
# Spins up 5 validating nodes. Let validators track a single shard.
# Add a dumper node for the state sync headers.
# Add an RPC node to issue tx and change the state.
# Send random transactions between accounts in different shards.
# Shuffle the shard assignment of validators and check if they can sync up.

from collections import defaultdict
import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from transaction import (sign_staking_tx)

from configured_logger import logger
from cluster import start_cluster
import state_sync_lib
from utils import wait_for_blocks
import simple_test

EPOCH_LENGTH = 10

NUM_VALIDATORS = 5

# Shard layout with 5 roughly equal size shards for convenience.
SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "fff",
            "lll",
            "rrr",
        ],
        "version": 2,
        "shards_split_map": [],
        "to_parent_shard_map": [],
    }
}

NUM_SHARDS = len(SHARD_LAYOUT["V1"]["boundary_accounts"]) + 1

ALL_ACCOUNTS_PREFIXES = [
    "aaa",
    "ggg",
    "lll",
    "sss",
]


class StateSyncValidatorShardSwap(unittest.TestCase):

    def _prepare_cluster(self,
                         with_rpc=False,
                         shuffle_shard_assignment=False,
                         centralized_state_sync=False,
                         decentralized_state_sync=False):
        (node_config_dump,
         node_config_sync) = state_sync_lib.get_state_sync_configs_pair(
             tracked_shards=None)

        # State snapshot is enabled for dumper if we test centralized state sync.
        # We want to dump the headers either way.
        node_config_dump[
            "store.state_snapshot_enabled"] = True if centralized_state_sync else False
        node_config_dump[
            "store.state_snapshot_config.state_snapshot_type"] = "EveryEpoch" if centralized_state_sync else "ForReshardingOnly"

        # State snapshot is enabled for validators if we test decentralized state sync.
        # They will share parts of the state.
        node_config_sync[
            "store.state_snapshot_enabled"] = True if decentralized_state_sync else False
        node_config_sync["tracked_shards"] = []

        # Validators
        configs = {x: node_config_sync.copy() for x in range(NUM_VALIDATORS)}

        # Dumper
        configs[NUM_VALIDATORS] = node_config_dump

        if with_rpc:
            # RPC
            configs[NUM_VALIDATORS + 1] = node_config_sync.copy()
            # RPC tracks all shards.
            configs[NUM_VALIDATORS + 1]["tracked_shards"] = [0]
            # RPC node does not participate in state parts distribution.
            configs[NUM_VALIDATORS + 1]["store.state_snapshot_enabled"] = False
            configs[NUM_VALIDATORS + 1][
                "store.state_snapshot_config.state_snapshot_type"] = "ForReshardingOnly"

        nodes = start_cluster(
            num_nodes=NUM_VALIDATORS,
            num_observers=1 + (1 if with_rpc else 0),
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                [
                    "shuffle_shard_assignment_for_chunk_producers",
                    shuffle_shard_assignment
                ], ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)

        for node in nodes:
            node.stop_checking_store()

        self.dumper_node = nodes[NUM_VALIDATORS]
        self.rpc_node = nodes[NUM_VALIDATORS +
                              1] if with_rpc else self.dumper_node
        self.nodes = nodes
        self.validators = nodes[:NUM_VALIDATORS]

    def _prepare_simple_transfers(self):
        self.testcase = simple_test.SimpleTransferBetweenAccounts(
            nodes=self.nodes,
            rpc_node=self.rpc_node,
            account_prefixes=ALL_ACCOUNTS_PREFIXES,
            epoch_length=EPOCH_LENGTH)

        self.testcase.wait_for_blocks(3)

        self.testcase.create_accounts()

        self.testcase.deploy_contracts()

    def _clear_cluster(self):
        self.testcase = None
        for node in self.nodes:
            node.cleanup()

    # Extra validator is the one that is assigned to a shard with more than one validator.
    def _get_extra_validator(self):
        res = self.rpc_node.get_validators()
        shards_to_validators = defaultdict(list)
        for validator in res["result"]["current_validators"]:
            for shard in validator["shards"]:
                shards_to_validators[shard].append(validator["account_id"])
        for s, validators in shards_to_validators.items():
            if len(validators) > 1:
                return validators[-1]

    def send_stake_tx(self, validator, stake):
        logger.info(f'Staking {stake} {validator.signer_key.account_id}...')
        nonce = self.rpc_node.get_nonce_for_pk(validator.signer_key.account_id,
                                               validator.signer_key.pk) + 10

        hash_ = self.rpc_node.get_latest_block().hash_bytes
        tx = sign_staking_tx(validator.signer_key, validator.validator_key,
                             stake, nonce, hash_)
        res = self.rpc_node.send_tx_and_wait(tx, timeout=15)

    # Test that validators can sync up after restaking.
    def _run_test_state_sync_with_restaking(self):
        validator = self.validators[int(self._get_extra_validator()[-1])]

        self.send_stake_tx(validator, 0)
        target_height = 6 * EPOCH_LENGTH
        self.testcase.random_workload_until(target_height)
        self.send_stake_tx(validator, 50000000000000000000000000000000)
        target_height = 9 * EPOCH_LENGTH
        # Wait for all nodes to reach epoch 9.
        for n in self.validators:
            logger.info(
                f"Waiting for node {n.ordinal} to reach target height {target_height}"
            )
            try:
                wait_for_blocks(n, target=target_height)
            except Exception as e:
                logger.error(e)
                assert False, f"Node {n.ordinal} did not reach target height {target_height} in time"
        logger.info("Test ended")

    def test_state_sync_with_restaking_decentralized_only(self):
        try:
            self._prepare_cluster(
                with_rpc=True,
                shuffle_shard_assignment=False,
                decentralized_state_sync=True,
            )
            self._prepare_simple_transfers()
            self._run_test_state_sync_with_restaking()
        except Exception as e:
            assert False, f"Test failed: {e}"

    def test_state_sync_with_restaking_centralized_only(self):
        try:
            self._prepare_cluster(
                with_rpc=True,
                shuffle_shard_assignment=False,
                centralized_state_sync=True,
            )
            self._prepare_simple_transfers()
            self._run_test_state_sync_with_restaking()
        except Exception as e:
            assert False, f"Test failed: {e}"

    def test_state_sync_with_restaking_both_dss_and_css(self):
        try:
            self._prepare_cluster(
                with_rpc=True,
                shuffle_shard_assignment=False,
                decentralized_state_sync=True,
                centralized_state_sync=True,
            )
            self._prepare_simple_transfers()
            self._run_test_state_sync_with_restaking()
        except Exception as e:
            assert False, f"Test failed: {e}"

    def tearDown(self):
        self._clear_cluster()


if __name__ == '__main__':
    unittest.main()

"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware

import copy
from mirror import CommandContext, run_env_cmd, run_remote_cmd
from utils import PartitionSelector
from datetime import datetime, timedelta


class TestReleaseCandidate(TestSetup):
    """
    Test case:
    - Runs an upgrade test from the previous release to the current release candidate.
    Features:
        - No state dumper.
        - Upgrade happens over 2 epochs.
        - Archival nodes.
        - 1 producer per shard
        - 2 validators.

    Required arguments:
        - neard_binary_url: The URL of the starting neard binary.
        - neard_upgrade_binary_url: The URL of the neard binary to upgrade to.
        - genesis_protocol_version: The starting protocol version to use for the network.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=11)
        self.epoch_len = 3600  # 3600 blocks / 2 bps / 60 = 30 min
        self.has_state_dumper = False
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"

        # Upgrade a quarter of the nodes at a time, spanning two epochs.
        self.upgrade_interval_minutes = 5
        self.upgrade_delay_minutes = 15

    # tx_validity_period must stay below 3 * epoch_len so epoch sync proofs fall
    # inside the validate_proof age window (see EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS).
    # The mainnet dump carries 86400, far above 3 * 3600.
    transaction_validity_period = 7200

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        self._override_protocol_upgrade_voting()
        self._amend_genesis(
            f".transaction_validity_period = {self.transaction_validity_period}")

    def _amend_genesis(self, jq_field):
        # genesis.json is read fresh into ChainStore at startup and is not part of
        # the genesis block hash, so amending it before start_network is safe.
        jq_cmd = f"""jq "{jq_field}" $1 > $1.tmp && mv $1.tmp $1"""
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'nodes'
        run_cmd_args.cmd = f"sh -c '{jq_cmd}' _ ~/.near/genesis.json"
        run_remote_cmd(CommandContext(run_cmd_args))
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = f"sh -c '{jq_cmd}' _ ~/.near/target/genesis.json"
        run_remote_cmd(CommandContext(run_cmd_args))

    def _override_protocol_upgrade_voting(self):
        # Release binaries gate protocol upgrade voting behind a mainnet
        # calendar date. Vote immediately so the upgrade happens during the test.
        env_args = copy.deepcopy(self.args)
        env_args.clear_all = False
        env_args.key_value = ["NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE=now"]
        run_env_cmd(CommandContext(env_args))

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = false")

    def _upgrade_nodes_in_four_batches(self):
        """
        Upgrade the nodes in two steps, upgrade_delay_minutes apart.
        At each step, we upgrade half of the nodes at a time.
        In total, we upgrade in 4 batches.
        """
        if not self._needs_upgrade():
            return
        first_upgrade_time = datetime.now() + timedelta(
            minutes=self.upgrade_delay_minutes)
        second_upgrade_time = first_upgrade_time + timedelta(
            minutes=self.upgrade_delay_minutes)

        upgrade_time = [
            batch_start_time +
            timedelta(minutes=i * self.upgrade_interval_minutes)
            for batch_start_time in [first_upgrade_time, second_upgrade_time]
            for i in range(0, 2)
        ]
        batches = len(upgrade_time)
        for quarter, batch_start_time in enumerate(upgrade_time):
            self.schedule_binary_upgrade(batch_start_time,
                                         0,
                                         binary_idx=1,
                                         partition=PartitionSelector(
                                             partitions_range=(quarter + 1,
                                                               quarter + 1),
                                             total_partitions=batches))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._upgrade_nodes_in_four_batches()

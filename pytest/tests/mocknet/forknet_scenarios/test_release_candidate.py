"""
Test case classes for release tests on forknet.
"""
from typing import Dict

from .base import TestSetup, NodeHardware, time_to_str
from mirror import CommandContext, update_config_cmd, run_remote_cmd

import copy
from utils import PartitionSelector, ScheduleMode
from datetime import datetime, timedelta


class TestReleaseCandidate(TestSetup):
    """
    Test case:
    - Runs an upgrade test from the previous release to the current release candidate.
    Features:
        - No state dumper.
        - Upgrade happens over 2 epochs.
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
        self.epoch_len = 2000  # 14500 blocks / 2 bps / 60 / 60 = 2h
        self.has_state_dumper = False
        self.regions = "us-east1,europe-west1,asia-east1,us-west1"

        # Upgrade 1/2 nodes at a time, a quarter at a time.
        self.upgrade_interval_minutes = 15  # 15 minutes between each upgrade batch.
        self.first_upgrade_delay_minutes = 45
        self.second_upgrade_delay_minutes = 75

    def amend_epoch_config(self):
        """
        This is a workaround to set the protocol upgrade threshold close to 1, so that the upgrade happens when all the **block producers** have upgraded.
        This avoid the need to properly calculate the upgrade timeline based on the epoch length and the upgrade interval.
        """
        super().amend_epoch_config()
        # Upgrade when all producers vote
        self._amend_epoch_config(
            ".protocol_upgrade_stake_threshold = [9999,10000]")

        # No kickouts dues to missed production/validation
        self._amend_epoch_config(".block_producer_kickout_threshold = 0")
        self._amend_epoch_config(".chunk_producer_kickout_threshold = 0")
        self._amend_epoch_config(".chunk_validator_only_kickout_threshold = 0")

    def _set_log_level(self, target_log_level: Dict[str, str]):
        """
        Append extra target=level directives to log_config.json on
        every node. Appending makes the new directives override any
        existing same-target ones, since tracing_subscriber's EnvFilter
        applies the last matching directive.
        """
        if not target_log_level:
            return
        extra = ",".join(
            f"{target}={level}" for target, level in target_log_level.items())
        jq_filter = (f'.opentelemetry = (.opentelemetry + ",{extra}") | '
                     f'.rust_log = (.rust_log + ",{extra}")')

        def patch_cmd(path):
            return (f"jq '{jq_filter}' {path} > {path}.tmp "
                    f"&& mv {path}.tmp {path}")

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'nodes'
        run_cmd_args.cmd = patch_cmd("/home/ubuntu/.near/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = patch_cmd(
            "/home/ubuntu/.near/target/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg = ["save_invalid_witnesses=true"]
        # Long block wait time to minimise
        cfg.append('consensus.max_block_production_delay={"secs":30,"nanos":0}')
        cfg.append('consensus.max_block_wait_delay={"secs":30,"nanos":0}')
        cfg_args = copy.deepcopy(self.args)
        cfg_args.set = ';'.join(cfg)
        update_config_cmd(CommandContext(cfg_args))

        self._set_log_level({"vm": "debug"})

    def _upgrade_nodes_in_four_batches(self):
        """
        Upgrade the nodes in two steps, upgrade_delay_minutes apart.
        At each step, we upgrade half of the nodes at a time.
        In total, we upgrade in 4 batches.
        """
        first_upgrade_time = datetime.now() + timedelta(
            minutes=self.first_upgrade_delay_minutes)
        second_upgrade_time = datetime.now() + timedelta(
            minutes=self.second_upgrade_delay_minutes)

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

    def _checkout_nearcore_on_traffic(self):
        """
        Sparse-checkout `pytest/` from slavas/bench-long-compile of
        nearcore at /home/ubuntu/nearcore on the traffic node so the
        scheduled stress test can run slow_compile_adversarial.py and
        import its pytest/lib helpers. Idempotent: clones if missing,
        otherwise fetches and hard-resets to the upstream tip.
        """
        repo = "https://github.com/near/nearcore.git"
        branch = "slavas/bench-long-compile"
        checkout_dir = "/home/ubuntu/nearcore"
        cmd = (
            f"set -e; "
            f"if [ ! -d {checkout_dir}/.git ]; then "
            f"  git clone --no-checkout --filter=blob:none {repo} {checkout_dir} "
            f"  && git -C {checkout_dir} sparse-checkout init --cone "
            f"  && git -C {checkout_dir} sparse-checkout set pytest; "
            f"fi; "
            f"git -C {checkout_dir} fetch origin {branch} "
            f"&& git -C {checkout_dir} checkout -B slow-compile-adversarial origin/{branch} "
            f"&& git -C {checkout_dir} reset --hard origin/{branch}")
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = cmd
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_slow_compile_stress_test(self, delay_minutes: int = 30):
        """
        Schedule slow_compile_adversarial.py to start delay_minutes after the
        network start. The traffic node is itself an RPC node, so we
        point the script at localhost:3030. The script signs as the
        forknet-injected full-access key for `astro-stakers.poolv1.near`.
        """

        run_at = datetime.now() + timedelta(minutes=delay_minutes)
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = (
            "cd /home/ubuntu/nearcore "
            "&& python3 pytest/tests/mocknet/slow_compile_adversarial.py "
            "--rpc-url http://localhost:3030 "
            "--tps 1")
        run_cmd_args.on = ScheduleMode(mode="calendar",
                                       value=time_to_str(run_at))
        run_cmd_args.schedule_id = "slow-compile-adversarial"
        run_remote_cmd(CommandContext(run_cmd_args))

    def before_test_setup(self):
        """
        Use this event to run any commands before the test is started.
        """
        super().before_test_setup()
        self._checkout_nearcore_on_traffic()

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._upgrade_nodes_in_four_batches()
        self._schedule_slow_compile_stress_test()


class TestReleaseCandidateFast(TestReleaseCandidate):
    """
    Variant of TestReleaseCandidate optimized for protocol-transition
    wall-clock time. Useful for smoke tests; not representative of a real
    release rollout.

    Differences from TestReleaseCandidate:
        - Shorter epochs (epoch_len=500). The new protocol activates 2 epoch
          boundaries after the vote, so epoch length dominates the post-
          upgrade tail.
        - Single-batch upgrade a few minutes after start, instead of four
          staged batches spread over ~45 minutes. The 99.99% stake threshold
          inherited from the parent is still fine because all nodes flip at
          once.
    """

    def __init__(self, args):
        super().__init__(args)
        self.epoch_len = 500
        self.first_upgrade_delay_minutes = 5

    def after_test_start(self):
        # Skip TestReleaseCandidate's 4-batch rollout; upgrade all nodes once.
        TestSetup.after_test_start(self)
        upgrade_time = datetime.now() + timedelta(
            minutes=self.first_upgrade_delay_minutes)
        self.schedule_binary_upgrade(upgrade_time, 0, binary_idx=1)
        self._schedule_slow_compile_stress_test()

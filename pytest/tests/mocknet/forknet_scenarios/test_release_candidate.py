"""
Test case classes for release tests on forknet.
"""
from typing import Dict

from .base import TestSetup, NodeHardware, time_to_str
from mirror import CommandContext, update_config_cmd, run_remote_cmd, start_nodes_cmd

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
        self.stress_start_delay_minutes = 90

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

    def _schedule_slow_compile_stress_test(self, schedule_id, run_at: datetime):
        """
        Schedule slow_compile_adversarial.py to start delay_minutes after the
        network start. The traffic node is itself an RPC node, so we
        point the script at localhost:3030. The script signs as the
        forknet-injected full-access key for `astro-stakers.poolv1.near`.
        """

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = (
            "cd /home/ubuntu/nearcore "
            "&& /home/ubuntu/.near/target/neard-runner/venv/bin/python pytest/tests/mocknet/slow_compile_adversarial.py "
            "--rpc-url http://localhost:3030 "
            "--tps 1")
        run_cmd_args.on = ScheduleMode(mode="calendar",
                                       value=time_to_str(run_at))
        run_cmd_args.schedule_id = schedule_id
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_stop_stress_test(self, filter_str, run_at: datetime):
        # run_at = datetime.now() + timedelta(minutes=delay_minutes)
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'traffic'
        cmd = f'systemctl --user stop "*{filter_str}"; systemctl --user reset-failed "*{filter_str}"'
        run_cmd_args.cmd = cmd
        run_cmd_args.on = ScheduleMode(mode="calendar",
                                       value=time_to_str(run_at))
        run_cmd_args.schedule_id = f"stop-{filter_str}"
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_config_change(self, max_block_production_delay,
                                max_block_wait_delay, run_at: datetime):
        cfg = [
            f'consensus.max_block_production_delay={{"secs":{max_block_production_delay[0]},"nanos":{max_block_production_delay[1]}}}',
            f'consensus.max_block_wait_delay={{"secs":{max_block_wait_delay[0]},"nanos":{max_block_wait_delay[1]}}}'
        ]
        cfg_args = copy.deepcopy(self.args)
        cfg_args.host_type = 'nodes'
        cfg_args.set = ';'.join(cfg)
        cfg_args.on = ScheduleMode(mode="calendar", value=time_to_str(run_at))
        cfg_args.schedule_id = f"config-change-{max_block_production_delay[0]}"
        update_config_cmd(CommandContext(cfg_args))

    def _schedule_binary_restart(self, idx, run_at: datetime):
        start_nodes_args = copy.deepcopy(self.args)
        start_nodes_args.host_type = 'nodes'
        start_nodes_args.force_restart = True
        start_nodes_args.on = ScheduleMode(mode="calendar",
                                           value=time_to_str(run_at))
        start_nodes_args.schedule_id = f"neard-restart-{idx}"
        start_nodes_cmd(CommandContext(start_nodes_args))

    def _schedule_looping_stress_test(self):
        now = datetime.now()
        max_block_production_delay_to_test = [(30, 0), (15, 0), (7, 0), (3, 0),
                                              (1, 800000000)]
        max_block_wait_delay_to_test = [(30, 0), (15, 0), (7, 0), (6, 0),
                                        (6, 0)]

        # time to first run
        stress_start_delay_minutes = self.stress_start_delay_minutes
        # time to stop the first test
        stress_stop_delay_minutes = stress_start_delay_minutes + 10
        # time to first config change. This will only take effect after restarting neard.
        config_change_delay_minutes = stress_stop_delay_minutes + 2
        # time to restart neard to pickup the new config. We will give the node 25 minutes after stopping the stress test to recover.
        neard_restart_delay_minutes = stress_stop_delay_minutes + 25
        # then 5 more minutes after the restart to make sure the node is stable before we start the next iteration of the stress test.
        test_period_minutes = 30

        for i, (max_block_production_delay, max_block_wait_delay) in enumerate(
                zip(max_block_production_delay_to_test,
                    max_block_wait_delay_to_test)):
            schedule_id = f"stress-test-{i}"
            self._schedule_slow_compile_stress_test(
                schedule_id,
                datetime.now() + timedelta(minutes=stress_start_delay_minutes +
                                           i * test_period_minutes))
            self._schedule_stop_stress_test(
                schedule_id, now + timedelta(minutes=stress_stop_delay_minutes +
                                             i * test_period_minutes))
            self._schedule_config_change(
                max_block_production_delay, max_block_wait_delay,
                now + timedelta(minutes=config_change_delay_minutes +
                                i * test_period_minutes))
            self._schedule_binary_restart(
                i, now + timedelta(minutes=neard_restart_delay_minutes +
                                   i * test_period_minutes))

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
        self._schedule_looping_stress_test()


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

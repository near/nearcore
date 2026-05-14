"""
Test case classes for release tests on forknet.
"""

from typing import Dict

from .base import TestSetup, NodeHardware, time_to_str
from mirror import CommandContext, update_config_cmd, run_remote_cmd, start_nodes_cmd

import copy
import shlex
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

    # (account_id, public_key, private_key). Only account_id is mandatory;
    # public/private must be supplied together or both left None (script
    # falls back to its defaults when only the account is given).
    stress_signers = [
        ("astro-stakers.poolv1.near", None, None),
    ]

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=11
        )
        self.epoch_len = 500  # 14500 blocks / 2 bps / 60 / 60 = 2h
        self.has_state_dumper = False
        self.regions = "us-east1,europe-west1,asia-east1,us-west1"

        # Upgrade 1/2 nodes at a time, a quarter at a time.
        self.upgrade_interval_minutes = 5  # 5 minutes between each upgrade batch.
        self.first_upgrade_delay_minutes = 10
        self.second_upgrade_delay_minutes = 20
        self.stress_start_delay_minutes = 60

    def amend_epoch_config(self):
        """
        This is a workaround to set the protocol upgrade threshold close to 1, so that the upgrade happens when all the **block producers** have upgraded.
        This avoid the need to properly calculate the upgrade timeline based on the epoch length and the upgrade interval.
        """
        super().amend_epoch_config()
        # Upgrade when all producers vote
        self._amend_epoch_config(".protocol_upgrade_stake_threshold = [9999,10000]")

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
            f"{target}={level}" for target, level in target_log_level.items()
        )
        jq_filter = (
            f'.opentelemetry = (.opentelemetry + ",{extra}") | '
            f'.rust_log = (.rust_log + ",{extra}")'
        )

        def patch_cmd(path):
            return f"jq '{jq_filter}' {path} > {path}.tmp && mv {path}.tmp {path}"

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "nodes"
        run_cmd_args.cmd = patch_cmd("/home/ubuntu/.near/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = patch_cmd("/home/ubuntu/.near/target/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg = ["save_invalid_witnesses=true"]
        # Long block wait time to minimise
        cfg.append('consensus.max_block_production_delay={"secs":30,"nanos":0}')
        cfg.append('consensus.max_block_wait_delay={"secs":30,"nanos":0}')
        cfg_args = copy.deepcopy(self.args)
        cfg_args.set = ";".join(cfg)
        update_config_cmd(CommandContext(cfg_args))

        self._set_log_level({"vm": "debug"})

    def _upgrade_nodes_in_four_batches(self):
        """
        Upgrade the nodes in two steps, upgrade_delay_minutes apart.
        At each step, we upgrade half of the nodes at a time.
        In total, we upgrade in 4 batches.
        """
        first_upgrade_time = datetime.now() + timedelta(
            minutes=self.first_upgrade_delay_minutes
        )
        second_upgrade_time = datetime.now() + timedelta(
            minutes=self.second_upgrade_delay_minutes
        )

        upgrade_time = [
            batch_start_time + timedelta(minutes=i * self.upgrade_interval_minutes)
            for batch_start_time in [first_upgrade_time, second_upgrade_time]
            for i in range(0, 2)
        ]
        batches = len(upgrade_time)
        for quarter, batch_start_time in enumerate(upgrade_time):
            self.schedule_binary_upgrade(
                batch_start_time,
                0,
                binary_idx=1,
                partition=PartitionSelector(
                    partitions_range=(quarter + 1, quarter + 1),
                    total_partitions=batches,
                ),
            )

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
            f"&& git -C {checkout_dir} reset --hard origin/{branch}"
        )
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = cmd
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_slow_compile_stress_test(self, schedule_id, run_at: datetime):
        """
        Schedule slow_compile_adversarial.py to start delay_minutes after the
        network start. The traffic node is itself an RPC node, so we
        point the script at localhost:3030. Every entry in
        `self.stress_signers` is forwarded as a `--signer` flag whose value
        is a 1- or 3-element CSV (`account_id` or
        `account_id,public_key,private_key`).
        """
        signer_flags = []
        for account_id, public_key, private_key in self.stress_signers:
            if public_key and private_key:
                csv = f"{account_id},{public_key},{private_key}"
            else:
                csv = account_id
            signer_flags.append(f"--signer {shlex.quote(csv)}")

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = (
            "cd /home/ubuntu/nearcore "
            "&& /home/ubuntu/.near/target/neard-runner/venv/bin/python pytest/tests/mocknet/slow_compile_adversarial.py "
            "--rpc-url http://localhost:3030 "
            "--concurrency 5 "
            "--tps 1 " + " ".join(signer_flags)
        )
        run_cmd_args.on = ScheduleMode(mode="calendar", value=time_to_str(run_at))
        run_cmd_args.schedule_id = schedule_id
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_stop_stress_test(self, filter_str, run_at: datetime):
        """
        Schedule a systemd one-shot on the traffic node that stops +
        reset-failed any units matching `mocknet-{filter_str}`.
        """
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "traffic"
        cmd = f'systemctl --user stop "mocknet-{filter_str}"; systemctl --user reset-failed "mocknet-{filter_str}"'
        run_cmd_args.cmd = cmd
        run_cmd_args.on = ScheduleMode(mode="calendar", value=time_to_str(run_at))
        run_cmd_args.schedule_id = f"stop-{filter_str}"
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_config_change(
        self, max_block_production_delay, max_block_wait_delay, run_at: datetime
    ):
        """
        Schedule an `update_config` on all nodes that rewrites
        `consensus.max_block_{production,wait}_delay`. Queued only —
        takes effect on the next neard restart.
        """
        cfg = [
            f'consensus.max_block_production_delay={{"secs":{max_block_production_delay[0]},"nanos":{max_block_production_delay[1]}}}',
            f'consensus.max_block_wait_delay={{"secs":{max_block_wait_delay[0]},"nanos":{max_block_wait_delay[1]}}}',
        ]
        cfg_args = copy.deepcopy(self.args)
        cfg_args.host_type = "nodes"
        cfg_args.set = ";".join(cfg)
        cfg_args.on = ScheduleMode(mode="calendar", value=time_to_str(run_at))
        cfg_args.schedule_id = f"config-change-{max_block_production_delay[0]}"
        update_config_cmd(CommandContext(cfg_args))

    def _schedule_binary_restart(self, idx, run_at: datetime):
        """
        Schedule a `force_restart` of neard on all nodes; used to pick up a
        previously-queued config change.
        """
        start_nodes_args = copy.deepcopy(self.args)
        start_nodes_args.host_type = "nodes"
        start_nodes_args.force_restart = True
        start_nodes_args.on = ScheduleMode(mode="calendar", value=time_to_str(run_at))
        start_nodes_args.schedule_id = f"neard-restart-{idx}"
        start_nodes_cmd(CommandContext(start_nodes_args))

    def _schedule_looping_stress_test(self):
        """
        Schedule a sequence of stress-test iterations on the traffic node,
        progressively tightening
        `consensus.max_block_{production,wait}_delay` so we can observe how
        the network behaves as block time shrinks under the
        `slow_compile_adversarial` load.

        Per-iteration sequence (`T` = iteration start):
            T+0   start slow_compile_adversarial.py on traffic
            T+10  stop the stress test
            T+12  queue the next config change on all nodes
                  (queued only — takes effect on the next restart)
            T+35  restart neard on all nodes (picks up the new config)
            T+40  next iteration starts (≈5 min settle after restart)

        Iterations are `test_period_minutes = 40` apart; the first starts
        `self.stress_start_delay_minutes` after network start (90 min).

        Configs swept (max_block_production_delay, max_block_wait_delay):
            initial (30, 30) → (15, 15) → (7, 7) → (3, 6) → (1.8, 6) seconds.

        With N config entries the loop schedules N+1 stress runs but only N
        config-change/restart pairs: the first run stresses the initial
        baseline, the trailing run stresses the final config without
        changing it further.
        """
        now = datetime.now()
        max_block_production_delay_to_test = [(15, 0), (7, 0), (3, 0), (1, 800000000)]
        max_block_wait_delay_to_test = [(15, 0), (7, 0), (6, 0), (6, 0)]

        # Per-iteration offsets, in minutes, relative to the iteration start.
        stress_start_delay_minutes = self.stress_start_delay_minutes
        stress_stop_delay_minutes = stress_start_delay_minutes + 10
        config_change_delay_minutes = stress_stop_delay_minutes + 2
        neard_restart_delay_minutes = stress_stop_delay_minutes + 25
        test_period_minutes = 40

        n = len(max_block_production_delay_to_test)
        # N+1 stress runs: baseline + one per config change.
        for i in range(n + 1):
            schedule_id = f"stress-test-{i}"
            self._schedule_slow_compile_stress_test(
                schedule_id,
                now
                + timedelta(
                    minutes=stress_start_delay_minutes + i * test_period_minutes
                ),
            )
            self._schedule_stop_stress_test(
                schedule_id,
                now
                + timedelta(
                    minutes=stress_stop_delay_minutes + i * test_period_minutes
                ),
            )

        # N config-change + restart pairs, slotted between consecutive runs.
        for i, (max_block_production_delay, max_block_wait_delay) in enumerate(
            zip(max_block_production_delay_to_test, max_block_wait_delay_to_test)
        ):
            self._schedule_config_change(
                max_block_production_delay,
                max_block_wait_delay,
                now
                + timedelta(
                    minutes=config_change_delay_minutes + i * test_period_minutes
                ),
            )
            self._schedule_binary_restart(
                i,
                now
                + timedelta(
                    minutes=neard_restart_delay_minutes + i * test_period_minutes
                ),
            )

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
            minutes=self.first_upgrade_delay_minutes
        )
        self.schedule_binary_upgrade(upgrade_time, 0, binary_idx=1)
        self._schedule_slow_compile_stress_test()

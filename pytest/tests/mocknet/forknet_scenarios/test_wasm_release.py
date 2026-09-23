"""
Test case classes for release tests on forknet.
"""

# cspell:words bisontrails ledgerbyfigment minimise poolv zavodil

from typing import Dict

from .base import TestSetup, NodeHardware, time_to_str
from mirror import CommandContext, update_config_cmd, run_remote_cmd, start_nodes_cmd

import copy
import shlex
from utils import PartitionSelector, ScheduleMode
from datetime import datetime, timedelta


class TestWasmCandidate(TestSetup):
    """
    Test case: upgrade test from the previous release to the current
    release candidate. After the upgrade is rolled out, a stress-test
    load is applied repeatedly on the traffic node while
    `consensus.max_block_{production,wait}_delay` is progressively
    tightened, so we can observe behavior as block time shrinks.

    Features:
        - No state dumper.
        - SameConfig hardware: 9 chunk-producer seats, 11 chunk-validator seats.
        - Binary rolled out in 4 partition-based batches across two delay windows.
        - Protocol-upgrade vote pinned at 9999/10000 so the upgrade activates
          as soon as every block producer has voted.

    Required arguments:
        - neard_binary_url: The URL of the starting neard binary.
        - neard_upgrade_binary_url: The URL of the neard binary to upgrade to.
        - genesis_protocol_version: The starting protocol version to use for the network.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=11)
        self.epoch_len = 500
        self.has_state_dumper = False
        self.has_archival = False
        self.regions = "us-east1,europe-west1,asia-east1,us-west1"

        # 4 upgrade batches: 2 batches at `first_upgrade_delay_minutes`,
        # then 2 more at `second_upgrade_delay_minutes`,
        # `upgrade_interval_minutes` apart within each window.
        self.upgrade_interval_minutes = 5
        self.first_upgrade_delay_minutes = 10
        self.second_upgrade_delay_minutes = 20
        self.stress_start_delay_minutes = 60
        # (account_id, public_key, private_key). Only account_id is mandatory;
        # public/private must be supplied together or both left None (script
        # falls back to its defaults when only the account is given).
        self.stress_signers = [
            ("astro-stakers.poolv1.near", None, None),
        ]
        # Per-signer deploys-per-second for the stress-test script. Default
        # and validation live on the `start` subparser; getattr keeps this
        # safe for create/destroy subcommands that don't define --tps.
        self.tps = getattr(args, 'tps', 1.0)
        # Value passed via --contract to the stress-test script. When None
        # the flag is omitted and the script uses its built-in default.
        # CLI --contract overrides; otherwise None.
        self.contract = getattr(args, 'contract', None)

    def amend_epoch_config(self):
        """
        This is a workaround to set the protocol upgrade threshold close to 1, so that the upgrade happens when all the **block producers** have upgraded.
        This avoids the need to properly calculate the upgrade timeline based on the epoch length and the upgrade interval.
        """
        super().amend_epoch_config()
        # Upgrade when all producers vote
        self._amend_epoch_config(
            ".protocol_upgrade_stake_threshold = [9999,10000]")

        # No kickouts due to missed production/validation
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
            return f"jq '{jq_filter}' {path} > {path}.tmp && mv {path}.tmp {path}"

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "nodes"
        run_cmd_args.cmd = patch_cmd("/home/ubuntu/.near/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = patch_cmd(
            "/home/ubuntu/.near/target/log_config.json")
        run_remote_cmd(CommandContext(run_cmd_args))

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg = ["save_invalid_witnesses=true"]
        # Long block wait time to minimise block and chunk miss due to long processing time.
        cfg.append('consensus.max_block_production_delay={"secs":30,"nanos":0}')
        cfg.append('consensus.max_block_wait_delay={"secs":30,"nanos":0}')
        cfg_args = copy.deepcopy(self.args)
        cfg_args.host_type = "all"
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
        Sparse-checkout `pytest/` from nearcore at /home/ubuntu/nearcore
        on the traffic node, so the scheduled stress-test script can be invoked
        from `pytest/tests/mocknet/` and import its `pytest/lib` helpers.
        Idempotent: clones if missing, otherwise fetches and hard-resets
        to the upstream tip.
        """
        repo = "https://github.com/near/nearcore.git"
        branch = "master"
        checkout_dir = "/home/ubuntu/nearcore"
        cmd = (
            f"set -e; "
            f"if [ ! -d {checkout_dir}/.git ]; then "
            f"  git clone --no-checkout --filter=blob:none {repo} {checkout_dir} "
            f"  && git -C {checkout_dir} sparse-checkout init --cone "
            f"  && git -C {checkout_dir} sparse-checkout set pytest; "
            f"fi; "
            # Reconcile origin with `repo` so an existing checkout from a
            # previous run (possibly pointing at a different fork) is
            # redirected to the current source.
            f"git -C {checkout_dir} remote set-url origin {repo} "
            f"&& git -C {checkout_dir} fetch origin {branch} "
            f"&& git -C {checkout_dir} checkout -B wasm-stress-test origin/{branch} "
            f"&& git -C {checkout_dir} reset --hard origin/{branch}")
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = cmd
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_slow_compile_stress_test(self, schedule_id, run_at: datetime):
        """
        Schedule the stress-test script to run on the traffic node at
        `run_at`. The traffic node is itself an RPC node, so we point the
        script at localhost:3030. Every entry in `self.stress_signers` is
        forwarded as a `--signer` flag whose value is a 1- or 3-element
        CSV (`account_id` or `account_id,public_key,private_key`). When
        `self.contract` is set it is forwarded as `--contract`.
        """
        extra_flags = []
        for account_id, public_key, private_key in self.stress_signers:
            if public_key and private_key:
                csv = f"{account_id},{public_key},{private_key}"
            else:
                csv = account_id
            extra_flags.append(f"--signer {shlex.quote(csv)}")
        if self.contract:
            extra_flags.append(f"--contract {shlex.quote(self.contract)}")
        extra_flags.append(f"--tps {shlex.quote(str(self.tps))}")

        # Scale workers with TPS so the per-signer queue (size = 2*concurrency)
        # has ~2s of slack before the ticker starts dropping.
        max_concurrency = 16
        concurrency = int(min(max(4, self.tps * 2), max_concurrency))

        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = "traffic"
        run_cmd_args.cmd = (
            "cd /home/ubuntu/nearcore "
            "&& /home/ubuntu/.near/target/neard-runner/venv/bin/python pytest/tests/mocknet/wasm_stress_test.py "
            "--rpc-url http://localhost:3030 "
            f"--concurrency {concurrency} " + " ".join(extra_flags))
        run_cmd_args.on = ScheduleMode(mode="calendar",
                                       value=time_to_str(run_at))
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
        run_cmd_args.on = ScheduleMode(mode="calendar",
                                       value=time_to_str(run_at))
        run_cmd_args.schedule_id = f"stop-{filter_str}"
        run_remote_cmd(CommandContext(run_cmd_args))

    def _schedule_config_change(self, max_block_production_delay,
                                max_block_wait_delay, run_at: datetime):
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
        start_nodes_args.on = ScheduleMode(mode="calendar",
                                           value=time_to_str(run_at))
        start_nodes_args.schedule_id = f"neard-restart-{idx}"
        start_nodes_cmd(CommandContext(start_nodes_args))

    def _schedule_looping_stress_test(self):
        """
        Schedule a sequence of stress-test iterations on the traffic node,
        progressively tightening
        `consensus.max_block_{production,wait}_delay` so we can observe
        how the network behaves as block time shrinks under load.

        Per-iteration sequence (`T` = iteration start):
            T+0   start the stress-test script on traffic
            T+10  stop the stress test
            T+12  queue the next config change on all nodes
                  (queued only — takes effect on the next restart)
            T+35  restart neard on all nodes (picks up the new config)
            T+40  next iteration starts (≈5 min settle after restart)

        Iterations are `test_period_minutes = 40` apart; the first starts
        `self.stress_start_delay_minutes` after network start.

        Configs swept (max_block_production_delay, max_block_wait_delay):
            initial (30, 30) → (15, 15) → (7, 7) → (3, 6) → (1.8, 6) seconds.

        With N config entries the loop schedules N+1 stress runs but only
        N config-change/restart pairs: the first run stresses the initial
        baseline, the trailing run stresses the final config without
        changing it further.
        """
        now = datetime.now()
        max_block_production_delay_to_test = [(15, 0), (7, 0), (3, 0),
                                              (1, 800000000)]
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
                now + timedelta(minutes=stress_start_delay_minutes +
                                i * test_period_minutes),
            )
            self._schedule_stop_stress_test(
                schedule_id,
                now + timedelta(minutes=stress_stop_delay_minutes +
                                i * test_period_minutes),
            )

        # N config-change + restart pairs, slotted between consecutive runs.
        for i, (max_block_production_delay, max_block_wait_delay) in enumerate(
                zip(max_block_production_delay_to_test,
                    max_block_wait_delay_to_test)):
            self._schedule_config_change(
                max_block_production_delay,
                max_block_wait_delay,
                now + timedelta(minutes=config_change_delay_minutes +
                                i * test_period_minutes),
            )
            self._schedule_binary_restart(
                i,
                now + timedelta(minutes=neard_restart_delay_minutes +
                                i * test_period_minutes),
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


"""
──────┬───────────────────────────────────────────────┬──────────────────────────────────────────────┬───────────────────┐
Shard │                     Range                     │      Validator-pool example (top-stake)      │ # validator pools │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
10    │ (start) → 650                                 │ 01node.poolv1.near                           │ 4                 │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
11    │ 650 → aurora                                  │ astro-stakers.poolv1.near                    │ 19                │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
1     │ aurora → aurora-0                             │ aurora (Aurora EVM bridge; no pool in range) │ 0                 │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
8     │ aurora-0 → earn.kaiching                      │ bisontrails2.poolv1.near                     │ 75                │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
9     │ earn.kaiching → game.hot.tg                   │ figment.poolv1.near                          │ 33                │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
6     │ game.hot.tg → game.hot.tg-0                   │ game.hot.tg (HOT.tg game; no pool in range)  │ 0                 │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
7     │ game.hot.tg-0 → kkuuue2akv_1630967379.near    │ kiln-1.poolv1.near                           │ 41                │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
4     │ kkuuue2akv_1630967379.near → tge-lockup.sweat │ ledgerbyfigment.poolv1.near                  │ 203               │
──────┼───────────────────────────────────────────────┼──────────────────────────────────────────────┼───────────────────┤
5     │ tge-lockup.sweat → (end)                      │ zavodil.poolv1.near                          │ 35                │
──────┴───────────────────────────────────────────────┴──────────────────────────────────────────────┴───────────────────┘
"""


class TestWasmCandidateAllShards(TestWasmCandidate):

    def __init__(self, args):
        super().__init__(args)
        self.stress_signers = [
            ("01node.poolv1.near", None, None),
            ("astro-stakers.poolv1.near", None, None),
            ("aurora", "ed25519:3P9tDPVEnL2VzqYLKxPWcwBfvab1dr9mypHzGqUxNR2k",
             "ed25519:21Db9BAqtogf1bWhJqJn42RB2s3usAW4p3BUQGRUkSKZtihPYiUGegS9DuhNq3bMP2kEyD9oN1g9sVctt9K8JK7a"
            ),
            ("bisontrails2.poolv1.near", None, None),
            ("figment.poolv1.near", None, None),
            ("game.hot.tg",
             "ed25519:BnqafTvwMCAeTkDMT71BL7oWZcRgbqWFg1wseDokBanH",
             "ed25519:4LKLF9mkipZqA1FwjTNBkRq6wMatvDNCjJbn8iYdqBATzW5uQtH8ZfyCQj7F53M55Zi62FY4A7dujuAQ289SK2V"
            ),
            ("kiln-1.poolv1.near", None, None),
            ("ledgerbyfigment.poolv1.near", None, None),
            ("zavodil.poolv1.near", None, None),
        ]

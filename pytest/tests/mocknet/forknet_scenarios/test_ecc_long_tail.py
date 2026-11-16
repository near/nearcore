"""
Test case for the release that stabilizes external contract calls: the cases
the first two clusters could not run: the upgrade with block producers left on
the old binary (UPG-12), and calls in flight while chunk producers change
shards (NOD-15, NOD-19), with mainnet-sized shards.
"""
import copy
import re

from mirror import CommandContext, run_remote_cmd, start_traffic_cmd, update_config_cmd
from rc import pmap

from .base import TestSetup, logger
from .test_ecc_release import SameConfigWithSpareHost

# Prints `spare` on a host whose validator account is not in the genesis
# validator set, `validator` otherwise.
_SPARE_CHECK_CMD = (
    'a=$(jq -r .account_id ~/.near/validator_key.json) && '
    'if jq -e --arg a "$a" \'any(.[]; .account_id == $a)\' '
    '~/.near/neard-runner/validators.json > /dev/null; '
    'then echo validator; else echo spare; fi')


class TestEccLongTail(TestSetup):
    """
    Test case: start on 2.14.1 at protocol 88 with the release candidate as
    binary index 1 on every host, and leave the upgrade to the tester, as
    TestEccRelease does. One overnight cluster, 5 epochs, for the cases
    that need old producers past the vote, shuffled producers and a spare
    host.

    Settings, and the case that needs each:
        - epoch_length 15800 (about 2.5 hours): a shuffled producer
          state-syncs and catches up its next shard within one epoch. Last
          week's forknets synced a shard in 10 to 60 minutes, at most 122.
        - transaction_validity_period 31600 (2 epochs) in genesis.json, and
          epoch_length 15800 in every epoch config file: epoch sync accepts a
          proof only within max(epoch_length, transaction_validity_period)
          and 3 epochs below the peer head. Mainnet's 86400 makes that window
          empty, as on ecc-88. With 2 epochs a node accepts the proof (seen on
          the local mock), though the forked genesis then strands it (see
          Conflicts). The startup check (validity <= 2 * epoch_length) reads
          the epoch config files, so they get the real epoch length too.
        - protocol_upgrade_stake_threshold [4, 5], mainnet's: with 12 block
          producers of equal stake, 10 on the new binary pass, so up to 2 stay
          on 2.14.1 past the vote (UPG-12).
        - Block and chunk producer kickout thresholds 50, chunk validators
          at mainnet's 70: a producer whose sync runs long misses chunks
          without being kicked out. An old producer is still kicked with
          ProtocolVersionTooOld (UPG-12).
        - shuffle_shard_assignment_for_chunk_producers on: producers change
          shards at most epoch boundaries (NOD-19, and NOD-15 alongside).
        - State dumper: dumps parts to GCS. Nodes state-sync from peers only
          (SyncConfig has only Peers), so the dumper serves as one more
          all-shards peer with snapshots.
        - 12 chunk producers and 2 chunk-validator-only hosts, plus a spare
          host outside the validator set. The spare keeps its forked database
          and is not started: the tester starts it on binary index 1 for
          NOD-14. It keeps outcomes and has the epoch sync horizon raised to
          100 epochs, so it block-syncs from the archival hosts instead of
          epoch-syncing.

    Conflicts:
        - An empty spare against the forked genesis: a node with an empty
          data directory, or one that epoch-synced and reset its data, builds
          a different genesis block from genesis.json (GenesisContents::
          StateRoots) and never connects. Seen on the local mock, both ways.
          So the spare is not wiped, NOD-14 state-syncs through catch-up of a
          shadowed producer's next shard, and no node may cross the 2-epoch
          horizon without raising it first: the epoch sync half of UPG-16
          and the 2-epoch NOD-10 cannot run on a forknet.
        - Shuffling: a moved producer must state-sync and catch up a
          mainnet-sized shard within one epoch, or it misses chunks. Measure
          the catch-up time in the first shuffled epoch, before the vote.
        - Shuffling with held hosts: a stopped or old producer leaves its
          shard of that epoch with no chunks if it is that shard's only
          producer. Pick held producers from two-producer shards of the epoch
          they are down in (UPG-12).

    Required arguments:
        - neard_binary_url: 2.14.1.
        - neard_upgrade_binary_url: the release candidate.
        - genesis_protocol_version: 88.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = SameConfigWithSpareHost(
            num_chunk_producer_seats=12, num_chunk_validator_seats=14)
        self.epoch_len = 15800
        self.transaction_validity_period = 2 * self.epoch_len
        self.has_state_dumper = True
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.spare_hosts = []

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(f".epoch_length = {self.epoch_len}")
        self._amend_epoch_config(".protocol_upgrade_stake_threshold = [4,5]")
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = true")
        self._amend_epoch_config(".block_producer_kickout_threshold = 50")
        self._amend_epoch_config(".chunk_producer_kickout_threshold = 50")

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        self._amend_genesis(".transaction_validity_period = "
                            f"{self.transaction_validity_period}")
        self._prepare_spare_hosts()
        self._check_settings_on_hosts()

    def _check_settings_on_hosts(self):
        """Fail the start unless every host holds the same amended values."""
        cmd = ("jq -c '[.transaction_validity_period, .epoch_length]' {g} && "
               "jq -c '[.epoch_length, .protocol_upgrade_stake_threshold, "
               ".shuffle_shard_assignment_for_chunk_producers, "
               ".block_producer_kickout_threshold, "
               ".chunk_producer_kickout_threshold]' {d}89.json")
        ctx = CommandContext(self._args_for('all'))
        hosts = [(n, '~/.near/genesis.json', '~/.near/epoch_configs/')
                 for n in ctx.nodes]
        if ctx.traffic_generator is not None:
            hosts.append((ctx.traffic_generator, '~/.near/target/genesis.json',
                          '~/.near/target/epoch_configs/'))
        answers = pmap(
            lambda h: (h[0].name(), h[0].run_cmd(cmd.format(g=h[1], d=h[2]),
                                                 return_on_fail=True)), hosts)
        values = {}
        for name, r in answers:
            values.setdefault((r.exitcode, r.stdout.strip()), []).append(name)
        for (code, out), names in values.items():
            logger.info(f'exit {code}: {out}: {names}')
        if len(values) != 1 or list(values)[0][0] != 0:
            raise ValueError('genesis or 89.json differ between hosts')

    def _amend_genesis(self, jq_field):
        """
        genesis.json is read at every start and the field is not in the
        genesis block hash. A `mirror reset` restores the genesis.json kept
        in the backup, so run this again after one.
        """
        for host_type, path in [('nodes', '~/.near/genesis.json'),
                                ('traffic', '~/.near/target/genesis.json')]:
            run_args = self._args_for(host_type)
            run_args.cmd = f"jq '{jq_field}' {path} > {path}.tmp && mv {path}.tmp {path}"
            run_remote_cmd(CommandContext(run_args))

    def _prepare_spare_hosts(self):
        ctx = CommandContext(self._args_for('nodes'))
        candidates = [n for n in ctx.nodes if n.can_validate]
        answers = pmap(
            lambda n:
            (n.name(), n.run_cmd(_SPARE_CHECK_CMD, raise_on_fail=True)),
            candidates)
        self.spare_hosts = sorted(
            name for name, r in answers if r.stdout.strip() == 'spare')
        logger.info(f'spare hosts: {self.spare_hosts}')
        if len(self.spare_hosts) != 1:
            raise ValueError(
                f'expected one spare host, found {self.spare_hosts}')

        cfg_args = self._args_for('nodes')
        cfg_args.host_filter = self._spare_filter()
        cfg_args.set = ';'.join([
            'save_tx_outcomes=true',
            'epoch_sync.epoch_sync_horizon_num_epochs=100',
        ])
        update_config_cmd(CommandContext(cfg_args))

    def _spare_filter(self):
        return '^(' + '|'.join(re.escape(n) for n in self.spare_hosts) + ')$'

    def start_network(self):
        """Start every node but the spare, then the traffic generator."""
        start_args = copy.deepcopy(self.args)
        start_args.batch_interval_millis = None
        start_args.host_filter = '^(?!(' + '|'.join(
            re.escape(n) for n in self.spare_hosts) + ')$)'
        start_traffic_cmd(CommandContext(start_args))

    def _args_for(self, host_type):
        args = copy.deepcopy(self.args)
        args.host_type = host_type
        return args

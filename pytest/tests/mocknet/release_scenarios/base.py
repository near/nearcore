"""
This script is used to define release tests for forknet.
"""

import copy
import pathlib
import sys
import tempfile
import time
from dataclasses import dataclass

from mirror import (CommandContext, amend_binaries_cmd, clear_scheduled_cmds,
                    get_nodes_status, hard_reset_cmd, new_test_cmd,
                    run_remote_cmd, run_remote_download_file,
                    run_remote_upload_file, start_nodes_cmd, start_traffic_cmd,
                    stop_nodes_cmd, update_config_cmd)
from utils import ScheduleMode

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger


class NodeHardware:
    """
    Hardware configuration for validators.
    """

    @dataclass
    class Config:
        # The number of chunk producers.
        num_chunk_producer_seats: int
        # The number of chunk validators.
        num_chunk_validator_seats: int

        def chunk_producers_hosts(self) -> int:
            pass

        def only_chunk_validators_hosts(self) -> int:
            pass

    class SameConfig(Config):
        """
        All validators have the same hardware.
        """

        def chunk_producers_hosts(self) -> int:
            return self.num_chunk_validator_seats

        def only_chunk_validators_hosts(self) -> int:
            return 0

    class SmallChunkValidatorsConfig(Config):
        """
        Separate chunk producers and validators hardware.
        """

        def chunk_producers_hosts(self) -> int:
            return self.num_chunk_producer_seats

        def only_chunk_validators_hosts(self) -> int:
            return self.num_chunk_validator_seats - self.num_chunk_producer_seats


class TestSetup:
    """
    Base class for all test cases.
    Do not use this class directly, use the subclasses instead and initialize the
    variables in the __init__ method.
    """

    def __init__(self, args):
        self.args = args
        # The forknet image height.
        self.args.start_height = None
        self.start_height = None
        # The unique id of the forknet.
        self.unique_id = args.unique_id

        self.has_archival = False
        self.has_state_dumper = False
        self.tracing_server = False
        # The GCP regions to be used for the nodes.
        self.regions = None
        # Hardware configuration for validators
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=0, num_chunk_validator_seats=0)
        # The base binary url to be used for the nodes.
        self.neard_binary_url = None
        # The new binary url to be used for the nodes.
        self.neard_upgrade_binary_url = getattr(args,
                                                'neard_upgrade_binary_url', '')

    def _needs_upgrade(self):
        """
        Check if the test needs to go through the upgrade process.
        """
        return self.neard_upgrade_binary_url is not None and self.neard_upgrade_binary_url != ''

    def init_env(self):
        """
        Setup the forknet environment for the test.
        """
        logger.info(
            f"Initializing environment for test case {self.args.test_case}")
        # Load test state orchestrator
        init_args = copy.deepcopy(self.args)
        init_args.neard_upgrade_binary_url = ''
        init_args.yes = True
        init_args.gcs_state_sync = self.has_state_dumper
        hard_reset_cmd(CommandContext(init_args))

        # Traffic node should always run the latest binary to avoid restarting it.
        if self._needs_upgrade():
            logger.info(
                f"Amending binaries on traffic node with url: {self.neard_upgrade_binary_url}"
            )
            amend_binaries_args = copy.deepcopy(self.args)
            amend_binaries_args.binary_idx = 0
            amend_binaries_args.epoch_height = None
            amend_binaries_args.neard_binary_url = self.neard_upgrade_binary_url
            amend_binaries_args.host_type = 'traffic'
            amend_binaries_cmd(CommandContext(amend_binaries_args))

        # Clear scheduled commands.
        logger.info(f"Clearing scheduled commands.")
        clear_args = copy.deepcopy(self.args)
        clear_args.filter = '*'
        clear_scheduled_cmds(CommandContext(clear_args))

    def before_test_setup(self):
        """
        This command will be called before the test case is created.
        """
        pass

    def _setup_archival(self):
        """
        Setup archival for the test.
        """
        # TODO: move this to a helper script on the hosts.
        cfg_args = copy.deepcopy(self.args)
        cfg_args.host_filter = '.*archival.*'
        cfg_args.set = ';'.join([
            'archive=true', 'gc_num_epochs_to_keep=3', 'save_trie_changes=true',
            'split_storage.enable_split_storage_view_client=true',
            'cold_store.path="/home/ubuntu/.near/cold"',
            'tracked_shards_config="AllShards"',
            'store.load_mem_tries_for_tracked_shards=false'
        ])
        update_config_cmd(CommandContext(cfg_args))

        run_args = copy.deepcopy(self.args)
        run_args.host_filter = '.*archival.*'
        run_args.cmd = "rm -rf /home/ubuntu/.near/cold /home/ubuntu/.near/validator_key.json && /home/ubuntu/.near/neard-runner/binaries/neard0 database change-db-kind --new-kind Hot change-hot"
        run_remote_cmd(CommandContext(run_args))

    def new_test(self):
        """
        Create a new test case.
        """

        new_test_args = copy.deepcopy(self.args)
        new_test_args.epoch_length = self.epoch_len
        new_test_args.genesis_protocol_version = self.genesis_protocol_version
        new_test_args.num_validators = self.node_hardware_config.num_chunk_validator_seats
        # Set all seats to the lower value. This will be increased later in epoch config.
        new_test_args.num_seats = self.node_hardware_config.num_chunk_producer_seats
        new_test_args.stateless_setup = True
        new_test_args.new_chain_id = self.unique_id
        new_test_args.yes = True
        new_test_args.gcs_state_sync = self.has_state_dumper
        new_test_args.state_source = 'dump'
        new_test_args.patches_path = None

        new_test_cmd(CommandContext(new_test_args))

    def wait_for_network_to_be_ready(self):
        """
        Wait for the network to be ready.
        """
        ctx = CommandContext(self.args)
        while True:
            nodes = ctx.get_targeted()
            not_ready_nodes = get_nodes_status(nodes)
            if len(not_ready_nodes) == 0:
                break
            logger.info(
                f"Waiting for network to be ready. {len(not_ready_nodes)} nodes not ready."
            )
            time.sleep(1)
        logger.info("Network is ready.")

    def _share_epoch_configs(self):
        """
        Share the latest epoch configs with the nodes.
        """
        # Download the epoch configs from the traffic node.
        # It potentially has the latest epoch configs.
        tmp_dir = tempfile.mkdtemp()
        download_args = copy.deepcopy(self.args)
        download_args.host_type = 'traffic'
        download_args.src = "~/.near/target/epoch_configs"
        download_args.dst = tmp_dir
        run_remote_download_file(CommandContext(download_args))

        # Copy the epoch configs to the rest of the nodes.
        upload_args = copy.deepcopy(self.args)
        upload_args.host_type = 'nodes'
        upload_args.src = f'{tmp_dir}/epoch_configs/*'
        upload_args.dst = '~/.near/epoch_configs/'
        run_remote_upload_file(CommandContext(upload_args))

    def _amend_epoch_config(self, jq_field):
        # TODO: move this in the helper scripts on the hosts.
        """
        Amend the epoch config with the given jq field.
        """
        jq_cmd = f"""-type f -name "*.json" -exec sh -c 'jq "{jq_field}" $1 > $1.tmp && mv $1.tmp $1' _ {{}} \;"""
        cmd_node = f"find ~/.near/epoch_configs/ {jq_cmd}"
        cmd_traffic = f"find ~/.near/target/epoch_configs/ {jq_cmd}"
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'nodes'
        run_cmd_args.cmd = cmd_node
        run_remote_cmd(CommandContext(run_cmd_args))
        run_cmd_args.host_type = 'traffic'
        run_cmd_args.cmd = cmd_traffic
        run_remote_cmd(CommandContext(run_cmd_args))

    def _reduce_chunk_validators_stake(self):
        """
        Reduce the stake of the chunk validators to avoid accidentally becoming the block producers.
        """
        # TODO: install the near validator tools and setup the environment.
        # TODO: run the validator tools to reduce the stake of the chunk validators.
        pass

    def amend_epoch_config(self):
        if self._needs_upgrade():
            # We need to share the epoch configs before the upgrade.
            self._share_epoch_configs()
        self._amend_epoch_config(
            f".num_chunk_validator_seats = {self.node_hardware_config.num_chunk_validator_seats}"
        )
        self._reduce_chunk_validators_stake()

    def amend_configs_before_test_start(self):
        """
        Amend the configs for the test.
        """
        if self.has_archival:
            self._setup_archival()

    def start_network(self):
        """
        mirror start-nodes
        """
        start_nodes_args = copy.deepcopy(self.args)
        start_nodes_args.batch_interval_millis = None
        start_traffic_cmd(CommandContext(start_nodes_args))

    def _schedule_upgrade_nodes_every_n_minutes(self, minutes):
        """
        Make 4 batches of upgrades.
        Stop the nodes, amend the binaries, and start the nodes.
        """
        if self.neard_upgrade_binary_url == '':
            return

        for i in range(1, 5):
            stop_nodes_args = copy.deepcopy(self.args)
            stop_nodes_args.host_type = 'nodes'
            stop_nodes_args.select_partition = (i, 4)
            stop_nodes_args.on = ScheduleMode(mode="active",
                                              value=f"{(i * minutes)}m")
            stop_nodes_args.schedule_id = f"up-stop-{i}"
            stop_nodes_cmd(CommandContext(stop_nodes_args))

            amend_binaries_args = copy.deepcopy(self.args)
            amend_binaries_args.binary_idx = 0
            amend_binaries_args.epoch_height = None
            amend_binaries_args.neard_binary_url = self.neard_upgrade_binary_url
            amend_binaries_args.host_type = 'nodes'
            amend_binaries_args.select_partition = (i, 4)
            amend_binaries_args.on = ScheduleMode(
                mode="active", value=f"{(i * minutes * 60 + 20)}")
            amend_binaries_args.schedule_id = f"up-change-{i}"
            amend_binaries_cmd(CommandContext(amend_binaries_args))

            start_nodes_args = copy.deepcopy(self.args)
            start_nodes_args.host_type = 'nodes'
            start_nodes_args.select_partition = (i, 4)
            start_nodes_args.on = ScheduleMode(mode="active",
                                               value=f"{(i*minutes + 1)}m")
            start_nodes_args.schedule_id = f"up-start-{i}"
            start_nodes_cmd(CommandContext(start_nodes_args))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        # Schedule the nodes to upgrade to the new binary every n minutes.
        # self._schedule_upgrade_nodes_every_n_minutes(40)

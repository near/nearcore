#!/usr/bin/env python3
"""
Cli tool for managing the mocknet instances.
"""
from argparse import ArgumentParser, Action
import copy
import datetime
import pathlib
from rc import pmap
import re
import sys
import time
from typing import Callable
import os

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import mocknet_ssh
import node_config
import remote_node
from node_handle import NodeHandle, check_env_key_values
from utils import ScheduleContext, ScheduleMode, build_stake_distribution, PartitionSelector

MOCKNET_STORE_PATH = os.getenv("MOCKNET_STORE_PATH",
                               "gs://near-mocknet-artefact-store")


def to_list(item):
    return [item] if item is not None else []


def prompt_setup_flags(args, dumper_node_names):
    if not args.yes:
        print(
            'this will reset all nodes\' home dirs and initialize them with new state. continue? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()

    if not args.gcs_state_sync and len(dumper_node_names) > 0:
        print(
            f'--gcs-state-sync not provided, but there are state dumper nodes: {dumper_node_names}. continue with dumper nodes as normal RPC nodes? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()

    if args.epoch_length is None:
        print('epoch length for the initialized genesis file?: ')
        args.epoch_length = int(sys.stdin.readline().strip())

    if args.num_validators is None:
        print('number of validators?: ')
        args.num_validators = int(sys.stdin.readline().strip())

    if args.num_seats is None:
        args.num_seats = args.num_validators


def prompt_init_flags(args):
    if args.neard_binary_url is None:
        print('neard binary URL?: ')
        args.neard_binary_url = sys.stdin.readline().strip()
        assert len(args.neard_binary_url) > 0

    if args.neard_upgrade_binary_url == "":
        args.neard_upgrade_binary_url = None
        return

    if args.neard_upgrade_binary_url is None:
        print(
            'add a second neard binary URL to upgrade to mid-test? enter nothing here to skip: '
        )
        url = sys.stdin.readline().strip()
        if len(url) > 0:
            args.neard_upgrade_binary_url = url


class CommandContext:

    def __init__(self, args):
        self.args = args
        self.nodes = []
        self.traffic_generator = None
        self.schedule_ctx = self._make_schedule_context()

        # will set the schedule context in the nodes if the command is scheduled
        self._get_remote_nodes()
        # Filter hosts based on the base args of the command
        self._filter_hosts()

    def is_scheduled(self) -> bool:
        return self.schedule_ctx is not None

    def get_targeted(self) -> list[NodeHandle]:
        """
        Returns the nodes and traffic generator with the previous schedule context.
        """
        return list(self.nodes) + to_list(self.traffic_generator)

    def get_targeted_with_schedule_ctx(self) -> list[NodeHandle]:
        """
        Sets the schedule context in the nodes and traffic generator.
        All the commands will be scheduled if schedule is set.
        """
        return [
            node.with_schedule_ctx(self.schedule_ctx)
            for node in self.get_targeted()
        ]

    def get_without_schedule_ctx(self) -> list[NodeHandle]:
        """
        Clears the schedule context in the nodes and traffic generator.
        All the commands will be executed now.
        """
        return [node.with_schedule_ctx(None) for node in self.get_targeted()]

    def get_mocknet_id(self):
        mocknet_id = None
        if (self.args.chain_id is not None and
                self.args.start_height is not None and
                self.args.unique_id is not None):
            mocknet_id = self.args.chain_id + '-' + str(
                self.args.start_height) + '-' + self.args.unique_id
        elif self.args.mocknet_id is not None:
            mocknet_id = self.args.mocknet_id
        else:
            sys.exit(
                f'must give all of --chain-id --start-height and --unique-id or --mocknet-id'
            )
        return mocknet_id

    def _get_remote_nodes(self):
        """
        Get nodes in the remote infrastructure.
        """
        mocknet_id = self.get_mocknet_id()
        traffic_generator, nodes = remote_node.get_nodes(mocknet_id)
        node_config.configure_nodes(nodes + to_list(traffic_generator))
        self.traffic_generator = traffic_generator
        self.nodes = nodes

    def _filter_hosts(self):
        """
        Select the affected hosts.
        traffic_generator can become None,
        nodes list can become empty
        """
        # Only keep nodes that run neard, not the auxiliary nodes i.e. tracers.
        self.nodes = [node for node in self.nodes if node.runs_neard]

        if self.args.host_filter is not None:
            if self.traffic_generator is not None and not re.search(
                    self.args.host_filter, self.traffic_generator.name()):
                self.traffic_generator = None
            self.nodes = [
                h for h in self.nodes
                if re.search(self.args.host_filter, h.name())
            ]
        if self.args.host_type not in ['all', 'traffic']:
            self.traffic_generator = None
        if self.args.host_type not in ['all', 'nodes']:
            self.nodes = []

        if len(self.nodes) == 0 and self.traffic_generator == None:
            logger.error(f'No hosts selected. Change filters and try again.')
            exit(1)

        if self.args.select_partition is not None:
            self.nodes.sort(key=lambda node: node.name())
            self.nodes = self.args.select_partition(self.nodes)

    def _make_schedule_context(self) -> ScheduleContext | None:
        """
        Make a schedule context if the command is scheduled.
        """
        schedule = getattr(self.args, 'on', None)
        if schedule is None:
            return None
        random_id = str(int(time.time() * 1000) % 100000000)
        context = ScheduleContext(
            id=getattr(self.args, 'schedule_id', None) or random_id,
            schedule=schedule,
        )
        logger.info(f'Schedule context: {context}')
        return context


def init_hosts(ctx: CommandContext, remove_home_dir=False):
    args = ctx.args
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator
    prompt_init_flags(args)

    if args.neard_upgrade_binary_url is not None and args.neard_upgrade_binary_url != '':
        neard_upgrade_binary_url = args.neard_upgrade_binary_url
    else:
        neard_upgrade_binary_url = None

    run_id = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    # Binary 0 runs first. The upgrade binary 1 is started with
    # `start-nodes --binary-idx 1`.
    node_binary_urls = [args.neard_binary_url]
    if neard_upgrade_binary_url:
        node_binary_urls.append(neard_upgrade_binary_url)
    hosts = [(node, node_binary_urls) for node in nodes]

    if traffic_generator:
        # If our scenario upgrades the binary, use the latest binary for the
        # traffic generator.
        hosts.append((traffic_generator,
                      [neard_upgrade_binary_url or args.neard_binary_url]))

    pmap(
        lambda host: host[0].init_host(host[1], run_id, MOCKNET_STORE_PATH,
                                       remove_home_dir), hosts)


def init_cmd(ctx: CommandContext):
    init_hosts(ctx, remove_home_dir=False)


def hard_reset_cmd(ctx: CommandContext):
    do_reset = getattr(ctx.args, 'yes', None)
    if do_reset is None:
        print("""
            WARNING!!!!
            WARNING!!!!
            This will undo all chain state, which will force a restart from the beginning,
            including the genesis state computation.
            Continue? [yes/no]""")
        do_reset = False
        if sys.stdin.readline().strip() != 'yes':
            do_reset = True
    if not do_reset:
        return
    init_hosts(ctx, remove_home_dir=True)
    _clear_state_parts_if_exists(_get_state_parts_location(ctx.args), ctx.nodes)


# returns boot nodes and validators we want for the new test network
def get_network_nodes(new_test_rpc_responses, num_validators,
                      stake_distribution):
    validators = []
    non_validators = []
    boot_nodes = []
    for node, response in new_test_rpc_responses:
        if len(validators) < num_validators:
            if node.can_validate:
                # We assume that if node validates a chain, it has account id
                # and public key.
                amount = stake_distribution.get_stake(node)

                validators.append({
                    'account_id': response['validator_account_id'],
                    'public_key': response['validator_public_key'],
                    'amount': str(amount),
                })
        else:
            non_validators.append(node.ip_addr())

        # We want to bootstrap chain data from nodes which have state
        # initially, which is the case iff node is not a chunk validator.
        if len(boot_nodes) < 20 and node.role() != 'validator':
            boot_nodes.append(
                f'{response["node_key"]}@{node.ip_addr()}:{response["listen_port"]}'
            )

        if len(validators) >= num_validators and len(boot_nodes) >= 20:
            break

    # Sort validators by decreasing amount (number) and then by increasing validator account id
    validators.sort(key=lambda v: (-int(v['amount']), v['account_id']))

    # neither of these should happen, since we check the number of available nodes in new_test(), and
    # only the traffic generator will respond with null validator_account_id and validator_public_key
    if len(validators) == 0:
        sys.exit('no validators available after new-test')
    if len(validators) < num_validators:
        logger.warning(
            f'wanted {num_validators} validators, but only {len(validators)} available'
        )
    return validators, boot_nodes


def _clear_state_parts_if_exists(location, nodes):
    # TODO: Maybe add an argument to set the epoch height from where we want to cleanup.
    # It still works without it because the dumper node will start dumping the current epoch after reset.

    if location is None:
        return

    state_dumper_node = next(filter(lambda n: n.want_state_dump, nodes), None)
    if state_dumper_node is None:
        logger.info('No state dumper node found, skipping state parts cleanup.')
        return
    logger.info('State dumper node found, cleaning up state parts.')

    # For GCS-based state sync, looks for the state dumper and clears the GCP
    # bucket where it dumped the parts.
    bucket_name = location['GCS']['bucket']

    state_dumper_node.run_cmd(f'gsutil -m rm -r gs://{bucket_name}/chain_id=*',
                              return_on_fail=True)


def _get_state_parts_bucket_name(args):
    return f'near-state-dumper-mocknet-{args.chain_id}-{args.start_height}-{args.unique_id}'


def _get_state_parts_location(args):
    return {"GCS": {"bucket": _get_state_parts_bucket_name(args)}}


def new_test_cmd(ctx: CommandContext):
    args = ctx.args
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator
    prompt_setup_flags(args, [n.name() for n in nodes if n.want_state_dump])

    if args.epoch_length <= 0:
        sys.exit(f'--epoch-length should be positive')
    if args.num_validators <= 0:
        sys.exit(f'--num-validators should be positive')
    if len(nodes) < args.num_validators:
        sys.exit(
            f'--num-validators is {args.num_validators} but only found {len(nodes)} under test'
        )

    genesis_time = datetime.datetime.now(
        tz=datetime.timezone.utc).isoformat().replace("`+00:00", "Z")

    targeted = nodes + to_list(traffic_generator)

    logger.info(f'resetting/initializing home dirs')

    if traffic_generator:
        rpc_ip = traffic_generator.ip_addr()
    else:
        rpc_ip = '0.0.0.0'
    test_keys = pmap(lambda node: node.new_test(), targeted)

    stake_distribution = build_stake_distribution(
        getattr(args, 'stake_distribution', None), args.num_seats)
    validators, boot_nodes = get_network_nodes(zip(nodes, test_keys),
                                               args.num_validators,
                                               stake_distribution)
    logger.info("""Setting validators: {0}
Run `status` to check if the nodes are ready. After they're ready,
 you can run `start-nodes` and `start-traffic`""".format(validators))
    location = _get_state_parts_location(args) if args.gcs_state_sync else None
    mocknet_id = ctx.get_mocknet_id()

    def network_init(node, keys):
        config_changes = [
            (['network', 'boot_nodes'], ','.join(
                b for b in boot_nodes if b.split('@')[0] != keys['node_key'])),
            (['tracked_shards_config'],
             'NoShards' if node.can_validate else 'AllShards'),
            (['store', 'load_mem_tries_for_tracked_shards'], node.can_validate),
        ]
        if location is not None and node.want_state_dump:
            dump_config = {'location': location}
            config_changes.append((['state_sync', 'dump'], dump_config))
        if len(validators) <= 3:
            min_num_peers = len(validators) - 1
            config_changes.append((['consensus',
                                    'min_num_peers'], min_num_peers))
        # Nodes with role "validator" are chunk validators. They start
        # without state.
        chunk_validator = node.role() == 'validator'
        # The first validator uploads genesis for the chunk validators.
        upload_setup = (node.can_validate and keys['validator_account_id']
                        == validators[0]['account_id'])
        setup_env = dict(
            MOCKNET_ID=mocknet_id,
            CHUNK_VALIDATOR=int(chunk_validator),
            UPLOAD_SETUP=int(upload_setup),
            STATE_SOURCE=args.state_source,
            PATCHES_PATH=args.patches_path,
            EPOCH_LENGTH=args.epoch_length,
            NUM_SEATS=args.num_seats,
            GENESIS_TIME=genesis_time,
            NEW_CHAIN_ID=args.new_chain_id,
            PROTOCOL_VERSION=args.genesis_protocol_version,
            CLI_CHAIN_ID=args.new_chain_id or 'mocknet',
            RPC_ADDR=f'{rpc_ip}:3030',
        )
        node.network_init(validators, config_changes, setup_env)

    pmap(lambda x: network_init(*x), zip(targeted, test_keys))

    _clear_state_parts_if_exists(location, nodes)


def get_nodes_pending(condition: Callable[[NodeHandle], bool],
                      nodes: list[NodeHandle]) -> list[NodeHandle]:
    # Returns the nodes that do not pass the condition.
    statuses = pmap(condition, nodes)
    return [node for ready, node in zip(statuses, nodes) if not ready]


def get_nodes_pending_new_test(nodes: list[NodeHandle]) -> list[NodeHandle]:
    # The node is ready when new-test command is finished.
    return get_nodes_pending(lambda node: node.is_ready(), nodes)


# TODO: Deprecate this function and use get_nodes_pending_new_test instead
def get_nodes_status(nodes: list[NodeHandle]) -> list[str]:
    not_ready_nodes = get_nodes_pending_new_test(nodes)
    return [node.name() for node in not_ready_nodes]


def status_cmd(ctx: CommandContext):
    nodes = ctx.get_targeted()
    states = pmap(lambda node: node.state(), nodes)
    not_ready = [(node.name(), state)
                 for node, state in zip(nodes, states)
                 if state != 'ready']
    if len(not_ready) == 0:
        print(f'all {len(nodes)} nodes ready')
    else:
        print(
            f'{len(nodes)-len(not_ready)}/{len(nodes)} ready. Nodes not ready: {not_ready[:3]}'
        )


def reset_cmd(ctx: CommandContext):
    args = ctx.args
    nodes = ctx.nodes

    if not args.yes:
        print(
            'this will reset selected nodes\' home dirs to their initial states right after test initialization finished. continue? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()

    pmap(lambda node: node.reset(), ctx.get_targeted_with_schedule_ctx())
    logger.info(
        'Data dir reset in progress. Run the `status` command to see when this is finished.'
    )
    # Do not clear state parts if scheduling
    # NOTE: This may be a problem if you want to schedule the reset command on a dumper node
    #       because the dumper node will start dumping the current epoch after reset.
    if not ctx.is_scheduled():
        _clear_state_parts_if_exists(_get_state_parts_location(args), nodes)


def stop_nodes_cmd(ctx: CommandContext):
    pmap(lambda node: node.stop_neard(), ctx.get_targeted_with_schedule_ctx())


def stop_traffic_cmd(ctx: CommandContext):
    ctx.traffic_generator.with_schedule_ctx(ctx.schedule_ctx).stop_neard()


def update_config_cmd(ctx: CommandContext):
    pmap(
        lambda node: node.update_config(ctx.args.set),
        ctx.get_targeted_with_schedule_ctx(),
    )


def start_nodes_cmd(ctx: CommandContext):
    nodes = ctx.nodes
    binary_idx = getattr(ctx.args, 'binary_idx', None)
    force_restart = getattr(ctx.args, 'force_restart', False)
    if not all(pmap(lambda node: node.is_ready(), nodes)):
        logger.warning(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(
        lambda node: node.with_schedule_ctx(ctx.schedule_ctx).start_neard(
            binary_idx=binary_idx, force_restart=force_restart), nodes)
    # Wait for the nodes to be up if not scheduling
    if not ctx.is_scheduled():
        pmap(lambda node: node.wait_node_up(), nodes)


def start_traffic_cmd(ctx: CommandContext):
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator
    force_restart = getattr(ctx.args, 'force_restart', False)

    if traffic_generator is None:
        logger.warning('No traffic node selected. Change filters.')
        return
    if not all(pmap(lambda node: node.is_ready(), ctx.get_targeted())):
        logger.warning(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(
        lambda node: node.with_schedule_ctx(ctx.schedule_ctx).start_neard(
            force_restart=force_restart), nodes)
    # Wait for the nodes to be up if not scheduling
    if not ctx.is_scheduled():
        logger.info("waiting for validators to be up")
        pmap(lambda node: node.wait_node_up(), nodes)
        logger.info(
            "waiting a bit after validators started before starting traffic")
        time.sleep(10)
    # TODO: maybe add 20 seconds delay to the schedule command to allow the other nodes to start
    traffic_generator.with_schedule_ctx(ctx.schedule_ctx).start_neard(
        batch_interval_millis=ctx.args.batch_interval_millis,
        force_restart=force_restart)
    if not ctx.is_scheduled():
        logger.info(
            f'test running. to check the traffic sent, try running "curl --silent http://{traffic_generator.ip_addr()}:{traffic_generator.neard_port()}/metrics | grep near_mirror"'
        )


def update_binaries_cmd(ctx: CommandContext):
    pmap(lambda node: node.update_binaries(),
         ctx.get_targeted_with_schedule_ctx())


def amend_binaries_cmd(ctx: CommandContext):
    url = ctx.args.neard_binary_url
    binary_idx = ctx.args.binary_idx
    pmap(lambda node: node.update_binaries(url, binary_idx),
         ctx.get_targeted_with_schedule_ctx())


# Only print stdout and stderr if they are not empty
def print_result(node, result):
    RED = '\033[91m'
    GREEN = '\033[92m'
    RESET = '\033[0m'
    stdout = f'\n{GREEN}stdout:{RESET}\n{result.stdout}' if result.stdout != "" else ''
    stderr = f'\n{RED}stderr:{RESET}\n{result.stderr}' if result.stderr != "" else ''
    logger.info('{0}:{1}{2}'.format(node.name(), stdout, stderr))


def _run_remote(hosts, cmd):
    pmap(
        lambda node: print_result(node, node.run_cmd(cmd, return_on_fail=True)),
        hosts,
        on_exception="")


def run_remote_cmd(ctx: CommandContext):
    targeted = ctx.get_targeted_with_schedule_ctx()
    logger.info(
        f'Running cmd `{ctx.args.cmd}` on {",".join([h.name() for h in targeted])}'
    )
    _run_remote(targeted, ctx.args.cmd)


def run_remote_upload_file(ctx: CommandContext):
    targeted = ctx.get_targeted()
    logger.info(
        f'Uploading {ctx.args.src} in {ctx.args.dst} on {",".join([h.name() for h in targeted])}'
    )
    pmap(lambda node: print_result(
        node, node.upload_file(ctx.args.src, ctx.args.dst)),
         targeted,
         on_exception="")


def upload_local_neard(args, local_path: str, remote_dir: str) -> str:
    """Upload a local neard binary to remote_dir on every targeted node.

    `scp` does not create destination directories, so this first runs
    `mkdir -p remote_dir` on every target, then uploads the file. Returns the
    absolute path on the remote where the binary now lives.
    """
    mkdir_args = copy.deepcopy(args)
    mkdir_args.cmd = f'mkdir -p {remote_dir}'
    run_remote_cmd(CommandContext(mkdir_args))

    upload_args = copy.deepcopy(args)
    upload_args.src = local_path
    upload_args.dst = remote_dir
    run_remote_upload_file(CommandContext(upload_args))

    return os.path.join(remote_dir, os.path.basename(local_path))


def add_node_name_to_path(node, path: str, prefix_if_path_is_dir: str) -> str:
    '''
    append the node name to the destination file as a suffix, before the extension if any
    '''
    node_full_name = node.name()
    name_parts = node_full_name.rsplit('-', 1)
    node_name = name_parts[1] if len(name_parts) == 2 else node_full_name

    path = pathlib.Path(path)
    if path.is_dir():
        path = path / f'{prefix_if_path_is_dir}'

    suffixes = ''.join(path.suffixes)
    base = str(path)[:-len(suffixes)] if suffixes else str(path)
    return f'{base}-{node_name}{suffixes}'


def run_remote_download_file(ctx: CommandContext, include_node_name=False):
    targeted = ctx.get_targeted()
    logger.info(
        f'Downloading {ctx.args.src} to {ctx.args.dst} on {",".join([h.name() for h in targeted])}'
    )

    filename = os.path.basename(ctx.args.src)
    pmap(lambda node: print_result(
        node,
        node.download_file(
            ctx.args.src,
            add_node_name_to_path(
                node, path=ctx.args.dst, prefix_if_path_is_dir=filename)
            if include_node_name else ctx.args.dst)),
         targeted,
         on_exception="")


def run_env_cmd(ctx: CommandContext):
    if ctx.args.clear_all:
        func = lambda node: node.clear_env()
    else:
        check_env_key_values(ctx.args.key_value)
        func = lambda node: node.update_env(ctx.args.key_value)
    pmap(func, ctx.get_targeted_with_schedule_ctx())


def list_scheduled_cmds(ctx: CommandContext):
    targeted = ctx.get_targeted()
    cmd = 'systemctl --user --legend=false list-timers "mocknet*" --all'
    if ctx.args.full:
        cmd += '; systemctl --user show "mocknet-*" -p Id -p Description --value'
    logger.info(
        f'Getting schedule from {",".join([h.name() for h in targeted])}')
    _run_remote(targeted, cmd)


def clear_scheduled_cmds(ctx: CommandContext):
    targeted = ctx.get_targeted()
    filter = ctx.args.filter
    if not filter.startswith('mocknet-'):
        filter = 'mocknet-' + filter
    logger.info(
        f'Clearing scheduled commands matching "{filter}" from {",".join([h.name() for h in targeted])}'
    )
    cmd = f'systemctl --user stop "{filter}"; systemctl --user reset-failed "{filter}"'
    _run_remote(targeted, cmd)


def snapshot_cmd(ctx: CommandContext):
    if ctx.args.create:
        pmap(
            lambda node: print_result(node,
                                      node.make_snapshot(ctx.args.snapshot_id)),
            ctx.get_targeted_with_schedule_ctx())
    elif ctx.args.restore:
        pmap(
            lambda node: print_result(
                node, node.restore_snapshot(ctx.args.snapshot_id)),
            ctx.get_targeted_with_schedule_ctx())
    elif ctx.args.list:
        pmap(lambda node: print_result(node, node.list_snapshots()),
             ctx.get_targeted_with_schedule_ctx())
    elif ctx.args.delete:
        pmap(
            lambda node: print_result(
                node, node.delete_snapshot(ctx.args.snapshot_id)),
            ctx.get_targeted_with_schedule_ctx())


class PartitionSelectorParser(Action):

    def __call__(self, parser, namespace, values, option_string=None):
        pattern = r"(\d+)(-(\d+))?/(\d+)"
        match = re.match(pattern, values)
        if not match:
            parser.error(
                f"Invalid input '{values}'. Expected format 'i/n' or 'i-j/n'.")
        total_partitions = int(match.group(4))
        if match.group(3) is None:
            i = int(match.group(1))
            if i <= 0 or i > total_partitions:
                parser.error(
                    f"Invalid input '{values}'. Expected format 'i/n' or 'i-j/n' where i is integer and 0 < i <= n."
                )
            partitions_range = (i, i)
        else:
            i, j = int(match.group(1)), int(match.group(3))
            if i <= 0 or i > j or j > total_partitions:
                parser.error(
                    f"Invalid input '{values}'. Expected format 'i/n' or 'i-j/n' where i and j are integers and 0 < i <= j <= n."
                )
            partitions_range = (i, j)

        setattr(
            namespace, self.dest,
            PartitionSelector(partitions_range=partitions_range,
                              total_partitions=total_partitions))


def build_parser():
    parser = ArgumentParser(description='Control a mocknet instance')
    parser.add_argument('--mocknet-id',
                        type=str,
                        help='''
                        Identifier of the mocknet instance to use. Can be used instead of specifying
                        `chain-id`, `start-height` and `unique-id`.
                        ''')
    parser.add_argument('--chain-id', type=str)
    parser.add_argument('--start-height', type=int)
    parser.add_argument('--unique-id', type=str)
    parser.add_argument('--host-type',
                        type=str,
                        choices=['all', 'nodes', 'traffic'],
                        default='all',
                        help='Type of hosts to select')
    parser.add_argument('--host-filter',
                        type=str,
                        help='Filter through the selected nodes using regex.')
    parser.add_argument('--select-partition',
                        action=PartitionSelectorParser,
                        type=str,
                        help='''
                        Input should be in the form of "i/n" where 0 < i <= n.
                        Or "i-j/n" where 0 < i <= j <= n.
                        Select a group of hosts based on the division provided.
                        For i/n, it will split the selected hosts into n groups and select the i-th group.
                        For i-j/n, it will split the selected hosts into n groups and select the group that contains the interval [i, j]. "1-4/4" will select the entire fleet.
                        Use this if you want to target just a partition of the hosts.'''
                       )

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    register_schedule_subcommands(subparsers)
    # register subcommands to root parser
    register_base_commands(subparsers)
    register_subcommands(subparsers)
    return parser


class ScheduleModeParser(Action):

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            mode, value = values.split(' ', 1)
            setattr(namespace, self.dest, ScheduleMode(mode=mode, value=value))
        except ValueError as e:
            parser.error(str(e))


def register_schedule_subcommands(subparsers):
    schedule_parser = subparsers.add_parser('schedule',
                                            help='Manage scheduled commands.')
    subparsers = schedule_parser.add_subparsers(
        title='subcommands',
        description='Manage scheduled commands.',
        help='additional help')
    cmd_subparsers = subparsers.add_parser(
        'cmd', help='Schedule commands to run in the future.')
    cmd_subparsers.add_argument(
        '--on',
        action=ScheduleModeParser,
        metavar="MODE VALUE",
        required=True,
        help=
        "Choose whether to schedule by calendar(date and time) or active(relative time). "
        "Format: 'MODE VALUE' where MODE is 'calendar' or 'active'. VALUE can be any valid time specification. "
        "Examples: 'calendar 2024-01-01 10:00:00' or 'active 1h 30m'. "
        "Check https://man.archlinux.org/man/systemd.time.7.en#PARSING_TIMESTAMPS "
        "and https://man.archlinux.org/man/systemd.time.7.en#PARSING_TIME_SPANS "
        "for the format of the values.",
    )
    cmd_subparsers.add_argument(
        '--schedule-id',
        type=str,
        help=
        'The id of the scheduled command. If not provided, random string will be generated.'
    )
    # Add all existing commands under 'cmd'
    cmd_subcommands = cmd_subparsers.add_subparsers(
        title='subcommands',
        description='valid subcommands',
        help='additional help',
        required=True)
    # register subcommands to schedule_subparsers
    register_subcommands(cmd_subcommands)

    list_subparsers = subparsers.add_parser('list',
                                            help='List all scheduled commands.')
    list_subparsers.add_argument(
        '--full',
        action='store_true',
        help='Show more details about the scheduled commands.')
    list_subparsers.set_defaults(func=list_scheduled_cmds)

    clear_subparsers = subparsers.add_parser(
        'clear', help='Clear all scheduled commands.')
    clear_subparsers.add_argument(
        '--filter',
        type=str,
        default='*',
        help='''Clear scheduled commands matching the regex.
    Use the ids provided by the list command including the ".timer" suffix.
    If not already, all values will be prefixed with 'mocknet-' to match the scheduled command name.
    If not provided, all scheduled commands will be cleared.''')
    clear_subparsers.set_defaults(func=clear_scheduled_cmds)


def register_base_commands(subparsers):
    init_parser = subparsers.add_parser('init',
                                        help='''
    Uploads the host scripts, installs the neard systemd unit and downloads the
    neard binaries on each of the nodes. Doesn't start initializing the test
    state, which is done with the `new-test` command.
    ''')
    init_parser.add_argument('--neard-binary-url', type=str)
    init_parser.add_argument('--neard-upgrade-binary-url', type=str)
    init_parser.set_defaults(func=init_cmd)

    hard_reset_parser = subparsers.add_parser(
        'hard-reset',
        help='''Stops neard and clears all test state on all nodes.''')
    hard_reset_parser.add_argument('--neard-binary-url', type=str)
    hard_reset_parser.add_argument('--neard-upgrade-binary-url', type=str)
    hard_reset_parser.add_argument('--yes', action='store_true')
    hard_reset_parser.set_defaults(func=hard_reset_cmd)

    new_test_parser = subparsers.add_parser('new-test',
                                            help='''
    Sets up new state from the prepared records and genesis files with the number
    of validators specified. This calls neard fork-network to create the new genesis
    and records files, and then starts the neard nodes and waits for them to be online
    after computing the genesis state roots. This step takes a long time (a few hours).
    ''')
    new_test_parser.add_argument('--state-source', type=str, default='dump')
    new_test_parser.add_argument('--patches-path', type=str)
    new_test_parser.add_argument('--epoch-length', type=int)
    new_test_parser.add_argument('--num-validators', type=int)
    new_test_parser.add_argument('--num-seats', type=int)
    new_test_parser.add_argument('--new-chain-id', type=str)
    new_test_parser.add_argument('--genesis-protocol-version', type=int)
    new_test_parser.add_argument(
        '--gcs-state-sync',
        action='store_true',
        help="""Enable state dumper nodes to sync state to GCS.""")
    new_test_parser.add_argument('--yes', action='store_true')
    new_test_parser.add_argument('--stake-distribution',
                                 type=str,
                                 default='static',
                                 choices=['static', 'mainnet'],
                                 help='''
                                 Stake distribution to use for the mocknet.
                                 ''')
    new_test_parser.set_defaults(func=new_test_cmd)

    status_parser = subparsers.add_parser(
        'status',
        help='''Checks the status of test initialization on each node''')
    status_parser.set_defaults(func=status_cmd)

    upload_file_parser = subparsers.add_parser('upload-file',
                                               help='''
        Upload a file or a directory on the hosts.
        Existing files are replaced.
        ''')
    upload_file_parser.add_argument('--src', type=str)
    upload_file_parser.add_argument('--dst', type=str)
    upload_file_parser.set_defaults(func=run_remote_upload_file)


def add_snapshot_parser(subparsers):
    snapshot_parser = subparsers.add_parser('snapshot', help='Manage snapshots')
    snapshot_parser.add_argument('--snapshot-id',
                                 type=str,
                                 help='Name of the snapshot')

    snapshot_group = snapshot_parser.add_mutually_exclusive_group()
    snapshot_group.add_argument('--create',
                                action='store_true',
                                help='Create a snapshot')
    snapshot_group.add_argument('--restore',
                                action='store_true',
                                help='Restore a snapshot')
    snapshot_group.add_argument('--list',
                                action='store_true',
                                help='List all snapshots')
    snapshot_group.add_argument('--delete',
                                action='store_true',
                                help='Delete a snapshot')

    snapshot_parser.set_defaults(func=snapshot_cmd)


def register_subcommands(subparsers):
    """
    This function registers the commands that can also be scheduled.
    Before adding a new command, make sure that is makes sense for it to be scheduled.
    Otherwise, just add it to the base commands.
    """
    update_config_parser = subparsers.add_parser(
        'update-config',
        help='''Update config.json with given flags for all nodes.''')
    update_config_parser.add_argument(
        '--set',
        help='''
        A key value pair to set in the config. The key will be interpreted as a
        json path to the config to be updated. The value will be parsed as json.   
        e.g.
        --set 'aaa.bbb.ccc=5'
        --set 'aaa.bbb.ccc="5"'
        --set 'aaa.bbb.ddd={"eee":6,"fff":"7"}' # no spaces!
        ''',
    )
    update_config_parser.set_defaults(func=update_config_cmd)

    start_traffic_parser = subparsers.add_parser(
        'start-traffic',
        help=
        'Starts all nodes and starts neard mirror run on the traffic generator.'
    )
    start_traffic_parser.add_argument(
        '--batch-interval-millis',
        type=int,
        help=
        '''Interval in millis between sending each mainnet block\'s worth of transactions.
        Without this flag, the traffic generator will try to match the per-block load on mainnet.
        So, transactions from consecutive mainnet blocks will be sent with delays
        between them such that they will probably appear in consecutive mocknet blocks.
        ''')
    start_traffic_parser.add_argument(
        '--force-restart',
        action='store_true',
        help=
        'Force restart the nodes. Applies to both traffic generator and other nodes.'
    )
    start_traffic_parser.set_defaults(func=start_traffic_cmd)

    start_nodes_parser = subparsers.add_parser(
        'start-nodes',
        help='Starts all nodes, but does not start the traffic generator.')
    start_nodes_parser.add_argument(
        '--binary-idx',
        type=int,
        help='''The index of the neard binary to start.
        If not provided, the current binary will be started.
        If the binary index is different from the current binary index, the nodes will be restarted with the binary at the given index.
        ''')
    start_nodes_parser.add_argument(
        '--force-restart',
        action='store_true',
        help='Force restart the nodes even if the binary index has not changed.'
    )
    start_nodes_parser.set_defaults(func=start_nodes_cmd)

    stop_parser = subparsers.add_parser('stop-nodes',
                                        help='kill all neard processes')
    stop_parser.set_defaults(func=stop_nodes_cmd)

    stop_parser = subparsers.add_parser(
        'stop-traffic',
        help='stop the traffic generator, but leave the other nodes running')
    stop_parser.set_defaults(func=stop_traffic_cmd)

    reset_parser = subparsers.add_parser('reset',
                                         help='''
    The new_test command saves the data directory after the genesis state roots are computed so that
    the test can be reset from the start without having to do that again. This command resets all nodes'
    data dirs to what was saved then, so that start-traffic will start the test all over again.
    ''')
    reset_parser.add_argument('--yes', action='store_true')
    reset_parser.set_defaults(func=reset_cmd)

    # It re-uses the same binary urls because it's quite easy to do it with the
    # nearcore-release buildkite and urls in the following format without commit
    # but only with the branch name:
    # https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/<branch-name>/neard"
    update_binaries_parser = subparsers.add_parser('update-binaries',
                                                   help='''
        Update the neard binaries by re-downloading them. The same urls are used.
        If you plan to restart the network multiple times, it is recommended to use
        URLs that only depend on the branch name. This way, every time you build,
        you will not need to amend the URL but just run update-binaries.''')
    update_binaries_parser.set_defaults(func=update_binaries_cmd)

    amend_binaries_parsers = subparsers.add_parser('amend-binaries',
                                                   help='''
        Replace the URL of the neard binary at the given index and download it again.'''
                                                  )

    amend_binaries_parsers.add_argument('--neard-binary-url',
                                        type=str,
                                        required=True,
                                        help='URL to the neard binary.')
    amend_binaries_parsers.add_argument('--binary-idx',
                                        type=int,
                                        required=True,
                                        help='''
        0 based indexing.
        The index in the binary list that you want to replace.
        The command fails if the index does not exist on the host.
        ''')
    amend_binaries_parsers.set_defaults(func=amend_binaries_cmd)

    run_cmd_parser = subparsers.add_parser('run-cmd',
                                           help='''Run the cmd on the hosts.''')
    run_cmd_parser.add_argument('--cmd', type=str)
    run_cmd_parser.set_defaults(func=run_remote_cmd)

    env_cmd_parser = subparsers.add_parser(
        'env', help='''Update the environment variable on the hosts.''')
    env_cmd_parser.add_argument('--clear-all', action='store_true')
    env_cmd_parser.add_argument('--key-value', type=str, nargs='+')
    env_cmd_parser.set_defaults(func=run_env_cmd)

    add_snapshot_parser(subparsers)


if __name__ == '__main__':
    # Manual, human-paced use: keep the shared ssh master connections alive
    # between invocations for longer than the scripted default.
    mocknet_ssh.set_default_ssh_control_persist('30m')
    parser = build_parser()
    args = parser.parse_args()
    ctx = CommandContext(args)
    args.func(ctx)

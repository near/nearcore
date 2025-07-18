#!/usr/bin/env python3
"""
Cli tool for managing the mocknet instances.
"""
from argparse import ArgumentParser, Action
import datetime
import pathlib
import json
import random
import shutil
from rc import pmap
import re
import sys
import time
import numpy as np
from functools import wraps
from typing import Optional

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import local_test_node
import node_config
import remote_node
from node_handle import NodeHandle
from utils import ScheduleContext


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
        self._set_nodes()
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

    def _set_nodes(self):
        """
        Get the nodes in the network.
        """
        if self.args.local_test:
            self._get_local_nodes()
        else:
            self._get_remote_nodes()

    def _get_local_nodes(self):
        """
        Get nodes in the local infrastructure.
        """
        if (self.args.chain_id is not None or
                self.args.start_height is not None or
                self.args.unique_id is not None or
                self.args.mocknet_id is not None):
            sys.exit(
                f'cannot give --chain-id, --start-height, --unique-id or --mocknet-id along with --local-test'
            )
            traffic_generator, nodes = local_test_node.get_nodes()
            node_config.configure_nodes(nodes + to_list(traffic_generator),
                                        node_config.TEST_CONFIG)
        self.traffic_generator = traffic_generator
        self.nodes = nodes

    def _get_remote_nodes(self):
        """
        Get nodes in the remote infrastructure.
        """
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
        traffic_generator, nodes = remote_node.get_nodes(mocknet_id)
        node_config.configure_nodes(nodes + to_list(traffic_generator),
                                    node_config.REMOTE_CONFIG)
        self.traffic_generator = traffic_generator
        self.nodes = nodes

    def _filter_hosts(self):
        """
        Select the affected hosts.
        traffic_generator can become None,
        nodes list can become empty
        """
        # Only keep nodes that want a neard runner not the auxiliary nodes i.e. tracers.
        self.nodes = [node for node in self.nodes if node.want_neard_runner]

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
            i, n = self.args.select_partition

            if len(self.nodes) < n and self.traffic_generator == None:
                logger.error(
                    f'Partitioning {len(self.nodes)} nodes in {n} groups will result in empty groups.'
                )
                exit(1)
            self.nodes.sort(key=lambda node: node.name())
            self.nodes = np.array_split(self.nodes, n)[i - 1]

    def _make_schedule_context(self) -> Optional[ScheduleContext]:
        """
        Make a schedule context if the command is scheduled.
        """
        if getattr(self.args, 'schedule_in', None) is None:
            return None

        context = ScheduleContext(
            id=getattr(self.args, 'schedule_id', None),
            time_spec=self.args.schedule_in,
        )
        return context


def init_neard_runners(ctx: CommandContext, remove_home_dir=False):
    args = ctx.args
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator
    prompt_init_flags(args)
    if args.neard_upgrade_binary_url is None or args.neard_upgrade_binary_url == '':
        configs = [{
            "is_traffic_generator": False,
            "binaries": [{
                "url": args.neard_binary_url,
                "epoch_height": 0
            }]
        }] * len(nodes)
        traffic_generator_config = {
            "is_traffic_generator": True,
            "binaries": [{
                "url": args.neard_binary_url,
                "epoch_height": 0
            }]
        }
    else:
        # for now this test starts all validators with the same stake, so just make the upgrade
        # epoch random. If we change the stakes, we should change this to choose how much stake
        # we want to upgrade during each epoch
        configs = []
        for i in range(len(nodes)):
            configs.append({
                "is_traffic_generator":
                    False,
                "binaries": [{
                    "url": args.neard_binary_url,
                    "epoch_height": 0
                }, {
                    "url": args.neard_upgrade_binary_url,
                    "epoch_height": random.randint(1, 4)
                }]
            })
        traffic_generator_config = {
            "is_traffic_generator":
                True,
            "binaries": [{
                "url": args.neard_upgrade_binary_url,
                "epoch_height": 0
            }]
        }

    if traffic_generator is not None:
        traffic_generator.init_neard_runner(traffic_generator_config,
                                            remove_home_dir)
    pmap(lambda x: x[0].init_neard_runner(x[1], remove_home_dir),
         zip(nodes, configs))


def init_cmd(ctx: CommandContext):
    init_neard_runners(ctx, remove_home_dir=False)


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
    init_neard_runners(ctx, remove_home_dir=True)
    _clear_state_parts_if_exists(_get_state_parts_location(ctx.args), ctx.nodes)


def restart_cmd(ctx: CommandContext):
    targeted = ctx.get_targeted()
    pmap(lambda node: node.stop_neard_runner(), targeted)
    if ctx.args.upload_program:
        pmap(lambda node: node.upload_neard_runner(), targeted)
    pmap(lambda node: node.start_neard_runner(), targeted)


def stop_runner_cmd(ctx: CommandContext):
    targeted = ctx.get_targeted()
    pmap(lambda node: node.stop_neard_runner(), targeted)


# returns boot nodes and validators we want for the new test network
def get_network_nodes(new_test_rpc_responses, num_validators):
    validators = []
    non_validators = []
    boot_nodes = []
    for node, response in new_test_rpc_responses:
        if len(validators) < num_validators:
            if node.can_validate and response[
                    'validator_account_id'] is not None:
                # we assume here that validator_account_id is not null, validator_public_key
                # better not be null either
                validators.append({
                    'account_id': response['validator_account_id'],
                    'public_key': response['validator_public_key'],
                    'amount': str(10**33),
                })
        else:
            non_validators.append(node.ip_addr())
        if len(boot_nodes) < 20:
            boot_nodes.append(
                f'{response["node_key"]}@{node.ip_addr()}:{response["listen_port"]}'
            )

        if len(validators) >= num_validators and len(boot_nodes) >= 20:
            break
    # neither of these should happen, since we check the number of available nodes in new_test(), and
    # only the traffic generator will respond with null validator_account_id and validator_public_key
    if len(validators) == 0:
        sys.exit('no validators available after new_test RPCs')
    if len(validators) < num_validators:
        logger.warning(
            f'wanted {num_validators} validators, but only {len(validators)} available'
        )
    return validators, boot_nodes


def new_genesis_timestamp(node):
    version = node.neard_runner_version()
    err = version.get('error')
    if err is not None:
        if err['code'] != -32601:
            sys.exit(
                f'bad response calling version RPC on {node.name()}: {err}')
        return None
    genesis_time = None
    result = version.get('result')
    if result is not None:
        if result.get('node_setup_version') == '1':
            genesis_time = datetime.datetime.now(
                tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    return genesis_time


def _apply_stateless_config(args, node):
    """Applies configuration changes to the node for stateless validation,
    including changing config.json file and updating TCP buffer size at OS level."""
    # TODO: it should be possible to update multiple keys in one RPC call so we dont have to make multiple round trips
    # TODO: Enable saving witness after fixing the performance problems.
    do_update_config(node, 'save_latest_witnesses=false')
    if not node.want_state_dump:
        do_update_config(node, 'tracked_shards=[]')
        do_update_config(node, 'store.load_mem_tries_for_tracked_shards=true')
    if not args.local_test:
        node.run_cmd(
            "sudo sysctl -w net.core.rmem_max=8388608 && sudo sysctl -w net.core.wmem_max=8388608 && sudo sysctl -w net.ipv4.tcp_rmem='4096 87380 8388608' && sudo sysctl -w net.ipv4.tcp_wmem='4096 16384 8388608' && sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0"
        )


def _apply_config_changes(node, state_sync_location):
    if state_sync_location is None:
        changes = {'state_sync_enabled': False}
    else:
        changes = {
            'state_sync.sync': {
                'ExternalStorage': {
                    'location': state_sync_location
                }
            }
        }
        if node.want_state_dump:
            changes['state_sync.dump.location'] = state_sync_location
            # TODO: Change this to Enabled once we remove support the for EveryEpoch alias.
            changes[
                'store.state_snapshot_config.state_snapshot_type'] = "EveryEpoch"
    for key, change in changes.items():
        do_update_config(node, f'{key}={json.dumps(change)}')


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

    if location.get('Filesystem') is not None:
        root_dir = location['Filesystem']['root_dir']
        shutil.rmtree(root_dir)
        return

    # For GCS-based state sync, looks for the state dumper and clears the GCP
    # bucket where it dumped the parts.
    bucket_name = location['GCS']['bucket']

    state_dumper_node.run_cmd(f'gsutil -m rm -r gs://{bucket_name}/chain_id=*',
                              return_on_fail=True)


def _get_state_parts_bucket_name(args):
    return f'near-state-dumper-mocknet-{args.chain_id}-{args.start_height}-{args.unique_id}'


def _get_state_parts_location(args):
    if args.local_test:
        return {
            "Filesystem": {
                "root_dir":
                    str(local_test_node.DEFAULT_LOCAL_MOCKNET_DIR /
                        'state-parts')
            }
        }
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

    ref_node = traffic_generator if traffic_generator else nodes[0]
    genesis_time = new_genesis_timestamp(ref_node)

    targeted = nodes + to_list(traffic_generator)

    logger.info(f'resetting/initializing home dirs')
    test_keys = pmap(lambda node: node.neard_runner_new_test(), targeted)

    validators, boot_nodes = get_network_nodes(zip(nodes, test_keys),
                                               args.num_validators)
    logger.info("""Setting validators: {0}
Run `status` to check if the nodes are ready. After they're ready,
 you can run `start-nodes` and `start-traffic`""".format(validators))

    pmap(
        lambda node: node.neard_runner_network_init(
            validators,
            boot_nodes,
            args.state_source,
            args.patches_path,
            args.epoch_length,
            args.num_seats,
            args.new_chain_id,
            args.genesis_protocol_version,
            genesis_time=genesis_time,
        ), targeted)

    location = None
    if args.gcs_state_sync:
        location = _get_state_parts_location(args)
    logger.info('Applying default config changes')
    pmap(lambda node: _apply_config_changes(node, location), targeted)
    if args.stateless_setup:
        logger.info('Configuring nodes for stateless protocol')
        pmap(lambda node: _apply_stateless_config(args, node), nodes)

    _clear_state_parts_if_exists(location, nodes)


def get_nodes_status(nodes: list[NodeHandle]) -> list[str]:
    statuses = pmap(lambda node: node.neard_runner_ready(), nodes)
    not_ready_nodes = []
    for ready, node in zip(statuses, nodes):
        if not ready:
            not_ready_nodes.append(node.name())
    return not_ready_nodes


def status_cmd(ctx: CommandContext):
    nodes = ctx.get_targeted()
    not_ready_nodes = get_nodes_status(nodes)
    if len(not_ready_nodes) == 0:
        print(f'all {len(nodes)} nodes ready')
    else:
        print(
            f'{len(nodes)-len(not_ready_nodes)}/{len(nodes)} ready. Nodes not ready: {not_ready_nodes[:3]}'
        )


def reset_cmd(ctx: CommandContext):
    args = ctx.args
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator

    if not args.yes:
        print(
            'this will reset selected nodes\' home dirs to their initial states right after test initialization finished. continue? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()
    if args.backup_id is None:
        ref_node = traffic_generator if traffic_generator else nodes[0]
        backups = ref_node.neard_runner_ls_backups()
        backups_msg = 'ID |  Time  | Description\n'
        if 'start' not in backups:
            backups_msg += 'start | None | initial test state after state root computation\n'
        for backup_id, backup_data in backups.items():
            backups_msg += f'{backup_id} | {backup_data.get("time")} | {backup_data.get("description")}\n'

        print(f'Backups as reported by {ref_node.name()}):\n\n{backups_msg}')
        print('please enter a backup ID here:')
        args.backup_id = sys.stdin.readline().strip()
        if args.backup_id != 'start' and args.backup_id not in backups:
            print(
                f'Given backup ID ({args.backup_id}) was not in the list given')
            sys.exit()

    pmap(lambda node: node.neard_runner_reset(backup_id=args.backup_id),
         ctx.get_targeted_with_schedule_ctx())
    logger.info(
        'Data dir reset in progress. Run the `status` command to see when this is finished. Until it is finished, neard runners may not respond to HTTP requests.'
    )
    # Do not clear state parts if scheduling
    # NOTE: This may be a problem if you want to schedule the reset command on a dumper node
    #       because the dumper node will start dumping the current epoch after reset.
    if not ctx.is_scheduled():
        _clear_state_parts_if_exists(_get_state_parts_location(args), nodes)


def make_backup_cmd(ctx: CommandContext):
    args = ctx.args
    if not args.yes:
        print(
            'this will stop all nodes and create a new backup of their home dirs. continue? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()

    if args.backup_id is None:
        print('please enter a backup ID:')
        args.backup_id = sys.stdin.readline().strip()
        if re.match(r'^[0-9a-zA-Z.][0-9a-zA-Z_\-.]+$', args.backup_id) is None:
            sys.exit('invalid backup ID')
        if args.description is None:
            print('please enter a description (enter nothing to skip):')
            description = sys.stdin.readline().strip()
            if len(description) > 0:
                args.description = description

    pmap(
        lambda node: node.neard_runner_make_backup(
            backup_id=args.backup_id, description=args.description),
        ctx.get_targeted_with_schedule_ctx())


def stop_nodes_cmd(ctx: CommandContext):
    pmap(lambda node: node.neard_runner_stop(),
         ctx.get_targeted_with_schedule_ctx())


def stop_traffic_cmd(ctx: CommandContext):
    ctx.traffic_generator.with_schedule_ctx(
        ctx.schedule_ctx).neard_runner_stop()


def do_update_config(node, config_change):
    result = node.neard_update_config(config_change)
    if not result:
        logger.warning(
            f'failed updating config on {node.name()}. result: {result}')


def update_config_cmd(ctx: CommandContext):
    pmap(
        lambda node: do_update_config(node, ctx.args.set),
        ctx.get_targeted_with_schedule_ctx(),
    )


def start_nodes_cmd(ctx: CommandContext):
    nodes = ctx.nodes
    if not all(pmap(lambda node: node.neard_runner_ready(), nodes)):
        logger.warning(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(
        lambda node: node.with_schedule_ctx(ctx.schedule_ctx).
        neard_runner_start(), nodes)
    # Wait for the nodes to be up if not scheduling
    if not ctx.is_scheduled():
        pmap(lambda node: node.wait_node_up(), nodes)


def start_traffic_cmd(ctx: CommandContext):
    nodes = ctx.nodes
    traffic_generator = ctx.traffic_generator

    if traffic_generator is None:
        logger.warning('No traffic node selected. Change filters.')
        return
    if not all(pmap(lambda node: node.neard_runner_ready(),
                    ctx.get_targeted())):
        logger.warning(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(
        lambda node: node.with_schedule_ctx(ctx.schedule_ctx).
        neard_runner_start(), nodes)
    # Wait for the nodes to be up if not scheduling
    if not ctx.is_scheduled():
        logger.info("waiting for validators to be up")
        pmap(lambda node: node.wait_node_up(), nodes)
        logger.info(
            "waiting a bit after validators started before starting traffic")
        time.sleep(10)
    # TODO: maybe add 20 seconds delay to the schedule command to allow the other nodes to start
    traffic_generator.with_schedule_ctx(ctx.schedule_ctx).neard_runner_start(
        batch_interval_millis=ctx.args.batch_interval_millis)
    if not ctx.is_scheduled():
        logger.info(
            f'test running. to check the traffic sent, try running "curl --silent http://{traffic_generator.ip_addr()}:{traffic_generator.neard_port()}/metrics | grep near_mirror"'
        )


def update_binaries_cmd(ctx: CommandContext):
    pmap(lambda node: node.neard_runner_update_binaries(),
         ctx.get_targeted_with_schedule_ctx())


def amend_binaries_cmd(ctx: CommandContext):
    args = ctx.args
    pmap(
        lambda node: node.neard_runner_update_binaries(
            args.neard_binary_url, args.epoch_height, args.binary_idx),
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


def run_remote_download_file(ctx: CommandContext):
    targeted = ctx.get_targeted()
    logger.info(
        f'Downloading {ctx.args.src} to {ctx.args.dst} on {",".join([h.name() for h in targeted])}'
    )
    pmap(lambda node: print_result(
        node, node.download_file(ctx.args.src, ctx.args.dst)),
         targeted,
         on_exception="")


def run_env_cmd(ctx: CommandContext):
    if ctx.args.clear_all:
        func = lambda node: node.neard_clear_env()
    else:
        func = lambda node: node.neard_update_env(ctx.args.key_value)
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
    cmd = f'systemctl --user stop "{filter}"'
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


class ParseFraction(Action):

    def __call__(self, parser, namespace, values, option_string=None):
        pattern = r"(\d+)/(\d+)"
        match = re.match(pattern, values)
        if not match:
            parser.error(f"Invalid input '{values}'. Expected format 'i/n'.")
        numerator = int(match.group(1))
        denominator = int(match.group(2))
        setattr(namespace, self.dest, (numerator, denominator))


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
    parser.add_argument('--local-test', action='store_true')
    parser.add_argument('--host-type',
                        type=str,
                        choices=['all', 'nodes', 'traffic'],
                        default='all',
                        help='Type of hosts to select')
    parser.add_argument('--host-filter',
                        type=str,
                        help='Filter through the selected nodes using regex.')
    parser.add_argument('--select-partition',
                        action=ParseFraction,
                        type=str,
                        help='''
                        Input should be in the form of "i/n" where 0 < i <= n.
                        Select a group of hosts based on the division provided.
                        For i/n, it will split the selected hosts into n groups and select the i-th group.
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
        '--schedule-in',
        required=True,
        type=str,
        help=
        'Schedule the command to run after the specified time. Can be in the format of "10s", "10m", "10h", "10d"'
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
    init_parser = subparsers.add_parser('init-neard-runner',
                                        help='''
    Sets up the helper servers on each of the nodes. Doesn't start initializing the test
    state, which is done with the `new-test` command.
    ''')
    init_parser.add_argument('--neard-binary-url', type=str)
    init_parser.add_argument('--neard-upgrade-binary-url', type=str)
    init_parser.set_defaults(func=init_cmd)

    restart_parser = subparsers.add_parser(
        'restart-neard-runner',
        help='''Restarts the neard runner on all nodes.''')
    restart_parser.add_argument('--upload-program', action='store_true')
    restart_parser.set_defaults(func=restart_cmd, upload_program=False)

    stop_runner_parser = subparsers.add_parser(
        'stop-neard-runner', help='''Stops the neard runner on all nodes.''')
    stop_runner_parser.set_defaults(func=stop_runner_cmd)

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
    of validators specified. This calls neard amend-genesis to create the new genesis
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
    new_test_parser.add_argument('--stateless-setup', action='store_true')
    new_test_parser.add_argument(
        '--gcs-state-sync',
        action='store_true',
        help=
        """Enable state dumper nodes to sync state to GCS. On localnet, it will dump locally."""
    )
    new_test_parser.add_argument('--yes', action='store_true')
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
    start_traffic_parser.set_defaults(func=start_traffic_cmd)

    start_nodes_parser = subparsers.add_parser(
        'start-nodes',
        help='Starts all nodes, but does not start the traffic generator.')
    start_nodes_parser.set_defaults(func=start_nodes_cmd)

    stop_parser = subparsers.add_parser('stop-nodes',
                                        help='kill all neard processes')
    stop_parser.set_defaults(func=stop_nodes_cmd)

    stop_parser = subparsers.add_parser(
        'stop-traffic',
        help='stop the traffic generator, but leave the other nodes running')
    stop_parser.set_defaults(func=stop_traffic_cmd)

    backup_parser = subparsers.add_parser('make-backup',
                                          help='''
    Stops all nodes and haves them make a backup of the data dir that can later be restored to with the reset command
    ''')
    backup_parser.add_argument('--yes', action='store_true')
    backup_parser.add_argument('--backup-id', type=str)
    backup_parser.add_argument('--description', type=str)
    backup_parser.set_defaults(func=make_backup_cmd)

    reset_parser = subparsers.add_parser('reset',
                                         help='''
    The new_test command saves the data directory after the genesis state roots are computed so that
    the test can be reset from the start without having to do that again. This command resets all nodes'
    data dirs to what was saved then, so that start-traffic will start the test all over again.
    ''')
    reset_parser.add_argument('--yes', action='store_true')
    reset_parser.add_argument('--backup-id', type=str)
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
        Add or override the neard URLs by specifying the epoch height or index if you have multiple binaries.

        If the network was started with 2 binaries, the epoch height for the second binary can be randomly assigned
        on each host. Use caution when updating --epoch-height so that it will not add a binary in between the upgrade
        window for another binary.''')

    amend_binaries_parsers.add_argument('--neard-binary-url',
                                        type=str,
                                        required=True,
                                        help='URL to the neard binary.')
    group = amend_binaries_parsers.add_mutually_exclusive_group(required=True)
    group.add_argument('--epoch-height',
                       type=int,
                       help='''
        The epoch height where this binary will begin to run.
        If a binary already exists on the host for this epoch height, the old one will be replaced.
        Otherwise a new binary will be added with this epoch height.
        ''')
    group.add_argument('--binary-idx',
                       type=int,
                       help='''
        0 based indexing.
        The index in the binary list that you want to replace.
        If the index does not exist on the host this operation will not do anything.
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
    parser = build_parser()
    args = parser.parse_args()
    ctx = CommandContext(args)
    args.func(ctx)

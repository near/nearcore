#!/usr/bin/env python3
"""

"""
from argparse import ArgumentParser, BooleanOptionalAction
import cmd_utils
import pathlib
import json
import random
from rc import pmap, run
import requests
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import mocknet

from configured_logger import logger


def get_nodes(args):
    pattern = args.chain_id + '-' + str(
        args.start_height) + '-' + args.unique_id
    all_nodes = mocknet.get_nodes(pattern=pattern)
    if len(all_nodes) < 1:
        sys.exit(f'no known nodes matching {pattern}')

    traffic_generator = None
    nodes = []
    for n in all_nodes:
        if n.instance_name.endswith('traffic'):
            if traffic_generator is not None:
                sys.exit(
                    f'more than one traffic generator instance found. {traffic_generator.instance_name} and {n.instance_name}'
                )
            traffic_generator = n
        else:
            nodes.append(n)

    if traffic_generator is None:
        sys.exit(f'no traffic generator instance found')
    return traffic_generator, nodes


def wait_node_up(node):
    while True:
        try:
            res = node.get_validators()
            if 'error' not in res:
                assert 'result' in res
                logger.info(f'Node {node.instance_name} is up')
                return
        except (ConnectionRefusedError,
                requests.exceptions.ConnectionError) as e:
            pass
        time.sleep(10)


def prompt_setup_flags(args):
    if not args.yes:
        print(
            'this will reset all nodes\' home dirs and initialize them with new state. continue? [yes/no]'
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
        print('number of block producer seats?: ')
        args.num_seats = int(sys.stdin.readline().strip())

    if args.genesis_protocol_version is None:
        print('genesis protocol version?: ')
        args.genesis_protocol_version = int(sys.stdin.readline().strip())


def start_neard_runner(node):
    cmd_utils.run_in_background(node, f'/home/ubuntu/neard-runner/venv/bin/python /home/ubuntu/neard-runner/neard_runner.py ' \
        '--home /home/ubuntu/neard-runner --neard-home /home/ubuntu/.near ' \
        '--neard-logs /home/ubuntu/neard-logs --port 3000', 'neard-runner.txt')


def upload_neard_runner(node):
    node.machine.upload('tests/mocknet/helpers/neard_runner.py',
                        '/home/ubuntu/neard-runner',
                        switch_user='ubuntu')
    node.machine.upload('tests/mocknet/helpers/requirements.txt',
                        '/home/ubuntu/neard-runner',
                        switch_user='ubuntu')


def init_neard_runner(node, config, remove_home_dir=False):
    stop_neard_runner(node)
    cmd_utils.init_node(node)
    if remove_home_dir:
        cmd_utils.run_cmd(
            node,
            'rm -rf /home/ubuntu/neard-runner && mkdir -p /home/ubuntu/neard-runner'
        )
    else:
        cmd_utils.run_cmd(node, 'mkdir -p /home/ubuntu/neard-runner')
    upload_neard_runner(node)
    mocknet.upload_json(node, '/home/ubuntu/neard-runner/config.json', config)
    cmd = 'cd /home/ubuntu/neard-runner && python3 -m virtualenv venv -p $(which python3)' \
    ' && ./venv/bin/pip install -r requirements.txt'
    cmd_utils.run_cmd(node, cmd)
    start_neard_runner(node)


def stop_neard_runner(node):
    # it's probably fine for now, but this is very heavy handed/not precise
    node.machine.run('kill $(ps -C python -o pid=)')


def prompt_init_flags(args):
    if args.neard_binary_url is None:
        print('neard binary URL?: ')
        args.neard_binary_url = sys.stdin.readline().strip()
        assert len(args.neard_binary_url) > 0

    if args.neard_upgrade_binary_url is None:
        print(
            'add a second neard binary URL to upgrade to mid-test? enter nothing here to skip: '
        )
        url = sys.stdin.readline().strip()
        if len(url) > 0:
            args.neard_upgrade_binary_url = url


def init_neard_runners(args, traffic_generator, nodes, remove_home_dir=False):
    prompt_init_flags(args)
    if args.neard_upgrade_binary_url is None:
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

    init_neard_runner(traffic_generator, traffic_generator_config,
                      remove_home_dir)
    pmap(lambda x: init_neard_runner(x[0], x[1], remove_home_dir),
         zip(nodes, configs))


def init_cmd(args, traffic_generator, nodes):
    init_neard_runners(args, traffic_generator, nodes, remove_home_dir=False)


def hard_reset_cmd(args, traffic_generator, nodes):
    print("""
        WARNING!!!!
        WARNING!!!!
        This will undo all chain state, which will force a restart from the beginning,
        icluding the genesis state computation which takes several hours.
        Continue? [yes/no]""")
    if sys.stdin.readline().strip() != 'yes':
        return
    all_nodes = nodes + [traffic_generator]
    pmap(stop_neard_runner, all_nodes)
    mocknet.stop_nodes(all_nodes)
    init_neard_runners(args, traffic_generator, nodes, remove_home_dir=True)


def restart_cmd(args, traffic_generator, nodes):
    all_nodes = nodes + [traffic_generator]
    pmap(stop_neard_runner, all_nodes)
    if args.upload_program:
        pmap(upload_neard_runner, all_nodes)
    pmap(start_neard_runner, all_nodes)


# returns boot nodes and validators we want for the new test network
def get_network_nodes(new_test_rpc_responses, num_validators):
    validators = []
    boot_nodes = []
    for ip_addr, response in new_test_rpc_responses:
        if len(validators) < num_validators:
            if response['validator_account_id'] is not None:
                # we assume here that validator_account_id is not null, validator_public_key
                # better not be null either
                validators.append({
                    'account_id': response['validator_account_id'],
                    'public_key': response['validator_public_key'],
                    'amount': str(10**33),
                })
        if len(boot_nodes) < 20:
            boot_nodes.append(
                f'{response["node_key"]}@{ip_addr}:{response["listen_port"]}')

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


def new_test(args, traffic_generator, nodes):
    prompt_setup_flags(args)

    if args.epoch_length <= 0:
        sys.exit(f'--epoch-length should be positive')
    if args.num_validators <= 0:
        sys.exit(f'--num-validators should be positive')
    if len(nodes) < args.num_validators:
        sys.exit(
            f'--num-validators is {args.num_validators} but only found {len(nodes)} under test'
        )

    all_nodes = nodes + [traffic_generator]

    logger.info(f'resetting/initializing home dirs')
    test_keys = pmap(neard_runner_new_test, all_nodes)

    validators, boot_nodes = get_network_nodes(
        zip([n.machine.ip for n in all_nodes], test_keys), args.num_validators)

    logger.info("""setting validators: {0}
Then running neard amend-genesis on all nodes, and starting neard to compute genesis \
state roots. This will take a few hours. Run `status` to check if the nodes are \
ready. After they're ready, you can run `start-traffic`""".format(validators))
    pmap(
        lambda node: neard_runner_network_init(
            node, validators, boot_nodes, args.epoch_length, args.num_seats,
            args.genesis_protocol_version), all_nodes)


def status_cmd(args, traffic_generator, nodes):
    all_nodes = nodes + [traffic_generator]
    statuses = pmap(neard_runner_ready, all_nodes)
    num_ready = 0
    not_ready = []
    for ready, node in zip(statuses, all_nodes):
        if not ready:
            not_ready.append(node.instance_name)

    if len(not_ready) == 0:
        print(f'all {len(all_nodes)} nodes ready')
    else:
        print(
            f'{len(all_nodes)-len(not_ready)}/{len(all_nodes)} ready. Nodes not ready: {not_ready[:3]}'
        )


def reset_cmd(args, traffic_generator, nodes):
    if not args.yes:
        print(
            'this will reset all nodes\' home dirs to their initial states right after test initialization finished. continue? [yes/no]'
        )
        if sys.stdin.readline().strip() != 'yes':
            sys.exit()
    all_nodes = nodes + [traffic_generator]
    pmap(neard_runner_reset, all_nodes)
    logger.info(
        'Data dir reset in progress. Run the `status` command to see when this is finished. Until it is finished, neard runners may not respond to HTTP requests.'
    )


def stop_nodes_cmd(args, traffic_generator, nodes):
    pmap(neard_runner_stop, nodes + [traffic_generator])


def stop_traffic_cmd(args, traffic_generator, nodes):
    neard_runner_stop(traffic_generator)


def neard_runner_jsonrpc(node, method, params=[]):
    body = {
        'method': method,
        'params': params,
        'id': 'dontcare',
        'jsonrpc': '2.0'
    }
    body = json.dumps(body)
    # '"'"' will be interpreted as ending the first quote and then concatenating it with "'",
    # followed by a new quote started with ' and the rest of the string, to get any single quotes
    # in method or params into the command correctly
    body = body.replace("'", "'\"'\"'")
    r = cmd_utils.run_cmd(node, f'curl localhost:3000 -d \'{body}\'')
    response = json.loads(r.stdout)
    if 'error' in response:
        # TODO: errors should be handled better here in general but just exit for now
        sys.exit(
            f'bad response trying to send {method} JSON RPC to neard runner on {node.instance_name}:\n{response}'
        )
    return response['result']


def neard_runner_start(node):
    neard_runner_jsonrpc(node, 'start')


def neard_runner_stop(node):
    neard_runner_jsonrpc(node, 'stop')


def neard_runner_new_test(node):
    return neard_runner_jsonrpc(node, 'new_test')


def neard_runner_network_init(node, validators, boot_nodes, epoch_length,
                              num_seats, protocol_version):
    return neard_runner_jsonrpc(node,
                                'network_init',
                                params={
                                    'validators': validators,
                                    'boot_nodes': boot_nodes,
                                    'epoch_length': epoch_length,
                                    'num_seats': num_seats,
                                    'protocol_version': protocol_version,
                                })


def neard_update_config(node, key_value):
    return neard_runner_jsonrpc(
        node,
        'update_config',
        params={
            "key_value": key_value,
        },
    )


def update_config_cmd(args, traffic_generator, nodes):
    nodes = nodes + [traffic_generator]
    results = pmap(
        lambda node: neard_update_config(
            node,
            args.set,
        ),
        nodes,
    )
    if not all(results):
        logger.warn('failed to update configs for some nodes')
        return


def neard_runner_ready(node):
    return neard_runner_jsonrpc(node, 'ready')


def neard_runner_reset(node):
    return neard_runner_jsonrpc(node, 'reset')


def start_nodes_cmd(args, traffic_generator, nodes):
    if not all(pmap(neard_runner_ready, nodes)):
        logger.warn(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(neard_runner_start, nodes)
    pmap(wait_node_up, nodes)


def start_traffic_cmd(args, traffic_generator, nodes):
    if not all(pmap(neard_runner_ready, nodes + [traffic_generator])):
        logger.warn(
            'not all nodes are ready to start yet. Run the `status` command to check their statuses'
        )
        return
    pmap(neard_runner_start, nodes)
    logger.info("waiting for validators to be up")
    pmap(wait_node_up, nodes)
    logger.info(
        "waiting a bit after validators started before starting traffic")
    time.sleep(10)
    neard_runner_start(traffic_generator)
    logger.info(
        f'test running. to check the traffic sent, try running "curl http://{traffic_generator.machine.ip}:3030/metrics | grep mirror"'
    )


def neard_runner_update_binaries(node):
    neard_runner_jsonrpc(node, 'update_binaries')


def update_binaries_cmd(args, traffic_generator, nodes):
    pmap(neard_runner_update_binaries, nodes + [traffic_generator])


if __name__ == '__main__':
    parser = ArgumentParser(description='Run a load test')
    parser.add_argument('--chain-id', type=str, required=True)
    parser.add_argument('--start-height', type=int, required=True)
    parser.add_argument('--unique-id', type=str, required=True)

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    init_parser = subparsers.add_parser('init-neard-runner',
                                        help='''
    Sets up the helper servers on each of the nodes. Doesn't start initializing the test
    state, which is done with the `new-test` command.
    ''')
    init_parser.add_argument('--neard-binary-url', type=str)
    init_parser.add_argument('--neard-upgrade-binary-url', type=str)
    init_parser.set_defaults(func=init_cmd)

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

    restart_parser = subparsers.add_parser(
        'restart-neard-runner',
        help='''Restarts the neard runner on all nodes.''')
    restart_parser.add_argument('--upload-program', action='store_true')
    restart_parser.set_defaults(func=restart_cmd, upload_program=False)

    hard_reset_parser = subparsers.add_parser(
        'hard-reset',
        help='''Stops neard and clears all test state on all nodes.''')
    hard_reset_parser.add_argument('--neard-binary-url', type=str)
    hard_reset_parser.add_argument('--neard-upgrade-binary-url', type=str)
    hard_reset_parser.set_defaults(func=hard_reset_cmd)

    new_test_parser = subparsers.add_parser('new-test',
                                            help='''
    Sets up new state from the prepared records and genesis files with the number
    of validators specified. This calls neard amend-genesis to create the new genesis
    and records files, and then starts the neard nodes and waits for them to be online
    after computing the genesis state roots. This step takes a long time (a few hours).
    ''')
    new_test_parser.add_argument('--epoch-length', type=int)
    new_test_parser.add_argument('--num-validators', type=int)
    new_test_parser.add_argument('--num-seats', type=int)
    new_test_parser.add_argument('--genesis-protocol-version', type=int)
    new_test_parser.add_argument('--yes', action='store_true')
    new_test_parser.set_defaults(func=new_test)

    status_parser = subparsers.add_parser(
        'status',
        help='''Checks the status of test initialization on each node''')
    status_parser.set_defaults(func=status_cmd)

    start_traffic_parser = subparsers.add_parser(
        'start-traffic',
        help=
        'Starts all nodes and starts neard mirror run on the traffic generator.'
    )
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
    update_binaries_parser = subparsers.add_parser(
        'update-binaries',
        help=
        'Update the neard binaries by re-downloading them. The same urls are used.'
    )
    update_binaries_parser.set_defaults(func=update_binaries_cmd)

    args = parser.parse_args()

    traffic_generator, nodes = get_nodes(args)
    args.func(args, traffic_generator, nodes)

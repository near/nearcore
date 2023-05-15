#!/usr/bin/env python3
"""

"""
import argparse
import pathlib
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


def get_node_peer_info(node):
    config = mocknet.download_and_read_json(node,
                                            '/home/ubuntu/.near/config.json')
    node_key = mocknet.download_and_read_json(
        node, '/home/ubuntu/.near/node_key.json')

    key = node_key['public_key']
    port = config['network']['addr'].split(':')[1]
    return f'{key}@{node.machine.ip}:{port}'


def get_boot_nodes(nodes):
    boot_nodes = pmap(get_node_peer_info, nodes[:20])
    return ','.join(boot_nodes)


def get_validator_list(nodes):
    validators = []

    validator_keys = pmap(
        lambda node: mocknet.download_and_read_json(
            node, '/home/ubuntu/.near/validator_key.json'), nodes)

    for i, validator_key in enumerate(validator_keys):
        validators.append({
            'account_id': validator_key['account_id'],
            'public_key': validator_key['public_key'],
            # TODO: give a way to specify the stakes
            'amount': str(10**33),
        })

    return validators


def run_cmd(node, cmd):
    r = node.machine.run(cmd)
    if r.exitcode != 0:
        sys.exit(
            f'failed running {cmd} on {node.instance_name}:\nstdout: {r.stdout}\nstderr: {r.stderr}'
        )
    return r


LOG_DIR = '/home/ubuntu/.near/logs'
STATUS_DIR = '/home/ubuntu/.near/logs/status'


def run_in_background(node, cmd, log_filename, env=''):
    setup_cmd = f'truncate --size 0 {STATUS_DIR}/{log_filename} '
    setup_cmd += f'&& for i in {{8..0}}; do if [ -f {LOG_DIR}/{log_filename}.$i ]; then mv {LOG_DIR}/{log_filename}.$i {LOG_DIR}/{log_filename}.$((i+1)); fi done'
    run_cmd(
        node,
        f'( {setup_cmd} && {env} nohup {cmd} > {LOG_DIR}/{log_filename}.0 2>&1; nohup echo "$?" ) > {STATUS_DIR}/{log_filename} 2>&1 &'
    )


def wait_process(node, log_filename):
    r = run_cmd(
        node,
        f'stat {STATUS_DIR}/{log_filename} >/dev/null && tail --retry -f {STATUS_DIR}/{log_filename} | head -n 1'
    )
    if r.stdout.strip() != '0':
        sys.exit(
            f'bad status in {STATUS_DIR}/{log_filename} on {node.instance_name}: {r.stdout}\ncheck {LOG_DIR}/{log_filename} for details'
        )


def check_process(node, log_filename):
    r = run_cmd(node, f'head -n 1 {STATUS_DIR}/{log_filename}')
    out = r.stdout.strip()
    if len(out) > 0 and out != '0':
        sys.exit(
            f'bad status in {STATUS_DIR}/{log_filename} on {node.instance_name}: {r.stdout}\ncheck {LOG_DIR}/{log_filename} for details'
        )


def set_boot_nodes(node, boot_nodes):
    if not node.instance_name.endswith('traffic'):
        home_dir = '/home/ubuntu/.near'
    else:
        home_dir = '/home/ubuntu/.near/target'

    run_cmd(
        node,
        f't=$(mktemp) && jq \'.network.boot_nodes = "{boot_nodes}"\' {home_dir}/config.json > $t && mv $t {home_dir}/config.json'
    )


# returns the peer ID of the resulting initialized NEAR dir
def init_home_dir(node, num_validators):
    run_cmd(node, f'mkdir -p /home/ubuntu/.near/setup/')
    run_cmd(node, f'mkdir -p {LOG_DIR}')
    run_cmd(node, f'mkdir -p {STATUS_DIR}')

    if not node.instance_name.endswith('traffic'):
        cmd = 'find /home/ubuntu/.near -type f -maxdepth 1 -delete && '
        cmd += 'rm -rf /home/ubuntu/.near/data && '
        cmd += '/home/ubuntu/neard init --account-id "$(hostname).near" && '
        cmd += 'rm -f /home/ubuntu/.near/genesis.json && '
        home_dir = '/home/ubuntu/.near'
    else:
        cmd = 'rm -rf /home/ubuntu/.near/target/ && '
        cmd += 'mkdir -p /home/ubuntu/.near/target/ && '
        cmd += '/home/ubuntu/neard --home /home/ubuntu/.near/target/ init && '
        cmd += 'rm -f /home/ubuntu/.near/target/validator_key.json && '
        cmd += 'rm -f /home/ubuntu/.near/target/genesis.json && '
        home_dir = '/home/ubuntu/.near/target'

    # TODO: don't hardcode tracked_shards. mirror run only sends txs for the shards in tracked shards. maybe should change that...
    config_changes = '.tracked_shards = [0, 1, 2, 3] | .archive = true | .log_summary_style="plain" | .rpc.addr = "0.0.0.0:3030" '
    config_changes += '| .network.skip_sync_wait=false | .genesis_records_file = "records.json" | .rpc.enable_debug_rpc = true '
    if num_validators < 3:
        config_changes += f'| .consensus.min_num_peers = {num_validators}'

    cmd += '''t=$(mktemp) && jq '{config_changes}' {home_dir}/config.json > $t && mv $t {home_dir}/config.json'''.format(
        config_changes=config_changes, home_dir=home_dir)
    run_cmd(node, cmd)

    config = mocknet.download_and_read_json(node, f'{home_dir}/config.json')
    node_key = mocknet.download_and_read_json(node, f'{home_dir}/node_key.json')
    key = node_key['public_key']
    port = config['network']['addr'].split(':')[1]
    return f'{key}@{node.machine.ip}:{port}'


BACKUP_DIR = '/home/ubuntu/.near/backup'


def make_backup(node, log_filename):
    if node.instance_name.endswith('traffic'):
        home = '/home/ubuntu/.near/target'
    else:
        home = '/home/ubuntu/.near'
    run_in_background(
        node,
        f'rm -rf {BACKUP_DIR} && mkdir {BACKUP_DIR} && cp -r {home}/data {BACKUP_DIR}/data && cp {home}/genesis.json {BACKUP_DIR}',
        log_filename)


def check_backup(node):
    if node.instance_name.endswith('traffic'):
        genesis = '/home/ubuntu/.near/target/genesis.json'
    else:
        genesis = '/home/ubuntu/.near/genesis.json'

    cmd = f'current=$(md5sum {genesis} | cut -d " " -f1) '
    cmd += f'&& saved=$(md5sum {BACKUP_DIR}/genesis.json | cut -d " " -f1)'
    cmd += f' && if [ $current == $saved ]; then exit 0; else echo "md5sum mismatch between {genesis} and {BACKUP_DIR}/genesis.json"; exit 1; fi'
    r = node.machine.run(cmd)
    if r.exitcode != 0:
        logger.warning(
            f'on {node.instance_name} could not check that saved state in {BACKUP_DIR} matches with ~/.near:\n{r.stdout}\n{r.stderr}'
        )
        return False
    return True


def reset_data_dir(node, log_filename):
    if node.instance_name.endswith('traffic'):
        data = '/home/ubuntu/.near/target/data'
    else:
        data = '/home/ubuntu/.near/data'
    run_in_background(node, f'rm -rf {data} && cp -r {BACKUP_DIR}/data {data}',
                      log_filename)


def amend_genesis_file(node, validators, epoch_length, num_seats, log_filename):
    mocknet.upload_json(node, '/home/ubuntu/.near/setup/validators.json',
                        validators)

    if not node.instance_name.endswith('traffic'):
        neard = '/home/ubuntu/neard-setup'
        genesis_file_out = '/home/ubuntu/.near/genesis.json'
        records_file_out = '/home/ubuntu/.near/records.json'
        config_file_path = '/home/ubuntu/.near/config.json'
    else:
        neard = '/home/ubuntu/neard'
        genesis_file_out = '/home/ubuntu/.near/target/genesis.json'
        records_file_out = '/home/ubuntu/.near/target/records.json'
    amend_genesis_cmd = [
        neard,
        'amend-genesis',
        '--genesis-file-in',
        '/home/ubuntu/.near/setup/genesis.json',
        '--records-file-in',
        '/home/ubuntu/.near/setup/records.json',
        '--genesis-file-out',
        genesis_file_out,
        '--records-file-out',
        records_file_out,
        '--validators',
        '/home/ubuntu/.near/setup/validators.json',
        '--chain-id',
        'mocknet',
        '--transaction-validity-period',
        '10000',
        '--epoch-length',
        str(epoch_length),
        '--num-seats',
        str(args.num_seats),
    ]
    amend_genesis_cmd = ' '.join(amend_genesis_cmd)
    run_in_background(node, amend_genesis_cmd, log_filename)


# like mocknet.wait_node_up() but we also check the status file
def wait_node_up(node, log_filename):
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
        check_process(node, log_filename)
        time.sleep(10)


def neard_running(node):
    return len(node.machine.run('ps cax | grep neard').stdout) > 0


def start_neard(node, log_filename):
    if node.instance_name.endswith('traffic'):
        home = '/home/ubuntu/.near/target'
    else:
        home = '/home/ubuntu/.near/'

    if not neard_running(node):
        run_in_background(
            node,
            f'/home/ubuntu/neard --unsafe-fast-startup --home {home} run',
            log_filename,
            env='RUST_LOG=debug')
        logger.info(f'started neard on {node.instance_name}')
    else:
        logger.info(f'neard already running on {node.instance_name}')


def start_mirror(node, log_filename):
    assert node.instance_name.endswith('traffic')

    if not neard_running(node):
        run_in_background(
            node,
            f'/home/ubuntu/neard mirror run --source-home /home/ubuntu/.near --target-home /home/ubuntu/.near/target --no-secret',
            log_filename,
            env='RUST_LOG=info,mirror=debug')
        logger.info(f'started neard mirror run on {node.instance_name}')
    else:
        logger.info(f'neard already running on {node.instance_name}')


def prompt_setup_flags(args):
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


def upload_neard_runner(node, config):
    node.machine.run(
        'rm -rf /home/ubuntu/neard-runner && mkdir /home/ubuntu/neard-runner && mkdir -p /home/ubuntu/neard-logs'
    )
    node.machine.upload('tests/mocknet/helpers/neard_runner.py',
                        '/home/ubuntu/neard-runner',
                        switch_user='ubuntu')
    node.machine.upload('tests/mocknet/helpers/requirements.txt',
                        '/home/ubuntu/neard-runner',
                        switch_user='ubuntu')
    mocknet.upload_json(node, '/home/ubuntu/neard-runner/config.json', config)
    cmd = 'cd /home/ubuntu/neard-runner && python3 -m virtualenv venv -p $(which python3)' \
    ' && ./venv/bin/pip install -r requirements.txt'
    run_cmd(node, cmd)
    run_in_background(node, f'/home/ubuntu/neard-runner/venv/bin/python /home/ubuntu/neard-runner/neard_runner.py ' \
        '--home /home/ubuntu/neard-runner --neard-home /home/ubuntu/.near ' \
        '--neard-logs /home/ubuntu/neard-logs --port 3000', 'neard-runner.txt')


def stop_neard_runner(node):
    # it's probably fine for now, but this is very heavy handed/not precise
    node.machine.run('kill $(ps -C python -o pid=)')


def init_neard_runner(nodes, binary_url, upgrade_binary_url):
    if upgrade_binary_url is None:
        configs = [{
            "binaries": [{
                "url": binary_url,
                "epoch_height": 0
            }]
        }] * len(nodes)
    else:
        # for now this test starts all validators with the same stake, so just make the upgrade
        # epoch random. If we change the stakes, we should change this to choose how much stake
        # we want to upgrade during each epoch
        configs = []
        for i in range(len(nodes)):
            configs.append({
                "binaries": [{
                    "url": binary_url,
                    "epoch_height": 0
                }, {
                    "url": upgrade_binary_url,
                    "epoch_height": random.randint(1, 4)
                }]
            })

    pmap(lambda x: upload_neard_runner(x[0], x[1]), zip(nodes, configs))


def setup(args, traffic_generator, nodes):
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
    pmap(stop_neard_runner, nodes)
    mocknet.stop_nodes(all_nodes)

    # TODO: move all the setup logic to neard_runner.py and just call it here, so
    # that each node does its own setup and we don't rely on the computer running this script
    logger.info(f'resetting/initializing home dirs')
    boot_nodes = pmap(lambda node: init_home_dir(node, args.num_validators),
                      all_nodes)
    pmap(lambda node: set_boot_nodes(node, ','.join(boot_nodes[:20])),
         all_nodes)
    logger.info(f'home dir initialization finished')

    random.shuffle(nodes)
    validators = get_validator_list(nodes[:args.num_validators])

    init_neard_runner(nodes, args.neard_binary_url,
                      args.neard_upgrade_binary_url)

    logger.info(
        f'setting validators and running neard amend-genesis on all nodes. validators: {validators}'
    )
    logger.info(f'this step will take a while (> 10 minutes)')
    pmap(
        lambda node: amend_genesis_file(node, validators, args.epoch_length,
                                        args.num_seats, 'amend-genesis.txt'),
        all_nodes)
    pmap(lambda node: wait_process(node, 'amend-genesis.txt'), all_nodes)
    logger.info(f'finished neard amend-genesis step')

    logger.info(
        f'starting neard nodes then waiting for them to be ready. This may take a long time (a couple hours)'
    )
    logger.info(
        'If your connection is broken in the meantime, run "make-backups" to resume'
    )

    make_backups(args, traffic_generator, nodes)

    logger.info('test setup complete')

    if args.start_traffic:
        start_traffic(args, traffic_generator, nodes)


def make_backups(args, traffic_generator, nodes):
    all_nodes = nodes + [traffic_generator]
    pmap(lambda node: start_neard(node, 'neard.txt'), all_nodes)
    pmap(lambda node: wait_node_up(node, 'neard.txt'), all_nodes)
    mocknet.stop_nodes(all_nodes)

    logger.info(f'copying data dirs to {BACKUP_DIR}')
    pmap(lambda node: make_backup(node, 'make-backup.txt'), all_nodes)
    pmap(lambda node: wait_process(node, 'make-backup.txt'), all_nodes)


def reset_data_dirs(args, traffic_generator, nodes):
    all_nodes = nodes + [traffic_generator]
    stop_nodes(args, traffic_generator, nodes)
    # TODO: maybe have the neard runner not return from the JSON rpc until it's stopped?
    while True:
        if not any(mocknet.is_binary_running_all_nodes(
                'neard',
                all_nodes,
        )):
            break
    if not all(pmap(check_backup, all_nodes)):
        logger.warning('Not continuing with backup restoration')
        return

    logger.info('restoring data dirs from /home/ubuntu/.near/data-backup')
    pmap(lambda node: reset_data_dir(node, 'reset-data.txt'), all_nodes)
    pmap(lambda node: wait_process(node, 'reset-data.txt'), all_nodes)

    if args.start_traffic:
        start_traffic(args, traffic_generator, nodes)


def stop_nodes(args, traffic_generator, nodes):
    mocknet.stop_nodes([traffic_generator])
    pmap(neard_runner_stop, nodes)


def neard_runner_jsonrpc(node, method):
    j = {'method': method, 'params': [], 'id': 'dontcare', 'jsonrpc': '2.0'}
    r = requests.post(f'http://{node.machine.ip}:3000', json=j, timeout=5)
    if r.status_code != 200:
        logger.warning(
            f'bad response {r.status_code} trying to send {method} JSON RPC to neard runner on {node.instance_name}:\n{r.content}'
        )
    r.raise_for_status()


def neard_runner_start(node):
    neard_runner_jsonrpc(node, 'start')


def neard_runner_stop(node):
    neard_runner_jsonrpc(node, 'stop')


def start_traffic(args, traffic_generator, nodes):
    if not all(pmap(check_backup, nodes + [traffic_generator])):
        logger.warning(
            f'Not sending traffic, as the backups in {BACKUP_DIR} dont seem to be up to date'
        )
        return

    pmap(neard_runner_start, nodes)
    logger.info("waiting for validators to be up")
    pmap(lambda node: wait_node_up(node, 'neard.txt'), nodes)
    logger.info(
        "waiting a bit after validators started before starting traffic")
    time.sleep(10)
    start_mirror(traffic_generator, 'mirror.txt')
    logger.info(
        f'test running. to check the traffic sent, try running "curl http://{traffic_generator.machine.ip}:3030/metrics | grep mirror"'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a load test')
    parser.add_argument('--chain-id', type=str, required=True)
    parser.add_argument('--start-height', type=int, required=True)
    parser.add_argument('--unique-id', type=str, required=True)

    parser.add_argument('--epoch-length', type=int)
    parser.add_argument('--num-validators', type=int)
    parser.add_argument('--num-seats', type=int)

    parser.add_argument('--neard-binary-url', type=str)
    parser.add_argument('--neard-upgrade-binary-url', type=str)

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    setup_parser = subparsers.add_parser('setup',
                                         help='''
    Sets up new state from the prepared records and genesis files with the number
    of validators specified. This calls neard amend-genesis to create the new genesis
    and records files, and then starts the neard nodes and waits for them to be online
    after computing the genesis state roots. This step takes a very long time (> 12 hours).
    Use --start-traffic to start traffic after the setup is complete, which is equivalent to
    just running the start-traffic subcommand manually after.
    ''')
    setup_parser.add_argument('--start-traffic',
                              default=False,
                              action='store_true')
    setup_parser.set_defaults(func=setup)

    start_parser = subparsers.add_parser(
        'start-traffic',
        help=
        'starts all nodes and starts neard mirror run on the traffic generator')
    start_parser.set_defaults(func=start_traffic)

    stop_parser = subparsers.add_parser('stop-nodes',
                                        help='kill all neard processes')
    stop_parser.set_defaults(func=stop_nodes)

    backup_parser = subparsers.add_parser('make-backups',
                                          help='''
    This is run automatically by "setup", but if your connection is interrupted during "setup", this will
    resume waiting for the nodes to compute the state roots, and then will make a backup of all data dirs
    ''')
    backup_parser.add_argument('--start-traffic',
                               default=False,
                               action='store_true')
    backup_parser.set_defaults(func=make_backups)

    reset_parser = subparsers.add_parser('reset',
                                         help='''
    The setup command saves the data directory after the genesis state roots are computed so that
    the test can be reset from the start without having to do that again. This command resets all nodes'
    data dirs to what was saved then, so that start-traffic will start the test all over again.
    ''')
    reset_parser.add_argument('--start-traffic',
                              default=False,
                              action='store_true')
    reset_parser.set_defaults(func=reset_data_dirs)

    args = parser.parse_args()

    traffic_generator, nodes = get_nodes(args)
    args.func(args, traffic_generator, nodes)

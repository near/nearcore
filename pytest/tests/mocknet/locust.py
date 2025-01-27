import argparse
import cmd_utils
import pathlib
from rc import pmap, run
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import mocknet

from configured_logger import logger


def init_locust_node(instance_name):
    node = mocknet.get_node(instance_name)
    if node is None:
        sys.exit(f'could not find node {instance_name}')
    cmd_utils.init_node(node)
    commands = [
        'sudo apt update',
        'sudo apt-get install -y git virtualenv build-essential python3-dev',
        'git clone https://github.com/near/nearcore /home/ubuntu/nearcore',
        'mkdir /home/ubuntu/locust',
        'cd /home/ubuntu/locust && python3 -m virtualenv venv -p $(which python3)',
        './venv/bin/pip install -r /home/ubuntu/nearcore/pytest/requirements.txt',
        './venv/bin/pip install locust',
    ]
    init_command = ' && '.join(commands)
    cmd_utils.run_cmd(node, init_command)


def init_cmd(args):
    nodes = [x for x in args.instance_names.split(',') if len(x) > 0]
    pmap(init_locust_node, nodes)


def parse_instance_names(args):
    if args.master is None:
        print('master instance name?: ')
        args.master = sys.stdin.readline().strip()

    if args.workers is None:
        print('''
worker instance names? Give a comma separated list. It is also valid to
have the machine used for the master process in this list as well:''')
        args.workers = sys.stdin.readline().strip()

    master = mocknet.get_node(args.master)
    if master is None:
        sys.exit(f'could not find node {args.master}')

    worker_names = [x for x in args.workers.split(',') if len(x) > 0]
    workers = [
        mocknet.get_node(instance_name) for instance_name in worker_names
    ]
    for (name, node) in zip(worker_names, workers):
        if node is None:
            sys.exit(f'could not find node {name}')

    return master, workers


def upload_key(node, filename):
    node.machine.upload(args.funding_key,
                        '/home/ubuntu/locust/funding_key.json',
                        switch_user='ubuntu')


def run_master(args, node, num_workers):
    upload_key(node, args.funding_key)
    cmd = f'/home/ubuntu/locust/venv/bin/python3 -m locust --web-port 3030 --master-bind-port 3000 -H {args.node_ip_port} -f locustfiles/{args.locustfile} --shard-layout-chain-id mainnet --funding-key=/home/ubuntu/locust/funding_key.json --max-workers {args.max_workers} --master'
    if args.num_users is not None:
        cmd += f' --users {args.num_users}'
    if args.run_time is not None:
        cmd += f' --run-time {args.run_time}'
    if not args.web_ui:
        cmd += f' --headless --expect-workers {num_workers}'

    logger.info(f'running "{cmd}" on master node {node.instance_name}')
    cmd_utils.run_in_background(
        node,
        cmd,
        'locust-master.txt',
        pre_cmd=
        'ulimit -S -n 100000 && cd /home/ubuntu/nearcore/pytest/tests/loadtest/locust'
    )


def wait_locust_inited(node, log_filename):
    # We want to wait for the locust process to finish the initialization steps. Is there a better way than
    # just waiting for the string "Starting Locust" to appear in the logs?
    cmd_utils.run_cmd(
        node,
        f'tail -f {cmd_utils.LOG_DIR}/{log_filename}.0 | grep --line-buffered -m 1 -q "Starting Locust"'
    )


def wait_master_inited(node):
    wait_locust_inited(node, 'locust-master.txt')
    logger.info(f'master locust node initialized')


def wait_worker_inited(node):
    wait_locust_inited(node, 'locust-worker.txt')
    logger.info(f'worker locust node {node.instance_name} initialized')


def run_worker(args, node, master_ip):
    cmd = f'/home/ubuntu/locust/venv/bin/python3 -m locust --web-port 3030 -H {args.node_ip_port} -f locustfiles/{args.locustfile} --shard-layout-chain-id mainnet --funding-key=/home/ubuntu/locust/funding_key.json --worker --master-port 3000'
    if master_ip != node.machine.ip:
        # if this node is also the master node, the key has already been uploaded
        upload_key(node, args.funding_key)
        cmd += f' --master-host {master_ip}'
    logger.info(f'running "{cmd}" on worker node {node.instance_name}')
    cmd_utils.run_in_background(
        node,
        cmd,
        'locust-worker.txt',
        pre_cmd=
        'ulimit -S -n 100000 && cd /home/ubuntu/nearcore/pytest/tests/loadtest/locust'
    )


def run_cmd(args):
    if not args.web_ui and args.num_users is None:
        sys.exit('unless you pass --web-ui, --num-users must be set')

    master, workers = parse_instance_names(args)

    run_master(args, master, len(workers))
    if args.web_ui:
        wait_master_inited(master)
    pmap(lambda n: run_worker(args, n, master.machine.ip), workers)
    if args.web_ui:
        pmap(wait_worker_inited, workers)
        logger.info(
            f'All locust workers initialized. Visit http://{master.machine.ip}:3030/ to start and control the test'
        )
    else:
        logger.info('All workers started.')


def stop_cmd(args):
    master, workers = parse_instance_names(args)
    # TODO: this feels kind of imprecise and heavy-handed, since we're just looking for a command that matches "python3.*locust.*master" and killing it,
    # instead of remembering what the process' IP was. Should be possible to do this right, but this will work for now
    cmd_utils.run_cmd(
        master,
        'pids=$(ps -C python3 -o pid=,cmd= | grep "locust" | cut -d " " -f 2) && if [ ! -z "$pids" ]; then kill $pids; fi'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a locust load test')

    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    init_parser = subparsers.add_parser('init',
                                        help='''
    Sets up the python environment and downloads the code on each node.
    ''')
    init_parser.add_argument('--instance-names', type=str)
    init_parser.set_defaults(func=init_cmd)

    run_parser = subparsers.add_parser('run',
                                       help='''
    Runs the locust load test on each node.
    ''')
    run_parser.add_argument('--master',
                            type=str,
                            required=True,
                            help='instance name of master node')
    run_parser.add_argument(
        '--workers',
        type=str,
        required=True,
        help='comma-separated list of instance names of worker nodes')
    run_parser.add_argument(
        '--node-ip-port',
        type=str,
        required=True,
        help='IP address and port of a node in the network under test')
    run_parser.add_argument(
        '--funding-key',
        type=str,
        required=True,
        help=
        'local path to a key file for the base account to be used in the test')
    run_parser.add_argument(
        '--locustfile',
        type=str,
        default='ft.py',
        help=
        'locustfile name in nearcore/pytest/tests/loadtest/locust/locustfiles')
    run_parser.add_argument(
        '--max-workers',
        type=int,
        default=16,
        help='max number of workers the test should support')
    run_parser.add_argument(
        '--web-ui',
        action='store_true',
        help=
        'if given, sets up a web UI to control the test, otherwise starts automatically'
    )
    run_parser.add_argument(
        '--num-users',
        type=int,
        help=
        'number of users to run the test with. Required unless --web-ui is given.'
    )
    run_parser.add_argument(
        '--run-time',
        type=str,
        help=
        'A string specifying the total run time of the test, passed to the locust --run-time argument. e.g. (300s, 20m, 3h, 1h30m, etc.)'
    )
    run_parser.set_defaults(func=run_cmd)

    stop_parser = subparsers.add_parser('stop',
                                        help='''
    Stops the locust load test on each node.
    ''')
    stop_parser.add_argument('--master',
                             type=str,
                             help='instance name of master node')
    stop_parser.add_argument(
        '--workers',
        type=str,
        help='comma-separated list of instance names of worker nodes')
    stop_parser.set_defaults(func=stop_cmd)

    args = parser.parse_args()

    args.func(args)

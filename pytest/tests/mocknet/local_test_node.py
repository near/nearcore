#!/usr/bin/env python3
"""
defines the LocalTestNeardRunner class meant to to test mocknet itself locally
"""
from argparse import ArgumentParser
import http.server
import json
import os
import pathlib
import psutil
import re
import requests
import shutil
import signal
import subprocess
import sys
import threading
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from node_handle import NodeHandle


# return the process with pid listed in `pid_path`, if it's running
def get_process(pid_path):
    try:
        with open(pid_path, 'r') as f:
            pid = int(f.read().strip())
    except FileNotFoundError:
        return None
    try:
        return psutil.Process(pid)
    except psutil.NoSuchProcess:
        return None


# kill the process with pid listed in `pid_path`
def kill_process(pid_path):
    p = get_process(pid_path)
    if p is not None:
        logger.info(f'killing process with pid {p.pid} indicated in {pid_path}')
        p.send_signal(signal.SIGTERM)
        p.wait()
    try:
        pid_path.unlink()
    except FileNotFoundError:
        pass


def http_post(addr, port, body):
    r = requests.post(f'http://{addr}:{port}', json=body, timeout=5)
    if r.status_code != 200:
        logger.warning(
            f'bad response {r.status_code} trying to post {body} to http://{addr}:{port}:\n{r.content}'
        )
    r.raise_for_status()
    return r.json()


class LocalTestNeardRunner:

    def __init__(self, home, port, neard_rpc_port, neard_protocol_port):
        # last part of the path. e.g. ~/.near/local-mocknet/traffic-generator -> traffic-generator
        self._name = os.path.basename(os.path.normpath(home))
        self.home = home
        self.port = port
        self.neard_rpc_port = neard_rpc_port
        self.neard_protocol_port = neard_protocol_port

    def name(self):
        return self._name

    def ip_addr(self):
        return '0.0.0.0'

    def neard_port(self):
        return self.neard_rpc_port

    def init(self):
        return

    def mk_neard_runner_home(self, remove_home_dir):
        # handled by local_test_setup_cmd()
        return

    def upload_neard_runner(self):
        return

    def upload_neard_runner_config(self, config):
        # handled by local_test_setup_cmd()
        return

    def run_cmd(self, cmd, raise_on_fail=False, return_on_fail=False):
        logger.error(
            "Does not make sense to run command on local host. The behaviour may not be the desired one."
        )

    def init_python(self):
        return

    def _pid_path(self):
        return self.home / 'pid.txt'

    def stop_neard_runner(self):
        kill_process(self._pid_path())

    def start_neard_runner(self):
        if get_process(self._pid_path()) is not None:
            return

        with open(self.home / 'stdout', 'ab') as stdout, \
            open(self.home / 'stderr', 'ab') as stderr:
            args = [
                sys.executable, 'tests/mocknet/helpers/neard_runner.py',
                '--home', self.home / 'neard-runner', '--neard-home',
                self.home / '.near', '--neard-logs', self.home / 'neard-logs',
                '--port',
                str(self.port)
            ]
            process = subprocess.Popen(args,
                                       stdin=subprocess.DEVNULL,
                                       stdout=stdout,
                                       stderr=stderr,
                                       process_group=0)
        with open(self._pid_path(), 'w') as f:
            f.write(f'{process.pid}\n')
        logger.info(
            f'started neard runner process with pid {process.pid} listening on port {self.port}'
        )

    def neard_runner_post(self, body):
        return http_post(self.ip_addr(), self.port, body)

    def new_test_params(self):
        return {
            'rpc_port': self.neard_rpc_port,
            'protocol_port': self.neard_protocol_port,
            'validator_id': self._name,
        }

    def get_validators(self):
        body = {
            'method': 'validators',
            'params': [None],
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        return http_post(self.ip_addr(), self.neard_rpc_port, body)


def prompt_flags(args):
    if args.num_nodes is None:
        print(
            'number of validating nodes? One instance of neard_runner.py will be run for each one, plus a traffic generator: '
        )
        args.num_nodes = int(sys.stdin.readline().strip())
        assert args.num_nodes > 0

    if args.neard_binary_path is None:
        print('neard binary path?: ')
        args.neard_binary_path = sys.stdin.readline().strip()
        assert len(args.neard_binary_path) > 0

    if args.fork_height is not None:
        if not args.legacy_records:
            print(
                '--legacy-records not given. Assuming it based on --fork-height'
            )
            args.legacy_records = True
    elif args.target_home_dir is not None:
        if args.legacy_records:
            sys.exit('cannot give --target-home-dir and --legacy-records')
    elif not args.legacy_records:
        print(
            'prepare nodes with fork-network tool instead of genesis records JSON? [yes/no]:'
        )
        while True:
            r = sys.stdin.readline().strip().lower()
            if r == 'yes':
                args.legacy_records = False
                break
            elif r == 'no':
                args.legacy_records = True
                break
            else:
                print('please say yes or no')

    if args.source_home_dir is None:
        if args.legacy_records:
            print('source home dir: ')
        else:
            print(
                'source home dir containing the HEAD block of target home, plus more blocks after that: '
            )
        args.source_home_dir = sys.stdin.readline().strip()
        assert len(args.source_home_dir) > 0

    if args.target_home_dir is None and not args.legacy_records:
        print('target home dir whose HEAD is contained in --source-home-dir: ')
        args.target_home_dir = sys.stdin.readline().strip()
        assert len(args.target_home_dir) > 0

    if args.legacy_records and args.fork_height is None:
        print('fork height: ')
        args.fork_height = sys.stdin.readline().strip()
        assert len(args.fork_height) > 0


def run_cmd(cmd):
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(
            f'running `{" ".join([str(a) for a in cmd])}` returned {e.returncode}. output:\n{e.output.decode("utf-8")}'
        )


# dumps records from `traffic_home_dir` and prepares records with keys changed
# for mirroring traffic
def make_records(neard_binary_path, traffic_home_dir, start_height):
    run_cmd([
        neard_binary_path, '--home', traffic_home_dir, 'view-state',
        'dump-state', '--stream', '--height',
        str(start_height)
    ])
    shutil.copyfile(traffic_home_dir / 'output/genesis.json',
                    traffic_home_dir / 'setup/genesis.json')
    run_cmd([
        neard_binary_path,
        'mirror',
        'prepare',
        '--records-file-in',
        traffic_home_dir / 'output/records.json',
        '--records-file-out',
        traffic_home_dir / 'setup/records.json',
        '--secret-file-out',
        '/dev/null',
        '--no-secret',
    ])


def make_legacy_records(neard_binary_path, traffic_generator_home, node_homes,
                        start_height):
    make_records(neard_binary_path, traffic_generator_home / '.near',
                 start_height)
    for node_home in node_homes:
        shutil.copyfile(traffic_generator_home / '.near/setup/genesis.json',
                        node_home / '.near/setup/genesis.json')
        shutil.copyfile(traffic_generator_home / '.near/setup/records.json',
                        node_home / '.near/setup/records.json')


def fork_db(neard_binary_path, target_home_dir, setup_dir):
    copy_source_home(target_home_dir, setup_dir)

    run_cmd([
        neard_binary_path,
        '--home',
        setup_dir,
        'fork-network',
        'init',
    ])
    run_cmd([
        neard_binary_path,
        '--home',
        setup_dir,
        'fork-network',
        'amend-access-keys',
    ])
    shutil.rmtree(setup_dir / 'data/fork-snapshot')


def make_forked_network(neard_binary_path, traffic_generator_setup, node_homes,
                        source_home_dir, target_home_dir):
    for setup_dir in [h / '.near/setup' for h in node_homes
                     ] + [traffic_generator_setup]:
        fork_db(neard_binary_path, target_home_dir, setup_dir)


def mkdirs(local_mocknet_path):
    traffic_generator_home = local_mocknet_path / 'traffic-generator'
    traffic_generator_home.mkdir()
    os.mkdir(traffic_generator_home / 'neard-runner')
    os.mkdir(traffic_generator_home / '.near')
    node_homes = []
    for i in range(args.num_nodes):
        node_home = local_mocknet_path / f'node{i}'
        node_home.mkdir()
        os.mkdir(node_home / f'neard-runner')
        os.mkdir(node_home / f'.near')
        os.mkdir(node_home / f'.near/setup')
        node_homes.append(node_home)
    return traffic_generator_home, node_homes


def copy_source_home(source_home_dir, traffic_generator_home):
    shutil.copyfile(source_home_dir / 'config.json',
                    traffic_generator_home / 'config.json')
    shutil.copyfile(source_home_dir / 'node_key.json',
                    traffic_generator_home / 'node_key.json')
    shutil.copyfile(source_home_dir / 'genesis.json',
                    traffic_generator_home / 'genesis.json')
    try:
        shutil.copyfile(source_home_dir / 'records.json',
                        traffic_generator_home / 'records.json')
    except FileNotFoundError:
        pass
    shutil.copytree(source_home_dir / 'data', traffic_generator_home / 'data')


def make_binaries_dir(local_mocknet_path, neard_binary_path):
    binaries_path = local_mocknet_path / 'binaries'
    binaries_path.mkdir()
    binary_path = binaries_path / 'neard'
    binary_path.symlink_to(neard_binary_path)
    return binaries_path


class Server(http.server.HTTPServer):

    def __init__(self, addr, directory):
        self.directory = directory
        super().__init__(addr, http.server.SimpleHTTPRequestHandler)

    def finish_request(self, request, client_address):
        self.RequestHandlerClass(request,
                                 client_address,
                                 self,
                                 directory=self.directory)


def write_config(home, config):
    with open(home / 'neard-runner' / 'config.json', 'w') as f:
        json.dump(config, f)


# looks for directories called node{i} in `local_mocknet_path`
def get_node_homes(local_mocknet_path):
    dirents = os.listdir(local_mocknet_path)
    node_homes = []
    for p in dirents:
        m = re.match(r'node(\d+)', p)
        if m is None:
            continue
        node_homes.append((p, int(m.groups()[0])))
    node_homes.sort(key=lambda x: x[1])
    idx = -1
    for (home, node_index) in node_homes:
        if node_index != idx + 1:
            raise ValueError(
                f'some neard runner node dirs missing? found: {[n[0] for n in node_homes]}'
            )
        idx = node_index
    return [local_mocknet_path / x[0] for x in node_homes]


DEFAULT_LOCAL_MOCKNET_DIR = pathlib.Path.home() / '.near/local-mocknet'

# return a NodeHandle for each of the neard runner directories in `local_mocknet_path`
def get_nodes(local_mocknet_path=DEFAULT_LOCAL_MOCKNET_DIR):
    runner_port = 3000
    neard_rpc_port = 3040
    neard_protocol_port = 24577
    traffic_generator = NodeHandle(
        LocalTestNeardRunner(local_mocknet_path / 'traffic-generator',
                             runner_port, neard_rpc_port, neard_protocol_port), can_validate=False)

    node_homes = get_node_homes(local_mocknet_path)
    nodes = []
    for home in node_homes:
        runner_port += 1
        neard_rpc_port += 1
        neard_protocol_port += 1
        nodes.append(
            NodeHandle(
                LocalTestNeardRunner(home, runner_port, neard_rpc_port,
                                     neard_protocol_port)))

    return traffic_generator, nodes


def kill_neard_runner(home):
    kill_process(home / 'pid.txt')


def kill_neard_runners(local_mocknet_path):
    kill_neard_runner(local_mocknet_path / 'traffic-generator')
    node_homes = get_node_homes(local_mocknet_path)
    for home in node_homes:
        kill_neard_runner(home)


def wait_node_serving(node):
    while True:
        try:
            node.neard_runner_ready()
            return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(0.5)


def local_test_setup_cmd(args):
    prompt_flags(args)
    if args.source_home_dir is None:
        sys.exit(f'must give --source-home-dir')
    if args.legacy_records:
        if args.target_home_dir is not None:
            sys.exit(f'cannot give --target-home-dir with --legacy-records')
        if args.fork_height is None:
            sys.exit('must give --fork-height with --legacy-records')
    else:
        if args.target_home_dir is None:
            sys.exit(f'must give --target-home-dir')
        if args.fork_height is not None:
            sys.exit('cannot give --fork-height without --legacy-records')

    local_mocknet_path = DEFAULT_LOCAL_MOCKNET_DIR
    if os.path.exists(local_mocknet_path):
        if not args.yes:
            print(
                f'{local_mocknet_path} already exists. This command will delete and reinitialize it. Continue? [yes/no]:'
            )
            if sys.stdin.readline().strip() != 'yes':
                return
        kill_neard_runners(local_mocknet_path)
        shutil.rmtree(local_mocknet_path)

    neard_binary_path = pathlib.Path(args.neard_binary_path)
    source_home_dir = pathlib.Path(args.source_home_dir)

    os.mkdir(local_mocknet_path)
    traffic_generator_home, node_homes = mkdirs(local_mocknet_path)

    if args.legacy_records:
        setup_dir = traffic_generator_home / '.near/setup'
        setup_dir.mkdir()
        copy_source_home(source_home_dir, traffic_generator_home / '.near')
        make_legacy_records(neard_binary_path, traffic_generator_home,
                            node_homes, args.fork_height)
    else:
        source_dir = traffic_generator_home / '.near/source'
        source_dir.mkdir()
        setup_dir = traffic_generator_home / '.near/target/setup'
        setup_dir.mkdir(parents=True)
        copy_source_home(source_home_dir, source_dir)
        target_home_dir = pathlib.Path(args.target_home_dir)
        make_forked_network(neard_binary_path, setup_dir, node_homes,
                            source_home_dir, target_home_dir)
    # now set up an HTTP server to serve the binary that each neard_runner.py will request
    binaries_path = make_binaries_dir(local_mocknet_path, neard_binary_path)
    binaries_server_addr = 'localhost'
    binaries_server_port = 8000
    binaries_server = Server(addr=(binaries_server_addr, binaries_server_port),
                             directory=binaries_path)
    server_thread = threading.Thread(
        target=lambda: binaries_server.serve_forever(), daemon=True)
    server_thread.start()

    node_config = {
        'is_traffic_generator':
            False,
        'binaries': [{
            'url':
                f'http://{binaries_server_addr}:{binaries_server_port}/neard',
            'epoch_height':
                0
        }]
    }
    traffic_generator_config = {
        'is_traffic_generator':
            True,
        'binaries': [{
            'url':
                f'http://{binaries_server_addr}:{binaries_server_port}/neard',
            'epoch_height':
                0
        }]
    }

    write_config(traffic_generator_home, traffic_generator_config)
    for node_home in node_homes:
        write_config(node_home, node_config)

    traffic_generator, nodes = get_nodes(local_mocknet_path)
    traffic_generator.start_neard_runner()
    for node in nodes:
        node.start_neard_runner()

    for node in [traffic_generator] + nodes:
        wait_node_serving(node)

    print(
        f'All directories initialized. neard runners are running in dirs: {[str(traffic_generator.node.home)] + [str(n.node.home) for n in nodes]}, listening on respective ports: {[traffic_generator.node.port] + [n.node.port for n in nodes]}'
    )


if __name__ == '__main__':
    parser = ArgumentParser(description='Set up a local instance of mocknet')
    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands')

    local_test_setup_parser = subparsers.add_parser('local-test-setup',
                                                    help='''
        Setup several instances of neard-runner to run locally. Then the mirror.py --local-test
        argument can be used to test these test scripts themselves.
        ''')
    local_test_setup_parser.add_argument('--num-nodes', type=int)
    # TODO: add a --neard-upgrade-binary-path flag too
    local_test_setup_parser.add_argument('--neard-binary-path', type=str)
    local_test_setup_parser.add_argument('--source-home-dir',
                                         type=str,
                                         help='''
    Near home directory containing some transactions that can be used to create a forked state
    for transaction mirroring. This could be a home dir from a pytest in tests/sanity, for example.
    ''')
    local_test_setup_parser.add_argument('--fork-height',
                                         type=int,
                                         help='''
    Height where state should be forked from in the directory indicated by --source-home-dir. Ideally this should
    be a height close to the node's tail. This is something that could be automated if there were an easy
    way to get machine-readable valid heights in a near data directory, but for now this flag must be given manually.
    ''')
    local_test_setup_parser.add_argument('--yes', action='store_true')
    local_test_setup_parser.add_argument('--legacy-records',
                                         action='store_true',
                                         help='''
    If given, setup a records.json file with forked state instead of using the neard fork-network command
    ''')
    local_test_setup_parser.add_argument('--target-home-dir',
                                         type=str,
                                         help='''
    todo
    ''')
    local_test_setup_parser.set_defaults(func=local_test_setup_cmd)

    args = parser.parse_args()
    args.func(args)

# TODO: reimplement this logic using standard tooling like systemd instead of relying on this
# python script to handle neard process management.

import argparse
import datetime
from enum import Enum
import fcntl
import json
import jsonrpc
import logging
import os
import psutil
import re
import requests
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import http
import http.server
import dotenv


def get_lock(home):
    lock_file = os.path.join(home, 'LOCK')

    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise Exception(f'{lock_file} is currently locked by another process')
    return fd


class JSONHandler(http.server.BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        self.dispatcher = jsonrpc.Dispatcher()
        self.dispatcher.add_method(server.neard_runner.do_new_test,
                                   name="new_test")
        self.dispatcher.add_method(server.neard_runner.do_network_init,
                                   name="network_init")
        self.dispatcher.add_method(server.neard_runner.do_update_config,
                                   name="update_config")
        self.dispatcher.add_method(server.neard_runner.do_ready, name="ready")
        self.dispatcher.add_method(server.neard_runner.do_version,
                                   name="version")
        self.dispatcher.add_method(server.neard_runner.do_start, name="start")
        self.dispatcher.add_method(server.neard_runner.do_stop, name="stop")
        self.dispatcher.add_method(server.neard_runner.do_reset, name="reset")
        self.dispatcher.add_method(server.neard_runner.do_update_binaries,
                                   name="update_binaries")
        self.dispatcher.add_method(server.neard_runner.do_make_backup,
                                   name="make_backup")
        self.dispatcher.add_method(server.neard_runner.do_ls_backups,
                                   name="ls_backups")
        self.dispatcher.add_method(server.neard_runner.do_clear_env,
                                   name="clear_env")
        self.dispatcher.add_method(server.neard_runner.do_add_env,
                                   name="add_env")
        super().__init__(request, client_address, server)

    def do_GET(self):
        if self.path == '/status':
            body = 'OK\n'.encode('UTF-8')
            self.send_response(http.HTTPStatus.OK)
            self.send_header("Content-Type", 'application/json')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(http.HTTPStatus.NOT_FOUND)

    def do_POST(self):
        l = self.headers.get('content-length')
        if l is None:
            self.send_error(http.HTTPStatus.BAD_REQUEST,
                            "Content-Length missing")
            return

        body = self.rfile.read(int(l))
        response = jsonrpc.JSONRPCResponseManager.handle(body, self.dispatcher)
        response_body = response.json.encode('UTF-8')

        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", 'application/json')
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)


class RpcServer(http.server.HTTPServer):

    def __init__(self, addr, neard_runner):
        self.neard_runner = neard_runner
        super().__init__(addr, JSONHandler)


class TestState(Enum):
    NONE = 1
    AWAITING_NETWORK_INIT = 2
    AMEND_GENESIS = 3
    STATE_ROOTS = 4
    RUNNING = 5
    STOPPED = 6
    RESETTING = 7
    ERROR = 8
    MAKING_BACKUP = 9
    SET_VALIDATORS = 10


backup_id_pattern = re.compile(r'^[0-9a-zA-Z.][0-9a-zA-Z_\-.]+$')

# Rotate the logs if they get larger than __neard_logs_file_size__
# Remove old files if the number of files is __neard_logs_max_file_count__
# Remove old files if all the logs are past __neard_logs_max_size__
# The prerotate logic serves us in case neard_runner is not running for a while
# and we end up with a file larger than the estimated size.
LOGROTATE_TEMPLATE = """__neard_logs_dir__/__neard_logs_file_name__ {
    su ubuntu ubuntu
    size __neard_logs_file_size__
    rotate __neard_logs_max_file_count__
    copytruncate
    missingok
    notifempty
    dateext
    dateformat -%Y-%m-%d-%H-%M-%S
    create 0644 ubuntu ubuntu
    prerotate
        total_size=$(du -sb __neard_logs_dir__/__neard_logs_file_name__* | awk '{total+=$1}END{print total}')
        while [ $total_size -gt __neard_logs_max_size__ ]; do
            # get the oldest file alphabetically 
            oldest_file=$(ls -1 __neard_logs_dir__/__neard_logs_file_name__-* | head -n1)
            rm -f "$oldest_file"
            total_size=$(du -sb __neard_logs_dir__/__neard_logs_file_name__* | awk '{total+=$1}END{print total}')
        done
    endscript
}"""


class NeardRunner:

    def __init__(self, args):
        self.home = args.home
        self.neard_home = args.neard_home
        self.neard_logs_dir = args.neard_logs_dir
        self.neard_logs_file_name = 'logs.txt'
        self._configure_neard_logs()
        with open(self.home_path('config.json'), 'r') as f:
            self.config = json.load(f)
        self.neard = None
        self.num_restarts = 0
        self.last_start = time.time()
        # self.data will contain data that we want to persist so we can
        # start where we left off if this process is killed and restarted
        # for now we save info on the neard binaries (their paths and epoch
        # heights where we want to run them), a bool that tells whether neard
        # should be running, and info on the currently running neard process
        try:
            with open(self.home_path('data.json'), 'r') as f:
                self.data = json.load(f)
                self.data['binaries'].sort(key=lambda x: x['epoch_height'])
        except FileNotFoundError:
            self.data = {
                'binaries': [],
                'neard_process': None,
                'current_neard_path': None,
                'state': TestState.NONE.value,
                'backups': {},
                'state_data': None,
            }
        self.legacy_records = self.is_legacy()
        # protects self.data, and its representation on disk,
        # because both the rpc server and the main loop touch them concurrently
        # TODO: consider locking the TestState variable separately, since there
        # is no need to block reading that when inside the update_binaries rpc for example
        self.lock = threading.Lock()

    def _configure_neard_logs(self):
        try:
            os.mkdir(self.neard_logs_dir)
        except FileExistsError:
            pass

        variables = {
            '__neard_logs_dir__': f'{self.neard_logs_dir}',
            '__neard_logs_file_name__': f'{self.neard_logs_file_name}',
            '__neard_logs_file_size__': '100M',
            '__neard_logs_max_file_count__': '900',
            '__neard_logs_max_size__': '100000000000',  # 100G
        }
        logrotate_config = LOGROTATE_TEMPLATE
        # Replace variables in the template
        for var, value in variables.items():
            logrotate_config = re.sub(re.escape(var), value, logrotate_config)
        self.logrotate_config_path = f'{self.neard_logs_dir}/.neard_logrotate_policy'
        logging.info(
            f'Setting log rotation policy in {self.logrotate_config_path}')
        with open(self.logrotate_config_path, 'w') as config_file:
            config_file.write(logrotate_config)
        self.logrotate_binary_path = shutil.which('logrotate')
        if self.logrotate_binary_path is None:
            logging.error('The logrotate tool was not found on this system.')

    # Try to rotate the logs based on the policy defined here: self.logrotate_config_path.
    def run_logrotate(self):
        run_logrotate_cmd = [
            self.logrotate_binary_path, '-s',
            f'{self.neard_logs_dir}/.logrotate_status',
            self.logrotate_config_path
        ]
        subprocess.Popen(
            run_logrotate_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def is_legacy(self):
        if os.path.exists(os.path.join(
                self.neard_home, 'setup', 'records.json')) and os.path.exists(
                    os.path.join(self.neard_home, 'setup', 'genesis.json')):
            if os.path.exists(os.path.join(self.neard_home, 'setup', 'data')):
                logging.warning(
                    f'found both records.json and data/ in {os.path.join(self.neard_home, "setup")}'
                )
            return True
        if self.is_traffic_generator():
            target_dir = os.path.join(self.neard_home, 'target', 'setup')
        else:
            target_dir = os.path.join(self.neard_home, 'setup')
        if os.path.exists(target_dir) and os.path.exists(
                os.path.join(target_dir, 'data')) and os.path.exists(
                    os.path.join(target_dir, 'config.json')):
            return False
        sys.exit(
            f'did not find either records.json and genesis.json in {os.path.join(self.neard_home, "setup")} or neard home in {target_dir}'
        )

    def is_traffic_generator(self):
        return self.config.get('is_traffic_generator', False)

    def save_data(self):
        with open(self.home_path('data.json'), 'w') as f:
            json.dump(self.data, f, indent=2)

    def save_config(self):
        with open(self.home_path('config.json'), 'w') as f:
            json.dump(self.config, f, indent=2)

    def parse_binaries_config(self):
        if 'binaries' not in self.config:
            raise ValueError('config does not have a "binaries" section')

        if not isinstance(self.config['binaries'], list):
            raise ValueError('config "binaries" section not a list')

        if len(self.config['binaries']) == 0:
            raise ValueError('no binaries in the config')

        self.config['binaries'].sort(key=lambda x: x['epoch_height'])
        last_epoch_height = -1

        binaries = []
        for i, b in enumerate(self.config['binaries']):
            epoch_height = b['epoch_height']
            if not isinstance(epoch_height, int) or epoch_height < 0:
                raise ValueError(f'bad epoch height in config: {epoch_height}')
            if last_epoch_height == -1:
                if epoch_height != 0:
                    # TODO: maybe it could make sense to allow this, meaning don't run any binary
                    # on this node until the network reaches that epoch, then we bring this node online
                    raise ValueError(
                        f'config should contain one binary with epoch_height 0')
            else:
                if epoch_height == last_epoch_height:
                    raise ValueError(
                        f'repeated epoch height in config: {epoch_height}')
            last_epoch_height = epoch_height
            binaries.append({
                'url': b['url'],
                'epoch_height': b['epoch_height'],
                'system_path': self.home_path('binaries', f'neard{i}')
            })
        return binaries

    def set_current_neard_path(self, path):
        self.data['current_neard_path'] = path

    def reset_current_neard_path(self):
        self.set_current_neard_path(self.data['binaries'][0]['system_path'])

    # tries to download the binaries specified in config.json, saving them in $home/binaries/
    # if force is set to true all binaries will be downloaded, otherwise only the missing ones
    def download_binaries(self, force):
        binaries = self.parse_binaries_config()

        try:
            os.mkdir(self.home_path('binaries'))
        except FileExistsError:
            pass

        if force:
            # always start from start_index = 0 and download all binaries
            self.data['binaries'] = []

        # start at the index of the first missing binary
        # typically it's all or nothing
        start_index = len(self.data['binaries'])

        # for now we assume that the binaries recorded in data.json as having been
        # dowloaded are still valid and were not touched. Also this assumes that their
        # filenames are neard0, neard1, etc. in the right order and with nothing skipped
        for i in range(start_index, len(binaries)):
            b = binaries[i]
            logging.info(f'downloading binary from {b["url"]}')
            with open(b['system_path'], 'wb') as f:
                r = requests.get(b['url'], stream=True)
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            os.chmod(b['system_path'], 0o755)
            logging.info(f'downloaded binary from {b["url"]}')

            self.data['binaries'].append(b)
            if self.data['current_neard_path'] is None:
                self.reset_current_neard_path()
            self.save_data()

    def setup_path(self, *args):
        if not self.is_traffic_generator() or self.legacy_records:
            args = ('setup',) + args
        else:
            args = (
                'target',
                'setup',
            ) + args
        return os.path.join(self.neard_home, *args)

    def target_near_home_path(self, *args):
        if self.is_traffic_generator():
            args = ('target',) + args
        return os.path.join(self.neard_home, *args)

    def home_path(self, *args):
        return os.path.join(self.home, *args)

    def tmp_near_home_path(self, *args):
        args = ('tmp-near-home',) + args
        return os.path.join(self.home, *args)

    def neard_init(self, rpc_port, protocol_port, validator_id):
        # We make neard init save files to self.tmp_near_home_path() just to make it
        # a bit cleaner, so we can init to a non-existent directory and then move
        # the files we want to the real near home without having to remove it first
        cmd = [
            self.data['binaries'][0]['system_path'], '--home',
            self.tmp_near_home_path(), 'init'
        ]
        if not self.is_traffic_generator():
            if validator_id is None:
                validator_id = f'{socket.gethostname()}.near'
            cmd += ['--account-id', validator_id]
        else:
            if validator_id is not None:
                logging.warning(
                    f'ignoring validator ID "{validator_id}" for traffic generator node'
                )
        subprocess.check_call(cmd)

        with open(self.tmp_near_home_path('config.json'), 'r') as f:
            config = json.load(f)
        config['rpc']['addr'] = f'0.0.0.0:{rpc_port}'
        config['network']['addr'] = f'0.0.0.0:{protocol_port}'
        self.data['neard_addr'] = config['rpc']['addr']
        config['tracked_shards'] = [0, 1, 2, 3]
        config['log_summary_style'] = 'plain'
        config['network']['skip_sync_wait'] = False
        if self.legacy_records:
            config['genesis_records_file'] = 'records.json'
        config['rpc']['enable_debug_rpc'] = True
        config['consensus']['min_block_production_delay']['secs'] = 1
        config['consensus']['min_block_production_delay']['nanos'] = 300000000
        config['consensus']['max_block_production_delay']['secs'] = 3
        config['consensus']['max_block_production_delay']['nanos'] = 0
        if self.is_traffic_generator():
            config['archive'] = True
        with open(self.tmp_near_home_path('config.json'), 'w') as f:
            json.dump(config, f, indent=2)

    def reset_starting_data_dir(self):
        try:
            shutil.rmtree(self.target_near_home_path('data'))
        except FileNotFoundError:
            pass
        if not self.legacy_records:
            cmd = [
                self.data['binaries'][0]['system_path'],
                '--home',
                self.setup_path(),
                'database',
                'run-migrations',
            ]
            logging.info(f'running {" ".join(cmd)}')
            subprocess.check_call(cmd)
            cmd = [
                self.data['binaries'][0]['system_path'],
                '--home',
                self.setup_path(),
                'database',
                'make-snapshot',
                '--destination',
                self.target_near_home_path(),
            ]
            logging.info(f'running {" ".join(cmd)}')
            subprocess.check_call(cmd)

    def move_init_files(self):
        try:
            os.mkdir(self.target_near_home_path())
        except FileExistsError:
            pass
        for p in os.listdir(self.target_near_home_path()):
            filename = self.target_near_home_path(p)
            if os.path.isfile(filename):
                os.remove(filename)
        self.reset_starting_data_dir()

        paths = ['config.json', 'node_key.json']
        if not self.is_traffic_generator():
            paths.append('validator_key.json')
        for path in paths:
            shutil.move(self.tmp_near_home_path(path),
                        self.target_near_home_path(path))
        if not self.legacy_records:
            shutil.copyfile(self.setup_path('genesis.json'),
                            self.target_near_home_path('genesis.json'))

    # This RPC method tells to stop neard and re-initialize its home dir. This returns the
    # validator and node key that resulted from the initialization. We can't yet call amend-genesis
    # and compute state roots, because the caller of this method needs to hear back from
    # each node before it can build the list of initial validators. So after this RPC method returns,
    # we'll be waiting for the network_init RPC.
    # TODO: add a binaries argument that tells what binaries we want to use in the test. Before we do
    # this, it is pretty mandatory to implement some sort of client authentication, because without it,
    # anyone would be able to get us to download and run arbitrary code
    def do_new_test(self,
                    rpc_port=3030,
                    protocol_port=24567,
                    validator_id=None):
        if not isinstance(rpc_port, int):
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='rpc_port argument not an int')
        if not isinstance(protocol_port, int):
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='protocol_port argument not an int')
        if validator_id is not None and not isinstance(validator_id, str):
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='validator_id argument not a string')

        with self.lock:
            self.kill_neard()
            try:
                shutil.rmtree(self.tmp_near_home_path())
            except FileNotFoundError:
                pass
            try:
                os.remove(self.home_path('validators.json'))
            except FileNotFoundError:
                pass
            try:
                os.remove(self.home_path('network_init.json'))
            except FileNotFoundError:
                pass

            self.neard_init(rpc_port, protocol_port, validator_id)
            self.move_init_files()

            with open(self.target_near_home_path('config.json'), 'r') as f:
                config = json.load(f)
            with open(self.target_near_home_path('node_key.json'), 'r') as f:
                node_key = json.load(f)
            if not self.is_traffic_generator():
                with open(self.target_near_home_path('validator_key.json'),
                          'r') as f:
                    validator_key = json.load(f)
                    validator_account_id = validator_key['account_id']
                    validator_public_key = validator_key['public_key']
            else:
                validator_account_id = None
                validator_public_key = None

            self.data['backups'] = {}
            self.set_state(TestState.AWAITING_NETWORK_INIT)
            self.save_data()

            self.configure_log_config()

            return {
                'validator_account_id': validator_account_id,
                'validator_public_key': validator_public_key,
                'node_key': node_key['public_key'],
                'listen_port': config['network']['addr'].split(':')[1],
            }

    # After the new_test RPC, we wait to get this RPC that gives us the list of validators
    # and boot nodes for the test network. After this RPC call, we run amend-genesis and
    # start neard to compute genesis state roots.
    def do_network_init(self,
                        validators,
                        boot_nodes,
                        epoch_length=1000,
                        num_seats=100,
                        new_chain_id=None,
                        protocol_version=None,
                        genesis_time=None):
        if not isinstance(validators, list):
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='validators argument not a list')
        if not isinstance(boot_nodes, list):
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='boot_nodes argument not a list')

        # TODO: maybe also check validity of these arguments?
        if len(validators) == 0:
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='validators argument must not be empty')
        if len(boot_nodes) == 0:
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600, message='boot_nodes argument must not be empty')

        if not self.legacy_records and genesis_time is None:
            raise jsonrpc.exceptions.JSONRPCDispatchException(
                code=-32600,
                message=
                'genesis_time argument required for nodes running via neard fork-network'
            )

        with self.lock:
            state = self.get_state()
            if state != TestState.AWAITING_NETWORK_INIT:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600,
                    message='Can only call network_init after a call to init')

            if len(validators) < 3:
                with open(self.target_near_home_path('config.json'), 'r') as f:
                    config = json.load(f)
                config['consensus']['min_num_peers'] = len(validators) - 1
                with open(self.target_near_home_path('config.json'), 'w') as f:
                    json.dump(config, f)
            with open(self.home_path('validators.json'), 'w') as f:
                json.dump(validators, f)
            with open(self.home_path('network_init.json'), 'w') as f:
                json.dump(
                    {
                        'boot_nodes': boot_nodes,
                        'epoch_length': epoch_length,
                        'num_seats': num_seats,
                        'new_chain_id': new_chain_id,
                        'protocol_version': protocol_version,
                        'genesis_time': genesis_time,
                    }, f)

    def do_update_config(self, key_value):
        with self.lock:
            logging.info(f'updating config with {key_value}')
            with open(self.target_near_home_path('config.json'), 'r') as f:
                config = json.load(f)

            [key, value] = key_value.split("=", 1)
            key_item_list = key.split(".")

            object = config
            for key_item in key_item_list[:-1]:
                if key_item not in object:
                    object[key_item] = {}
                object = object[key_item]

            value = json.loads(value)

            object[key_item_list[-1]] = value

            with open(self.target_near_home_path('config.json'), 'w') as f:
                json.dump(config, f, indent=2)

        return True

    def do_start(self, batch_interval_millis=None):
        if batch_interval_millis is not None and not isinstance(
                batch_interval_millis, int):
            raise ValueError(
                f'batch_interval_millis: {batch_interval_millis} not an int')
        with self.lock:
            state = self.get_state()
            if state == TestState.STOPPED:
                if batch_interval_millis is not None and not self.is_traffic_generator(
                ):
                    logging.warn(
                        f'got batch_interval_millis = {batch_interval_millis} on non traffic generator node. Ignoring it.'
                    )
                    batch_interval_millis = None
                # TODO: restart it if we get a different batch_interval_millis than last time
                self.start_neard(batch_interval_millis)
                self.set_state(TestState.RUNNING)
                self.save_data()
            elif state != TestState.RUNNING:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600,
                    message=
                    'Cannot start node as test state has not been initialized yet'
                )

    # right now only has an effect if the test setup has been initialized. Should it also mean stop setting up
    # the test if we're in the middle of initializing it?
    def do_stop(self):
        with self.lock:
            state = self.get_state()
            if state == TestState.RUNNING:
                self.kill_neard()
                self.set_state(TestState.STOPPED)
                self.save_data()

    def do_reset(self, backup_id=None):
        with self.lock:
            state = self.get_state()
            logging.info(f"do_reset {state}")
            if state != TestState.RUNNING and state != TestState.STOPPED:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600,
                    message='Cannot reset data dir as test state is not ready')

            backups = self.data.get('backups', {})
            if backup_id is not None and backup_id != 'start' and backup_id not in backups:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600, message=f'backup ID {backup_id} not known')

            if backup_id is None or backup_id == 'start':
                path = self.data['binaries'][0]['system_path']
            else:
                path = backups[backup_id]['neard_path']

            if state == TestState.RUNNING:
                self.kill_neard()
            self.set_state(TestState.RESETTING, data=backup_id)
            self.set_current_neard_path(path)
            self.save_data()

    def do_make_backup(self, backup_id, description=None):
        with self.lock:
            state = self.get_state()
            if state != TestState.RUNNING and state != TestState.STOPPED:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600,
                    message='Cannot make backup as test state is not ready')

            if backup_id_pattern.match(backup_id) is None:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600, message=f'invalid backup ID: {backup_id}')

            if backup_id in self.data.get('backups', {}):
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600, message=f'backup {backup_id} already exists')
            if state == TestState.RUNNING:
                self.kill_neard()
            self.making_backup(backup_id, description)
            self.save_data()

    def do_ls_backups(self):
        with self.lock:
            return self.data.get('backups', {})

    # Updates the URL for the given epoch height or binary idx. adds a new one if the epoch height does not exit
    def update_binaries_url(self, neard_binary_url, epoch_height, binary_idx):
        if neard_binary_url is not None and ((epoch_height is None)
                                             != (binary_idx is None)):
            logging.info(
                f'Updating binary list for height:{epoch_height} or idx:{binary_idx} with '
                f'url: {neard_binary_url}')
        else:
            logging.error(
                f'Update binaries failed. Wrong params: url: {neard_binary_url}, height:{epoch_height}, idx:{binary_idx}'
            )
            raise jsonrpc.exceptions.JSONRPCInvalidParams()

        if 'binaries' not in self.config:
            self.config['binaries'] = []

        if not isinstance(self.config['binaries'], list):
            self.config['binaries'] = []

        if epoch_height is not None:
            binary = next((b for b in self.config['binaries']
                           if b['epoch_height'] == epoch_height), None)
            if binary:
                binary['url'] = neard_binary_url
            else:
                self.config['binaries'].append({
                    'url': neard_binary_url,
                    'epoch_height': epoch_height
                })
                self.config['binaries'].sort(
                    key=lambda binary: binary['epoch_height'])
        if binary_idx is not None:
            binaries_number = len(self.config['binaries'])
            if binary_idx >= binaries_number:
                logging.error(
                    f'idx {binary_idx} is out of bounds for the binary list of length {binaries_number}'
                )
                raise jsonrpc.exceptions.JSONRPCInvalidParams(
                    message=
                    f'Invalid binary idx. Out of bounds for list of length {binaries_number}'
                )
            self.config['binaries'][binary_idx]['url'] = neard_binary_url

    def do_update_binaries(self, neard_binary_url, epoch_height, binary_idx):
        with self.lock:
            logging.info('update binaries')
            if any(arg is not None
                   for arg in [neard_binary_url, epoch_height, binary_idx]):
                self.update_binaries_url(neard_binary_url, epoch_height,
                                         binary_idx)
                self.save_config()

            try:
                self.download_binaries(force=True)
            except ValueError as e:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32603,
                    message=f'Internal error downloading binaries: {e}')
                self.set_state(TestState.ERROR)
                self.save_data()
            logging.info('update binaries finished')

    def do_version(self):
        if self.legacy_records:
            node_setup_version = '0'
        else:
            node_setup_version = '1'
        return {'node_setup_version': node_setup_version}

    def do_ready(self):
        with self.lock:
            state = self.get_state()
            return state == TestState.RUNNING or state == TestState.STOPPED

    def do_clear_env(self):
        with self.lock:
            env_file_path = self.home_path('.env')
            open(env_file_path, 'w').close()
            print(f'File {env_file_path} has been successfully cleared.')

    def do_add_env(self, key_values):
        with self.lock:
            env_file_path = self.home_path('.env')
            # Create the file if it does not exit
            open(env_file_path, 'a').close()
            for key_value in key_values:
                logging.info(f'Updating env with {key_value}')
                [key, value] = key_value.split("=", 1)
                dotenv.set_key(env_file_path, key, value)

    # check the current epoch height, and return the binary path that we should
    # be running given the epoch heights specified in config.json
    # TODO: should we update it at a random time in the middle of the
    # epoch instead of the beginning?
    def wanted_neard_path(self):
        j = {
            'method': 'validators',
            'params': [None],
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        try:
            r = requests.post(f'http://{self.data["neard_addr"]}',
                              json=j,
                              timeout=5)
            r.raise_for_status()
            response = json.loads(r.content)
            epoch_height = response['result']['epoch_height']
            path = self.data['binaries'][0]['system_path']
            for b in self.data['binaries']:
                # this logic assumes the binaries are sorted by epoch height
                if b['epoch_height'] <= epoch_height:
                    path = b['system_path']
                else:
                    break
            return path
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout, KeyError):
            return self.data['current_neard_path']

    def run_neard(self, cmd, out_file=None):
        assert (self.neard is None)
        assert (self.data['neard_process'] is None)
        home_path = os.path.expanduser('~')
        env = {
            **os.environ,  # override loaded values with environment variables
            **dotenv.dotenv_values(os.path.join(home_path, '.secrets')),  # load sensitive variables
            **dotenv.dotenv_values(self.home_path('.env')),  # load neard variables
        }
        logging.info(f'running {" ".join(cmd)}')
        self.neard = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=out_file,
            stderr=out_file,
            env=env,
        )
        # we save the create_time so we can tell if the process with pid equal
        # to the one we saved is the same process. It's not that likely, but
        # if we don't do this (or maybe something else like it), then it's possble
        # that if this process is killed and restarted and we see a process with that PID,
        # it could actually be a different process that was started later after neard exited
        try:
            create_time = int(psutil.Process(self.neard.pid).create_time())
        except psutil.NoSuchProcess:
            # not that likely, but if it already exited, catch the exception so
            # at least this process doesn't die
            create_time = 0

        self.data['neard_process'] = {
            'pid': self.neard.pid,
            'create_time': create_time,
            'path': cmd[0],
        }
        self.save_data()

    def poll_neard(self):
        if self.neard is not None:
            code = self.neard.poll()
            path = self.data['neard_process']['path']
            running = code is None
            if not running:
                self.neard = None
                self.data['neard_process'] = None
                self.save_data()
            return path, running, code
        elif self.data['neard_process'] is not None:
            path = self.data['neard_process']['path']
            # we land here if this process previously died and is now restarted,
            # and the old neard process is still running
            try:
                p = psutil.Process(self.data['neard_process']['pid'])
                if int(p.create_time()
                      ) == self.data['neard_process']['create_time']:
                    return path, True, None
            except psutil.NoSuchProcess:
                self.neard = None
                self.data['neard_process'] = None
                self.save_data()
                return path, False, None
        else:
            return None, False, None

    def kill_neard(self):
        if self.neard is not None:
            logging.info('stopping neard')
            self.neard.send_signal(signal.SIGINT)
            self.neard.wait()
            self.neard = None
            self.data['neard_process'] = None
            self.save_data()
            return

        if self.data['neard_process'] is None:
            return

        try:
            p = psutil.Process(self.data['neard_process']['pid'])
            if int(p.create_time()
                  ) == self.data['neard_process']['create_time']:
                logging.info('stopping neard')
                p.send_signal(signal.SIGINT)
                p.wait()
        except psutil.NoSuchProcess:
            pass
        self.neard = None
        self.data['neard_process'] = None
        self.save_data()

    def source_near_home_path(self):
        if not self.is_traffic_generator():
            logging.warn(
                'source_near_home_path() called on non-traffic-generator node')
            return self.neard_home
        if self.legacy_records:
            return self.neard_home
        else:
            return os.path.join(self.neard_home, 'source')

    # If this is a regular node, starts neard run. If it's a traffic generator, starts neard mirror run
    def start_neard(self, batch_interval_millis=None):
        out_path = os.path.join(self.neard_logs_dir, self.neard_logs_file_name)
        with open(out_path, 'ab') as out:
            if self.is_traffic_generator():
                cmd = self.get_start_traffic_generator_cmd(
                    batch_interval_millis)
            else:
                cmd = self.get_start_cmd()

            self.run_neard(
                cmd,
                out_file=out,
            )
            self.last_start = time.time()

    # Configure the logs config file to control the level of rust and opentelemetry logs.
    # Default config sets level to DEBUG for "client" and "chain" logs, WARN for tokio+actix, and INFO for everything else.
    def configure_log_config(self):
        default_log_filter = 'client=debug,chain=debug,mirror=debug,actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn,info'
        log_config_path = self.target_near_home_path('log_config.json')

        logging.info("Creating log_config.json with default log filter.")
        with open(log_config_path, 'w') as log_config_file:
            config_json = {
                'opentelemetry': default_log_filter,
                'rust_log': default_log_filter,
            }
            json.dump(config_json, log_config_file, indent=2)

    # Get the command to start neard for regular nodes.
    # For starting the traffic generator see the get_start_traffic_generator_cmd.
    def get_start_cmd(self):
        cmd = [
            self.data['current_neard_path'],
            '--log-span-events',
            '--home',
            self.neard_home,
            '--unsafe-fast-startup',
            'run',
        ]

        return cmd

    # Get the command to start neard for traffic generator node.
    def get_start_traffic_generator_cmd(self, batch_interval_millis):
        cmd = [
            self.data['current_neard_path'],
            'mirror',
            'run',
            '--source-home',
            self.source_near_home_path(),
            '--target-home',
            self.target_near_home_path(),
        ]
        if os.path.exists(self.setup_path('mirror-secret.json')):
            cmd.append('--secret-file')
            cmd.append(self.setup_path('mirror-secret.json'))
        else:
            cmd.append('--no-secret')

        if batch_interval_millis is not None:
            secs = batch_interval_millis // 1000
            nanos = (batch_interval_millis % 1000) * 1000000
            config_json = {'tx_batch_interval': {'secs': secs, 'nanos': nanos}}

            mirror_config_path = self.target_near_home_path(
                'mirror-config.json')
            with open(mirror_config_path, 'w') as f:
                json.dump(config_json, f, indent=2)

            cmd.append('--config-path')
            cmd.append(self.target_near_home_path('mirror-config.json'))

        return cmd

    # returns a bool that tells whether we should attempt a restart
    def on_neard_died(self):
        if self.is_traffic_generator():
            # TODO: This is just a lazy way to deal with the fact
            # that the mirror command is expected to exit after it finishes sending all the traffic.
            # For now just don't restart neard on the traffic generator. Here we should be smart
            # about restarting only if it makes sense, and we also shouldn't restart over and over.
            # Right now we can't just check the exit_code because there's a bug that makes the
            # neard mirror run command not exit cleanly when it's finished
            return False

        now = time.time()
        if now - self.last_start > 600:
            self.num_restarts = 1
            return True
        if self.num_restarts >= 5:
            self.set_state(TestState.STOPPED)
            self.save_data()
            return False
        else:
            self.num_restarts += 1
            return True

    def check_upgrade_neard(self):
        neard_path = self.wanted_neard_path()

        path, running, exit_code = self.poll_neard()
        if path is None:
            start_neard = self.on_neard_died()
        elif not running:
            if exit_code is not None:
                logging.info(f'neard exited with code {exit_code}.')
            start_neard = self.on_neard_died()
        else:
            if path == neard_path:
                start_neard = False
            else:
                logging.info('upgrading neard upon new epoch')
                self.kill_neard()
                start_neard = True

        if start_neard:
            self.set_current_neard_path(neard_path)
            self.start_neard()

    def get_state(self):
        return TestState(self.data['state'])

    def set_state(self, state, data=None):
        self.data['state'] = state.value
        self.data['state_data'] = data

    def making_backup(self, backup_id, description=None):
        backup_data = {'backup_id': backup_id, 'description': description}
        self.set_state(TestState.MAKING_BACKUP, data=backup_data)

    def network_init(self):
        # wait til we get a network_init RPC
        if not os.path.exists(self.home_path('validators.json')):
            return

        with open(self.home_path('network_init.json'), 'r') as f:
            n = json.load(f)
        with open(self.target_near_home_path('node_key.json'), 'r') as f:
            node_key = json.load(f)
        with open(self.target_near_home_path('config.json'), 'r') as f:
            config = json.load(f)
        boot_nodes = []
        for b in n['boot_nodes']:
            if node_key['public_key'] != b.split('@')[0]:
                boot_nodes.append(b)

        config['network']['boot_nodes'] = ','.join(boot_nodes)
        with open(self.target_near_home_path('config.json'), 'w') as f:
            config = json.dump(config, f, indent=2)

        new_chain_id = n.get('new_chain_id')

        if self.legacy_records:
            cmd = [
                self.data['binaries'][0]['system_path'],
                'amend-genesis',
                '--genesis-file-in',
                self.setup_path('genesis.json'),
                '--records-file-in',
                self.setup_path('records.json'),
                '--genesis-file-out',
                self.target_near_home_path('genesis.json'),
                '--records-file-out',
                self.target_near_home_path('records.json'),
                '--validators',
                self.home_path('validators.json'),
                '--chain-id',
                new_chain_id if new_chain_id is not None else 'mocknet',
                '--transaction-validity-period',
                '10000',
                '--epoch-length',
                str(n['epoch_length']),
                '--num-seats',
                str(n['num_seats']),
                '--protocol-reward-rate',
                '1/10',
            ]
            if n['protocol_version'] is not None:
                cmd.append('--protocol-version')
                cmd.append(str(n['protocol_version']))

            self.run_neard(cmd)
            self.set_state(TestState.AMEND_GENESIS)
        else:
            cmd = [
                self.data['binaries'][0]['system_path'], '--home',
                self.target_near_home_path(), 'fork-network', 'set-validators',
                '--validators',
                self.home_path('validators.json'), '--epoch-length',
                str(n['epoch_length']), '--genesis-time',
                str(n['genesis_time'])
            ]
            if new_chain_id is not None:
                cmd.append('--chain-id')
                cmd.append(new_chain_id)
            else:
                cmd.append('--chain-id-suffix')
                cmd.append('_mocknet')

            if n['protocol_version'] is not None:
                cmd.append('--protocol-version')
                cmd.append(str(n['protocol_version']))

            self.run_neard(cmd)
            self.set_state(TestState.SET_VALIDATORS)
        self.save_data()

    def check_set_validators(self):
        path, running, exit_code = self.poll_neard()
        if path is None:
            logging.error(
                'state is SET_VALIDATORS, but no amend-genesis process is known'
            )
            self.set_state(TestState.AWAITING_NETWORK_INIT)
            self.save_data()
        elif not running:
            if exit_code is not None and exit_code != 0:
                logging.error(
                    f'neard fork-network set-validators exited with code {exit_code}'
                )
                # for now just set the state to ERROR, and if this ever happens, the
                # test operator will have to intervene manually. Probably shouldn't
                # really happen in practice
                self.set_state(TestState.ERROR)
                self.save_data()
            else:
                cmd = [
                    self.data['binaries'][0]['system_path'],
                    '--home',
                    self.target_near_home_path(),
                    'fork-network',
                    'finalize',
                ]
                logging.info(f'running {" ".join(cmd)}')
                subprocess.check_call(cmd)
                logging.info(
                    f'neard fork-network finalize succeeded. Node is ready')
                self.make_initial_backup()

    def check_amend_genesis(self):
        path, running, exit_code = self.poll_neard()
        if path is None:
            logging.error(
                'state is AMEND_GENESIS, but no amend-genesis process is known')
            self.set_state(TestState.AWAITING_NETWORK_INIT)
            self.save_data()
        elif not running:
            if exit_code is not None and exit_code != 0:
                logging.error(
                    f'neard amend-genesis exited with code {exit_code}')
                # for now just set the state to ERROR, and if this ever happens, the
                # test operator will have to intervene manually. Probably shouldn't
                # really happen in practice
                self.set_state(TestState.ERROR)
                self.save_data()
            else:
                # TODO: if exit_code is None then we were interrupted and restarted after starting
                # the amend-genesis command. We assume here that the command was successful. Ok for now since
                # the command probably won't fail. But should somehow check that it was OK

                logging.info('setting use_production_config to true')
                genesis_path = self.target_near_home_path('genesis.json')
                with open(genesis_path, 'r') as f:
                    genesis_config = json.load(f)
                with open(genesis_path, 'w') as f:
                    genesis_config['use_production_config'] = True
                    # with the normal min_gas_price (10x higher than this one)
                    # many mirrored mainnet transactions fail with too little balance
                    # One way to fix that would be to increase everybody's balances in
                    # the amend-genesis command. But we can also just make this change here.
                    genesis_config['min_gas_price'] = 10000000
                    # protocol_versions in range [56, 63] need to have these
                    # genesis parameters, otherwise nodes get stuck because at
                    # some point it produces an incompatible EpochInfo.
                    # TODO: remove these changes once mocknet tests will probably
                    # only ever be run with binaries including https://github.com/near/nearcore/pull/10722
                    genesis_config['num_block_producer_seats'] = 100
                    genesis_config['num_block_producer_seats_per_shard'] = [
                        100, 100, 100, 100
                    ]
                    genesis_config['block_producer_kickout_threshold'] = 80
                    genesis_config['chunk_producer_kickout_threshold'] = 80
                    genesis_config['shard_layout'] = {
                        'V1': {
                            'boundary_accounts': [
                                'aurora', 'aurora-0',
                                'kkuuue2akv_1630967379.near'
                            ],
                            'shards_split_map': [[0, 1, 2, 3]],
                            'to_parent_shard_map': [0, 0, 0, 0],
                            'version': 1
                        }
                    }
                    genesis_config['num_chunk_only_producer_seats'] = 200
                    genesis_config['max_kickout_stake_perc'] = 30
                    json.dump(genesis_config, f, indent=2)
                initlog_path = os.path.join(self.neard_logs_dir, 'initlog.txt')
                with open(initlog_path, 'ab') as out:
                    cmd = [
                        self.data['binaries'][0]['system_path'],
                        '--home',
                        self.target_near_home_path(),
                        '--unsafe-fast-startup',
                        'run',
                    ]
                    self.run_neard(
                        cmd,
                        out_file=out,
                    )
                self.set_state(TestState.STATE_ROOTS)
                self.save_data()

    def make_backup(self):
        now = str(datetime.datetime.now())
        backup_data = self.data['state_data']
        name = backup_data['backup_id']
        description = backup_data.get('description', None)

        backup_dir = self.home_path('backups', name)
        if os.path.exists(backup_dir):
            # we already checked that this backup ID didn't already exist, so if this path
            # exists, someone probably manually added it. for now just set the state to ERROR
            # and make the human intervene, but it shouldn't happen in practice
            logging.warn(f'{backup_dir} already exists')
            self.set_state(TestState.ERROR)
            return
        # Make a RockDB snapshot of the db to save space. This takes advantage of the immutability of sst files.
        logging.info(f'copying data dir to {backup_dir}')
        cmd = [
            self.data['current_neard_path'], '--home',
            self.target_near_home_path(), '--unsafe-fast-startup', 'database',
            'make-snapshot', '--destination', backup_dir
        ]
        logging.info(f'running {" ".join(cmd)}')
        exit_code = subprocess.check_call(cmd)
        logging.info(
            f'copying data dir to {backup_dir} finished with code {exit_code}')
        # Copy config, genesis and node_key to the backup folder to use them to restore the db.
        for file in ["config.json", "genesis.json", "node_key.json"]:
            shutil.copyfile(self.target_near_home_path(file),
                            os.path.join(backup_dir, file))
        # Binaries can be replaced during a test.
        # Save the binary that was used to make the backup to prevent db version incompatibility when restoring the backup.
        neard_path = os.path.join(backup_dir, "neard")
        shutil.copy2(self.data['current_neard_path'], neard_path)

        backups = self.data.get('backups', {})
        if name in backups:
            # shouldn't happen if we check this in do_make_backups(), but fine to be paranoid and at least warn here
            logging.warn(
                f'backup {name} already existed in data.json, but it was not present before'
            )
        backups[name] = {
            'time': now,
            'description': description,
            'neard_path': self.data['current_neard_path']
        }
        self.data['backups'] = backups
        self.set_state(TestState.STOPPED)
        self.save_data()

    def make_initial_backup(self):
        try:
            shutil.rmtree(self.home_path('backups'))
        except FileNotFoundError:
            pass
        os.mkdir(self.home_path('backups'))
        self.making_backup(
            'start',
            description='initial test state after state root computation')
        self.save_data()
        self.make_backup()

    def check_genesis_state(self):
        path, running, exit_code = self.poll_neard()
        if not running:
            logging.error(
                f'neard exited with code {exit_code} on the first run')
            # For now just set the state to ERROR, because if this happens, there is something pretty wrong with
            # the setup, so a human needs to investigate and fix the bug
            self.set_state(TestState.ERROR)
            self.save_data()
        try:
            r = requests.get(f'http://{self.data["neard_addr"]}/status',
                             timeout=5)
        except requests.exceptions.ConnectionError:
            return
        if r.status_code == 200:
            logging.info('neard finished computing state roots')
            self.kill_neard()
            self.make_initial_backup()

    def run_restore_from_backup_cmd(self, backup_path):
        logging.info(f'restoring data dir from backup at {backup_path}')
        neard_path = os.path.join(backup_path, "neard")
        cmd = [
            neard_path, '--home', backup_path, '--unsafe-fast-startup',
            'database', 'make-snapshot', '--destination',
            self.target_near_home_path()
        ]
        logging.info(f'running {" ".join(cmd)}')
        exit_code = subprocess.check_call(cmd)
        logging.info(
            f'snapshot restoration of {backup_path} terminated with code {exit_code}'
        )

    def reset_near_home(self):
        backup_id = self.data['state_data']
        if backup_id is None:
            backup_id = 'start'
        backup_path = self.home_path('backups', backup_id)
        if not os.path.exists(backup_path):
            logging.error(f'backup dir {backup_path} does not exist')
            self.set_state(TestState.ERROR)
            self.save_data()
        try:
            logging.info("removing the old directory")
            shutil.rmtree(self.target_near_home_path('data'))
        except FileNotFoundError:
            pass
        self.run_restore_from_backup_cmd(backup_path)
        self.set_state(TestState.STOPPED)
        self.save_data()

    # periodically check if we should update neard after a new epoch
    def main_loop(self):
        while True:
            with self.lock:
                state = self.get_state()
                if state == TestState.AWAITING_NETWORK_INIT:
                    self.network_init()
                elif state == TestState.AMEND_GENESIS:
                    self.check_amend_genesis()
                elif state == TestState.SET_VALIDATORS:
                    self.check_set_validators()
                elif state == TestState.STATE_ROOTS:
                    self.check_genesis_state()
                elif state == TestState.RUNNING:
                    self.check_upgrade_neard()
                elif state == TestState.RESETTING:
                    self.reset_near_home()
                elif state == TestState.MAKING_BACKUP:
                    self.make_backup()
                self.run_logrotate()
            time.sleep(10)

    def serve(self, port):
        # TODO: maybe use asyncio? kind of silly to use multiple threads for
        # something so lightweight
        main_loop = threading.Thread(target=self.main_loop)
        main_loop.start()
        # this will listen only on the loopback interface and won't be accessible
        # over the internet. If connecting to another machine, we can SSH and then make
        # the request locally
        s = RpcServer(('localhost', port), self)
        s.serve_forever()


def main():
    parser = argparse.ArgumentParser(description='run neard')
    parser.add_argument('--home', type=str, required=True)
    parser.add_argument('--neard-home', type=str, required=True)
    parser.add_argument('--neard-logs-dir', type=str, required=True)
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)

    config_path = os.path.join(args.home, 'config.json')
    if not os.path.isdir(args.home) or not os.path.exists(config_path):
        sys.exit(
            f'please create the directory at {args.home} and write a config file at {config_path}'
        )

    # only let one instance of this code run at a time
    _fd = get_lock(args.home)

    logging.info("creating neard runner")
    runner = NeardRunner(args)

    logging.info("downloading binaries")
    with runner.lock:
        runner.download_binaries(force=False)

    logging.info("serve")
    runner.serve(args.port)


if __name__ == '__main__':
    main()

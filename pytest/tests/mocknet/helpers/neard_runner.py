# TODO: reimplement this logic using standard tooling like systemd instead of relying on this
# python script to handle neard process management.

import argparse
from enum import Enum
import fcntl
import json
import jsonrpc
import logging
import os
import psutil
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


def get_lock(home):
    lock_file = os.path.join(home, 'LOCK')

    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise Exception(f'{lock_file} is currently locked by another process')
    return fd


def http_code(jsonrpc_error):
    if jsonrpc_error is None:
        return http.HTTPStatus.OK

    if jsonrpc_error['code'] == -32700 or jsonrpc_error[
            'code'] == -32600 or jsonrpc_error['code'] == -32602:
        return http.HTTPStatus.BAD_REQUEST
    elif jsonrpc_error['code'] == -32601:
        return http.HTTPStatus.NOT_FOUND
    else:
        return http.HTTPStatus.INTERNAL_SERVER_ERROR


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
        self.dispatcher.add_method(server.neard_runner.do_start, name="start")
        self.dispatcher.add_method(server.neard_runner.do_stop, name="stop")
        self.dispatcher.add_method(server.neard_runner.do_reset, name="reset")
        self.dispatcher.add_method(server.neard_runner.do_update_binaries,
                                   name="update_binaries")
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

        self.send_response(http_code(response.error))
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


class NeardRunner:

    def __init__(self, args):
        self.home = args.home
        self.neard_home = args.neard_home
        self.neard_logs_dir = args.neard_logs_dir
        try:
            os.mkdir(self.neard_logs_dir)
        except FileExistsError:
            pass
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
            }
        # protects self.data, and its representation on disk,
        # because both the rpc server and the main loop touch them concurrently
        self.lock = threading.Lock()

    def is_traffic_generator(self):
        return self.config.get('is_traffic_generator', False)

    def save_data(self):
        with open(self.home_path('data.json'), 'w') as f:
            json.dump(self.data, f)

    def parse_binaries_config(self):
        if 'binaries' not in self.config:
            sys.exit('config does not have a "binaries" section')

        if not isinstance(self.config['binaries'], list):
            sys.exit('config "binaries" section not a list')

        if len(self.config['binaries']) == 0:
            sys.exit('no binaries in the config')

        self.config['binaries'].sort(key=lambda x: x['epoch_height'])
        last_epoch_height = -1

        binaries = []
        for i, b in enumerate(self.config['binaries']):
            epoch_height = b['epoch_height']
            if not isinstance(epoch_height, int) or epoch_height < 0:
                sys.exit(f'bad epoch height in config: {epoch_height}')
            if last_epoch_height == -1:
                if epoch_height != 0:
                    # TODO: maybe it could make sense to allow this, meaning don't run any binary
                    # on this node until the network reaches that epoch, then we bring this node online
                    sys.exit(
                        f'config should contain one binary with epoch_height 0')
            else:
                if epoch_height == last_epoch_height:
                    sys.exit(f'repeated epoch height in config: {epoch_height}')
            last_epoch_height = epoch_height
            binaries.append({
                'url': b['url'],
                'epoch_height': b['epoch_height'],
                'system_path': self.home_path('binaries', f'neard{i}')
            })
        return binaries

    def reset_current_neard_path(self):
        self.data['current_neard_path'] = self.data['binaries'][0][
            'system_path']

    # tries to download the binaries specified in config.json, saving them in $home/binaries/
    # if force is set to true all binaries will be downloaded, otherwise only the missing ones
    def download_binaries(self, force):
        binaries = self.parse_binaries_config()

        try:
            os.mkdir(self.home_path('binaries'))
        except FileExistsError:
            pass

        if force:
            # always start from 0 and download all binaries
            start_index = 0
        else:
            # start at the index of the first missing binary
            # typically it's all or nothing
            with self.lock:
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

            with self.lock:
                self.data['binaries'].append(b)
                if self.data['current_neard_path'] is None:
                    self.reset_current_neard_path()
                self.save_data()

    def target_near_home_path(self, *args):
        if self.is_traffic_generator():
            args = ('target',) + args
        return os.path.join(self.neard_home, *args)

    def home_path(self, *args):
        return os.path.join(self.home, *args)

    def tmp_near_home_path(self, *args):
        args = ('tmp-near-home',) + args
        return os.path.join(self.home, *args)

    def neard_init(self):
        # We make neard init save files to self.tmp_near_home_path() just to make it
        # a bit cleaner, so we can init to a non-existent directory and then move
        # the files we want to the real near home without having to remove it first
        cmd = [
            self.data['binaries'][0]['system_path'], '--home',
            self.tmp_near_home_path(), 'init'
        ]
        if not self.is_traffic_generator():
            cmd += ['--account-id', f'{socket.gethostname()}.near']
        subprocess.check_call(cmd)

        with open(self.tmp_near_home_path('config.json'), 'r') as f:
            config = json.load(f)
        self.data['neard_addr'] = config['rpc']['addr']
        config['tracked_shards'] = [0, 1, 2, 3]
        config['log_summary_style'] = 'plain'
        config['network']['skip_sync_wait'] = False
        config['genesis_records_file'] = 'records.json'
        config['rpc']['enable_debug_rpc'] = True
        if self.is_traffic_generator():
            config['archive'] = True
        with open(self.tmp_near_home_path('config.json'), 'w') as f:
            json.dump(config, f, indent=2)

    def move_init_files(self):
        try:
            os.mkdir(self.target_near_home_path())
        except FileExistsError:
            pass
        for p in os.listdir(self.target_near_home_path()):
            filename = self.target_near_home_path(p)
            if os.path.isfile(filename):
                os.remove(filename)
        try:
            shutil.rmtree(self.target_near_home_path('data'))
        except FileNotFoundError:
            pass
        paths = ['config.json', 'node_key.json']
        if not self.is_traffic_generator():
            paths.append('validator_key.json')
        for path in paths:
            shutil.move(self.tmp_near_home_path(path),
                        self.target_near_home_path(path))

    # This RPC method tells to stop neard and re-initialize its home dir. This returns the
    # validator and node key that resulted from the initialization. We can't yet call amend-genesis
    # and compute state roots, because the caller of this method needs to hear back from
    # each node before it can build the list of initial validators. So after this RPC method returns,
    # we'll be waiting for the network_init RPC.
    # TODO: add a binaries argument that tells what binaries we want to use in the test. Before we do
    # this, it is pretty mandatory to implement some sort of client authentication, because without it,
    # anyone would be able to get us to download and run arbitrary code
    def do_new_test(self):
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

            self.neard_init()
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

            self.set_state(TestState.AWAITING_NETWORK_INIT)
            self.save_data()

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
                        protocol_version=None):
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
                        'protocol_version': protocol_version,
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

    def do_start(self):
        with self.lock:
            state = self.get_state()
            if state == TestState.STOPPED:
                self.start_neard()
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

    def do_reset(self):
        with self.lock:
            state = self.get_state()
            logging.info(f"do_reset {state}")
            if state == TestState.RUNNING:
                self.kill_neard()
                self.set_state(TestState.RESETTING)
                self.reset_current_neard_path()
                self.save_data()
            elif state == TestState.STOPPED:
                self.set_state(TestState.RESETTING)
                self.reset_current_neard_path()
                self.save_data()
            else:
                raise jsonrpc.exceptions.JSONRPCDispatchException(
                    code=-32600,
                    message=
                    'Cannot reset node as test state has not been initialized yet'
                )

    def do_update_binaries(self):
        logging.info('update binaries')
        self.download_binaries(force=True)
        logging.info('update binaries finished')

    def do_ready(self):
        with self.lock:
            state = self.get_state()
            return state == TestState.RUNNING or state == TestState.STOPPED

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
        except (requests.exceptions.ConnectionError, KeyError):
            return self.data['current_neard_path']

    def run_neard(self, cmd, out_file=None):
        assert (self.neard is None)
        assert (self.data['neard_process'] is None)
        env = os.environ.copy()
        if 'RUST_LOG' not in env:
            env['RUST_LOG'] = 'actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn,indexer=info,debug'
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

    # If this is a regular node, starts neard run. If it's a traffic generator, starts neard mirror run
    def start_neard(self):
        for i in range(20, -1, -1):
            old_log = os.path.join(self.neard_logs_dir, f'log-{i}.txt')
            new_log = os.path.join(self.neard_logs_dir, f'log-{i+1}.txt')
            try:
                os.rename(old_log, new_log)
            except FileNotFoundError:
                pass

        with open(os.path.join(self.neard_logs_dir, 'log-0.txt'), 'ab') as out:
            if self.is_traffic_generator():
                cmd = [
                    self.data['current_neard_path'],
                    'mirror',
                    'run',
                    '--source-home',
                    self.neard_home,
                    '--target-home',
                    self.target_near_home_path(),
                    '--no-secret',
                ]
            else:
                cmd = [
                    self.data['current_neard_path'], '--log-span-events',
                    '--home', self.neard_home, '--unsafe-fast-startup', 'run'
                ]
            self.run_neard(
                cmd,
                out_file=out,
            )
            self.last_start = time.time()

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
            self.data['current_neard_path'] = neard_path
            self.start_neard()

    def get_state(self):
        return TestState(self.data['state'])

    def set_state(self, state):
        self.data['state'] = state.value

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

        cmd = [
            self.data['binaries'][0]['system_path'],
            'amend-genesis',
            '--genesis-file-in',
            os.path.join(self.neard_home, 'setup', 'genesis.json'),
            '--records-file-in',
            os.path.join(self.neard_home, 'setup', 'records.json'),
            '--genesis-file-out',
            self.target_near_home_path('genesis.json'),
            '--records-file-out',
            self.target_near_home_path('records.json'),
            '--validators',
            self.home_path('validators.json'),
            '--chain-id',
            'mocknet',
            '--transaction-validity-period',
            '10000',
            '--epoch-length',
            str(n['epoch_length']),
            '--num-seats',
            str(n['num_seats']),
        ]
        if n['protocol_version'] is not None:
            cmd.append('--protocol-version')
            cmd.append(str(n['protocol_version']))

        self.run_neard(cmd)
        self.set_state(TestState.AMEND_GENESIS)
        self.save_data()

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
                # for now just set the state to None, and if this ever happens, the
                # test operator will have to intervene manually. Probably shouldn't
                # really happen in practice
                self.set_state(TestState.NONE)
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

    def check_genesis_state(self):
        path, running, exit_code = self.poll_neard()
        if not running:
            logging.error(
                f'neard exited with code {exit_code} on the first run')
            # For now just exit, because if this happens, there is something pretty wrong with
            # the setup, so a human needs to investigate and fix the bug
            sys.exit(1)
        try:
            r = requests.get(f'http://{self.data["neard_addr"]}/status',
                             timeout=5)
            if r.status_code == 200:
                logging.info('neard finished computing state roots')
                self.kill_neard()

                try:
                    shutil.rmtree(self.home_path('backups'))
                except FileNotFoundError:
                    pass
                os.mkdir(self.home_path('backups'))
                # Right now we save the backup to backups/start and in the future
                # it would be nice to support a feature that lets you stop all the nodes and
                # make another backup to restore to
                backup_dir = self.home_path('backups', 'start')
                logging.info(f'copying data dir to {backup_dir}')
                shutil.copytree(self.target_near_home_path('data'), backup_dir)
                self.set_state(TestState.STOPPED)
                self.save_data()
        except requests.exceptions.ConnectionError:
            pass

    def reset_near_home(self):
        try:
            logging.info("removing the old directory")
            shutil.rmtree(self.target_near_home_path('data'))
        except FileNotFoundError:
            pass
        logging.info('restoring data dir from backup')
        shutil.copytree(self.home_path('backups', 'start'),
                        self.target_near_home_path('data'))
        logging.info('data dir restored')
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
                elif state == TestState.STATE_ROOTS:
                    self.check_genesis_state()
                elif state == TestState.RUNNING:
                    self.check_upgrade_neard()
                elif state == TestState.RESETTING:
                    self.reset_near_home()
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
    runner.download_binaries(force=False)

    logging.info("serve")
    runner.serve(args.port)


if __name__ == '__main__':
    main()

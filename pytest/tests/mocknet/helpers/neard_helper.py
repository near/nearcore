import argparse
import fcntl
import json
import jsonrpc
import logging
import os
import psutil
import requests
import signal
import subprocess
import sys
import threading
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
        self.dispatcher.add_method(server.neard_runner.get_metrics,
                                   name="metrics")
        self.dispatcher.add_method(server.neard_runner.do_modify_config,
                                   name="modify_config")
        self.dispatcher.add_method(server.neard_runner.do_stop, name="stop")
        self.dispatcher.add_method(server.neard_runner.do_restart,
                                   name="restart")
        self.dispatcher.add_method(server.neard_runner.do_restart_mirror,
                                   name="restart_mirror")
        self.dispatcher.add_method(server.neard_runner.do_json_rpc,
                                   name="json_rpc")
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


def nested_modification(config, modification):
    for mkey, mvalue in modification.items():
        if mkey in config:
            value = config[mkey]
            if isinstance(value, dict) and isinstance(mvalue, dict):
                nested_modification(value, mvalue)
            else:
                config[mkey] = mvalue
        else:
            config[mkey] = mvalue


class NeardRunner:

    def __init__(self, args):
        self.home = args.home
        self.neard_home = args.neard_home
        self.neard_logs_dir = args.neard_logs_dir
        self.binary_dir = args.binary_dir
        self.neard = None
        # self.data will contain data that we want to persist so we can
        # start where we left off if this process is killed and restarted
        # for now we save info on the neard binaries (their paths and epoch
        # heights where we want to run them), a bool that tells whether neard
        # should be running, and info on the currently running neard process
        try:
            with open(os.path.join(self.home, 'data.json'), 'r') as f:
                self.data = json.load(f)
                self.data['binaries'].sort(key=lambda x: x['epoch_height'])
        except FileNotFoundError:
            self.data = {
                'binaries': [],
                'run': False,
                'neard_process': {
                    'pid': None,
                    # we save the create_time so we can tell if the process with pid equal
                    # to the one we saved is the same process. It's not that likely, but
                    # if we don't do this (or maybe something else like it), then it's possble
                    # that if this process is killed and restarted and we see a process with that PID,
                    # it could actually be a different process that was started later after neard exited
                    'create_time': 0,
                    'path': None,
                }
            }
        # here we assume the home dir has already been inited. In the future we will want to
        # initialize it in this program
        self.neard_addr = self.get_addr()
        # protects self.data, and its representation on disk, because both the rpc server
        # and the loop that checks if we should upgrade the binary after a new epoch touch
        # it concurrently
        self.lock = threading.Lock()

    def get_addr(self):
        with open(os.path.join(self.neard_home, 'config.json'), 'r') as f:
            config = json.load(f)
            return config['rpc']['addr']

    def save_data(self):
        with open(os.path.join(self.home, 'data.json'), 'w') as f:
            json.dump(self.data, f)

    def shift_logs(self):
        try:
            os.mkdir(self.neard_logs_dir)
        except FileExistsError:
            pass
        for i in range(20, -1, -1):
            old_log = os.path.join(self.neard_logs_dir, f'log-{i}.txt')
            new_log = os.path.join(self.neard_logs_dir, f'log-{i+1}.txt')
            try:
                os.rename(old_log, new_log)
            except FileNotFoundError:
                pass

    def do_restart_generic(self, binary_name, add_cmd):
        self.do_stop()

        self.shift_logs()

        neard_path = os.path.join(self.binary_dir, binary_name)

        with open(os.path.join(self.neard_logs_dir, 'log-0.txt'), 'ab') as out:
            cmd = [
                neard_path,
                '--home',
                self.neard_home,
            ] + add_cmd
            env = os.environ.copy()
            if 'RUST_LOG' not in env:
                env['RUST_LOG'] = 'actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn,debug'
            logging.info(f'running {" ".join(cmd)}')
            self.neard = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=out,
                env=env,
            )

        try:
            create_time = int(psutil.Process(self.neard.pid).create_time())
        except psutil.NoSuchProcess:
            # not that likely, but if neard already exited, catch the exception so
            # at least this process doesn't die
            create_time = 0

        self.neard_addr = self.get_addr()

        self.data['neard_process'] = {
            'pid': self.neard.pid,
            'create_time': create_time,
            'path': neard_path,
        }
        self.save_data()

    def do_restart(self, binary_name='neard', unsafe=True):
        add_cmd = []
        if unsafe:
            add_cmd.append('--unsafe-fast-startup')
        add_cmd.append('run')
        self.do_restart_generic(binary_name, add_cmd)

    def do_restart_mirror(self, binary_name='neard'):
        add_cmd = [
            'mirror', 'run', '--source-home', self.neard_home, '--target-home',
            f'{self.neard_home}/target', '--no-secret'
        ]
        self.do_restart_generic(binary_name, add_cmd)

    def do_stop(self):
        if self.neard is not None:
            logging.info('stopping neard')
            self.neard.send_signal(signal.SIGINT)
            self.neard.wait()
            self.neard = None
            self.data['neard_process']['pid'] = None
            self.data['neard_process']['create_time'] = 0
            self.save_data()
            return "stopped neard"
        elif self.data['neard_process']['pid'] is not None:
            result = "stopped neard"
            try:
                p = psutil.Process(self.data['neard_process']['pid'])
                if int(p.create_time()
                      ) == self.data['neard_process']['create_time']:
                    logging.info('stopping neard')
                    p.send_signal(signal.SIGINT)
                    p.wait()
            except psutil.NoSuchProcess:
                result = "no running neard found"
            self.data['neard_process']['pid'] = None
            self.data['neard_process']['create_time'] = 0
            self.save_data()
            return result
        return "no running neard found"

    def do_json_rpc(self, method, params):
        try:
            j = {
                'method': method,
                'params': params,
                'id': 'dontcare',
                'jsonrpc': '2.0'
            }
            r = requests.post(f'http://{self.neard_addr}', json=j, timeout=5)
            r.raise_for_status()
            return r.text
        except requests.exceptions.ConnectionError:
            return "{}"

    def get_metrics(self):
        try:
            r = requests.get(f'http://{self.neard_addr}/metrics', timeout=5)
            r.raise_for_status()
            return r.text
        except requests.exceptions.ConnectionError:
            return ""

    def do_modify_config(self, modifications):
        with open(os.path.join(self.neard_home, 'config.json'), 'r') as f:
            config = json.load(f)
        nested_modification(config, modifications)
        with open(os.path.join(self.neard_home, 'config.json'), 'w') as f:
            json.dump(config, f, indent=1)

    def serve(self, port):
        s = RpcServer(('0.0.0.0', port), self)
        s.serve_forever()


def main():
    parser = argparse.ArgumentParser(description='run neard')
    parser.add_argument('--home', type=str, required=True)
    parser.add_argument('--neard-home', type=str, required=True)
    parser.add_argument('--binary-dir', type=str, required=True)
    parser.add_argument('--neard-logs-dir', type=str, required=True)
    parser.add_argument('--port', type=int, required=True)
    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s] %(levelname)s: %(message)s',
                        level=logging.INFO)

    config_path = os.path.join(args.home, 'config.json')
    if not os.path.isdir(args.home):
        sys.exit(f'please create the directory at {args.home}')

    # only let one instance of this code run at a time
    _fd = get_lock(args.home)

    runner = NeardRunner(args)

    runner.serve(args.port)


if __name__ == '__main__':
    main()

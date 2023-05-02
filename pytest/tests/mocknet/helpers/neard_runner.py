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
        self.dispatcher.add_method(server.neard_runner.do_start, name="start")
        self.dispatcher.add_method(server.neard_runner.do_stop, name="stop")
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


class NeardRunner:

    def __init__(self, args):
        self.home = args.home
        self.neard_home = args.neard_home
        self.neard_logs_dir = args.neard_logs_dir
        with open(os.path.join(self.home, 'config.json'), 'r') as f:
            self.config = json.load(f)
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
        with open(os.path.join(self.neard_home, 'config.json'), 'r') as f:
            config = json.load(f)
            self.neard_addr = config['rpc']['addr']
        # protects self.data, and its representation on disk, because both the rpc server
        # and the loop that checks if we should upgrade the binary after a new epoch touch
        # it concurrently
        self.lock = threading.Lock()

    def save_data(self):
        with open(os.path.join(self.home, 'data.json'), 'w') as f:
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
                'system_path': os.path.join(self.home, 'binaries', f'neard{i}')
            })
        return binaries

    # tries to download the binaries specified in config.json, saving them in $home/binaries/
    def download_binaries(self):
        binaries = self.parse_binaries_config()

        try:
            os.mkdir(os.path.join(self.home, 'binaries'))
        except FileExistsError:
            pass

        with self.lock:
            num_binaries_saved = len(self.data['binaries'])
            neard_process = self.data['neard_process']
            if neard_process['path'] is None:
                neard_process['path'] = binaries[0]['system_path']
                self.save_data()

        # for now we assume that the binaries recorded in data.json as having been
        # dowloaded are still valid and were not touched. Also this assumes that their
        # filenames are neard0, neard1, etc. in the right order and with nothing skipped
        for i in range(num_binaries_saved, len(binaries)):
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
                self.save_data()

    def do_start(self):
        with self.lock:
            # we should already have tried running it if this is true
            if self.data['run']:
                return

            # for now just set this and wait for the next iteration of the update loop to start it
            self.data['run'] = True
            self.save_data()

    def do_stop(self):
        with self.lock:
            if not self.data['run']:
                return

            # for now just set this and wait for the next iteration of the update loop to stop it
            self.data['run'] = False
            self.save_data()

    # check the current epoch height, and return the binary path that we should
    # be running given the epoch heights specified in config.json
    def wanted_neard_path(self):
        j = {
            'method': 'validators',
            'params': [None],
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        try:
            r = requests.post(f'http://{self.neard_addr}', json=j, timeout=5)
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
            return self.data['neard_process']['path']

    def check_run_neard(self):
        neard_path = self.wanted_neard_path()
        start_neard = False

        if self.neard is not None:
            code = self.neard.poll()
            if code is not None:
                logging.info(f'neard exited with code {code}. restarting')
                start_neard = True
            else:
                if self.data['neard_process']['path'] != neard_path:
                    logging.info('upgrading neard upon new epoch')
                    self.neard.send_signal(signal.SIGINT)
                    self.neard.wait()
                    start_neard = True
        elif self.data['neard_process']['pid'] is not None:
            # we land here if this process previously died and is now restarted,
            # and the old neard process is still running
            try:
                p = psutil.Process(self.data['neard_process']['pid'])
                if int(p.create_time()
                      ) == self.data['neard_process']['create_time']:
                    if self.data['neard_process']['path'] != neard_path:
                        logging.info('upgrading neard upon new epoch')
                        p.send_signal(signal.SIGINT)
                        p.wait()
                        start_neard = True
                else:
                    start_neard = True
            except psutil.NoSuchProcess:
                start_neard = True
        else:
            start_neard = True

        if start_neard:
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

            with open(os.path.join(self.neard_logs_dir, 'log-0.txt'),
                      'ab') as out:
                cmd = [
                    neard_path, '--home', self.neard_home,
                    '--unsafe-fast-startup', 'run'
                ]
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

            self.data['neard_process'] = {
                'pid': self.neard.pid,
                'create_time': create_time,
                'path': neard_path,
            }
            self.save_data()

    def check_stop_neard(self):
        if self.neard is not None:
            logging.info('stopping neard')
            self.neard.send_signal(signal.SIGINT)
            self.neard.wait()
            self.neard = None
            self.data['neard_process']['pid'] = None
            self.data['neard_process']['create_time'] = 0
            self.save_data()
        elif self.data['neard_process']['pid'] is not None:
            try:
                p = psutil.Process(self.data['neard_process']['pid'])
                if int(p.create_time()
                      ) == self.data['neard_process']['create_time']:
                    logging.info('stopping neard')
                    p.send_signal(signal.SIGINT)
                    p.wait()
            except psutil.NoSuchProcess:
                pass
            self.data['neard_process']['pid'] = None
            self.data['neard_process']['create_time'] = 0
            self.save_data()

    # periodically check if we should update neard after a new epoch
    # TODO: should we update it at a random time in the middle of the
    # epoch instead of the beginning?
    def upgrade_neard(self):
        while True:
            with self.lock:
                if self.data['run']:
                    self.check_run_neard()
                else:
                    self.check_stop_neard()

            time.sleep(10)

    def serve(self, port):
        # TODO: maybe use asyncio? kind of silly to use multiple threads for
        # something so lightweight
        upgrade_loop = threading.Thread(target=self.upgrade_neard)
        upgrade_loop.start()
        s = RpcServer(('0.0.0.0', port), self)
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

    runner = NeardRunner(args)

    runner.download_binaries()

    runner.serve(args.port)


if __name__ == '__main__':
    main()

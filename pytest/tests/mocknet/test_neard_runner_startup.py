#!/usr/bin/env python3
"""
Startup test for neard_runner.py (pytest/tests/mocknet/helpers/neard_runner.py).

neard_runner downloads the neard binaries. The test holds one binary download
open and checks that the runner's RPC port answers `ready` with False during the
download, instead of refusing the connection. It then checks that the download
finishes once the transfer is released.

The test starts neard_runner.py as a subprocess. It does not start neard.

Run it locally with:

    python3 pytest/tests/mocknet/test_neard_runner_startup.py
"""
import http.server
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

RUNNER = pathlib.Path(__file__).parent / 'helpers' / 'neard_runner.py'
BINARY_BYTES = b'0123456789'
# TestState.STOPPED in helpers/neard_runner.py. The value is written to
# data.json directly, because importing neard_runner needs its dependencies.
STOPPED_STATE = 6


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class SlowBinaryServer:
    """Serves one binary. The body is held back until `release` is set."""

    def __init__(self):
        self.request_seen = threading.Event()
        self.release = threading.Event()
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):

            def do_GET(self):
                outer.request_seen.set()
                self.send_response(200)
                self.send_header('Content-Length', str(len(BINARY_BYTES)))
                self.end_headers()
                self.wfile.write(BINARY_BYTES[:4])
                self.wfile.flush()
                outer.release.wait(timeout=120)
                self.wfile.write(BINARY_BYTES[4:])

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def url(self):
        return f'http://127.0.0.1:{self.port}/neard'

    def close(self):
        self.release.set()
        self.httpd.shutdown()
        self.httpd.server_close()


def ready_rpc(port):
    """Returns the `result` of a `ready` call, or None if nothing answered."""
    body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'ready'}).encode()
    req = urllib.request.Request(f'http://localhost:{port}/',
                                 data=body,
                                 headers={'Content-Type': 'application/json'},
                                 method='POST')
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())['result']
    except (urllib.error.URLError, ConnectionError, OSError):
        return None


class NeardRunnerStartupTest(unittest.TestCase):

    def test_ready_answers_during_binary_download(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.check_ready_answers_during_binary_download(pathlib.Path(tmp))

    def check_ready_answers_during_binary_download(self, tmp):
        slow = SlowBinaryServer()
        self.addCleanup(slow.close)
        home = tmp / 'home'
        home.mkdir()
        (home / 'config.json').write_text(
            json.dumps({'binaries': [{
                'epoch_height': 0,
                'url': slow.url()
            }]}))
        # A runner restarted after `stop`: the saved state is STOPPED, so
        # `ready` must turn True once the binaries are downloaded. No binary is
        # recorded as downloaded yet, so the runner fetches it on startup.
        (home / 'data.json').write_text(
            json.dumps({
                'binaries': [],
                'neard_process': None,
                'current_neard_path': None,
                'state': STOPPED_STATE,
                'backups': {},
                'state_data': None,
            }))
        port = free_port()
        cmd = [
            sys.executable,
            str(RUNNER),
            '--home',
            str(home),
            '--neard-home',
            str(tmp / 'neard-home'),
            '--neard-logs-dir',
            str(tmp / 'logs'),
            '--port',
            str(port),
        ]
        stderr_path = tmp / 'runner-stderr.txt'
        with open(stderr_path, 'w') as stderr:
            proc = subprocess.Popen(cmd,
                                    stdout=subprocess.DEVNULL,
                                    stderr=stderr)
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)

        self.assertTrue(slow.request_seen.wait(timeout=60),
                        'neard_runner never started the binary download')

        # The download is still held open. The RPC port must answer.
        deadline = time.time() + 30
        result = None
        while time.time() < deadline:
            result = ready_rpc(port)
            if result is not None:
                break
            time.sleep(0.2)
        self.assertIs(
            result, False,
            'neard_runner did not answer `ready` with False while the binary '
            f'was downloading; last answer: {result!r}')

        slow.release.set()
        binary = home / 'binaries' / 'neard0'
        deadline = time.time() + 60
        while time.time() < deadline:
            if binary.exists() and binary.read_bytes() == BINARY_BYTES:
                break
            if proc.poll() is not None:
                self.fail('neard_runner exited early: '
                          f'{stderr_path.read_text()}')
            time.sleep(0.2)
        self.assertEqual(binary.read_bytes(), BINARY_BYTES)

        # Once the download is done, `ready` reports the saved state again.
        deadline = time.time() + 30
        while time.time() < deadline:
            result = ready_rpc(port)
            if result is True:
                break
            time.sleep(0.2)
        self.assertIs(
            result, True,
            'neard_runner did not answer `ready` with True after the binary '
            f'download finished; last answer: {result!r}')


if __name__ == '__main__':
    unittest.main()

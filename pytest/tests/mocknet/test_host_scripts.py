#!/usr/bin/env python3
"""
Runs the forknet host scripts in helpers/host/ with the local bash and jq, in
a temporary forknet home. Fake neard, gsutil, systemctl and systemd-run come
first on PATH; the fake systemd-run runs the script in the foreground.
NayDuck runs this in the merge queue and nightly (nightly/pytest-mocknet.txt).

    python3 pytest/tests/mocknet/test_host_scripts.py
"""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from node_handle import HOST_SCRIPTS_DIR, env_file_content

FAKES = {
    'neard':
        r'''
echo "neard $*" >>"$FAKE_LOG"
[ "$1" = --home ] && home=$2 && shift 2
[ "$1" = --unsafe-fast-startup ] && shift
case "$1 $2" in
"init "* | "init --account-id")
    mkdir -p "$home"
    echo '{"network": {"addr": "0.0.0.0:24567"}}' >"$home/config.json"
    echo '{"public_key": "node"}' >"$home/node_key.json"
    if [ -n "$3" ]; then
        echo "{\"account_id\": \"$3\"}" >"$home/validator_key.json"
    fi
    ;;
"database make-snapshot") mkdir -p "$4/data" && cp -r "$home/data/." "$4/data/" ;;
"fork-network set-validators")
    mkdir -p "$home/data" "$home/epoch_configs"
    echo forked >"$home/data/state"
    echo '{}' >"$home/genesis.json"
    ;;
esac
''',
    # Maps gs://path to $FAKE_DIR/gcs/path.
    'gsutil':
        r'''
echo "gsutil $*" >>"$FAKE_LOG"
[ "$1" = -m ] && shift
args=("${@//gs:\/\//$FAKE_DIR/gcs/}")
case "$1" in
ls) [ -e "${args[1]}" ] ;;
rsync) mkdir -p "${args[3]}" && cp -r "${args[2]}/." "${args[3]}/" ;;
rm) rm -rf "${args[2]}" ;;
esac
''',
    # The file $FAKE_DIR/active means that the neard unit runs.
    'systemctl':
        r'''
echo "systemctl $*" >>"$FAKE_LOG"
case "$2" in
is-active) [ -f "$FAKE_DIR/active" ] ;;
start | restart) touch "$FAKE_DIR/active" ;;
stop) rm -f "$FAKE_DIR/active" ;;
esac
''',
    'flock':
        '',
    'systemd-run':
        r'''
while [ "$1" != /bin/bash ]; do shift; done
exec "$@"
''',
}


class HostScriptsTest(unittest.TestCase):

    def setUp(self):
        # Resolve symlinks, as the scripts do with readlink -f.
        self.dir = pathlib.Path(tempfile.mkdtemp()).resolve()
        self.addCleanup(shutil.rmtree, self.dir)
        self.home = self.dir / 'forknet'
        self.near = self.dir / 'near'
        self.log = self.dir / 'log'
        (self.dir / 'bin').mkdir()
        for name, body in FAKES.items():
            path = self.dir / 'bin' / name
            path.write_text('#!/usr/bin/env bash' + body)
            path.chmod(0o755)
        self.env = {
            **os.environ,
            'PATH': f'{self.dir}/bin:{os.environ["PATH"]}',
            'HOME': str(self.dir),
            'FAKE_LOG': str(self.log),
            'FAKE_DIR': str(self.dir),
        }
        shutil.copytree(HOST_SCRIPTS_DIR, self.home)
        # The real script installs near-cli from the internet.
        (self.home / 'setup-near-cli.sh').write_text('')
        (self.home / 'host.env').write_text(
            env_file_content({
                'NEAR_HOME': self.near,
                'SOURCE_HOME': self.dir / 'source',
                'IS_TRAFFIC': 0,
                'RUN_ID': 'run',
                'MOCKNET_STORE_PATH': 'gs://store',
            }))
        (self.home / 'binaries').mkdir()
        neard = self.dir / 'bin/neard'
        (self.home / 'binaries/urls').write_text(f'{neard}\n{neard}\n')

    def run_script(self, script, *args, check=True):
        r = subprocess.run(['bash', self.home / script, *map(str, args)],
                           env=self.env,
                           capture_output=True,
                           text=True)
        if check:
            self.assertEqual(r.returncode, 0, r.stderr)
        return r

    def read_log(self):
        lines = self.log.read_text().splitlines()
        self.log.unlink()
        return lines

    def state(self):
        return self.run_script('state.sh').stdout.strip()

    def new_test(self, chunk_validator=0):
        self.run_script('download-binaries.sh')
        keys = self.run_script('new-test.sh').stdout.splitlines()[-1]
        (self.home / 'validators.json.new').write_text('[]')
        (self.home / 'setup.env.new').write_text(
            env_file_content({
                'MOCKNET_ID': 'mocknet',
                'CHUNK_VALIDATOR': chunk_validator,
                'UPLOAD_SETUP': 1 - chunk_validator,
                'STATE_SOURCE': 'dump',
                'PATCHES_PATH': None,
                'EPOCH_LENGTH': 100,
                'NUM_SEATS': 2,
                'GENESIS_TIME': 'now',
                'NEW_CHAIN_ID': None,
                'PROTOCOL_VERSION': 80,
                'CLI_CHAIN_ID': 'mocknet',
                'RPC_ADDR': 'rpc:3030',
            }))
        self.run_script('network-init.sh', '[[["network", "boot_nodes"], "b"]]')
        return json.loads(keys)

    def test_lifecycle(self):
        # Setup: new-test builds genesis, uploads it and makes the backup.
        keys = self.new_test()
        self.assertEqual(keys['node_key'], 'node')
        self.assertEqual(keys['listen_port'], '24567')
        self.assertEqual(self.state(), 'ready')
        config = json.loads((self.near / 'config.json').read_text())
        self.assertEqual(config['network']['boot_nodes'], 'b')
        self.assertIn(
            f'neard --home {self.near} fork-network set-validators '
            f'--validators {self.home}/validators.json --epoch-length 100 '
            '--genesis-time now --num-seats 2 --state-source dump '
            '--protocol-version 80', self.read_log())
        self.assertTrue(
            (self.dir / 'gcs/store/mocknet/run/genesis.json').exists())

        # Start, start again while running, then upgrade to binary 1.
        self.run_script('start-neard.sh')
        self.run_script('start-neard.sh')
        self.run_script('start-neard.sh', '--binary-idx', 1)
        log = [l for l in self.read_log() if 'start ' in l]
        self.assertEqual(
            log,
            ['systemctl --user start neard', 'systemctl --user restart neard'])
        self.assertEqual(os.readlink(self.home / 'binaries/current'), 'neard1')

        # Reset restores the start state and binary 0.
        (self.near / 'data/state').write_text('changed\n')
        self.run_script('reset.sh')
        self.assertEqual(self.state(), 'ready')
        self.assertEqual((self.near / 'data/state').read_text(), 'forked\n')
        self.assertEqual(os.readlink(self.home / 'binaries/current'), 'neard0')
        self.assertFalse((self.dir / 'gcs/store/mocknet/run').exists())

    def test_chunk_validator_downloads_genesis(self):
        # Setup: the first validator uploaded genesis.
        uploaded = self.dir / 'gcs/store/mocknet/run'
        uploaded.mkdir(parents=True)
        (uploaded / 'genesis.json').write_text('uploaded')

        self.new_test(chunk_validator=1)

        self.assertEqual(self.state(), 'ready')
        self.assertFalse(any('fork-network' in l for l in self.read_log()))
        self.assertEqual((self.near / 'genesis.json').read_text(), 'uploaded')
        self.assertFalse((self.near / 'data').exists())

    def test_refuses_wrong_state(self):
        # Before new-test: no start, no network-init.
        r = self.run_script('start-neard.sh', check=False)
        self.assertIn('state is no test, expected ready', r.stderr)
        r = self.run_script('network-init.sh', '[]', check=False)
        self.assertIn('expected awaiting-network-init', r.stderr)

        # A second network-init does not replace the setup of the first.
        self.new_test()
        setup_env = (self.home / 'setup.env').read_text()
        (self.home / 'setup.env.new').write_text('')
        r = self.run_script('network-init.sh', '[]', check=False)
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual((self.home / 'setup.env').read_text(), setup_env)

    def test_traffic_source(self):
        # Setup: this host is the traffic generator, and neard runs.
        host_env = self.home / 'host.env'
        host_env.write_text(host_env.read_text().replace(
            'IS_TRAFFIC=0', 'IS_TRAFFIC=1'))
        self.new_test()
        self.run_script('start-neard.sh')
        self.read_log()

        # A cloud archive source restarts neard; the same source again does
        # not, and a plain start keeps it.
        r = self.run_script('start-neard.sh',
                            '--source-cloud-archive',
                            check=False)
        self.assertIn('needs --source-home', r.stderr)
        self.run_script('start-neard.sh', '--source-cloud-archive',
                        '--source-home', '/reader')
        self.run_script('start-neard.sh', '--source-cloud-archive',
                        '--source-home', '/reader')
        self.run_script('start-neard.sh')
        log = [l for l in self.read_log() if 'start ' in l]
        self.assertEqual(log, ['systemctl --user restart neard'])
        self.run_script('run-neard.sh')
        self.assertEqual(self.read_log(), [
            f'neard mirror run --source-home /reader --target-home {self.near} '
            '--source-cloud-archive --no-secret'
        ])

        # --source-rpc-backup goes back to SOURCE_HOME.
        self.run_script('start-neard.sh', '--source-rpc-backup')
        self.run_script('run-neard.sh')
        self.assertEqual(
            self.read_log()[-1],
            f'neard mirror run --source-home {self.dir}/source '
            f'--target-home {self.near} --no-secret')

    def test_binaries(self):
        # A local binary is linked and made executable.
        neard = self.dir / 'bin/neard'
        neard.chmod(0o644)
        self.run_script('download-binaries.sh')
        self.assertTrue(os.access(self.home / 'binaries/neard0', os.X_OK))
        self.assertEqual(os.readlink(self.home / 'binaries/current'), 'neard0')

        # amend-binary replaces one URL; a missing index fails.
        (self.dir / 'new').write_text('')
        self.run_script('amend-binary.sh', 1, self.dir / 'new')
        r = self.run_script('amend-binary.sh', 2, neard, check=False)
        self.assertEqual((self.home / 'binaries/urls').read_text(),
                         f'{neard}\n{self.dir}/new\n')
        self.assertIn('no neard binary with index 2', r.stderr)

    def test_set_env(self):
        self.run_script('set-env.sh', 'A=1', 'B=two words')
        self.run_script('set-env.sh', 'A=3')
        self.assertEqual((self.home / 'neard.env').read_text(),
                         'B=two words\nA=3\n')
        self.run_script('set-env.sh', '--clear')
        self.assertEqual((self.home / 'neard.env').read_text(), '')


if __name__ == '__main__':
    unittest.main()

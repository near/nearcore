"""
This is an abstraction over the node.
It manages neard on the host through the scripts in helpers/host/.
"""
import base64
import json
import pathlib
import re
import requests
import shlex
import sys
import time
from typing import Optional, Self

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from utils import ScheduleContext

# Uploaded to the forknet home of each host.
HOST_SCRIPTS_DIR = f'{pathlib.Path(__file__).resolve().parent}/helpers/host/'


def env_file_content(values: dict) -> str:
    """Content of a file that bash sources to set `values`."""
    lines = []
    for key, value in values.items():
        if value is None:
            value = ''
        lines.append(f'{key}={shlex.quote(str(value))}\n')
    return ''.join(lines)


def write_file_cmd(path: str, content: str) -> str:
    """Shell command that writes `content` to `path`."""
    encoded = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    return f'echo {encoded} | base64 -d > {shlex.quote(path)}'


def check_env_key_values(key_values: list[str]):
    """
    Exits unless each item is NAME=value with a valid variable name and no
    newline. A newline would add another variable to neard.env.
    """
    for key_value in key_values:
        key = key_value.split('=', 1)[0]
        if '=' not in key_value or '\n' in key_value or not re.fullmatch(
                r'[A-Za-z_][A-Za-z0-9_]*', key):
            sys.exit(f'invalid environment variable assignment: {key_value}')


def parse_config_changes(key_value: str) -> list[tuple[list[str], object]]:
    """
    Parses 'a.b.c=5;d="x"' into [(['a', 'b', 'c'], 5), (['d'], 'x')]. Values
    are JSON.
    """
    changes = []
    for kv in key_value.split(';'):
        key, value = kv.split('=', 1)
        changes.append((key.split('.'), json.loads(value)))
    return changes


class NodeHandle:

    def __init__(self, node, can_validate=True, want_state_dump=False):
        self.node = node
        self.can_validate = can_validate
        self.want_state_dump = want_state_dump
        self.runs_neard = True
        self.schedule_ctx = None

    def with_schedule_ctx(self,
                          schedule_ctx: Optional[ScheduleContext]) -> Self:
        """
        Sets the schedule context in the node.
        If schedule is set, all the commands will be scheduled.
        If schedule is not set, all the commands will be executed now.
        """
        self.schedule_ctx = schedule_ctx
        return self

    def name(self):
        return self.node.name()

    def role(self):
        return self.node.get_label('role')

    def ip_addr(self):
        return self.node.ip_addr()

    def neard_port(self):
        return self.node.neard_port()

    def run_cmd(self, cmd, raise_on_fail=False, return_on_fail=False):
        return self.node.run_cmd(self.schedule_ctx, cmd, raise_on_fail,
                                 return_on_fail)

    def run_cmd_now(self, cmd, return_on_fail=False):
        """Runs `cmd` now, even if the schedule context is set."""
        return self.node.run_cmd(None, cmd, return_on_fail=return_on_fail)

    def make_snapshot(self, snapshot_id):
        return self.node.make_snapshot(snapshot_id)

    def restore_snapshot(self, snapshot_id):
        return self.node.restore_snapshot(snapshot_id)

    def list_snapshots(self):
        return self.node.list_snapshots()

    def delete_snapshot(self, snapshot_id):
        return self.node.delete_snapshot(snapshot_id)

    def upload_file(self, src, dst):
        return self.node.upload_file(src, dst)

    def download_file(self, src, dst):
        return self.node.download_file(src, dst)

    def host_path(self, *parts):
        return '/'.join([self.node.forknet_home, *parts])

    def script_cmd(self, script, *args):
        """Shell command that runs a script from helpers/host/ on the host."""
        return shlex.join(['bash', self.host_path(script), *map(str, args)])

    def init_host(self,
                  binary_urls: list[str],
                  run_id: str,
                  mocknet_store_path: str,
                  remove_home_dir=False):
        """
        Uploads the host scripts, installs the neard unit and downloads the
        binaries. With remove_home_dir, it also removes the test state and the
        start backup.
        """
        forknet_home = shlex.quote(self.node.forknet_home)
        # The units do not exist on a host that was never initialized, and the
        # transient setup and reset units exist only while they run.
        stop_cmd = ' ; '.join(
            f'systemctl --user stop {unit} 2>/dev/null'
            for unit in ['neard-setup', 'neard-reset', 'neard'])
        home_cmd = f'mkdir -p {forknet_home}/binaries'
        if remove_home_dir:
            home_cmd = f'rm -rf {forknet_home} && {home_cmd}'
        self.node.init()
        self.run_cmd_now(f'{stop_cmd} ; {home_cmd}')
        self.node.upload_file(HOST_SCRIPTS_DIR, self.node.forknet_home)
        host_env = self.node.host_env(run_id, mocknet_store_path)
        self.run_cmd_now(' && '.join([
            write_file_cmd(self.host_path('host.env'),
                           env_file_content(host_env)),
            write_file_cmd(self.host_path('binaries', 'urls'),
                           ''.join(f'{url}\n' for url in binary_urls)),
            self.script_cmd('init-host.sh'),
        ]))

    # cspell:ignore culted
    # TODO: is the validators RPC the best way to do this? What are we trying to
    # test for exactly? The use of this is basically just cargo culted from a while ago,
    # but maybe we should consider something else
    def wait_node_up(self):
        while True:
            try:
                res = self.node.get_validators()
                if 'error' not in res:
                    assert 'result' in res
                    logger.info(f'Node {self.node.name()} is up')
                    return
            except (ConnectionRefusedError,
                    requests.exceptions.ConnectionError) as e:
                pass
            logger.info(
                f'Node {self.node.name()} is not ready yet, will check again in 10 seconds'
            )
            time.sleep(10)

    def new_test(self):
        """
        Stops neard and creates new keys and config. Returns the node key,
        the validator key (None on the traffic generator) and the listen port.
        """
        r = self.run_cmd_now(self.script_cmd('new-test.sh'))
        return json.loads(r.stdout.strip().splitlines()[-1])

    def network_init(self, validators, config_changes, setup_env):
        """
        Starts setup-node.sh, which builds the genesis state in the background.
        `is_ready` returns True when it has finished. config_changes is a list
        of (path, value) pairs for config.json.
        """
        self.run_cmd_now(' && '.join([
            # network-init.sh moves them into place if the state allows it.
            write_file_cmd(self.host_path('validators.json.new'),
                           json.dumps(validators, indent=2)),
            write_file_cmd(self.host_path('setup.env.new'),
                           env_file_content(setup_env)),
            self.script_cmd('network-init.sh', json.dumps(config_changes)),
        ]))

    def state(self):
        """The state of the test on the host. See helpers/host/common.sh."""
        return self.run_cmd_now(self.script_cmd('state.sh')).stdout.strip()

    def is_ready(self):
        return self.state() == 'ready'

    def start_neard(self,
                    binary_idx=None,
                    force_restart=False,
                    batch_interval_millis=None,
                    source_cloud_archive=False,
                    source_home=None,
                    source_rpc_backup=False):
        args = []
        if binary_idx is not None:
            args += ['--binary-idx', int(binary_idx)]
        if force_restart:
            args += ['--force-restart']
        if batch_interval_millis is not None:
            args += ['--batch-interval-millis', int(batch_interval_millis)]
        if source_cloud_archive:
            args += ['--source-cloud-archive']
        if source_home is not None:
            args += ['--source-home', source_home]
        if source_rpc_backup:
            args += ['--source-rpc-backup']
        return self.run_cmd(self.script_cmd('start-neard.sh', *args))

    def stop_neard(self):
        return self.run_cmd('systemctl --user stop neard')

    def reset(self):
        """
        Starts reset-node.sh, which restores the start backup in the
        background. `is_ready` returns True when it has finished.
        """
        return self.run_cmd(self.script_cmd('reset.sh'))

    def update_binaries(self, neard_binary_url=None, binary_idx=None):
        """
        Downloads the binaries again. If neard_binary_url is set, it first
        replaces the URL of the binary at binary_idx.
        """
        if neard_binary_url is None:
            return self.run_cmd(self.script_cmd('download-binaries.sh'))
        return self.run_cmd(
            self.script_cmd('amend-binary.sh', int(binary_idx),
                            neard_binary_url))

    def update_config(self, key_value):
        changes = parse_config_changes(key_value)
        return self.run_cmd(
            self.script_cmd('patch-config.sh', json.dumps(changes)))

    def update_env(self, key_values):
        """
        Sets variables in neard.env, which the neard unit loads. They take
        effect when neard restarts.
        """
        check_env_key_values(key_values)
        return self.run_cmd(self.script_cmd('set-env.sh', *key_values))

    def clear_env(self):
        return self.run_cmd(self.script_cmd('set-env.sh', '--clear'))

#!/usr/bin/env python3
"""
Unit tests for SshSharingMachine (pytest/lib/mocknet_ssh.py).

SshSharingMachine depends on python-rc internals. NayDuck runs this test in the
merge queue and nightly (nightly/pytest-mocknet.txt). Run it locally when you
change the python-rc version in pytest/requirements.txt:

    python3 pytest/tests/mocknet/test_mocknet_ssh.py

The tests do not connect to any host. They compare the commands of
SshSharingMachine with the commands that the installed python-rc builds, not
with fixed strings. A test fails only when SshSharingMachine would send a
command that differs from python-rc's, apart from the sharing options.
"""
import os
import pathlib
import shlex
import stat
import sys
import tempfile
import unittest
from unittest import mock

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from rc.machine import Machine
from rc.util import RunResult

import mocknet_ssh
from mocknet_ssh import SshSharingMachine

# A fake host. The forknet hosts use the remote user 'ubuntu'.
IP = '10.1.2.3'
USER = 'ubuntu'
KEY = '/keys/mocknet_key'

SHARING_OPTIONS = [
    '-o',
    'ControlMaster=auto',
    '-o',
    'ControlPersist=10m',
    '-o',
    f'ControlPath={mocknet_ssh.SSH_CONTROL_PATH}',
    '-o',
    'ServerAliveInterval=15',
    '-o',
    'ServerAliveCountMax=3',
]


def make_machine():
    return Machine(provider=None,
                   name='mocknet-test-node',
                   ip=IP,
                   username=USER,
                   ssh_key_path=KEY)


def with_sharing_options(ssh_command):
    """`ssh_command` with SHARING_OPTIONS added after 'ssh'."""
    return [ssh_command[0], *SHARING_OPTIONS, *ssh_command[1:]]


def split_rsync_command(cmd):
    """
    Splits an rsync command into (ssh command of `-e`, other rsync words).
    python-rc puts backslash-newline in its commands.
    """
    words = shlex.split(cmd.replace('\\\n', ' '))
    i = words.index('-e')
    return shlex.split(words[i + 1]), words[:i] + words[i + 2:]


def transfer_commands(machine, run_patch_target):
    """The rsync commands of upload() and download(), with all their options."""
    ok = RunResult(stdout='', stderr='', returncode=0)
    with mock.patch(run_patch_target, return_value=ok) as run:
        machine.upload('/local/a', '/remote/a', switch_user='ubuntu')
        machine.upload('/local/b', '/remote/b')
        machine.download('/remote/c', '/local/c')
        machine.download('/remote/d', '/local/d', sudo=False)
    return [split_rsync_command(c.args[0]) for c in run.call_args_list]


class SshSharingMachineTest(unittest.TestCase):

    def setUp(self):
        # Use a ~/.ssh in a temporary directory, not the real one.
        tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        self.control_dir = os.path.join(tmp_dir.name, '.ssh')
        for p in [
                mock.patch.object(mocknet_ssh, 'SSH_CONTROL_DIR',
                                  self.control_dir),
                mock.patch.object(mocknet_ssh, 'SSH_CONTROL_PERSIST_DEFAULT',
                                  '10m'),
                mock.patch.dict(os.environ),
        ]:
            p.start()
            self.addCleanup(p.stop)
        os.environ.pop('MOCKNET_SSH_CONTROLMASTER', None)
        os.environ.pop('MOCKNET_SSH_CONTROL_PERSIST', None)
        self.rc_machine = make_machine()
        self.shared = SshSharingMachine.from_machine(make_machine())

    def test_ssh_shell(self):
        rc_shell = self.rc_machine._ssh_shell()
        self.assertEqual(self.shared._ssh_shell(),
                         with_sharing_options(rc_shell))
        # ssh needs the ControlPath directory to exist and be private.
        self.assertEqual(stat.S_IMODE(os.stat(self.control_dir).st_mode), 0o700)

        os.environ['MOCKNET_SSH_CONTROLMASTER'] = '0'
        self.assertEqual(self.shared._ssh_shell(), rc_shell)

    def test_run_uses_shared_ssh_shell(self):
        # mocknet calls machine.run() (also through run_detach_tmux()).
        # python-rc must build the ssh command with self._ssh_shell(), or the
        # override has no effect.
        ok = RunResult(stdout='', stderr='', returncode=0)
        with mock.patch('rc.machine.run', return_value=ok) as run:
            self.shared.run('hostname')
        self.assertEqual(run.call_args.kwargs['shell'],
                         with_sharing_options(self.rc_machine._ssh_shell()))

    def test_upload_download_match_python_rc(self):
        # upload() and download() are copies of the python-rc methods. They
        # must use the same rsync options as python-rc, and the ssh command of
        # python-rc with the sharing options added.
        rc_commands = transfer_commands(self.rc_machine, 'rc.machine.run')
        for sharing in ['1', '0']:
            with self.subTest(MOCKNET_SSH_CONTROLMASTER=sharing):
                os.environ['MOCKNET_SSH_CONTROLMASTER'] = sharing
                commands = transfer_commands(self.shared, 'mocknet_ssh.run')
                expected = [
                    (with_sharing_options(ssh) if sharing == '1' else ssh,
                     rsync) for ssh, rsync in rc_commands
                ]
                self.assertEqual(commands, expected)

    def test_upload_download_errors_match_python_rc(self):
        failed = RunResult(stdout='', stderr='rsync failed', returncode=23)
        for method, args in [('upload', ('/local/a', '/remote/a')),
                             ('download', ('/remote/a', '/local/a'))]:
            with self.subTest(method=method):
                with mock.patch('rc.machine.run', return_value=failed):
                    with self.assertRaises(Exception) as rc_error:
                        getattr(self.rc_machine, method)(*args)
                with mock.patch('mocknet_ssh.run', return_value=failed):
                    with self.assertRaises(type(rc_error.exception)):
                        getattr(self.shared, method)(*args)

    def test_unexpected_python_rc_ssh_shell_fails(self):
        # _rsync_ssh_arg() removes the trailing 'user@ip --'. If python-rc
        # changes that shape, fail instead of building a wrong command.
        changed = self.rc_machine._ssh_shell()[:-1]
        with mock.patch.object(Machine, '_ssh_shell', return_value=changed):
            with self.assertRaises(RuntimeError):
                self.shared._ssh_shell()
            with self.assertRaises(RuntimeError):
                self.shared.upload('/local/a', '/remote/a')

    def test_enable_flag(self):
        for value, enabled in [(None, True), ('1', True), ('0', False),
                               (' Off ', False)]:
            with self.subTest(MOCKNET_SSH_CONTROLMASTER=value):
                if value is None:
                    os.environ.pop('MOCKNET_SSH_CONTROLMASTER', None)
                else:
                    os.environ['MOCKNET_SSH_CONTROLMASTER'] = value
                self.assertEqual(mocknet_ssh.ssh_control_master_enabled(),
                                 enabled)

    def test_control_persist(self):
        mocknet_ssh.set_default_ssh_control_persist('30m')
        self.assertIn('ControlPersist=30m', self.shared._ssh_shell())
        os.environ['MOCKNET_SSH_CONTROL_PERSIST'] = '5m'
        self.assertIn('ControlPersist=5m', self.shared._ssh_shell())

    def test_share_ssh_connections(self):
        node = mock.Mock(machine=make_machine())
        self.assertIs(mocknet_ssh.share_ssh_connections(node), node)
        shared = node.machine
        self.assertIsInstance(shared, SshSharingMachine)
        # A second call keeps the same machine.
        mocknet_ssh.share_ssh_connections(node)
        self.assertIs(node.machine, shared)


if __name__ == '__main__':
    unittest.main()

"""
SSH connection sharing for the mocknet / forknet tooling.

python-rc opens a new ssh session (TCP handshake, key exchange, auth) for
every Machine.run()/upload()/download() call. The forknet tooling sends many
short commands to each host (neard-runner JSON-RPC over `curl
localhost:3000`, systemd-run, file uploads...). This module makes OpenSSH send
all of them over one persistent master connection per (user, host, port), with
ControlMaster/ControlPersist. The first command to a host still does the full
handshake. Later commands use the master socket.

Set MOCKNET_SSH_CONTROLMASTER=0 to disable the sharing. ssh and rsync then get
the plain python-rc command lines.

How long an idle master stays alive (ssh_config(5) ControlPersist) depends on
the entry point, because people use the tooling at different speeds:
  * tests/mocknet/mirror.py (manual use) sets 30m,
  * tests/mocknet/forknet_scenario.py (scripted runs) sets 10m,
with set_default_ssh_control_persist(). MOCKNET_SSH_CONTROL_PERSIST overrides
both. ssh fixes the value when it creates a master. A later invocation that
uses an existing master gets the timeout of that master.

Master sockets are ~/.ssh/mocknet-cm-<hash>. %C is sha1(local hostname, remote
host, port, remote user), so the name does not depend on cwd, TMPDIR, the key
path or the process. All mirror.py / forknet_scenario.py runs from the same
user and machine use the same master. The `mocknet-` prefix keeps these
sockets apart from personal ControlPath setups, which often use `cm-`.

  List masters:   ls ~/.ssh/mocknet-cm-*
  Check one:      ssh -O check -o ControlPath=~/.ssh/mocknet-cm-%C ubuntu@<ip>
  Close one:      ssh -O exit -o ControlPath=~/.ssh/mocknet-cm-%C ubuntu@<ip>
  Close all:      for s in ~/.ssh/mocknet-cm-*; do
                    ssh -O exit -o ControlPath="$s" mocknet; done

Use `ssh -O exit` to close a live master (e.g. after a host reboot). Do not
`rm` the socket of a live master: that does not stop the master process, and
then `ssh -O exit` cannot reach it. ssh itself removes stale sockets of dead
masters.

SshSharingMachine depends on python-rc internals: the argv shape of
Machine._ssh_shell() and the rsync commands in Machine.upload()/download().
tests/mocknet/test_mocknet_ssh.py checks both. NayDuck runs it (see
nightly/pytest-mocknet.txt). Also run it when you change the python-rc version
in requirements.txt.
"""
import os
import shlex

from rc import run
from rc.exception import DownloadException, UploadException
from rc.machine import Machine

SSH_CONTROL_DIR = os.path.expanduser('~/.ssh')
SSH_CONTROL_PATH = os.path.join(SSH_CONTROL_DIR, 'mocknet-cm-%C')
SSH_CONTROL_PERSIST_DEFAULT = '10m'

# Values of MOCKNET_SSH_CONTROLMASTER that disable the sharing. Unset, empty or
# any other value keeps it enabled.
_DISABLED_VALUES = ('0', 'false', 'no', 'off')


def set_default_ssh_control_persist(value):
    """
    Entry points call this to set the ControlPersist used when a master is
    created. MOCKNET_SSH_CONTROL_PERSIST still overrides it.
    """
    global SSH_CONTROL_PERSIST_DEFAULT
    SSH_CONTROL_PERSIST_DEFAULT = value


def ssh_control_persist():
    return os.getenv(
        'MOCKNET_SSH_CONTROL_PERSIST') or SSH_CONTROL_PERSIST_DEFAULT


def ssh_control_master_enabled():
    value = os.getenv('MOCKNET_SSH_CONTROLMASTER', '').strip().lower()
    return value not in _DISABLED_VALUES


def ssh_control_master_options():
    """ssh(1) arguments that make every session use one master connection."""
    if not ssh_control_master_enabled():
        return []
    # ControlPath must be in an existing, private directory.
    os.makedirs(SSH_CONTROL_DIR, mode=0o700, exist_ok=True)
    return [
        '-o',
        'ControlMaster=auto',
        '-o',
        f'ControlPersist={ssh_control_persist()}',
        '-o',
        f'ControlPath={SSH_CONTROL_PATH}',
        # Only used when a master is created. A master whose peer is gone
        # without a message (host reboot, network partition) exits within a
        # minute, so the commands that use it do not wait forever.
        '-o',
        'ServerAliveInterval=15',
        '-o',
        'ServerAliveCountMax=3',
    ]


class SshSharingMachine(Machine):
    """
    rc.Machine whose ssh and rsync invocations go through a shared
    ControlMaster connection. Everything else (key, user, StrictHostKeyChecking,
    rsync flags, sudo handling) is the same as rc.Machine.
    """

    @classmethod
    def from_machine(cls, machine):
        if isinstance(machine, cls):
            return machine
        shared = cls.__new__(cls)
        shared.__dict__.update(machine.__dict__)
        return shared

    # Used by run(), running(), run_stream(), bash(), sudo(), python()...
    def _ssh_shell(self):
        shell = super()._ssh_shell()
        # The options go after 'ssh', and _rsync_ssh_arg() removes the
        # trailing 'user@ip --'. Fail if python-rc changes this shape, instead
        # of building a wrong command.
        destination = f'{self.username}@{self.ip}'
        if shell[0] != 'ssh' or shell[-2:] != [destination, '--']:
            raise RuntimeError(
                f'unexpected python-rc ssh command {shell}; expected '
                f"['ssh', ..., '{destination}', '--']. Update SshSharingMachine "
                'for the installed python-rc version.')
        return [shell[0], *ssh_control_master_options(), *shell[1:]]

    def _rsync_ssh_arg(self):
        # Same ssh command as _ssh_shell() without the trailing 'user@ip --'.
        return shlex.quote(shlex.join(self._ssh_shell()[:-2]))

    # upload() and download() are copies of python-rc 0.4.1 Machine.upload()
    # and Machine.download(). The only change is the `rsync -e` command.
    def upload(self,
               local_path,
               machine_path,
               switch_user=None,
               su=None,
               user=None):
        user = user or su or switch_user
        if user:
            rsync = f"--rsync-path='sudo -u {user} rsync' "
        else:
            rsync = ''
        p = run(f"rsync -e {self._rsync_ssh_arg()} -r "
                f"{rsync}--progress {local_path} "
                f"{self.username}@{self.ip}:{machine_path}")
        if p.returncode != 0:
            raise UploadException(p.stderr)
        return p

    def download(self, machine_path, local_path, sudo=True):
        if sudo:
            rsync = "--rsync-path='sudo rsync' "
        else:
            rsync = ''
        p = run(f"rsync -e {self._rsync_ssh_arg()} -r "
                f"{rsync}--progress {self.username}@{self.ip}:{machine_path} "
                f"{local_path}")
        if p.returncode != 0:
            raise DownloadException(p.stderr)
        return p


def share_ssh_connections(node):
    """Make all ssh and rsync commands to `node` use a shared master."""
    node.machine = SshSharingMachine.from_machine(node.machine)
    return node

#!/usr/bin/env python3
"""Verifies that node can be started inside of a Docker image.

The script builds Docker image using 'make docker-nearcore' command and then
starts a cluster with all nodes running inside of containers.  As sanity check
to see if the cluster works correctly, the test waits for a several blocks and
then interrogates each node about block with specified hash expecting all of
them to return the same data.

The purpose of the test is to verify that:
- `make docker-nearcore` builds a working Docker image,
- `docker run ... nearcore` (i.e. the `run_docker.sh` script) works,
- `docker run -eBOOT_NODE=... ... nearcore` (i.e. passing boot nodes via
  BOOT_NODE environment variable) works and
- `docker run ... nearcore sh -c 'neard ...'` works.
"""

import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import typing
import uuid

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils

BLOCKS = 42
NUM_NODES = 3

_REPO_DIR = pathlib.Path(__file__).resolve().parents[3]
_CD_PRINTED = False
_Command = typing.Sequence[typing.Union[str, pathlib.Path]]

_DOCKER_IMAGE_TAG = 'nearcore-testimage-' + uuid.uuid4().hex


def run(cmd: _Command, *, capture_output: bool = False) -> typing.Optional[str]:
    """Trivial wrapper around subprocess.check_({all,output}.

    Args:
        cmd: Command to execute.
        capture_output: If true, captures standard output of the command and
            returns it.
    Returns:
        Command's stripped standard output if `capture_output` is true, None
        otherwise.
    """
    global _CD_PRINTED
    if not _CD_PRINTED:
        logger.debug(f'+ cd {shlex.quote(str(_REPO_DIR))}')
        _CD_PRINTED = True
    logger.debug('+ ' + ' '.join(shlex.quote(str(arg)) for arg in cmd))
    if capture_output:
        return subprocess.check_output(cmd, cwd=_REPO_DIR,
                                       encoding='utf-8').strip()
    subprocess.check_call(cmd, cwd=_REPO_DIR)
    return None


def docker_run(shell_cmd: typing.Optional[str] = None,
               *,
               detach: bool = False,
               network: bool = False,
               volume: typing.Tuple[pathlib.Path, str],
               env: typing.Dict[str, str] = {}) -> typing.Optional[str]:
    """Runs a `docker run` command.

    Args:
        shell_cmd: Optionally a shell command to execute inside of the
            container.  It's going to be run using `sh -c` and it's caller's
            responsibility to make sure all data inside is properly sanitised.
            If not given, command configured via CMD when building the container
            will be executed as typical for Docker images.
        detach: Whether the `docker run` command should detach or not.  If
            False, standard output and standard error will be passed through to
            test outputs.  If True, the container will be detached and the
            function will return its container id.
        network: Whether to enable network.  If True, the container will be
            configured to use host's network.  This allows processes in
            different containers to connect with each other easily.
        volume: A (path, container_path) tuple denoting that local `path` should
            be mounted under `container_path` inside of the container.
        env: *Additional* environment variables set inside of the container.

    Returns:
        Command's stripped standard output if `detach` is true, None otherwise.
    """
    cmd = ['docker', 'run', '--read-only', f'-v{volume[0]}:{volume[1]}']

    # Either run detached or attach standard output and standard error so they
    # are visible.
    if detach:
        cmd.append('-d')
    else:
        cmd.extend(('-astdout', '-astderr'))

    if network:
        # Don’t create separate network.  This makes it much simpler for nodes
        # inside of the containers to communicate with each other.
        cmd.append('--network=host')
    else:
        # The command does not need networking so disable it.
        cmd.append('--network=none')

    # Use current user to run the code inside the container so that data saved
    # in home will be readable by us outside of the container (otherwise, the
    # command would be run as root and data would be owned by root).
    cmd.extend(('-u', f'{os.getuid()}:{os.getgid()}', '--userns=host'))

    # Set environment variables.
    rust_log = ('actix_web=warn,mio=warn,tokio_util=warn,'
                'actix_server=warn,actix_http=warn,' +
                os.environ.get('RUST_LOG', 'debug'))
    cmd.extend(('-eRUST_BACKTRACE=1', f'-eRUST_LOG={rust_log}'))
    for key, value in env.items():
        cmd.append((f'-e{key}={value}'))

    # Specify the image to run.
    cmd.append(_DOCKER_IMAGE_TAG)

    # And finally, specify the command.
    if shell_cmd:
        cmd.extend(('sh', '-c', shell_cmd))

    return run(cmd, capture_output=detach)


class DockerNode(cluster.LocalNode):
    """A node run inside of a Docker container."""

    def __init__(self, ordinal: int, node_dir: pathlib.Path) -> None:
        super().__init__(port=24567 + 10 + ordinal,
                         rpc_port=3030 + 10 + ordinal,
                         near_root='',
                         node_dir=str(node_dir),
                         blacklist=[])
        self._container_id = None

    def start(self, *, boot_node: cluster.BootNode = None) -> None:
        """Starts a node inside of a Docker container.

        Args:
            boot_node: Optional boot node to pass to the node.
        """
        assert self._container_id is None
        assert not self.cleaned

        env = {}
        if boot_node:
            env['BOOT_NODES'] = cluster.make_boot_nodes_arg(boot_node)[1]

        cid = docker_run(detach=True,
                         network=True,
                         volume=(self.node_dir, '/srv/near'),
                         env=env)
        self._container_id = cid
        logger.info(f'Node started in Docker container {cid}')

    def kill(self):
        cid = self._container_id
        if cid:
            self._container_id = None
            logger.info(f'Stopping container {cid}')
            run(('docker', 'stop', cid))

    __WARN_LOGS = True

    def output_logs(self):
        # Unfortunately because we’re running the containers as detached
        # anything neard writes to stdout and stderr is lost.  We could start
        # nodes using `docker run -d ... sh -c 'neard ... >stdout 2>stderr'` but
        # that would mean that we’re not testing the `run_docker.sh` script
        # which we do want to read.
        if self.__WARN_LOGS:
            logger.info(
                'Due to technical limitations logs from node is not available')
            type(self).__WARN_LOGS = False
        pass


def main():
    nodes = []

    logger.info("Build the container")
    run(('make', 'DOCKER_TAG=' + _DOCKER_IMAGE_TAG, 'docker-nearcore'))
    try:
        dot_near = pathlib.Path.home() / '.near'

        logger.info("Initialise local network nodes config.")
        cmd = f'neard --home /home/near localnet --v {NUM_NODES} --prefix test'
        docker_run(cmd, volume=(dot_near, '/home/near'), network=True)

        # Start all the nodes
        for ordinal in range(NUM_NODES):
            logger.info(f'Starting node {ordinal}')
            node = DockerNode(ordinal, dot_near / f'test{ordinal}')
            node.start(boot_node=nodes)
            nodes.append(node)

        # Wait for them to initialise
        for ordinal, node in enumerate(nodes):
            logger.info(f'Waiting for node {ordinal} to respond')
            node.wait_for_rpc(10)

        # Wait for BLOCKS blocks to be generated
        latest = utils.wait_for_blocks(nodes[0], target=BLOCKS)

        # Fetch latest block from all the nodes
        blocks = []
        for ordinal, node in enumerate(nodes):
            utils.wait_for_blocks(node, target=latest.height)
            response = node.get_block(latest.hash)
            assert 'result' in response, (ordinal, block)
            block = response['result']
            blocks.append(block)
            bid = cluster.BlockId.from_header(block['header'])
            logger.info(f'Node {ordinal} sees block: {bid}')

        # All blocks should be equal
        for ordinal in range(1, NUM_NODES):
            assert blocks[0] == blocks[ordinal], (ordinal, blocks)

        logger.info('All good')

    finally:
        # `docker stop` takes a few seconds so stop all containers in parallel.
        # atexit we’ll call DockerNode.cleanup method for each node as well and
        # it’ll handle all the other cleanups.
        cids = tuple(filter(None, (node._container_id for node in nodes)))
        if cids:
            logger.info('Stopping containers')
            run(('docker', 'rm', '-f') + cids)
        for node in nodes:
            node._container_id = None

        subprocess.check_call(
            ('docker', 'image', 'rm', '-f', _DOCKER_IMAGE_TAG))


if __name__ == '__main__':
    main()

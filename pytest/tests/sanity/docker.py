#!/usr/bin/env python3
"""Verifies that node can be started inside of a Docker image.

The script builds Docker image using ‘make docker’ command and then starts
a cluster with all nodes running inside of containers.  To see if the cluster
works correctly, the test simply waits for a several blocks to be generated and
then interrogates each node about that block expecting all of them to return the
same data.
"""

import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import typing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils

NUM_NODES = 2
BLOCKS = 42

_REPO_DIR = pathlib.Path(__file__).resolve().parents[3]
_CD_PRINTED = False
_Command = typing.Sequence[typing.Union[str, pathlib.Path]]


def run(cmd: _Command, *, capture_output: bool = False) -> typing.Optional[str]:
    """Trivial wrapper around subprocess.check_({all,output}.

    Args:
        cmd: Command to execute.
        capture_output: If true, captures standard output of the command and
            returns it.
    Returns:
        Command’s stripped standard output if `capture_output` is true, None
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


def docker_run(shell_cmd: str,
               *,
               detach: bool = False,
               network: bool = False,
               home: pathlib.Path) -> typing.Optional[str]:
    """Runs a `docker run` command.

    Args:
        shell_cmd: A shell command to execute inside of the container.  It’s
            going to be run using `sh -c` and it’s caller’s responsibility to
            make sure all data inside is properly sanitised.
        detach: Whether the `docker run` command should detach or not.  If
            False, standard output and standard error will be passed through to
            test outputs.  If True, the container will be detached and the
            function will return its container id.
        network: Whether to enable network.  If True, the container will be
            configured to use host’s network.  This allows processes in
            different containers to connect with each other easily.
        home: A directory to mount as /home/near volume inside of the container.

    Returns:
        Command’s stripped standard output if `detach` is true, None otherwise.
    """
    cmd = ['docker', 'run']

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
        cmd.append('--network=none')

    # Map home to /home/near and since the command will not modify anything else
    # run in read only mode.
    cmd.extend(('--read-only', '-v', f'{home}:/home/near'))

    # Use current user to run the code inside the container so that data saved
    # in home will be readable by us outside of the container (otherwise, the
    # command would be run as root and data would be owned by root).
    cmd.extend(('-u', f'{os.getuid()}:{os.getgid()}', '--userns=host'))

    # Configure logging and debugging via environment variables.
    rust_log = ('actix_web=warn,mio=warn,tokio_util=warn,'
                'actix_server=warn,actix_http=warn,' +
                os.environ.get('RUST_LOG', 'debug'))
    cmd.extend(('-e', 'RUST_BACKTRACE=1', '-e', f'RUST_LOG={rust_log}'))

    # Specify the image to run.
    cmd.append('nearcore')

    # And finally, specify the command.
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

    def start(self, boot_node: cluster.BootNode = None) -> None:
        assert self._container_id is None
        assert not self.cleaned

        script = 'neard --home /home/near run '
        script += ' '.join(
            shlex.quote(arg) for arg in cluster.make_boot_nodes_arg(boot_node))
        script += '>/home/near/stdout 2>/home/near/stderr'

        cid = docker_run(script, detach=True, network=True, home=self.node_dir)
        self._container_id = cid
        logger.info(f'Node started in Docker container {cid}')

    def kill(self):
        cid = self._container_id
        if cid:
            self._container_id = None
            logger.info(f'Stopping container {cid}')
            run(('docker', 'stop', cid))


def main():
    # Build the container
    run(('make', 'docker-nearcore'))

    dot_near = pathlib.Path.home() / '.near'

    # Initialise local network
    cmd = f'neard --home /home/near localnet --v {NUM_NODES} --prefix test'
    docker_run(cmd, home=dot_near)

    # Start all the nodes
    nodes = []
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

    # Fetch latest block from all the nodess
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
        assert blocks[0] == blocks[1], (ordinal, blocks)

    logger.info(f'All good')


if __name__ == '__main__':
    main()

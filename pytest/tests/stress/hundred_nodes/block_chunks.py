#!/usr/bin/env python3
import sys, time
import subprocess
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import GCloudNode, RpcNode
from configured_logger import new_logger
from utils import chain_query


def print_chain_data(block, logger):
    chunks = []
    for c in block['chunks']:
        chunks.append(
            f'{c["chunk_hash"]} {c["shard_id"]} {c["height_created"]} {c["height_included"]}'
        )
    logger.info(
        f"{block['header']['height']} {block['header']['hash']} {','.join(chunks)}"
    )


subprocess.run('mkdir -p /tmp/100_node/', shell=True)

f = []
for node in range(100):
    f.append(new_logger(outfile=f'/tmp/100_node/pytest-node-{node}.txt'))


def query_node(i):
    node = GCloudNode(f'pytest-node-{i}')
    chain_query(node, lambda b: print_chain_data(b, f[i]), max_blocks=20)


# pmap(query_node, range(100))

node = GCloudNode('pytest-node-0')
chain_query(node,
            print_chain_data,
            block_hash='9rnC5G6qDpXgT4gTG4znowmdSUavC1etuV99F18ByxxK')

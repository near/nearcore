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
    all_height = list(map(lambda b: b['height_included'], block['chunks']))
    all_catch_up = False
    if all(map(lambda h: h == block['header']['height'], all_height)):
        all_catch_up = True
    logger.info(
        f"{block['header']['hash']} {block['header']['height']} {block['header']['approvals']} {all_catch_up} {all_height}"
    )


subprocess.run('mkdir -p /tmp/100_node/', shell=True)

f = []
for node in range(100):
    f.append(new_logger(outfile=f'/tmp/100_node/pytest-node-{node}.txt'))


def query_node(i):
    node = GCloudNode(f'pytest-node-{i}')
    chain_query(node, lambda b: print_chain_data(b, f[i]), max_blocks=20)


pmap(query_node, range(100))

# node = RpcNode('localhost', 3030)
# chain_query(node, print_chain_data, max_blocks=20)

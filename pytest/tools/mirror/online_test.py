#!/usr/bin/env python3

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import spin_up_node, load_config
from configured_logger import logger

import mirror_utils

# This sets up an environment to test the tools/mirror process. It starts a localnet with a few validators
# and waits for some blocks to be produced. Then we fork the state and start a new chain from that, and
# start the mirror process while the sending more traffic to the source chain


def main():
    config = load_config()

    near_root, source_nodes, target_node_dirs, traffic_data = mirror_utils.start_source_chain(
        config)

    target_nodes = [
        spin_up_node(config, near_root, target_node_dirs[i],
                     len(source_nodes) + 1 + i)
        for i in range(len(target_node_dirs))
    ]

    mirror = mirror_utils.MirrorProcess(near_root,
                                        mirror_utils.dot_near() /
                                        f'{mirror_utils.MIRROR_DIR}/source',
                                        online_source=True)
    mirror_utils.send_traffic(near_root, source_nodes, traffic_data,
                              mirror.restart_once)

    end_source_height = source_nodes[0].get_latest_block().height
    time.sleep(5)
    # we don't need these anymore
    for node in source_nodes[1:]:
        node.kill()

    total_time_allowed = mirror_utils.allowed_run_time(target_node_dirs[0],
                                                       mirror.start_time,
                                                       end_source_height)
    time_elapsed = time.time() - mirror.start_time
    if time_elapsed < total_time_allowed:
        time_left = total_time_allowed - time_elapsed
        logger.info(
            f'waiting for {int(time_left)} seconds to allow transactions to make it to the target chain'
        )
        time.sleep(time_left)
    mirror_utils.check_num_txs(source_nodes[0], target_nodes[0])


if __name__ == '__main__':
    main()

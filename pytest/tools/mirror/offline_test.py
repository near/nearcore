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
# send some traffic. After a while we stop the source chain nodes and start the target chain nodes,
# and run the mirror binary that should send the source chain traffic to the target chain


def main():
    config = load_config()

    near_root, source_nodes, target_node_dirs, traffic_data = mirror_utils.start_source_chain(
        config)

    # sleep for a bit to allow test0 to catch up after restarting before we send traffic
    time.sleep(5)
    mirror_utils.send_traffic(near_root, source_nodes, traffic_data,
                              lambda: True)

    target_nodes = [
        spin_up_node(config, near_root, target_node_dirs[i],
                     len(source_nodes) + 1 + i)
        for i in range(len(target_node_dirs))
    ]

    end_source_height = source_nodes[0].get_latest_block().height
    time.sleep(5)
    # we don't need these anymore
    for node in source_nodes[1:]:
        node.kill()

    mirror = mirror_utils.MirrorProcess(near_root,
                                        source_nodes[1].node_dir,
                                        online_source=False)

    total_time_allowed = mirror_utils.allowed_run_time(target_node_dirs[0],
                                                       mirror.start_time,
                                                       end_source_height)
    while True:
        time.sleep(5)

        mirror_utils.check_target_validators(target_nodes[0])

        # this will restart the binary one time during this test, and it will return false
        # when it exits on its own, which should happen once it finishes sending all the
        # transactions in its source chain (~/.near/test1/)
        if not mirror.restart_once():
            break
        elapsed = time.time() - mirror.start_time
        if elapsed > total_time_allowed:
            logger.warn(
                f'mirror process has not exited after {int(elapsed)} seconds. stopping the test now'
            )
            break

    mirror_utils.check_num_txs(source_nodes[0], target_nodes[0])


if __name__ == '__main__':
    main()

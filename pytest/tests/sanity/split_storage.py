#!/usr/bin/python3
"""
Spins up an archival node with cold store configured and verifies that blocks are copied from hot to cold store.
"""

import sys
import pathlib
import os
import copy
import json

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from utils import poll_blocks
from cluster import init_cluster, spin_up_node, load_config, get_config_json, set_config_json
from configured_logger import logger


def main():
    config = load_config()
    near_root, [node_dir] = init_cluster(1, 0, 1, config, [], {})

    node_config = get_config_json(node_dir)

    node_config["archive"] = True
    # Need to create a deepcopy of the store config, otherwise
    # store and cold store will point to the same instance and
    # both will end up with the same path.
    node_config["cold_store"] = copy.deepcopy(node_config["store"])
    node_config["store"]["path"] = os.path.join(node_dir, 'data')
    node_config["cold_store"]["path"] = os.path.join(node_dir, 'cold_data')

    set_config_json(node_dir, node_config)

    node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

    n = 20

    for height, hash in poll_blocks(node, timeout=n + 10):
        if height > n:
            break

    split_storage_info = node.json_rpc("EXPERIMENTAL_split_storage_info", {})

    logger.info(
        f"split storage info \n{json.dumps(split_storage_info, indent=2)}")

    assert "error" not in split_storage_info
    assert "result" in split_storage_info
    result = split_storage_info["result"]
    head_height = result["head_height"]
    final_head_height = result["final_head_height"]
    cold_head_height = result["cold_head_height"]

    assert head_height >= n
    assert final_head_height >= n - 3
    assert cold_head_height >= final_head_height - 3

    node.kill(gentle=True)


if __name__ == "__main__":
    main()

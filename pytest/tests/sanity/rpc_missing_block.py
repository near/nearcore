import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / "lib"))

from cluster import start_cluster
from configured_logger import logger
import utils
from geventhttpclient import useragent

nodes = start_cluster(
    num_nodes=4,
    num_observers=1,
    num_shards=4,
    config=None,
    extra_state_dumper=True,
    genesis_config_changes=[
        ["min_gas_price", 0],
        ["max_inflation_rate", [0, 1]],
        ["epoch_length", 10],
        ["block_producer_kickout_threshold", 70],
    ],
    client_config_changes={
        0: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        1: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        2: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        3: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        4: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            },
            "tracked_shards_config": "AllShards",
        },
    },
)

nodes[1].kill()


def check_bad_block(node, height):
    try:
        node.get_block_by_height(height)
        assert False, f"Expected an exception for block height {height} but none was raised"
    except useragent.BadStatusCode as e:
        assert "code=422" in str(
            e), f"Expected status code 422 in exception, got: {e}"
    except Exception as e:
        assert False, f"Unexpected exception type raised: {type(e)}. Exception: {e}"


last_height = -1

for height, hash in utils.poll_blocks(nodes[0]):
    if height >= 20:
        break

    response = nodes[0].get_block_by_height(height)
    assert not "error" in response
    logger.info(f"good RPC response for: {height}")

    if last_height != -1:
        for bad_height in range(last_height + 1, height):
            response = check_bad_block(nodes[0], bad_height)
            logger.info(f"422 response for: {bad_height}")

    last_height = height

response = nodes[0].get_block_by_height(last_height + 9999)
assert "error" in response, f"Expected an error for block height 9999, got: {response}"
assert (
    response["error"]["cause"]["name"] == "UNKNOWN_BLOCK"
), f"Expected UNKNOWN_BLOCK error, got: {response['error']['cause']['name']}"

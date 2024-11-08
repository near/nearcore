import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / "lib"))

from cluster import start_cluster
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
        0: {"consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
        1: {"consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
        2: {"consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
        3: {"consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}}},
        4: {
            "consensus": {"state_sync_timeout": {"secs": 2, "nanos": 0}},
            "tracked_shards": [0, 1, 2, 3],
        },
    },
)

time.sleep(20)

response = nodes[0].get_block_by_height(9999)
assert "error" in response, f"Expected an error for block height 9999, got: {response}"
assert (
    response["error"]["cause"]["name"] == "UNKNOWN_BLOCK"
), f"Expected UNKNOWN_BLOCK error, got: {response['error']['cause']['name']}"

try:
    nodes[0].get_block_by_height(1)
    assert False, "Expected an exception for block height 1 but none was raised"
except useragent.BadStatusCode as e:
    assert "code=422" in str(e), f"Expected status code 422 in exception, got: {e}"
except Exception as e:
    assert False, f"Unexpected exception type raised: {type(e)}. Exception: {e}"

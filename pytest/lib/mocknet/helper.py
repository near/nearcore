import json
import requests
import time
import collections
from prometheus_client.parser import text_string_to_metric_families
from configured_logger import logger

MAX_RETRIES = 10


# TODO(posvyatokum) reuse retry_and_ignore_errors method from mocknet_helpers
def request_from_helper(address, method, params=[]):
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(
                f'Requesting {method} method from {address} helper. Attempt #{attempt}'
            )
            j = {
                'method': method,
                'params': params,
                'id': 'dontcare',
                'jsonrpc': '2.0'
            }
            r = requests.post(f'http://{address}', json=j, timeout=5)
            r.raise_for_status()
            response = json.loads(r.content)
            return response['result']
        except Exception as e:
            logger.error(e)
            time.sleep(0.1 * 2**attempt)
    return ""


def modify_config(node, modification):
    request_from_helper(node['address'], 'modify_config', [modification])


def stop_node(node):
    request_from_helper(node['address'], 'stop')


def stop_nodes(nodes):
    for node in nodes:
        stop_node(node)


def restart_node(node, binary, unsafe):
    request_from_helper(node['address'], 'restart', {
        'unsafe': unsafe,
        'binary_name': binary
    })


def restart_nodes(nodes, binary='neard', unsafe=False):
    for node in nodes:
        restart_node(node, binary, unsafe)


def send_json_rpc_request(node, method, params):
    return request_from_helper(node['address'], 'json_rpc', {
        'method': method,
        'params': params,
    })


def check_node_is_live(node):
    result = send_json_rpc_request(node, 'validators', [None])
    if len(result) == 0:
        return False
    response = json.loads(result)
    return 'error' not in response and 'result' in response


def set_s3_state_parts_dump_mode_for_node(node, bucket, region):
    modify_config(
        node, {
            "state_sync": {
                "dump": {
                    "location": {
                        "S3": {
                            "bucket": bucket,
                            "region": region,
                        }
                    }
                }
            }
        })


def set_local_state_parts_dump_mode_for_node(node, root_dir):
    modify_config(
        node, {
            "state_sync": {
                "dump": {
                    "location": {
                        "Filesystem": {
                            "root_dir": root_dir,
                        }
                    }
                }
            }
        })


def set_state_parts_dump_mode_for_nodes(nodes,
                                        bucket=None,
                                        region=None,
                                        root_dir=None):
    for node in nodes:
        if root_dir is not None:
            set_local_state_parts_dump_mode_for_node(node, root_dir)
        elif bucket is not None and region is not None:
            set_s3_state_parts_dump_mode_for_node(node, bucket, region)
        else:
            raise Exception(
                "Must provide either (root_dir) or (bucket, region) to enable StatePart dump"
            )


def set_s3_state_parts_sync_mode_for_node(node, bucket, region):
    modify_config(
        node, {
            "state_sync": {
                "sync": {
                    "location": {
                        "S3": {
                            "bucket": bucket,
                            "region": region,
                        }
                    }
                }
            }
        })


def set_local_state_parts_sync_mode_for_node(node, root_dir):
    modify_config(
        node, {
            "state_sync": {
                "sync": {
                    "location": {
                        "Filesystem": {
                            "root_dir": root_dir,
                        }
                    }
                }
            }
        })


def set_state_parts_sync_mode_for_nodes(nodes,
                                        bucket=None,
                                        region=None,
                                        root_dir=None):
    for node in nodes:
        if root_dir is not None:
            set_local_state_parts_sync_mode_for_node(node, root_dir)
        elif bucket is not None and region is not None:
            set_s3_state_parts_sync_mode_for_node(node, bucket, region)
        else:
            raise Exception(
                "Must provide either (root_dir) or (bucket, region) to enable StatePart sync"
            )


def set_tracked_shard_for_node(node, tracked_shards):
    modify_config(node, {
        "tracked_shards": tracked_shards,
    })


def set_tracked_shard_for_nodes(nodes, tracked_shards):
    for node in nodes:
        set_tracked_shard_for_node(node, tracked_shards)


def enable_state_sync(nodes):
    for node in nodes:
        modify_config(node, {"state_sync_enabled": True})


def get_metrics(node):
    response_result = request_from_helper(node["address"], "metrics", {})
    final_result = collections.defaultdict(dict)
    for metric_family in text_string_to_metric_families(response_result):
        for sample in metric_family.samples:
            final_result[sample.name][str(set(
                (sample.labels.values())))] = sample.value
    return final_result

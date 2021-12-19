#!/usr/bin/env python3
# Simulates routing table exchange with two nodes A, B. We are testing a few test cases depending on the number of
# edges A, has but B doesn't and vise-versa. For each configuration, we simulate doing routing table exchange, and
# we check whenever both have the same version of routing table at the end.
#
# Uses JsonRPC to tell nodes, not to verify edges, for performance reasons, don't propagate added edges, and disable
# edge pruning in order to eliminate those factors for sake of testing. In addition uses JsonRPC to add/remove edges
# check current state of routing table on both sites.

import os
import sys
import time
import pathlib

import base58
import ed25519

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from peer import logger
from proxy import ProxyHandler
from multiprocessing import Value

TIMEOUT = 300
success = Value('i', 0)


class Handler(ProxyHandler):

    async def handle(self, msg, fr, to):
        if msg.enum == "RoutingTableSync":
            logger.info("RoutingTableSync")
        if msg.enum == "RoutingTableSyncV2" and msg.RoutingTableSyncV2.enum == "Version2":
            if msg.RoutingTableSyncV2.Version2.routing_state.enum == "Done":
                success.value = 1
            logger.info("ROUTING_STATE %s" %
                        msg.RoutingTableSyncV2.Version2.routing_state.enum)
            logger.info("* known_edges %s" %
                        msg.RoutingTableSyncV2.Version2.known_edges)
            logger.info("* edges %s" %
                        len(msg.RoutingTableSyncV2.Version2.edges))
        return True


nodes = start_cluster(2, 0, 1, None, [], {}, Handler)

time.sleep(10)

signing_key, verifying_key = ed25519.create_keypair()

logger.info(str(signing_key))


def gen():
    return base58.b58encode(
        ed25519.create_keypair()[0].to_bytes()).decode('ascii')


def new_edge(peer_a, peer_b, nonce):
    s1 = base58.b58encode(str(peer_a).zfill(32)).decode("ascii")
    s2 = base58.b58encode(str(peer_b).zfill(32)).decode("ascii")
    return {
        'nonce':
            nonce,
        'key': [
            'ed25519:' + min(s1, s2),
            'ed25519:' + max(s1, s2),
        ],
        'removal_info':
            None,
        'signature0':
            'ed25519:5ca6A3uzrK3FKGjeAEi7pLNDiBbrzQ1ZPseXF1Rw8rfr3RWYNXPckVDUGGKMZQFE8eKWXeeCws6ZvmeQW8PZEzP6',
        'signature1':
            'ed25519:5ca6A3uzrK3FKGjeAEi7pLNDiBbrzQ1ZPseXF1Rw8rfr3RWYNXPckVDUGGKMZQFE8eKWXeeCws6ZvmeQW8PZEzP6'
    }


def chunks(ary, n):
    n = max(1, n)
    return (ary[i:i + n] for i in range(0, len(ary), n))


tests = [
    # (unique edges left node has, ... for right node, common edges, TIMEOUT)
    [3, 3, 0, 5],
    # Only first node has new edges
    [3, 0, 0, 5],
    # Only second node has new edges
    [0, 3, 0, 5],
    # check edge case, where full sync is required
    [25000, 25000, 0, 15],
    # Both nodes have new edges
    [1, 11, 0, 5],
    # Both nodes have 1 each other doesn't know about, and there are bunch of common edges
    [1, 1, 50000, 5],
    # medium test, both nodes have some edges
    [10000, 10000, 0, 15],
]

logger.info(nodes[0].json_rpc(
    "adv_set_options", {
        "disable_edge_propagation": False,
        "disable_edge_signature_verification": True,
        "adv_disable_edge_pruning": True,
    }))
logger.info(nodes[1].json_rpc(
    "adv_set_options", {
        "disable_edge_propagation": False,
        "disable_edge_signature_verification": True,
        "adv_disable_edge_pruning": True,
    }))

peer_id1 = nodes[1].json_rpc("adv_get_peer_id", {})["result"]["peer_id"]
logger.info("peer_id1 %s", peer_id1)

test_id = 0

for (left, right, common, TIMEOUT) in tests:
    test_id += 1
    start = time.time()
    logger.info("test with params left: %s right: %s common: %s" %
                (left, right, common))
    time.sleep(1)
    # set options

    common_nodes = [
        new_edge(test_id * int(1e9) + int(2e7),
                 test_id * int(1e9) + x + 1 + left + right, 2 * test_id + 1)
        for x in range(common)
    ]
    left_nodes = [
        new_edge(test_id * int(1e9) + 0,
                 test_id * int(1e9) + x + 1, 2 * test_id + 1)
        for x in range(left)
    ]
    right_nodes = [
        new_edge(test_id * int(1e9) + int(1e7),
                 test_id * int(1e9) + x + 1 + left, 2 * test_id + 1)
        for x in range(right)
    ]
    to_node0 = left_nodes + common_nodes
    to_node1 = right_nodes + common_nodes

    def simplify(edges):
        return [{'key': edge['key'], 'nonce': edge['nonce']} for edge in edges]

    all_edges = simplify(left_nodes + to_node1)

    # add edges
    logger.info("sending new edges")

    for chunk in chunks(to_node0, 10000):
        logger.info(nodes[0].json_rpc("adv_set_routing_table",
                                      {"add_edges": chunk},
                                      timeout=30))
    for chunk in chunks(to_node1, 10000):
        logger.info(nodes[1].json_rpc("adv_set_routing_table",
                                      {"add_edges": chunk},
                                      timeout=30))

    time.sleep(1)

    def adv_get_routing_table(n):
        logger.info("getting routing tables")
        var_a = n[0].json_rpc("adv_get_routing_table", {},
                              timeout=60)["result"]["edges_info"]
        var_b = n[1].json_rpc("adv_get_routing_table", {},
                              timeout=60)["result"]["edges_info"]
        var_a = {(x['key'][0], x['key'][1], x['nonce']) for x in var_a}
        var_b = {(x['key'][0], x['key'][1], x['nonce']) for x in var_b}
        return var_a, var_b

    # compute set difference of routing tables
    a, b = adv_get_routing_table(nodes)
    logger.info("case 1 nodes %s vs %s diff %s " %
                (len(a), len(b), len(a.symmetric_difference(b))))
    assert (len(a) == 1 + left + common)
    assert (len(b) == 1 + right + common)
    assert (len(a.symmetric_difference(b)) == left + right)

    time.sleep(1)

    success.value = 0
    logger.info("starting_sync to %s" % (nodes[0].json_rpc(
        "adv_start_routing_table_syncv2", {"peer_id": peer_id1})))

    # wait for sync
    started = time.time()
    while success.value != 1:
        assert time.time() - started < TIMEOUT
        nodes[0].get_status(timeout=30)
        time.sleep(1)

    # wait some time for node to process adding edges
    time.sleep(max(5, TIMEOUT))

    # force synchronization

    a, b = adv_get_routing_table(nodes)
    logger.info("case 2 nodes %s vs %s diff %s " %
                (len(a), len(b), len(a.symmetric_difference(b))))
    assert (len(a.symmetric_difference(b)) == 0)

    # remove edges
    logger.info("removing edges")
    nodes[0].json_rpc("adv_set_routing_table", {"prune_edges": True},
                      timeout=30)
    nodes[1].json_rpc("adv_set_routing_table", {"prune_edges": True},
                      timeout=30)
    time.sleep(5)

    nodes[0].json_rpc("adv_set_routing_table", {"prune_edges": True},
                      timeout=30)
    nodes[1].json_rpc("adv_set_routing_table", {"prune_edges": True},
                      timeout=30)

    a, b = adv_get_routing_table(nodes)
    logger.info("case 3: %s %s" % (len(a), len(b)))
    assert (len(a) == 1 and len(b) == 1)
    logger.info("test took %s" % round(time.time() - start))

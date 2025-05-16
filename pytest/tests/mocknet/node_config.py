"""
Mocknet node configuration.

This module contains the configuration for the mocknet nodes.
It is used to configure the nodes for the test.
"""
import sys
from typing import Optional
from node_handle import NodeHandle
from utils import ScheduleContext


# TODO: these should be identified by tags
def _is_tracing_server(node):
    return node.name().endswith('tracing-server')


def _is_dumper(node):
    return node.name().endswith('dumper')


def _is_archival(node):
    return '-archival-' in node.name()


REMOTE_CONFIG = [
    {
        'node_matches': _is_dumper,
        'can_validate': False,
        'want_state_dump': True,
        'want_neard_runner': True,
    },
    {
        'node_matches': _is_tracing_server,
        'can_validate': False,
        'want_state_dump': False,
        'want_neard_runner': False,
    },
    {
        'node_matches': _is_archival,
        'can_validate': False,
        'want_state_dump': False,
        'want_neard_runner': True,
    },
]


def _is_two(node):
    return node.name() == 'node2'


def _is_three(node):
    return node.name() == 'node3'


TEST_CONFIG = [
    # {
    #     'node_matches': _is_two,
    #     'can_validate': False,
    #     'want_state_dump': True,
    #     'want_neard_runner': True,
    # },
    # {
    #     'node_matches': _is_three,
    #     'can_validate': False,
    #     'want_state_dump': False,
    #     'want_neard_runner': False,
    # },
]


# set attributes (e.g. do we want this node to possibly validate?) on this particular node
# schedule_ctx is optional, it is used to set the schedule context on the remote node only
def configure_nodes(nodes: list[NodeHandle], node_setup_config,
                    schedule_ctx: Optional[ScheduleContext]):
    for node in nodes:
        node.schedule_ctx = schedule_ctx
        node_config = None
        for c in node_setup_config:
            if c['node_matches'](node):
                if node_config is not None:
                    sys.exit(
                        f'multiple node configuration matches for {node.name()}'
                    )
                node_config = c
        if node_config is None:
            continue
        node.can_validate = node_config['can_validate']
        node.want_neard_runner = node_config['want_neard_runner']
        node.want_state_dump = node_config['want_state_dump']

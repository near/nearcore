"""
Mocknet node configuration.

This module contains the configuration for the mocknet nodes.
It is used to configure the nodes for the test.
"""
import logging

CONFIG_BY_ROLE = {
    'validator': {
        'can_validate': True,
        'want_state_dump': False,
        'want_neard_runner': True,
    },
    'producer': {
        'can_validate': True,
        'want_state_dump': False,
        'want_neard_runner': True,
    },
    'mocknet-archival': {
        'can_validate': False,
        'want_state_dump': False,
        'want_neard_runner': True,
    },
    'state-dumper': {
        'can_validate': False,
        'want_state_dump': True,
        'want_neard_runner': True,
    },
    'traffic': {
        'can_validate': False,
        'want_state_dump': False,
        'want_neard_runner': True,
    },
    'mocknet-tracing-server': {
        'can_validate': False,
        'want_state_dump': False,
        'want_neard_runner': False,
    },
}


# set attributes (e.g. do we want this node to possibly validate?) on this particular node
def configure_nodes(nodes):
    for node in nodes:
        role = node.role()
        if role not in CONFIG_BY_ROLE:
            logging.error(f'unknown role: {role}. Skipping node {node.name()}')
            continue

        node_config = CONFIG_BY_ROLE[role]

        node.can_validate = node_config['can_validate']
        node.want_neard_runner = node_config['want_neard_runner']
        node.want_state_dump = node_config['want_state_dump']

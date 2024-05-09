
# TODO: these should be identified by tags
def _is_tracing_server(node):
    return node.name().endswith('tracing-server')

def _is_dumper(node):
    return node.name().endswith('dumper')

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
def configure_nodes(nodes, node_setup_config):
    for node in nodes:
        node_config = None
        for c in node_setup_config:
            if c['node_matches'](node):
                if node_config is not None:
                    sys.exit(f'multiple node configuration matches for {f.node()}')
                node_config = c
        if node_config is None:
            continue
        node.can_validate = node_config['can_validate']
        node.want_neard_runner = node_config['want_neard_runner']
        node.want_state_dump = node_config['want_state_dump']



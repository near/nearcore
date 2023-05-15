import subprocess
import json


# Get list of all nodes in `project`.
# Select the ones that have `chain_id` in the name.
# Return their name, address (external ip), and labels.
# TODO(posvyatokum) this should be in some lib.
def get_gcloud_nodes(project, chain_id):
    nodes_call = subprocess.run(
        [
            f"gcloud compute instances list --project={project}\
             --format='json(name, labels, networkInterfaces[].accessConfigs[0].natIP)'"
        ],
        stdout=subprocess.PIPE,
        check=True,
        text=True,
        shell=True,
    )
    all_nodes = json.loads(nodes_call.stdout)
    all_nodes = list(filter(lambda x: chain_id in x['name'], all_nodes))
    traffic_nodes = list(filter(lambda x: 'traffic' in x['name'], all_nodes))
    regular_nodes = list(filter(lambda x: 'traffic' not in x['name'],
                                all_nodes))

    def create_node(x):
        return {
            'name': x['name'],
            'address': x['networkInterfaces'][0]['accessConfigs'][0]['natIP'],
            'labels': x['labels']
        }

    return list(map(create_node,
                    regular_nodes)), list(map(create_node, traffic_nodes))


def get_nodes_by_label(nodes, labels):
    result = []
    for node in nodes:
        for label_key, label_value in labels.items():
            if node['labels'].get(label_key) != label_value:
                break
        else:
            result.append(node.copy())
    return result


def get_localnet_nodes(test, num_validators=0, num_rpc=0, default_port=3000):
    if test == 'state_sync':
        regular_nodes = [{
            'name': f's3ss{i}',
            'address': f'0.0.0.0:{default_port + i}',
            'labels': {
                'role': 'validator',
            },
        } for i in range(num_validators)] + [{
            'name': f's3ss{num_validators + i}',
            'address': f'0.0.0.0:{default_port + num_validators + i}',
            'labels': {
                'role': 'rpc',
            },
        } for i in range(num_rpc)]
        regular_nodes[num_validators - 1]['labels']['late'] = 'yes'
        regular_nodes[-1]['labels']['late'] = 'yes'
        regular_nodes[num_validators]['labels']['state-parts-producer'] = 'yes'
        regular_nodes[num_validators +
                      1]['labels']['state-parts-producer'] = 'yes'
        return regular_nodes, []

    return [{
        'name': 'localhost',
        'address': f'0.0.0.0:{default_port}',
        'labels': {},
    }]


def exclude_nodes(all_nodes, nodes_to_exclude):
    node_names_to_exclude = set(map(lambda x: x['name'], nodes_to_exclude))
    return list(
        filter(lambda x: x['name'] not in node_names_to_exclude, all_nodes))


def intersect_nodes(all_nodes, nodes_to_exclude):
    node_names_to_exclude = set(map(lambda x: x['name'], nodes_to_exclude))
    return list(filter(lambda x: x['name'] in node_names_to_exclude, all_nodes))

import argparse
import requests
import sys
from rc import pmap
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

validators_found = {}
visited_nodes = set()
known_nodes = set()
next_to_visit = []


def learn_about_node(node_ip):
    global known_nodes
    global next_to_visit

    if node_ip not in known_nodes:
        known_nodes.add(node_ip)
        next_to_visit.append(node_ip)


def visit_node(node_ip, timeout):
    global known_nodes
    global visited_nodes
    global validators_found

    # logger.info(f'Visiting node {node_ip}')

    visited_nodes.add(node_ip)
    assert node_ip in known_nodes

    try:
        data = requests.get(f'http://{node_ip}:3030/debug/api/status',
                            timeout=timeout).json()
        validators = [x['account_id'] for x in data['validators']]
        if 'validator_account_id' in data:
            account_id = data['validator_account_id']
            if account_id in validators and not node_ip in validators_found:
                validators_found[node_ip] = account_id
                logger.info(f'Scraped validator {account_id}')
        for peer in data['detailed_debug_status']['network_info'][
                'connected_peers']:
            ip = peer['addr'].split(':')[0]
            learn_about_node(ip)

            account_id = peer['account_id']
            if account_id in validators and ip not in validators_found:
                validators_found[ip] = account_id
                logger.info(f'Learned IP of a validator {account_id}')

    except Exception as e:
        logger.warn(f'Failed to connect {node_ip} {e}')


def discover_ips(node_ip, timeout):
    global next_to_visit

    learn_about_node(node_ip)

    while next_to_visit:
        to_visit = next_to_visit[:]
        logger.info(
            f'Will visit {len(to_visit)} nodes. Validators found: {len(validators_found)}. Visited nodes: {len(visited_nodes)}. Known nodes: {len(known_nodes)}.'
        )
        next_to_visit = []
        pmap(lambda ip: visit_node(ip, timeout), to_visit)

    with open('validators.csv', 'w') as f:
        for ip in validators_found:
            f.write('%s,%s\n' % (validators_found[ip], ip))


if __name__ == '__main__':
    logger.info('Starting IP collector')
    parser = argparse.ArgumentParser(description='Run IP collector')
    parser.add_argument('--timeout', type=float, required=True)
    parser.add_argument('--node_ip', required=True)
    args = parser.parse_args()

    timeout = args.timeout
    assert timeout
    node_ip = args.node_ip
    assert node_ip

    discover_ips(node_ip, timeout)

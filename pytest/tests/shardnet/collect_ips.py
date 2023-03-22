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
active_validators = set()
active_validators_height = 0


def learn_about_node(node_ip):
    if node_ip not in known_nodes:
        known_nodes.add(node_ip)
        next_to_visit.append(node_ip)


def visit_node(node_ip, timeout):
    # logger.info(f'Visiting node {node_ip}')

    visited_nodes.add(node_ip)
    assert node_ip in known_nodes
    global active_validators
    global active_validators_height

    account_id = None
    try:
        data = requests.get(f'http://{node_ip}:3030/status',
                            timeout=timeout).json()
        validators = set([x['account_id'] for x in data['validators']])
        height = int(data['sync_info']['latest_block_height'])
        if not active_validators or height > active_validators_height + 1000:
            active_validators = validators
        if 'validator_account_id' in data:
            account_id = data['validator_account_id']
            if account_id in active_validators and not node_ip in validators_found:
                validators_found[node_ip] = account_id
                logger.info(f'Scraped validator {account_id}')

        data = requests.get(f'http://{node_ip}:3030/network_info',
                            timeout=timeout).json()
        peer_id_to_account_id = {}
        for known_producer in data['known_producers']:
            peer_id_to_account_id[
                known_producer['peer_id']] = known_producer['account_id']
        for peer in data['active_peers']:
            ip = peer['addr'].split(':')[0]
            learn_about_node(ip)
            peer_id = peer['id']
            if peer_id in peer_id_to_account_id:
                account_id = peer_id_to_account_id[peer_id]
                if account_id in validators and not ip in validators_found:
                    validators_found[ip] = account_id
                    logger.info(f'Found a validator {account_id} in peers')
            # Note that peer['account_id'] is always 'null'

    except Exception as e:
        logger.exception(f'Error scraping {node_ip}')


def discover_ips(node_ip, timeout):
    learn_about_node(node_ip)

    while next_to_visit:
        to_visit = next_to_visit[:]
        logger.info(f'Will visit {len(to_visit)} nodes. '
                    f'Validators found: {len(validators_found)}. '
                    f'Visited nodes: {len(visited_nodes)}. '
                    f'Known nodes: {len(known_nodes)}.')
        next_to_visit.clear()
        pmap(lambda ip: visit_node(ip, timeout), to_visit)

    logger.info(f'Validators found: {len(validators_found)}.')
    logger.info(f'Visited nodes: {len(visited_nodes)}.')
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

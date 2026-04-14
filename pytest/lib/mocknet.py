import json
import os
import os.path
import shlex
import tempfile
import time

import requests
from rc import run, pmap

import data
from cluster import GCloudNode
from configured_logger import logger
from key import Key
from metrics import Metrics

# cspell:ignore loadtester
NODE_SSH_KEY_PATH = None
NODE_USERNAME = 'ubuntu'
PROJECT = os.getenv('MOCKNET_PROJECT', 'nearone-mocknet')
PUBLIC_KEY = 'ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN'

TREASURY_ACCOUNT = 'test.near'
MASTER_ACCOUNT = 'near'
SKYWARD_ACCOUNT = 'skyward.near'
TOKEN1_ACCOUNT = 'token1.near'
TOKEN2_ACCOUNT = 'token2.near'
TOKEN2_OWNER_ACCOUNT = 'account.token2.near'
ACCOUNT1_ACCOUNT = 'account1.near'

TMUX_STOP_SCRIPT = '''
while tmux has-session -t near; do
tmux kill-session -t near || true
done
'''

ONE_NEAR = 10**24
MIN_STAKE = 64 * (10**3)


def get_node(hostname, project=PROJECT):
    instance_name = hostname
    n = GCloudNode(
        instance_name,
        username=NODE_USERNAME,
        project=project,
        ssh_key_path=NODE_SSH_KEY_PATH,
    )
    return n


def get_nodes(pattern=None, project=PROJECT):
    return GCloudNode.get_nodes_by_mocknet_id(
        mocknet_id=pattern,
        project=project,
        username=NODE_USERNAME,
        ssh_key_path=NODE_SSH_KEY_PATH,
    )


def get_validator_account(node):
    return Key.from_json(
        download_and_read_json(node, '/home/ubuntu/.near/validator_key.json'))


def get_metrics(node):
    (addr, port) = node.rpc_addr()
    metrics_url = f'http://{addr}:{port}/metrics'
    return Metrics.from_url(metrics_url)


def get_timestamp(block):
    return block['header']['timestamp'] / 1e9


def get_chunk_txn(index, chunks, archival_node, result):
    chunk = chunks[index]
    chunk_hash = chunk['chunk_hash']
    result[index] = len(
        archival_node.get_chunk(chunk_hash)['result']['transactions'])


# Measure bps and tps by directly checking block timestamps and number of transactions
# in each block.
def chain_measure_bps_and_tps(
    archival_node,
    start_time,
    end_time,
    duration=None,
):
    latest_block_hash = archival_node.get_latest_block().hash
    curr_block = archival_node.get_block(latest_block_hash)['result']
    curr_time = get_timestamp(curr_block)

    if end_time is None:
        end_time = curr_time
    if start_time is None:
        start_time = end_time - duration
    logger.info(
        f'Measuring BPS and TPS in the time range {start_time} to {end_time}')

    # One entry per block, equal to the timestamp of that block.
    block_times = []
    # One entry per block, containing the count of transactions in all chunks of the block.
    tx_count = []
    while curr_time > start_time:
        if curr_time < end_time:
            block_times.append(curr_time)
            gas_per_chunk = []
            for chunk in curr_block['chunks']:
                gas_per_chunk.append(chunk['gas_used'] * 1e-12)
            gas_block = sum(gas_per_chunk)
            tx_per_chunk = [None] * len(curr_block['chunks'])
            pmap(
                lambda i: get_chunk_txn(i, curr_block['chunks'], archival_node,
                                        tx_per_chunk),
                range(len(curr_block['chunks'])))
            txs = sum(tx_per_chunk)
            tx_count.append(txs)
            logger.info(
                f'Processed block at time {curr_time} height #{curr_block["header"]["height"]}, # txs in a block: {txs}, per chunk: {tx_per_chunk}, gas in block: {gas_block}, gas per chunk: {gas_per_chunk}'
            )
        prev_hash = curr_block['header']['prev_hash']
        curr_block = archival_node.get_block(prev_hash)['result']
        curr_time = get_timestamp(curr_block)
    block_times.reverse()
    tx_count.reverse()
    assert block_times
    tx_cumulative = data.compute_cumulative(tx_count)
    bps = data.compute_rate(block_times)
    tps_fit = data.linear_regression(block_times, tx_cumulative)
    logger.info(
        f'Num blocks: {len(block_times)}, num transactions: {len(tx_count)}, bps: {bps}, tps_fit: {tps_fit}'
    )
    return {'bps': bps, 'tps': tps_fit['slope']}


def get_tx_events_single_node(node, tx_filename):
    try:
        target_file = f'./logs/{node.instance_name}_txs'
        node.machine.download(tx_filename, target_file)
        with open(target_file, 'r') as f:
            return [float(line.strip()) for line in f.readlines()]
    except Exception as e:
        logger.exception(f'Getting tx_events from {node.instance_name} failed')
        return []


def get_tx_events(nodes, tx_filename):
    run('mkdir ./logs/')
    run('rm -rf ./logs/*_txs')
    all_events = pmap(
        lambda node: get_tx_events_single_node(node, tx_filename),
        nodes,
    )
    return sorted(data.flatten(all_events))


def kill_process_script(pid):
    return f'''
        sudo kill {pid}
        while kill -0 {pid}; do
            sleep 1
        done
    '''


def get_near_pid(machine):
    p = machine.run(
        "ps aux | grep 'near.* run' | grep -v grep | awk '{print $2}'")
    return p.stdout.strip()


def is_binary_running(binary_name: str, node) -> bool:
    result = node.machine.run(f'ps -A -o comm= | grep {binary_name}')
    return result.returncode == 0


def stop_node(node):
    m = node.machine
    logger.info(f'Stopping node {m.name}')
    pids = get_near_pid(m).split()

    for pid in pids:
        m.run('bash', input=kill_process_script(pid))
        m.run('sudo -u ubuntu -i', input=TMUX_STOP_SCRIPT)


# cspell:ignore redownload
def redownload_neard(nodes, binary_url):
    pmap(
        lambda node: node.machine.
        run('sudo -u ubuntu -i',
            input='wget -O /home/ubuntu/neard "{}"; chmod +x /home/ubuntu/neard'
            .format(binary_url)), nodes)


def create_and_upload_genesis_file_from_empty_genesis(
    validator_node_and_stakes,
    rpc_nodes,
    chain_id=None,
    epoch_length=None,
    num_seats=None,
):
    node0 = validator_node_and_stakes[0][0]
    node0.machine.run(
        'rm -rf /home/ubuntu/.near-tmp && mkdir /home/ubuntu/.near-tmp && /home/ubuntu/neard --home /home/ubuntu/.near-tmp init --chain-id {}'
        .format(chain_id))
    genesis_config = download_and_read_json(
        node0, "/home/ubuntu/.near-tmp/genesis.json")
    records = []

    VALIDATOR_BALANCE = (10**2) * ONE_NEAR
    RPC_BALANCE = (10**1) * ONE_NEAR
    TREASURY_ACCOUNT = 'test.near'
    TREASURY_BALANCE = (10**7) * ONE_NEAR
    LOAD_TESTER_BALANCE = (10**8) * ONE_NEAR

    SKYWARD_CONTRACT_BALANCE = (10**6) * ONE_NEAR
    TOKEN1_BALANCE = (10**6) * ONE_NEAR
    TOKEN2_BALANCE = (10**6) * ONE_NEAR
    TOKEN2_OWNER_BALANCE = (10**6) * ONE_NEAR
    ACCOUNT1_BALANCE = (10**6) * ONE_NEAR

    genesis_config['chain_id'] = chain_id

    master_balance = 10**7
    assert master_balance > 0
    accounts = {
        TREASURY_ACCOUNT: TREASURY_BALANCE,
        MASTER_ACCOUNT: master_balance,
        SKYWARD_ACCOUNT: SKYWARD_CONTRACT_BALANCE,
        TOKEN1_ACCOUNT: TOKEN1_BALANCE,
        TOKEN2_ACCOUNT: TOKEN2_BALANCE,
        TOKEN2_OWNER_ACCOUNT: TOKEN2_OWNER_BALANCE,
        ACCOUNT1_ACCOUNT: ACCOUNT1_BALANCE
    }

    for account_id, balance in accounts.items():
        records.append({
            'Account': {
                'account_id': account_id,
                'account': {
                    'amount': str(balance),
                    'locked': '0',
                    'code_hash': '11111111111111111111111111111111',
                    'storage_usage': 0,
                    'version': 'V1'
                }
            }
        })
        records.append({
            'AccessKey': {
                'account_id': account_id,
                'public_key': PUBLIC_KEY,
                'access_key': {
                    'nonce': 0,
                    'permission': 'FullAccess'
                }
            }
        })

    stakes = []
    account_id_to_validator_pk = {}
    for i, (node, stake_multiplier) in enumerate(validator_node_and_stakes):
        validator = get_validator_account(node)
        logger.info(f'Adding account {validator.account_id}')
        account_id_to_validator_pk[validator.account_id] = validator.pk
        staked = MIN_STAKE * stake_multiplier
        stakes.append((staked, validator.account_id))
        records.append({
            'Account': {
                'account_id': validator.account_id,
                'account': {
                    'amount': str(VALIDATOR_BALANCE),
                    'locked': str(staked),
                    'code_hash': '11111111111111111111111111111111',
                    'storage_usage': 0,
                    'version': 'V1'
                }
            }
        })
        records.append({
            'AccessKey': {
                'account_id': validator.account_id,
                'public_key': PUBLIC_KEY,
                'access_key': {
                    'nonce': 0,
                    'permission': 'FullAccess'
                }
            }
        })

    load_testing_account = 'loadtester'
    logger.info(f'Adding load testing account {load_testing_account}')
    records.append({
        'Account': {
            'account_id': load_testing_account,
            'account': {
                'amount': str(LOAD_TESTER_BALANCE),
                'locked': str(0),
                'code_hash': '11111111111111111111111111111111',
                'storage_usage': 0,
                'version': 'V1'
            }
        }
    })
    records.append({
        'AccessKey': {
            'account_id': load_testing_account,
            'public_key': PUBLIC_KEY,
            'access_key': {
                'nonce': 0,
                'permission': 'FullAccess'
            }
        }
    })
    genesis_config['validators'] = []
    seats = compute_seats(stakes, num_seats)
    seats_taken = 0
    for seats, staked, account_id in seats:
        if seats + seats_taken > num_seats:
            break
        genesis_config['validators'].append({
            'account_id': account_id,
            'public_key': account_id_to_validator_pk[account_id],
            'amount': str(staked),
        })
        seats_taken += seats

    total_supply = 0
    for record in records:
        account = record.get('Account', {}).get('account', {})
        total_supply += int(account.get('locked', 0))
        total_supply += int(account.get('amount', 0))
    genesis_config['total_supply'] = str(total_supply)
    genesis_config['protocol_version'] = 57
    genesis_config['epoch_length'] = int(epoch_length)
    genesis_config['num_block_producer_seats'] = int(num_seats)
    genesis_config['protocol_reward_rate'] = [1, 10]
    # Loadtest helper signs all transactions using the same block.
    # Extend validity period to allow the same hash to be used for the whole duration of the test.
    genesis_config['transaction_validity_period'] = 10**9
    # Protocol upgrades require downtime, therefore make it harder to kickout validators.
    # The default value of this parameter is 90.
    genesis_config['block_producer_kickout_threshold'] = 10

    genesis_config['shard_layout'] = {'V0': {'num_shards': 4, 'version': 0}}
    genesis_config['simple_nightshade_shard_layout'] = {}

    genesis_config['records'] = records
    pmap(
        lambda node: upload_json(node, '/home/ubuntu/.near/genesis.json',
                                 genesis_config),
        [node for (node, _) in validator_node_and_stakes] + rpc_nodes)


def download_and_read_json(node, filename):
    try:
        tmp_file = tempfile.NamedTemporaryFile(mode='r+', delete=False)
        node.machine.download(filename, tmp_file.name)
        tmp_file.close()
        with open(tmp_file.name, 'r') as f:
            return json.load(f)
    except:
        logger.error(
            f'Failed to download json file. Node: {node.instance_name}. Filename: {filename}'
        )
        raise


def upload_json(node, filename, data):
    try:
        logger.info(f'Upload file {filename} to {node.instance_name}')
        tmp_file = tempfile.NamedTemporaryFile(mode='r+', delete=False)
        with open(tmp_file.name, 'w') as f:
            json.dump(data, f, indent=2)
        node.machine.upload(tmp_file.name, filename)
        tmp_file.close()
    except:
        logger.error(
            f'Failed to upload json file. Node: {node.instance_name}. Filename: {filename}'
        )
        raise


def get_node_addr(node, port):
    node_key_json = download_and_read_json(node,
                                           '/home/ubuntu/.near/node_key.json')
    return f'{node_key_json["public_key"]}@{node.ip}:{port}'


def upload_config(node, config_json, override_fn):
    copied_config = json.loads(json.dumps(config_json))
    if override_fn:
        override_fn(node, copied_config)
    upload_json(node, '/home/ubuntu/.near/config.json', copied_config)


def create_and_upload_config_file_from_default(nodes,
                                               chain_id,
                                               override_fn=None):
    nodes[0].machine.run(
        'rm -rf /home/ubuntu/.near-tmp && mkdir /home/ubuntu/.near-tmp && /home/ubuntu/neard --home /home/ubuntu/.near-tmp init --chain-id {}'
        .format(chain_id))
    config_json = download_and_read_json(
        nodes[0],
        '/home/ubuntu/.near-tmp/config.json',
    )
    config_json['tracked_shards_config'] = 'AllShards'
    config_json['archive'] = True
    config_json['archival_peer_connections_lower_bound'] = 1
    node_addresses = [get_node_addr(node, 24567) for node in nodes]
    config_json['network']['boot_nodes'] = ','.join(node_addresses)
    config_json['network']['skip_sync_wait'] = False
    config_json['rpc']['addr'] = '0.0.0.0:3030'
    config_json['rpc']['enable_debug_rpc'] = True
    if 'telemetry' in config_json:
        config_json['telemetry']['endpoints'] = []

    pmap(lambda node: upload_config(node, config_json, override_fn), nodes)


def update_existing_config_file(node, override_fn=None):
    config_json = download_and_read_json(
        node,
        '/home/ubuntu/.near/config.json',
    )
    override_fn(node, config_json)
    upload_json(node, '/home/ubuntu/.near/config.json', config_json)


def update_existing_config_files(nodes, override_fn=None):
    pmap(
        lambda node: update_existing_config_file(node, override_fn=override_fn),
        nodes,
    )


def start_nodes(nodes, upgrade_schedule=None):
    pmap(
        lambda node: start_node(node, upgrade_schedule=upgrade_schedule),
        nodes,
    )


def stop_nodes(nodes):
    pmap(stop_node, nodes)


def clear_data(nodes):
    pmap(lambda node: node.machine.run('rm -rf /home/ubuntu/.near/data'), nodes)


def neard_start_script(node, upgrade_schedule=None, epoch_height=None):
    if upgrade_schedule and upgrade_schedule.get(node.instance_name,
                                                 0) <= epoch_height:
        neard_binary = '/home/ubuntu/neard.upgrade'
    else:
        neard_binary = '/home/ubuntu/neard'
    return '''
        sudo mv /home/ubuntu/near.log /home/ubuntu/near.log.1 2>/dev/null
        sudo mv /home/ubuntu/near.upgrade.log /home/ubuntu/near.upgrade.log.1 2>/dev/null
        tmux new -s near -d bash
        sudo rm -rf /home/ubuntu/neard.log
        tmux send-keys -t near 'RUST_BACKTRACE=full RUST_LOG=debug {neard_binary} run 2>&1 | tee -a {neard_binary}.log' C-m
    '''.format(neard_binary=shlex.quote(neard_binary))


def start_node(node, upgrade_schedule=None):
    m = node.machine
    logger.info(f'Starting node {m.name}')
    attempt = 0
    success = False
    while attempt < 3:
        pid = get_near_pid(m)
        if pid != '':
            success = True
            break
        start_process = m.run(
            'sudo -u ubuntu -i',
            input=neard_start_script(
                node,
                upgrade_schedule=upgrade_schedule,
                epoch_height=0,
            ),
        )
        if start_process.returncode == 0:
            success = True
            break
        logger.warn(
            f'Failed to start process, returncode: {start_process.returncode}\n{node.instance_name}\n{start_process.stderr}'
        )
        attempt += 1
        time.sleep(1)
    if not success:
        raise Exception(f'Could not start node {node.instance_name}')


def reset_data(node, retries=0):
    try:
        m = node.machine
        stop_node(node)
        logger.info(f'Clearing data directory of node {m.name}')
        start_process = m.run('bash', input='rm -r /home/ubuntu/.near')
        assert start_process.returncode == 0, m.name + '\n' + start_process.stderr
    except:
        if retries < 3:
            logger.warning(
                'And error occurred while clearing data directory, retrying')
            reset_data(node, retries=retries + 1)
        else:
            raise Exception(
                f'ERROR: Could not clear data directory for {node.machine.name}'
            )


# Waits until the node becomes responsive to RPC requests. Assumes the node can
# still be starting and expects connection errors and the data not being available.
def wait_node_up(node):
    msg = f'Waiting for node {node.instance_name} to start'
    logger.info(msg)
    attempt = 0
    while True:
        try:
            if not is_binary_running('neard', node):
                raise Exception(f'{msg} - failed. The neard process crashed.')

            response = node.get_validators()

            if 'error' in response:
                attempt += 1
                logger.info(f'{msg}, attempt {attempt} error response.')
                continue
            if 'result' not in response:
                attempt += 1
                logger.info(f'{msg}, attempt {attempt} result missing.')
                continue

            logger.info(f'{msg} - done.')
            return True
        except (ConnectionRefusedError, requests.exceptions.ConnectionError):
            attempt += 1
            logger.info(f'{msg}, attempt {attempt} connection refused.')
        time.sleep(30)


def wait_all_nodes_up(all_nodes):
    pmap(lambda node: wait_node_up(node), all_nodes)


def compute_seats(stakes, num_block_producer_seats):
    max_stake = 0
    for i in stakes:
        max_stake = max(max_stake, i[0])

    # Compute seats assignment.
    l = 0
    r = max_stake + 1
    seat_price = -1
    while r - l > 1:
        tmp_seat_price = (l + r) // 2
        num_seats = 0
        for i in range(len(stakes)):
            num_seats += stakes[i][0] // tmp_seat_price
        if num_seats <= num_block_producer_seats:
            r = tmp_seat_price
        else:
            l = tmp_seat_price
    seat_price = r
    logger.info(f'compute_seats seat_price: {seat_price}')

    seats = []
    for stake, item in stakes:
        seats.append((stake // seat_price, stake, item))
    seats.sort(reverse=True)
    return seats

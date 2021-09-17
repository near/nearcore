import base58
import json
import os
import shlex
import os.path
import tempfile
import time
from rc import run, pmap, gcloud

import data
from cluster import GCloudNode
from configured_logger import logger
from key import Key
from metrics import Metrics
from transaction import sign_payment_tx_and_get_hash, sign_staking_tx_and_get_hash

DEFAULT_KEY_TARGET = '/tmp/mocknet'
KEY_TARGET_ENV_VAR = 'NEAR_PYTEST_KEY_TARGET'
NODE_BASE_NAME = 'mocknet'
# NODE_SSH_KEY_PATH = '~/.ssh/near_ops'
NODE_SSH_KEY_PATH = None
NODE_USERNAME = 'ubuntu'
NUM_SHARDS = 8
NUM_ACCOUNTS = 100
PROJECT = 'near-mocknet'
PUBLIC_KEY = "ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN"
TX_OUT_FILE = '/home/ubuntu/tx_events'
WASM_FILENAME = 'simple_contract.wasm'

TMUX_STOP_SCRIPT = '''
while tmux has-session -t near; do
tmux kill-session -t near || true
done
'''

TMUX_START_SCRIPT_WITH_DEBUG_OUTPUT = '''
sudo mv /home/ubuntu/near.log /home/ubuntu/near.log.1 2>/dev/null
sudo rm -rf /home/ubuntu/.near/data /home/ubuntu/.near/pytest
tmux new -s near -d bash
tmux send-keys -t near 'RUST_BACKTRACE=full RUST_LOG=debug,actix_web=info /home/ubuntu/neard run 2>&1 | tee /home/ubuntu/near.log' C-m
'''

TMUX_START_SCRIPT_NO_DEBUG_OUTPUT = '''
sudo mv /home/ubuntu/near.log /home/ubuntu/near.log.1 2>/dev/null
sudo rm -rf /home/ubuntu/.near/data /home/ubuntu/.near/pytest
tmux new -s near -d bash
tmux send-keys -t near 'RUST_BACKTRACE=full /home/ubuntu/neard run 2>&1 | tee /home/ubuntu/near.log' C-m
'''

TMUX_START_SCRIPT = TMUX_START_SCRIPT_WITH_DEBUG_OUTPUT

PYTHON_DIR = '/home/ubuntu/.near/pytest/'

PYTHON_SETUP_SCRIPT = f'''
rm -rf {PYTHON_DIR}
mkdir -p {PYTHON_DIR}
python3 -m pip install pip --upgrade
python3 -m pip install virtualenv --upgrade
cd {PYTHON_DIR}
python3 -m virtualenv venv -p $(which python3)
'''

INSTALL_PYTHON_REQUIREMENTS = f'''
cd {PYTHON_DIR}
./venv/bin/pip install -r requirements.txt
'''


def get_node(hostname):
    instance_name = hostname
    n = GCloudNode(instance_name,
                   username=NODE_USERNAME,
                   project=PROJECT,
                   ssh_key_path=NODE_SSH_KEY_PATH)
    return n


def get_nodes():
    machines = gcloud.list(project=PROJECT,
                           username=NODE_USERNAME,
                           ssh_key_path=NODE_SSH_KEY_PATH)
    nodes = pmap(
        lambda machine: GCloudNode(machine.name,
                                   username=NODE_USERNAME,
                                   project=PROJECT,
                                   ssh_key_path=NODE_SSH_KEY_PATH), machines)
    return nodes


def node_account_name(node):
    return f'{node.instance_name}.near'


def load_testing_account_id(node_account_id, i):
    return f'load_testing_{i}_{node_account_id}'


def create_target_dir(node):
    base_target_dir = os.environ.get(KEY_TARGET_ENV_VAR, DEFAULT_KEY_TARGET)
    target_dir = f'{base_target_dir}/{node.instance_name}'
    run(f'mkdir -p {target_dir}')
    return target_dir


def get_validator_account(node):
    validator_key_file = tempfile.NamedTemporaryFile(
        mode='r+', delete=False, suffix='.mocknet.loadtest.validator_key')
    node.machine.download('/home/ubuntu/.near/validator_key.json',
                          validator_key_file.name)
    return Key.from_json_file(validator_key_file.name)


def list_validators(node):
    validators = node.get_validators()['result']
    validator_accounts = set(
        map(lambda v: v['account_id'], validators['current_validators']))
    return validator_accounts


def setup_python_environment(node, wasm_contract):
    m = node.machine
    logger.info(f'Setting up python environment on {m.name}')
    m.run('bash', input=PYTHON_SETUP_SCRIPT)
    m.upload('lib', PYTHON_DIR, switch_user='ubuntu')
    m.upload('requirements.txt', PYTHON_DIR, switch_user='ubuntu')
    m.upload(wasm_contract,
             os.path.join(PYTHON_DIR, WASM_FILENAME),
             switch_user='ubuntu')
    m.upload('tests/mocknet/load_testing_helper.py',
             PYTHON_DIR,
             switch_user='ubuntu')
    m.run('bash', input=INSTALL_PYTHON_REQUIREMENTS)
    logger.info(f'{m.name} python setup complete')


def setup_python_environments(nodes, wasm_contract):
    pmap(lambda n: setup_python_environment(n, wasm_contract), nodes)


def start_load_test_helper_script(script, node_account_id, pk, sk):
    return '''
        cd {dir}
        nohup ./venv/bin/python {script} {node_account_id} {pk} {sk} > load_test.out 2> load_test.err < /dev/null &
    '''.format(dir=shlex.quote(PYTHON_DIR),
               script=shlex.quote(script),
               node_account_id=shlex.quote(node_account_id),
               pk=shlex.quote(pk),
               sk=shlex.quote(sk))


def start_load_test_helper(node, script, pk, sk):
    logger.info(f'Starting load_test_helper on {node.instance_name}')
    node.machine.run('bash',
                     input=start_load_test_helper_script(
                         script, node_account_name(node), pk, sk))


def start_load_test_helpers(nodes, script):
    account = get_validator_account(nodes[0])
    pmap(
        lambda node: start_load_test_helper(node, script, account.pk, account.sk
                                           ), nodes)


def get_log(node):
    target_file = f'./logs/{node.instance_name}.log'
    node.machine.download('/home/ubuntu/near.log', target_file)


def get_logs(nodes):
    pmap(get_log, nodes)


def get_epoch_length_in_blocks(node):
    target_dir = create_target_dir(node)
    node.machine.download('/home/ubuntu/.near/genesis.json', target_dir)
    with open(f'{target_dir}/genesis.json') as f:
        config = json.load(f)
    epoch_length_in_blocks = config['epoch_length']
    return epoch_length_in_blocks


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
def chain_measure_bps_and_tps(archival_node,
                              start_time,
                              end_time,
                              duration=None):
    latest_block_hash = archival_node.get_status(
    )['sync_info']['latest_block_hash']
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
    block_counter = 0
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
    all_events = pmap(lambda node: get_tx_events_single_node(node, tx_filename),
                      nodes)
    return sorted(data.flatten(all_events))


# Sends the transaction to the network via `node` and checks for success.
# Some retrying is done when the node returns a Timeout error.
def send_transaction(node, tx, tx_hash, account_id, timeout=120):
    response = node.send_tx_and_wait(tx, timeout)
    loop_start = time.time()
    missing_count = 0
    while 'error' in response.keys():
        error_data = response['error']['data']
        if 'timeout' in error_data.lower():
            logger.warning(
                f'transaction {tx_hash} returned Timout, checking status again.'
            )
            time.sleep(5)
            response = node.get_tx(tx_hash, account_id)
        elif "doesn't exist" in error_data:
            missing_count += 1
            logger.warning(
                f'transaction {tx_hash} falied to be recieved by the node, checking again.'
            )
            if missing_count < 20:
                time.sleep(5)
                response = node.get_tx(tx_hash, account_id)
            else:
                logger.warning(f're-sending transaction {tx_hash}.')
                response = node.send_tx_and_wait(tx, timeout)
                missing_count = 0
        else:
            raise RuntimeError(
                f'Error in processing transaction {tx_hash}: {response}')
        if time.time() - loop_start > timeout:
            raise TimeoutError(
                f'Transaction {tx_hash} did not complete successfully within the timeout'
            )

    if 'SuccessValue' not in response['result']['status']:
        raise RuntimeError(
            f'ERROR: Failed transaction {tx_hash}. Response: {response}')


def transfer_between_nodes(nodes):
    logger.info('Testing transfer between mocknet validators')
    node = nodes[0]
    alice = get_validator_account(nodes[1])
    bob = get_validator_account(nodes[0])
    transfer_amount = 100
    get_balance = lambda account: int(
        node.get_account(account.account_id)['result']['amount'])

    alice_initial_balance = get_balance(alice)
    alice_nonce = node.get_nonce_for_pk(alice.account_id, alice.pk)
    bob_initial_balance = get_balance(bob)
    logger.info(f'Alice initial balance: {alice_initial_balance}')
    logger.info(f'Bob initial balance: {bob_initial_balance}')

    last_block_hash = node.get_status()['sync_info']['latest_block_hash']
    last_block_hash_decoded = base58.b58decode(last_block_hash.encode('utf8'))

    tx, tx_hash = sign_payment_tx_and_get_hash(alice, bob.account_id,
                                               transfer_amount, alice_nonce + 1,
                                               last_block_hash_decoded)
    send_transaction(node, tx, tx_hash, alice.account_id)

    alice_final_balance = get_balance(alice)
    bob_final_balance = get_balance(bob)
    logger.info(f'Alice final balance: {alice_final_balance}')
    logger.info(f'Bob final balance: {bob_final_balance}')

    # Check mod 1000 to ignore the cost of the transaction itself
    assert (alice_initial_balance -
            alice_final_balance) % 1000 == transfer_amount
    assert bob_final_balance - bob_initial_balance == transfer_amount


def stake_node(node):
    account = get_validator_account(node)
    logger.info(f'Staking {account.account_id}.')
    nonce = node.get_nonce_for_pk(account.account_id, account.pk)

    validators = node.get_validators()['result']
    if account.account_id in validators['current_validators']:
        return
    stake_amount = max(
        map(lambda v: int(v['stake']), validators['current_validators']))

    latest_block_hash = node.get_status()['sync_info']['latest_block_hash']
    last_block_hash_decoded = base58.b58decode(latest_block_hash.encode('utf8'))

    staking_tx, staking_tx_hash = sign_staking_tx_and_get_hash(
        account, account, stake_amount, nonce + 1, last_block_hash_decoded)
    send_transaction(node, staking_tx, staking_tx_hash, account.account_id)


def accounts_from_nodes(nodes):
    return pmap(get_validator_account, nodes)


def kill_proccess_script(pid):
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


def stop_node(node):
    m = node.machine
    logger.info(f'Stopping node {m.name}')
    pid = get_near_pid(m)
    if pid != '':
        m.run('bash', input=kill_proccess_script(pid))
        m.run('sudo -u ubuntu -i', input=TMUX_STOP_SCRIPT)


def upload_and_extract(node, src_filename, dst_filename):
    node.machine.upload(f'{src_filename}.gz',
                        f'{dst_filename}.gz',
                        switch_user='ubuntu')
    node.machine.run('gunzip -f {dst_filename}.gz'.format(
        dst_filename=shlex.quote(dst_filename)))


def compress_and_upload(nodes, src_filename, dst_filename):
    res = run(f'gzip {src_filename}')
    assert res.returncode == 0
    pmap(lambda node: upload_and_extract(node, src_filename, dst_filename),
         nodes)


# We assume that the nodes already have the .near directory with the files
# node_key.json, validator_key.json and config.json.
def create_and_upload_genesis(nodes, genesis_template_filename):
    logger.info('Uploading genesis and config files')
    with tempfile.TemporaryDirectory() as tmp_dir:
        mocknet_genesis_filename = os.path.join(tmp_dir, "genesis.json")
        create_genesis_file(nodes, genesis_template_filename,
                            mocknet_genesis_filename)
        # Save time and bandwidth by uploading a compressed file, which is 2% the size of the genesis file.
        compress_and_upload(nodes, mocknet_genesis_filename,
                            '/home/ubuntu/.near/genesis.json')
        update_config_file(nodes, tmp_dir)


def create_genesis_file(nodes, genesis_template_filename,
                        mocknet_genesis_filename):
    with open(genesis_template_filename) as f:
        genesis_config = json.load(f)

    ONE_NEAR = 10**24
    TOTAL_SUPPLY = (10**9) * ONE_NEAR
    TREASURY_BALANCE = (10**7) * ONE_NEAR
    VALIDATOR_BALANCE = 5 * (10**5) * ONE_NEAR
    STAKED_BALANCE = 15 * (10**5) * ONE_NEAR
    MASTER_ACCOUNT = "near"
    TREASURY_ACCOUNT = "test.near"
    LOAD_TESTER_BALANCE = (10**4) * ONE_NEAR

    genesis_config['chain_id'] = "mocknet"
    genesis_config['total_supply'] = str(TOTAL_SUPPLY)
    master_balance = TOTAL_SUPPLY - (TREASURY_BALANCE + len(nodes) *
                                     (VALIDATOR_BALANCE + STAKED_BALANCE +
                                      NUM_ACCOUNTS * LOAD_TESTER_BALANCE))
    assert master_balance > 0
    genesis_config['records'] = []
    genesis_config['validators'] = []
    genesis_config['records'].append({
        "Account": {
            "account_id": TREASURY_ACCOUNT,
            "account": {
                "amount": str(TREASURY_BALANCE),
                "locked": "0",
                "code_hash": "11111111111111111111111111111111",
                "storage_usage": 0,
                "version": "V1"
            }
        }
    })
    genesis_config['records'].append({
        "AccessKey": {
            "account_id": TREASURY_ACCOUNT,
            "public_key": PUBLIC_KEY,
            "access_key": {
                "nonce": 0,
                "permission": "FullAccess"
            }
        }
    })
    genesis_config['records'].append({
        "Account": {
            "account_id": MASTER_ACCOUNT,
            "account": {
                "amount": str(master_balance),
                "locked": "0",
                "code_hash": "11111111111111111111111111111111",
                "storage_usage": 0,
                "version": "V1"
            }
        }
    })
    genesis_config['records'].append({
        "AccessKey": {
            "account_id": MASTER_ACCOUNT,
            "public_key": PUBLIC_KEY,
            "access_key": {
                "nonce": 0,
                "permission": "FullAccess"
            }
        }
    })
    for node in nodes:
        account_id = node_account_name(node)
        genesis_config['records'].append({
            "Account": {
                "account_id": account_id,
                "account": {
                    "amount": str(VALIDATOR_BALANCE),
                    "locked": str(STAKED_BALANCE),
                    "code_hash": "11111111111111111111111111111111",
                    "storage_usage": 0,
                    "version": "V1"
                }
            }
        })
        genesis_config['records'].append({
            "AccessKey": {
                "account_id": account_id,
                "public_key": PUBLIC_KEY,
                "access_key": {
                    "nonce": 0,
                    "permission": "FullAccess"
                }
            }
        })
        genesis_config['validators'].append({
            "account_id": account_id,
            "public_key": PUBLIC_KEY,
            "amount": str(STAKED_BALANCE)
        })
        for i in range(NUM_ACCOUNTS):
            load_testing_account = load_testing_account_id(account_id, i)
            genesis_config['records'].append({
                "Account": {
                    "account_id": load_testing_account,
                    "account": {
                        "amount": str(LOAD_TESTER_BALANCE),
                        "locked": str(0),
                        "code_hash": "11111111111111111111111111111111",
                        "storage_usage": 0,
                        "version": "V1"
                    }
                }
            })
            genesis_config['records'].append({
                "AccessKey": {
                    "account_id": load_testing_account,
                    "public_key": PUBLIC_KEY,
                    "access_key": {
                        "nonce": 0,
                        "permission": "FullAccess"
                    }
                }
            })
    genesis_config["epoch_length"] = 2000
    genesis_config["num_block_producer_seats"] = len(nodes)
    genesis_config["num_block_producer_seats_per_shard"] = [len(nodes)
                                                           ] * NUM_SHARDS
    genesis_config["avg_hidden_validator_seats_per_shard"] = [0] * NUM_SHARDS
    # Loadtest helper signs all transactions using the same block.
    # Extend validity period to allow the same hash to be used for the whole duration of the test.
    genesis_config["transaction_validity_period"] = 10**9
    genesis_config["shard_layout"]["V0"]["num_shards"] = NUM_SHARDS
    # The json object gets truncated if I don't close and reopen the file.
    with open(mocknet_genesis_filename, "w") as f:
        json.dump(genesis_config, f, indent=2)


def get_node_addr(node, port, tmp_dir):
    node_key_filename = os.path.join(tmp_dir,
                                     f'node_key.{node.instance_name}.json')
    node.machine.download('/home/ubuntu/.near/node_key.json', node_key_filename)
    with open(node_key_filename, 'r') as f:
        node_key_json = json.load(f)
    return f'{node_key_json["public_key"]}@{node.ip}:{port}'


def update_config_file(nodes, tmp_dir):
    first_node = nodes[0]

    # Download and read.
    mocknet_config_filename = os.path.join(tmp_dir, "config.json")
    first_node.machine.download('/home/ubuntu/.near/config.json',
                                mocknet_config_filename)
    with open(mocknet_config_filename, 'r') as f:
        config_json = json.load(f)
    port = config_json["network"]["addr"].split(':')[
        1]  # Usually the port is 24567
    node_addresses = []
    pmap(lambda node: node_addresses.append(get_node_addr(node, port, tmp_dir)),
         nodes)

    config_json["tracked_shards"] = list(range(0, NUM_SHARDS))

    # Update the config and save it to the file.
    config_json['network']['boot_nodes'] = ','.join(node_addresses)
    with open(mocknet_config_filename, 'w') as f:
        json.dump(config_json, f, indent=2)

    pmap(
        lambda node: node.machine.upload(mocknet_config_filename,
                                         '/home/ubuntu/.near/config.json',
                                         switch_user='ubuntu'), nodes)


def start_nodes(nodes):
    pmap(start_node, nodes)


def stop_nodes(nodes):
    pmap(stop_node, nodes)


def start_node(node):
    m = node.machine
    logger.info(f'Starting node {m.name}')
    attempt = 0
    success = False
    while attempt < 3:
        pid = get_near_pid(m)
        if pid != '':
            success = True
            break
        start_process = m.run('sudo -u ubuntu -i', input=TMUX_START_SCRIPT)
        if start_process.returncode == 0:
            success = True
            break
        logger.warn(
            f"Failed to start process, returncode: {start_process.returncode}\n{node.instance_name}\n{start_process.stderr}"
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
        start_process = m.run('bash',
                              input='/home/ubuntu/neard unsafe_reset_data')
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

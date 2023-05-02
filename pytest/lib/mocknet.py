import json
import os
import random
import os.path
import shlex
import subprocess
import tempfile
import time
import functools

import base58
import requests
from rc import run, pmap, gcloud

import data
from cluster import GCloudNode
from configured_logger import logger
from key import Key
from metrics import Metrics
from transaction import sign_payment_tx_and_get_hash, sign_staking_tx_and_get_hash

DEFAULT_KEY_TARGET = '/tmp/mocknet'
KEY_TARGET_ENV_VAR = 'NEAR_PYTEST_KEY_TARGET'
# NODE_SSH_KEY_PATH = '~/.ssh/near_ops'
NODE_SSH_KEY_PATH = None
NODE_USERNAME = 'ubuntu'
NUM_ACCOUNTS = 26 * 2
PROJECT = 'near-mocknet'
PUBLIC_KEY = 'ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN'
SECRET_KEY = 'ed25519:3cCk8KUWBySGCxBcn1syMoY5u73wx5eaPLRbQcMi23LwBA3aLsqEbA33Ww1bsJaFrchmDciGe9otdn45SrDSkow2'
TX_OUT_FILE = '/home/ubuntu/tx_events'
WASM_FILENAME = 'simple_contract.wasm'

TREASURY_ACCOUNT = 'test.near'
MASTER_ACCOUNT = 'near'
SKYWARD_ACCOUNT = 'skyward.near'
SKYWARD_TOKEN_ACCOUNT = 'token.skyward.near'
TOKEN1_ACCOUNT = 'token1.near'
TOKEN2_ACCOUNT = 'token2.near'
TOKEN2_OWNER_ACCOUNT = 'account.token2.near'
ACCOUNT1_ACCOUNT = 'account1.near'

TMUX_STOP_SCRIPT = '''
while tmux has-session -t near; do
tmux kill-session -t near || true
done
'''

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

ONE_NEAR = 10**24
MIN_STAKE = 64 * (10**3)
STAKE_STEP = 15 * (10**3)
OTHER_STAKE = 10**6
MAINNET_STAKES = [
    43566361, 20091202, 19783811, 18990335, 18196731, 12284685, 10770734,
    10769428, 9858038, 9704977, 8871933, 8296476, 7731153, 7499051, 7322703,
    7307458, 6477856, 6293083, 6242196, 6093107, 6085802, 5553788, 5508664,
    5286843, 5056137, 4944414, 4859235, 4732286, 4615542, 4565243, 4468179,
    4451510, 4444888, 4412264, 4221909, 4219451, 4210541, 4161553, 4116102,
    4085627, 4075090, 3988387, 3932601, 3923842, 3921959, 3915353, 3907857,
    3905980, 3898791, 3886957, 3851553, 3831536, 3790646, 3784485, 3777647,
    3760931, 3746129, 3741225, 3727313, 3699201, 3620341
]

ACCOUNTS = {
    TREASURY_ACCOUNT: (10**7) * ONE_NEAR,
    MASTER_ACCOUNT: (10**7) * ONE_NEAR,
    SKYWARD_ACCOUNT: (10**6) * ONE_NEAR,
    TOKEN1_ACCOUNT: (10**6) * ONE_NEAR,
    TOKEN2_ACCOUNT: (10**6) * ONE_NEAR,
    TOKEN2_OWNER_ACCOUNT: (10**6) * ONE_NEAR,
    ACCOUNT1_ACCOUNT: (10**6) * ONE_NEAR,
}


def get_node(hostname):
    instance_name = hostname
    n = GCloudNode(
        instance_name,
        username=NODE_USERNAME,
        project=PROJECT,
        ssh_key_path=NODE_SSH_KEY_PATH,
    )
    return n


def get_nodes(pattern=None):
    machines = gcloud.list(
        pattern=pattern,
        project=PROJECT,
        username=NODE_USERNAME,
        ssh_key_path=NODE_SSH_KEY_PATH,
    )
    nodes = pmap(
        lambda machine: GCloudNode(
            machine.name,
            username=NODE_USERNAME,
            project=PROJECT,
            ssh_key_path=NODE_SSH_KEY_PATH,
        ),
        machines,
    )
    return nodes


# Needs to be in-sync with init.sh.tmpl in terraform.
def node_account_name(node_name):
    # Assuming node_name is a hostname and looks like
    # 'mocknet-betanet-spoon-abcd' or 'mocknet-zxcv'.
    parts = node_name.split('-')
    return f'{parts[-1]}-load-test.near'


# Constructs an account name given the basic account name.
# Accounts are generated in this order:
# `a00.base`, `b00.base`, .., `z00.base`, `a01.base`, ...
def load_testing_account_id(node_account_id, i):
    NUM_LETTERS = 26
    letter = i % NUM_LETTERS
    num = i // NUM_LETTERS
    return '%s%02d.%s' % (chr(ord('a') + letter), num, node_account_id)


def get_validator_account(node):
    return Key.from_json(
        download_and_read_json(node, '/home/ubuntu/.near/validator_key.json'))


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
    m.upload('tests/mocknet/helpers/*.py', PYTHON_DIR, switch_user='ubuntu')
    m.run('bash', input=INSTALL_PYTHON_REQUIREMENTS)
    logger.info(f'{m.name} python setup complete')


def setup_python_environments(nodes, wasm_contract):
    pmap(lambda n: setup_python_environment(n, wasm_contract), nodes)


def start_load_test_helper_script(
    script,
    node_account_id,
    rpc_nodes,
    num_nodes,
    max_tps,
    test_timeout,
    contract_deploy_time,
):
    s = '''
        cd {dir}
        nohup ./venv/bin/python {script} \\
            --node-account-id {node_account_id} \\
            --rpc-nodes {rpc_nodes} \\
            --num-nodes {num_nodes} \\
            --max-tps {max_tps} \\
            --test-timeout {test_timeout} \\
            --contract-deploy-time {contract_deploy_time} \\
            1>load_test.out 2>load_test.err < /dev/null &
    '''.format(
        dir=shlex.quote(PYTHON_DIR),
        script=shlex.quote(script),
        node_account_id=shlex.quote(node_account_id),
        rpc_nodes=shlex.quote(rpc_nodes),
        num_nodes=shlex.quote(str(num_nodes)),
        max_tps=shlex.quote(str(max_tps)),
        test_timeout=shlex.quote(str(test_timeout)),
        contract_deploy_time=shlex.quote(str(contract_deploy_time)),
    )
    logger.info(
        f'Starting load test helper. Node accound id: {node_account_id}.')
    logger.debug(f'The load test helper script is:{s}')
    return s


def start_load_test_helper(
    script,
    node,
    rpc_nodes,
    num_nodes,
    max_tps,
    test_timeout,
    contract_deploy_time,
):
    logger.info(f'Starting load_test_helper on {node.instance_name}')
    rpc_node_ips = ','.join([rpc_node.ip for rpc_node in rpc_nodes])
    node.machine.run(
        'bash',
        input=start_load_test_helper_script(
            script,
            node_account_name(node.instance_name),
            rpc_node_ips,
            num_nodes,
            max_tps,
            test_timeout,
            contract_deploy_time,
        ),
    )


def start_load_test_helpers(
    script,
    validator_nodes,
    rpc_nodes,
    max_tps,
    test_timeout,
    contract_deploy_time,
):
    pmap(
        lambda node: start_load_test_helper(
            script,
            node,
            rpc_nodes,
            len(validator_nodes),
            max_tps,
            test_timeout,
            contract_deploy_time,
        ),
        validator_nodes,
    )


def get_log(node):
    target_file = f'./logs/{node.instance_name}.log'
    node.machine.download('/home/ubuntu/near.log', target_file)


def get_logs(nodes):
    pmap(get_log, nodes)


def get_epoch_length_in_blocks(node):
    config = download_and_read_json(node, '/home/ubuntu/.near/genesis.json')
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
        elif 'does not exist' in error_data:
            missing_count += 1
            logger.warning(
                f'transaction {tx_hash} failed to be received by the node, checking again.'
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

    last_block_hash = node.get_latest_block().hash_bytes

    tx, tx_hash = sign_payment_tx_and_get_hash(alice, bob.account_id,
                                               transfer_amount, alice_nonce + 1,
                                               last_block_hash)
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

    validators = node.get_validators(timeout=None)['result']
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


def is_binary_running(binary_name: str, node) -> bool:
    result = node.machine.run(f'ps -A -o comm= | grep {binary_name}')
    return result.returncode == 0


def is_binary_running_all_nodes(binary_name: str, all_nodes) -> bool:
    return pmap(functools.partial(is_binary_running, binary_name), all_nodes)


def stop_node(node):
    m = node.machine
    logger.info(f'Stopping node {m.name}')
    pids = get_near_pid(m).split()

    for pid in pids:
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


def redownload_neard(nodes, binary_url):
    pmap(
        lambda node: node.machine.
        run('sudo -u ubuntu -i',
            input='wget -O /home/ubuntu/neard {}; chmod +x /home/ubuntu/neard'.
            format(binary_url)), nodes)


# Check /home/ubuntu/neard.upgrade to see whether the amend-genesis command is
# available. Returns the path to the neard binary or throws an exception.
def neard_amend_genesis_path(node):
    r = node.machine.run(
        '/home/ubuntu/neard.upgrade amend-genesis --help',
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if r.exitcode == 0:
        return '/home/ubuntu/neard.upgrade'
    raise Exception(f'`neard.upgrade amend-genesis` not available')


# We assume that the nodes already have the .near directory with the files
# node_key.json, validator_key.json and config.json.
def create_and_upload_genesis(
    validator_nodes,
    chain_id,
    rpc_nodes=None,
    epoch_length=20000,
    node_pks=None,
    increasing_stakes=0.0,
    num_seats=100,
    single_shard=False,
    all_node_pks=None,
    node_ips=None,
):
    logger.info('Uploading genesis and config files')
    assert chain_id
    logger.info(
        'Assuming that genesis_updater.py is available on the instances.')
    validator_keys = dict(pmap(get_validator_key, validator_nodes))
    rpc_node_names = [node.instance_name for node in rpc_nodes]
    assert '-spoon' in chain_id, f'Expecting chain_id like "testnet-spoon" or "mainnet-spoon", got {chain_id}'
    chain_id_in = chain_id.split('-spoon')[0]
    genesis_filename_in = f'/home/ubuntu/.near/{chain_id_in}-genesis/genesis.json'
    records_filename_in = f'/home/ubuntu/.near/{chain_id_in}-genesis/records.json'
    config_filename_in = f'/home/ubuntu/.near/{chain_id_in}-genesis/config.json'
    stamp = time.strftime('%Y%m%d-%H%M%S', time.gmtime())
    done_filename = f'/home/ubuntu/genesis_update_done_{stamp}.txt'
    neard = neard_amend_genesis_path(validator_nodes[1])
    pmap(
        lambda node: start_genesis_updater(
            node=node,
            script='genesis_updater.py',
            genesis_filename_in=genesis_filename_in,
            records_filename_in=records_filename_in,
            config_filename_in=config_filename_in,
            out_dir='/home/ubuntu/.near/',
            chain_id=chain_id,
            validator_keys=validator_keys,
            rpc_nodes=rpc_node_names,
            done_filename=done_filename,
            epoch_length=epoch_length,
            node_pks=node_pks,
            increasing_stakes=increasing_stakes,
            num_seats=num_seats,
            single_shard=single_shard,
            all_node_pks=all_node_pks,
            node_ips=node_ips,
            neard=neard,
        ),
        validator_nodes + rpc_nodes,
    )
    pmap(lambda node: wait_genesis_updater_done(node, done_filename),
         validator_nodes + rpc_nodes)


def extra_genesis_records(validator_keys, rpc_node_names, node_pks,
                          seen_accounts, num_seats, increasing_stakes):
    records = []

    VALIDATOR_BALANCE = (10**2) * ONE_NEAR
    RPC_BALANCE = (10**1) * ONE_NEAR
    LOAD_TESTER_BALANCE = (10**4) * ONE_NEAR

    for account_id, balance in ACCOUNTS.items():
        if account_id not in seen_accounts:
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
        pkeys = [PUBLIC_KEY]
        if node_pks:
            pkeys += node_pks
        for pk in pkeys:
            records.append({
                'AccessKey': {
                    'account_id': account_id,
                    'public_key': pk,
                    'access_key': {
                        'nonce': 0,
                        'permission': 'FullAccess'
                    }
                }
            })

    stakes = []
    prev_stake = None
    for i, account_id in enumerate(validator_keys):
        logger.info(f'Adding account {account_id}')
        if increasing_stakes:
            if i * 5 < num_seats * 3 and i < len(MAINNET_STAKES):
                staked = MAINNET_STAKES[i] * ONE_NEAR
            elif prev_stake is None:
                prev_stake = MIN_STAKE - STAKE_STEP
                staked = prev_stake * ONE_NEAR
            else:
                prev_stake = prev_stake + STAKE_STEP
                staked = prev_stake * ONE_NEAR
        else:
            staked = MIN_STAKE
        stakes.append((staked, account_id))
        records.append({
            'Account': {
                'account_id': account_id,
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
                'account_id': account_id,
                'public_key': PUBLIC_KEY,
                'access_key': {
                    'nonce': 0,
                    'permission': 'FullAccess'
                }
            }
        })
        for i in range(NUM_ACCOUNTS):
            load_testing_account = load_testing_account_id(account_id, i)
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
    for node_name in rpc_node_names:
        account_id = node_account_name(node_name)
        logger.info(f'Adding rpc node account {account_id}')
        records.append({
            'Account': {
                'account_id': account_id,
                'account': {
                    'amount': str(RPC_BALANCE),
                    'locked': str(0),
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

    validators = []
    seats = compute_seats(stakes, num_seats)
    seats_taken = 0
    for seats, staked, account_id in seats:
        if seats + seats_taken > num_seats:
            break
        validators.append({
            'account_id': account_id,
            'public_key': validator_keys[account_id],
            'amount': str(staked),
        })
        seats_taken += seats

    return records, validators


def neard_amend_genesis(neard, validator_keys, genesis_filename_in,
                        records_filename_in, out_dir, rpc_node_names, chain_id,
                        epoch_length, node_pks, increasing_stakes, num_seats,
                        single_shard):
    extra_records, validators = extra_genesis_records(validator_keys,
                                                      rpc_node_names, node_pks,
                                                      set(), num_seats,
                                                      increasing_stakes)

    validators_filename = os.path.join(out_dir, 'validators.json')
    extra_records_filename = os.path.join(out_dir, 'extra-records.json')
    genesis_filename_out = os.path.join(out_dir, 'genesis.json')
    records_filename_out = os.path.join(out_dir, 'records.json')

    with open(validators_filename, 'w') as f:
        json.dump(validators, f)
    with open(extra_records_filename, 'w') as f:
        json.dump(extra_records, f)

    cmd = [
        neard,
        'amend-genesis',
        '--genesis-file-in',
        genesis_filename_in,
        '--records-file-in',
        records_filename_in,
        '--extra-records',
        extra_records_filename,
        '--validators',
        validators_filename,
        '--genesis-file-out',
        genesis_filename_out,
        '--records-file-out',
        records_filename_out,
        '--num-seats',
        str(int(num_seats)),
        '--transaction-validity-period',
        str(10**9),
        '--protocol-version',
        '49',
        '--protocol-reward-rate',
        '1/10',
        '--block-producer-kickout-threshold',
        '10',
    ]
    if chain_id is not None:
        cmd.extend(['--chain-id', chain_id])
    if epoch_length is not None:
        cmd.extend(['--epoch-length', str(epoch_length)])
    if single_shard:
        shard_layout_filename = os.path.join(out_dir, 'shard_layout.json')
        with open(shard_layout_filename, 'w') as f:
            json.dump({'V0': {'num_shards': 1, 'version': 0}}, f)

        cmd.extend(['--shard-layout-file', shard_layout_filename])

    subprocess.run(cmd, text=True)


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
    LOAD_TESTER_BALANCE = (10**4) * ONE_NEAR

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
        for i in range(NUM_ACCOUNTS):
            load_testing_account = load_testing_account_id(
                validator.account_id, i)
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
    genesis_config['num_block_producer_seats_per_shard'] = [int(num_seats)] * 4

    genesis_config['records'] = records
    for node in [node for (node, _) in validator_node_and_stakes] + rpc_nodes:
        upload_json(node, '/home/ubuntu/.near/genesis.json', genesis_config)


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


def get_validator_account_id(node):
    node_key_json = download_and_read_json(
        node, '/home/ubuntu/.near/validator_key.json')
    return node_key_json["account_id"]


def get_validator_key(node):
    node_key_json = download_and_read_json(
        node, '/home/ubuntu/.near/validator_key.json')
    return node_key_json["account_id"], node_key_json["public_key"]


def get_node_keys(node):
    logger.info(f'get_node_keys from {node.instance_name}')
    node_key_json = download_and_read_json(
        node,
        '/home/ubuntu/.near/node_key.json',
    )
    return node_key_json['public_key'], node_key_json['secret_key']


def init_validator_key(node):
    account_id = node_account_name(node.instance_name)
    node.machine.run(
        f'dir=$(mktemp -d) && /home/ubuntu/neard --home $dir init --account-id {account_id} && mv $dir/validator_key.json /home/ubuntu/.near/validator_key.json'
    )


def update_config_file(
    config_filename_in,
    config_filename_out,
    all_node_pks,
    node_ips,
):
    with open(config_filename_in) as f:
        config_json = json.load(f)

    # Usually the port is 24567
    port = config_json['network']['addr'].split(':')[1]
    node_addresses = [
        f'{node_key}@{node_ip}:{port}'
        for node_key, node_ip in zip(all_node_pks, node_ips)
    ]

    config_json['tracked_shards'] = [0]
    config_json['archive'] = True
    config_json['archival_peer_connections_lower_bound'] = 1
    config_json['network']['boot_nodes'] = ','.join(node_addresses)
    config_json['rpc']['addr'] = '0.0.0.0:3030'
    if 'telemetry' in config_json:
        config_json['telemetry']['endpoints'] = []

    with open(config_filename_out, 'w') as f:
        json.dump(config_json, f, indent=2)


def create_and_upload_config_file_from_default(nodes, chain_id, overrider=None):
    nodes[0].machine.run(
        'rm -rf /home/ubuntu/.near-tmp && mkdir /home/ubuntu/.near-tmp && /home/ubuntu/neard --home /home/ubuntu/.near-tmp init --chain-id {}'
        .format(chain_id))
    config_json = download_and_read_json(
        nodes[0],
        '/home/ubuntu/.near-tmp/config.json',
    )
    config_json['tracked_shards'] = [0, 1, 2, 3]
    config_json['archive'] = True
    config_json['archival_peer_connections_lower_bound'] = 1
    node_addresses = [get_node_addr(node, 24567) for node in nodes]
    config_json['network']['boot_nodes'] = ','.join(node_addresses)
    config_json['network']['skip_sync_wait'] = False
    config_json['rpc']['addr'] = '0.0.0.0:3030'
    config_json['rpc']['enable_debug_rpc'] = True
    if 'telemetry' in config_json:
        config_json['telemetry']['endpoints'] = []

    for node in nodes:
        copied_config = json.loads(json.dumps(config_json))
        if overrider:
            overrider(node, copied_config)
        upload_json(node, '/home/ubuntu/.near/config.json', copied_config)


def update_existing_config_file(nodes, overrider=None):
    for node in nodes:
        config_json = download_and_read_json(
            nodes[0],
            '/home/ubuntu/.near/config.json',
        )
        overrider(node, config_json)
        upload_json(node, '/home/ubuntu/.near/config.json', config_json)


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
        sudo rm -rf /home/ubuntu/.near/data
        tmux new -s near -d bash
        tmux send-keys -t near 'RUST_BACKTRACE=full RUST_LOG=debug,actix_web=info {neard_binary} run 2>&1 | tee -a {neard_binary}.log' C-m
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


def start_genesis_updater_script(
    script,
    genesis_filename_in,
    records_filename_in,
    config_filename_in,
    out_dir,
    chain_id,
    validator_keys,
    rpc_nodes,
    done_filename,
    epoch_length,
    node_pks,
    increasing_stakes,
    num_seats,
    single_shard,
    all_node_pks,
    node_ips,
    neard,
):
    validators = ','.join(
        [f'{account_id}={key}' for (account_id, key) in validator_keys.items()])
    cmd = ' '.join([
        shlex.quote(str(arg)) for arg in [
            'nohup',
            './venv/bin/python',
            script,
            genesis_filename_in,
            records_filename_in,
            config_filename_in,
            out_dir,
            chain_id,
            validators,
            ','.join(rpc_nodes),
            done_filename,
            epoch_length,
            ','.join(node_pks),
            increasing_stakes,
            num_seats,
            single_shard,
            ','.join(all_node_pks),
            ','.join(node_ips),
            neard if neard is not None else 'None',
        ]
    ])
    return '''
        cd {dir}
        {cmd} 1> genesis_updater.out 2> genesis_updater.err < /dev/null &
        '''.format(dir=shlex.quote(PYTHON_DIR), cmd=cmd)


def start_genesis_updater(node, script, genesis_filename_in,
                          records_filename_in, config_filename_in, out_dir,
                          chain_id, validator_keys, rpc_nodes, done_filename,
                          epoch_length, node_pks, increasing_stakes, num_seats,
                          single_shard, all_node_pks, node_ips, neard):
    logger.info(f'Starting genesis_updater on {node.instance_name}')
    node.machine.run(
        'bash',
        input=start_genesis_updater_script(
            script,
            genesis_filename_in,
            records_filename_in,
            config_filename_in,
            out_dir,
            chain_id,
            validator_keys,
            rpc_nodes,
            done_filename,
            epoch_length,
            node_pks,
            increasing_stakes,
            num_seats,
            single_shard,
            all_node_pks,
            node_ips,
            neard,
        ),
    )


def wait_genesis_updater_done(node, done_filename):
    msg = f'Waiting for the genesis updater on {node.instance_name}'
    logger.info(msg)

    # Wait until the neard process stops running.
    while True:
        time.sleep(5)
        if not is_binary_running('neard', node):
            break

    # Check if the done_filename was produced.
    result = node.machine.run(f'ls {done_filename}')
    if result.returncode != 0:
        # Get the stderr of the genesis updater.
        script = f'cat {shlex.quote(PYTHON_DIR)}/genesis_updater.err'
        stderr = node.machine.run(script).stdout
        error_msg = f'The {done_filename} is missing! The stderr is \n\n\n{stderr}\n'
        raise Exception(f'{msg} - failed. {error_msg}')

    logger.info(f'{msg} -- done')


# Waits until the node becomes responsive to RPC requests. It works the same as
# list_validators but assumes that the node can still be starting and expects
# connection errors and the data not being available.
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


def create_upgrade_schedule(
    rpc_nodes,
    validator_nodes,
    progressive_upgrade,
    increasing_stakes,
    num_block_producer_seats,
):
    schedule = {}
    if progressive_upgrade:
        # Re-create stakes assignment.
        stakes = []
        if increasing_stakes:
            prev_stake = None
            for i, node in enumerate(validator_nodes):
                if (i * 5 < num_block_producer_seats * 3 and
                        i < len(MAINNET_STAKES)):
                    staked = MAINNET_STAKES[i] * ONE_NEAR
                elif prev_stake is None:
                    prev_stake = MIN_STAKE - STAKE_STEP
                    staked = prev_stake * ONE_NEAR
                else:
                    prev_stake = prev_stake + STAKE_STEP
                    staked = prev_stake * ONE_NEAR
                stakes.append((staked, node.instance_name))

        else:
            for node in validator_nodes:
                stakes.append((MIN_STAKE, node.instance_name))
        logger.info(f'create_upgrade_schedule')
        for stake, instance_name in stakes:
            logger.debug(f'instance_name: {instance_name} - stake: {stake}')

        # Compute seat assignments.
        seats = compute_seats(stakes, num_block_producer_seats)

        seats_upgraded = 0
        for seat, stake, instance_name in seats:
            # As the protocol upgrade takes place after 80% of the nodes are
            # upgraded, stop a bit earlier to start in a non-upgraded state.
            if (seats_upgraded + seat) * 100 > 75 * num_block_producer_seats:
                break
            schedule[instance_name] = 0
            logger.info(
                f'validator node {node.instance_name} will start upgraded')
            seats_upgraded += seat

        # Upgrade the remaining validators during 4 epochs.
        for node in validator_nodes:
            if node.instance_name not in schedule:
                schedule[node.instance_name] = random.randint(1, 4)
                logger.info(
                    f'validator node {node.instance_name} will upgrade at {schedule[node.instance_name]}'
                )

        for node in rpc_nodes:
            schedule[node.instance_name] = random.randint(0, 4)
            if not schedule[node.instance_name]:
                logger.info(
                    f'rpc node {node.instance_name} will start upgraded')
            else:
                logger.info(
                    f'rpc node {node.instance_name} will upgrade at {schedule[node.instance_name]}'
                )
    else:
        # Start all nodes upgraded.
        for node in rpc_nodes:
            schedule[node.instance_name] = 0
        for node in validator_nodes:
            schedule[node.instance_name] = 0

    return schedule


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


def upgrade_nodes(epoch_height, upgrade_schedule, all_nodes):
    logger.info(f'Upgrading nodes for epoch height {epoch_height}')
    for node in all_nodes:
        if upgrade_schedule.get(node.instance_name, 0) == epoch_height:
            upgrade_node(node)


def get_epoch_height(rpc_nodes, prev_epoch_height):
    nodes = rpc_nodes.copy()
    random.shuffle(nodes)
    max_height = prev_epoch_height
    for node in nodes:
        (addr, port) = node.rpc_addr()
        j = {
            'method': 'validators',
            'params': [None],
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        try:
            r = requests.post('http://%s:%s' % (addr, port), json=j, timeout=15)
            if r.ok:
                response = r.json()
                max_height = max(
                    max_height,
                    int(response.get('result', {}).get('epoch_height', 0)),
                )
        except Exception as e:
            continue
    return max_height


def neard_restart_script(node):
    neard_binary = '/home/ubuntu/neard.upgrade'
    return '''
        tmux send-keys -t near C-c
        sudo mv /home/ubuntu/near.log /home/ubuntu/near.log.1 2>/dev/null
        sudo mv /home/ubuntu/near.upgrade.log /home/ubuntu/near.upgrade.log.1 2>/dev/null
        tmux send-keys -t near 'RUST_BACKTRACE=full RUST_LOG=debug,actix_web=info {neard_binary} run 2>&1 | tee -a {neard_binary}.log' C-m
    '''.format(neard_binary=shlex.quote(neard_binary))


def upgrade_node(node):
    logger.info(f'Upgrading node {node.instance_name}')
    attempt = 0
    success = False
    while attempt < 3:
        start_process = node.machine.run(
            'sudo -u ubuntu -i',
            input=neard_restart_script(node),
        )
        if start_process.returncode == 0:
            success = True
            break
        logger.warn(
            f'Failed to upgrade neard, return code: {start_process.returncode}\n{node.instance_name}\n{start_process.stderr}'
        )
        attempt += 1
        time.sleep(1)
    if not success:
        raise Exception(f'Could not upgrade node {node.instance_name}')


STAKING_TIMEOUT = 60


# If the available amount of whole NEAR tokens is above 10**3, then stakes all available amount.
# Runs only if `last_staking` is at least `STAKING_TIMEOUT` seconds in the past.
def stake_available_amount(node_account, last_staking):
    # Repeat the staking transactions in case the validator selection algorithm changes.
    # Don't query the balance too often, avoid overloading the RPC node.
    if time.time() - last_staking > STAKING_TIMEOUT:
        # Make several attempts just in case the RPC node doesn't respond.
        for attempt in range(3):
            try:
                stake_amount = node_account.get_amount_yoctonear()
                logger.info(
                    f'Amount of {node_account.key.account_id} is {stake_amount}'
                )
                if stake_amount > (10**3) * ONE_NEAR:
                    logger.info(
                        f'Staking {stake_amount} for {node_account.key.account_id}'
                    )
                    node_account.send_stake_tx(stake_amount)
                logger.info(
                    f'Staked {stake_amount} for {node_account.key.account_id}')
                return time.time()
            except Exception as e:
                logger.info('Failed to stake')
    return None

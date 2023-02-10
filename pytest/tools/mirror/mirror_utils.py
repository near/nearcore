import sys
import time
import base58
import atexit
import json
import os
import pathlib
import shutil
import signal
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
import transaction
import utils
import key

MIRROR_DIR = 'test-mirror'
TIMEOUT = 240
TARGET_VALIDATORS = ['foo0', 'foo1', 'foo2']

CONTRACT_PATH = pathlib.Path(__file__).resolve().parents[
    0] / 'contract/target/wasm32-unknown-unknown/release/addkey_contract.wasm'


def mkdir_clean(dirname):
    try:
        dirname.mkdir()
    except FileExistsError:
        shutil.rmtree(dirname)
        dirname.mkdir()


def dot_near():
    return pathlib.Path.home() / '.near'


def ordinal_to_addr(port, ordinal):
    return f'0.0.0.0:{port + 10 + ordinal}'


def copy_genesis(home):
    shutil.copy(dot_near() / 'test0/forked/genesis.json', home / 'genesis.json')
    shutil.copy(dot_near() / 'test0/forked/records.json', home / 'records.json')


def init_target_dir(neard,
                    home,
                    ordinal,
                    boot_node_home,
                    validator_account=None):
    mkdir_clean(home)

    try:
        args = [neard, '--home', home, 'init']
        if validator_account is not None:
            args.extend(['--account-id', validator_account])
        subprocess.check_output(args, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"neard init" command failed: output: {e.stdout}')
    shutil.copy(dot_near() / 'test0/config.json', home / 'config.json')

    with open(home / 'config.json', 'r') as f:
        config = json.load(f)
        config['genesis_records_file'] = 'records.json'
        config['network']['addr'] = ordinal_to_addr(24567, ordinal)
        if boot_node_home is not None:
            config['network']['boot_nodes'] = read_addr_pk(boot_node_home)
        config['rpc']['addr'] = ordinal_to_addr(3030, ordinal)

    with open(home / 'config.json', 'w') as f:
        json.dump(config, f)

    if validator_account is None:
        os.remove(home / 'validator_key.json')


def init_target_dirs(neard, last_ordinal, target_validators):
    ordinal = last_ordinal + 1
    dirs = []

    for i in range(len(target_validators)):
        account_id = target_validators[i]
        if i > 0:
            boot_node_home = dirs[0]
        else:
            boot_node_home = None
        home = dot_near() / f'test_target_{account_id}'
        dirs.append(home)
        init_target_dir(neard,
                        home,
                        ordinal,
                        boot_node_home,
                        validator_account=account_id)
        ordinal += 1

    return dirs


def create_forked_chain(config, near_root, source_node_homes,
                        target_validators):
    mkdir_clean(dot_near() / MIRROR_DIR)
    binary_name = config.get('binary_name', 'neard')
    neard = os.path.join(near_root, binary_name)
    try:
        subprocess.check_output([
            neard, "--home",
            dot_near() / 'test0', "view-state", "dump-state", "--stream"
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"dump-state" command failed: output: {e.stdout}')
    try:
        subprocess.check_output([
            neard,
            'mirror',
            'prepare',
            '--records-file-in',
            dot_near() / 'test0/output/records.json',
            '--records-file-out',
            dot_near() / 'test0/output/mirror-records.json',
            '--secret-file-out',
            dot_near() / 'test0/output/mirror-secret.json',
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"mirror prepare" command failed: output: {e.stdout}')

    os.rename(source_node_homes[-1], dot_near() / f'{MIRROR_DIR}/source')
    ordinal = len(source_node_homes) - 1
    with open(dot_near() / f'{MIRROR_DIR}/source/config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = read_addr_pk(source_node_homes[0])
        config['network']['addr'] = ordinal_to_addr(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_addr(3030, ordinal)
    with open(dot_near() / f'{MIRROR_DIR}/source/config.json', 'w') as f:
        json.dump(config, f)

    dirs = init_target_dirs(neard, ordinal, target_validators)

    target_dir = dot_near() / f'{MIRROR_DIR}/target'
    init_target_dir(neard,
                    target_dir,
                    len(source_node_homes) + len(dirs),
                    dirs[0],
                    validator_account=None)
    shutil.copy(dot_near() / 'test0/output/mirror-secret.json',
                target_dir / 'mirror-secret.json')

    os.mkdir(dot_near() / 'test0/forked')
    genesis_file_in = dot_near() / 'test0/output/genesis.json'
    genesis_file_out = dot_near() / 'test0/forked/genesis.json'
    records_file_in = dot_near() / 'test0/output/mirror-records.json'
    records_file_out = dot_near() / 'test0/forked/records.json'

    validators = []
    for d in dirs:
        with open(d / 'validator_key.json') as f:
            key = json.load(f)
        validators.append({
            'account_id': key['account_id'],
            'public_key': key['public_key'],
            'amount': '700000000000000'
        })

    validators_file = dot_near() / 'test0/forked/validators.json'
    with open(validators_file, 'w') as f:
        json.dump(validators, f)

    try:
        # we want to set transaction-validity-period to a bigger number
        # because the mirror code sets the block hash on transactions up to a couple minutes
        # before sending them, and that can cause problems for the default localnet
        # setting of transaction_validity_period. Not really worth changing the code since
        # transaction_validity_period is large on mainnet and testnet anyway
        subprocess.check_output([
            neard, 'amend-genesis', '--genesis-file-in', genesis_file_in,
            '--records-file-in', records_file_in, '--genesis-file-out',
            genesis_file_out, '--records-file-out', records_file_out,
            '--validators', validators_file, '--chain-id', 'foonet',
            '--transaction-validity-period', '10000'
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"amend-genesis" command failed: output: {e.stdout}')

    for d in dirs:
        copy_genesis(d)
    copy_genesis(target_dir)

    return [str(d) for d in dirs]


def read_addr_pk(home):
    with open(os.path.join(home, 'config.json'), 'r') as f:
        config = json.load(f)
        addr = config['network']['addr']

    with open(os.path.join(home, 'node_key.json'), 'r') as f:
        k = json.load(f)
        public_key = k['public_key']

    return f'{public_key}@{addr}'


def mirror_cleanup(process):
    process.send_signal(signal.SIGINT)
    try:
        process.wait(5)
    except:
        process.kill()
        logger.error('can\'t kill mirror process')


# helper class so we can pass restart_once() as a callback to send_traffic()
class MirrorProcess:

    def __init__(self, near_root, source_home, online_source):
        self.online_source = online_source
        self.source_home = source_home
        self.neard = os.path.join(near_root, 'neard')
        self.start()
        self.start_time = time.time()
        self.restarted = False

    def start(self):
        env = os.environ.copy()
        env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
            "RUST_LOG", "debug")
        with open(dot_near() / f'{MIRROR_DIR}/stdout', 'ab') as stdout, \
            open(dot_near() / f'{MIRROR_DIR}/stderr', 'ab') as stderr:
            args = [
                self.neard, 'mirror', 'run', "--source-home", self.source_home,
                "--target-home",
                dot_near() / f'{MIRROR_DIR}/target/', '--secret-file',
                dot_near() / f'{MIRROR_DIR}/target/mirror-secret.json'
            ]
            if self.online_source:
                args.append('--online-source')
            self.process = subprocess.Popen(args,
                                            stdin=subprocess.DEVNULL,
                                            stdout=stdout,
                                            stderr=stderr,
                                            env=env)
        logger.info("Started mirror process")
        atexit.register(mirror_cleanup, self.process)

    def restart(self):
        logger.info('stopping mirror process')
        self.process.terminate()
        self.process.wait()
        with open(dot_near() / f'{MIRROR_DIR}/stderr', 'ab') as stderr:
            stderr.write(
                b'<><><><><><><><><><><><> restarting <><><><><><><><><><><><><><><><><><><><>\n'
            )
            stderr.write(
                b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
            )
            stderr.write(
                b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
            )
        self.start()

    # meant to be used in the callback to send_traffic(). restarts the process once after 30 seconds
    def restart_once(self):
        code = self.process.poll()
        if code is not None:
            assert code == 0
            return False

        if not self.restarted and time.time() - self.start_time > 30:
            self.restart()
            self.restarted = True
        return True


def check_target_validators(target_node):
    try:
        validators = target_node.get_validators()['result']
    except KeyError:
        return

    for v in validators['current_validators']:
        assert v['account_id'] in TARGET_VALIDATORS, v['account_id']
    for v in validators['next_validators']:
        assert v['account_id'] in TARGET_VALIDATORS, v['account_id']


# we'll test out adding an access key and then sending txs signed with it
# since that hits some codepaths we want to test
def send_add_access_key(node, key, target_key, nonce, block_hash):
    action = transaction.create_full_access_key_action(target_key.decoded_pk())
    tx = transaction.sign_and_serialize_transaction(target_key.account_id,
                                                    nonce, [action], block_hash,
                                                    key.account_id,
                                                    key.decoded_pk(),
                                                    key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent add key tx for {target_key.account_id} {target_key.pk}: {res}')


def send_delete_access_key(node, key, target_key, nonce, block_hash):
    action = transaction.create_delete_access_key_action(
        target_key.decoded_pk())
    tx = transaction.sign_and_serialize_transaction(target_key.account_id,
                                                    nonce, [action], block_hash,
                                                    target_key.account_id,
                                                    key.decoded_pk(),
                                                    key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent delete key tx for {target_key.account_id} {target_key.pk}: {res}'
    )


def create_subaccount(node, signer_key, nonce, block_hash):
    k = key.Key.from_random('foo.' + signer_key.account_id)
    actions = []
    actions.append(transaction.create_create_account_action())
    actions.append(transaction.create_full_access_key_action(k.decoded_pk()))
    actions.append(transaction.create_payment_action(10**29))
    # add an extra one just to exercise some more corner cases
    actions.append(
        transaction.create_full_access_key_action(
            key.Key.from_random(k.account_id).decoded_pk()))

    tx = transaction.sign_and_serialize_transaction(k.account_id, nonce,
                                                    actions, block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(f'sent create account tx for {k.account_id} {k.pk}: {res}')
    return k


def deploy_addkey_contract(node, signer_key, contract_path, nonce, block_hash):
    code = utils.load_binary_file(contract_path)
    tx = transaction.sign_deploy_contract_tx(signer_key, code, nonce,
                                             block_hash)
    res = node.send_tx(tx)
    logger.info(f'sent deploy contract tx for {signer_key.account_id}: {res}')


def call_addkey(node, signer_key, new_key, nonce, block_hash, extra_actions=[]):
    args = bytearray(json.dumps({'public_key': new_key.pk}), encoding='utf-8')

    # add a transfer action and the extra_actions to exercise some more code paths
    actions = [
        transaction.create_function_call_action('add_key', args, 10**14, 0),
        transaction.create_payment_action(123)
    ]
    actions.extend(extra_actions)
    tx = transaction.sign_and_serialize_transaction('test0', nonce, actions,
                                                    block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(f'called add_key for {new_key.account_id} {new_key.pk}: {res}')


def call_create_account(node, signer_key, account_id, nonce, block_hash):
    k = key.Key.from_random(account_id)
    args = json.dumps({'account_id': account_id, 'public_key': k.pk})
    args = bytearray(args, encoding='utf-8')

    actions = [
        transaction.create_function_call_action('create_account', args, 10**13,
                                                10**24),
        transaction.create_payment_action(123)
    ]
    tx = transaction.sign_and_serialize_transaction('test0', nonce, actions,
                                                    block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'called create account contract for {account_id} {k.pk}: {res}')
    return k


def call_stake(node, signer_key, amount, public_key, nonce, block_hash):
    args = json.dumps({'amount': amount, 'public_key': public_key})
    args = bytearray(args, encoding='utf-8')

    actions = [
        transaction.create_function_call_action('stake', args, 10**13, 0),
    ]
    tx = transaction.sign_and_serialize_transaction(signer_key.account_id,
                                                    nonce, actions, block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'called stake contract for {signer_key.account_id} {amount} {public_key}: {res}'
    )


def contract_deployed(node, account_id):
    return 'error' not in node.json_rpc('query', {
        "request_type": "view_code",
        "account_id": account_id,
        "finality": "final"
    })


# a key that we added with an AddKey tx or implicit account transfer.
# just for nonce handling convenience
class AddedKey:

    def __init__(self, key):
        self.nonce = None
        self.key = key

    def send_if_inited(self, node, transfers, block_hash):
        if self.nonce is None:
            self.nonce = node.get_nonce_for_pk(self.key.account_id,
                                               self.key.pk,
                                               finality='final')
            if self.nonce is not None:
                logger.info(
                    f'added key {self.key.account_id} {self.key.pk} inited @ {self.nonce}'
                )

        if self.nonce is not None:
            for (receiver_id, amount) in transfers:
                self.nonce += 1
                tx = transaction.sign_payment_tx(self.key, receiver_id, amount,
                                                 self.nonce, block_hash)
                node.send_tx(tx)

    def account_id(self):
        return self.key.account_id

    def inited(self):
        return self.nonce is not None


class ImplicitAccount:

    def __init__(self):
        self.key = AddedKey(key.Key.implicit_account())

    def account_id(self):
        return self.key.account_id()

    def transfer(self, node, sender_key, amount, block_hash, nonce):
        tx = transaction.sign_payment_tx(sender_key, self.account_id(), amount,
                                         nonce, block_hash)
        node.send_tx(tx)
        logger.info(
            f'sent {amount} to initialize implicit account {self.account_id()}')

    def send_if_inited(self, node, transfers, block_hash):
        self.key.send_if_inited(node, transfers, block_hash)

    def inited(self):
        return self.key.inited()


def count_total_txs(node, min_height=0):
    total = 0
    h = node.get_latest_block().hash
    while True:
        block = node.get_block(h)['result']
        height = int(block['header']['height'])
        if height < min_height:
            return total

        for c in block['chunks']:
            if int(c['height_included']) == height:
                chunk = node.get_chunk(c['chunk_hash'])['result']
                total += len(chunk['transactions'])

        h = block['header']['prev_hash']
        if h == '11111111111111111111111111111111':
            return total


def allowed_run_time(target_node_dir, start_time, end_source_height):
    with open(os.path.join(target_node_dir, 'genesis.json'), 'r') as f:
        genesis_height = json.load(f)['genesis_height']
    with open(os.path.join(target_node_dir, 'config.json'), 'r') as f:
        delay = json.load(f)['consensus']['min_block_production_delay']
        block_delay = 10**9 * int(delay['secs']) + int(delay['nanos'])
        block_delay = block_delay / 10**9

    # start_time is the time the mirror binary was started. Give it 20 seconds to
    # sync and then 50% more than min_block_production_delay for each block between
    # the start and end points of the source chain. Not ideal to be basing a test on time
    # like this but there's no real strong guarantee on when the transactions should
    # make it on chain, so this is some kind of reasonable timeout

    return 20 + (end_source_height - genesis_height) * block_delay * 1.5


def check_num_txs(source_node, target_node):
    with open(os.path.join(target_node.node_dir, 'genesis.json'), 'r') as f:
        genesis_height = json.load(f)['genesis_height']

    total_source_txs = count_total_txs(source_node, min_height=genesis_height)
    total_target_txs = count_total_txs(target_node)
    assert total_source_txs <= total_target_txs, (total_source_txs,
                                                  total_target_txs)
    logger.info(
        f'passed. num source txs: {total_source_txs} num target txs: {total_target_txs}'
    )


# keeps info initialized during start_source_chain() for use in send_traffic()
class TrafficData:

    def __init__(self, num_accounts):
        self.nonces = [2] * num_accounts
        self.implicit_account = None

    def send_transfers(self, nodes, block_hash, skip_senders=None):
        for sender in range(len(self.nonces)):
            if skip_senders is not None and sender in skip_senders:
                continue
            receiver = (sender + 1) % len(self.nonces)
            receiver_id = nodes[receiver].signer_key.account_id

            tx = transaction.sign_payment_tx(nodes[sender].signer_key,
                                             receiver_id, 300,
                                             self.nonces[sender], block_hash)
            nodes[sender].send_tx(tx)
            self.nonces[sender] += 1


def added_keys_send_transfers(nodes, added_keys, receivers, amount, block_hash):
    node_idx = 0
    for key in added_keys:
        key.send_if_inited(nodes[node_idx],
                           [(receiver, amount) for receiver in receivers],
                           block_hash)
        node_idx += 1
        node_idx %= len(nodes)


def start_source_chain(config, num_source_validators=3):
    # for now we need at least 2 because we're sending traffic for source_nodes[1].signer_key
    # Could fix that but for now this assert is fine
    assert num_source_validators >= 2

    if not os.path.exists(CONTRACT_PATH):
        sys.exit(
            'please build the addkey contract by running cargo build --target wasm32-unknown-unknown --release from the ./contract/ dir'
        )

    config_changes = {}
    for i in range(num_source_validators + 1):
        config_changes[i] = {"tracked_shards": [0, 1, 2, 3], "archive": True}

    config = load_config()
    near_root, source_node_dirs = init_cluster(
        num_nodes=num_source_validators,
        num_observers=1,
        num_shards=4,
        config=config,
        # set epoch length to a larger number because otherwise there
        # are often problems with validators getting kicked for missing
        # only one block or chunk
        genesis_config_changes=[
            ["epoch_length", 100],
        ],
        client_config_changes=config_changes)

    source_nodes = [spin_up_node(config, near_root, source_node_dirs[0], 0)]

    for i in range(1, num_source_validators):
        source_nodes.append(
            spin_up_node(config,
                         near_root,
                         source_node_dirs[i],
                         i,
                         boot_node=source_nodes[0]))
    traffic_data = TrafficData(len(source_nodes))

    traffic_data.implicit_account = ImplicitAccount()
    for height, block_hash in utils.poll_blocks(source_nodes[0],
                                                timeout=TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))
        traffic_data.implicit_account.transfer(source_nodes[0],
                                               source_nodes[0].signer_key,
                                               10**24, block_hash_bytes,
                                               traffic_data.nonces[0])
        traffic_data.nonces[0] += 1

        deploy_addkey_contract(source_nodes[0], source_nodes[0].signer_key,
                               CONTRACT_PATH, traffic_data.nonces[0],
                               block_hash_bytes)
        traffic_data.nonces[0] += 1
        deploy_addkey_contract(source_nodes[0], source_nodes[1].signer_key,
                               CONTRACT_PATH, traffic_data.nonces[1],
                               block_hash_bytes)
        traffic_data.nonces[1] += 1
        break

    for height, block_hash in utils.poll_blocks(source_nodes[0],
                                                timeout=TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        traffic_data.implicit_account.send_if_inited(source_nodes[0],
                                                     [('test2', height),
                                                      ('test3', height)],
                                                     block_hash_bytes)
        traffic_data.send_transfers(source_nodes, block_hash_bytes)

        if height > 12:
            break

    source_nodes[0].kill()
    target_node_dirs = create_forked_chain(config, near_root, source_node_dirs,
                                           TARGET_VALIDATORS)
    source_nodes[0].start(boot_node=source_nodes[1])
    return near_root, source_nodes, target_node_dirs, traffic_data


# callback will be called once for every iteration of the utils.poll_blocks()
# loop, and we break if it returns False
def send_traffic(near_root, source_nodes, traffic_data, callback):
    tip = source_nodes[1].get_latest_block()
    block_hash_bytes = base58.b58decode(tip.hash.encode('utf8'))
    start_source_height = tip.height

    subaccount_key = AddedKey(
        create_subaccount(source_nodes[1], source_nodes[0].signer_key,
                          traffic_data.nonces[0], block_hash_bytes))
    traffic_data.nonces[0] += 1

    k = key.Key.from_random('test0')
    new_key = AddedKey(k)
    send_add_access_key(source_nodes[1], source_nodes[0].signer_key, k,
                        traffic_data.nonces[0], block_hash_bytes)
    traffic_data.nonces[0] += 1

    test0_contract_key = key.Key.from_random('test0')
    test0_contract_extra_key = key.Key.from_random('test0')

    # here we are assuming that the deployed contract has landed since we called start_source_chain()
    # we will add an extra AddKey action to hit some more code paths
    call_addkey(source_nodes[1],
                source_nodes[0].signer_key,
                test0_contract_key,
                traffic_data.nonces[0],
                block_hash_bytes,
                extra_actions=[
                    transaction.create_full_access_key_action(
                        test0_contract_extra_key.decoded_pk())
                ])
    traffic_data.nonces[0] += 1

    test0_contract_key = AddedKey(test0_contract_key)
    test0_contract_extra_key = AddedKey(test0_contract_extra_key)

    test1_contract_key = key.Key.from_random('test1')

    call_addkey(source_nodes[1], source_nodes[1].signer_key, test1_contract_key,
                traffic_data.nonces[1], block_hash_bytes)
    traffic_data.nonces[1] += 1
    test1_contract_key = AddedKey(test1_contract_key)

    test0_subaccount_contract_key = AddedKey(
        call_create_account(source_nodes[1], source_nodes[0].signer_key,
                            'test0.test0', traffic_data.nonces[0],
                            block_hash_bytes))
    traffic_data.nonces[0] += 1
    test1_subaccount_contract_key = AddedKey(
        call_create_account(source_nodes[1], source_nodes[1].signer_key,
                            'test1.test0', traffic_data.nonces[1],
                            block_hash_bytes))
    traffic_data.nonces[1] += 1

    test0_deleted_height = None
    test0_readded_key = None
    implicit_added = None
    implicit_deleted = None
    implicit_account2 = ImplicitAccount()
    subaccount_contract_deployed = False
    subaccount_staked = False

    # here we are gonna send a tiny amount (1 yoctoNEAR) to the implicit account and
    # then wait a bit before properly initializing it. This hits a corner case where the
    # mirror binary needs to properly look for the second tx's outcome to find the starting
    # nonce because the first one failed
    implicit_account2.transfer(source_nodes[1], source_nodes[0].signer_key, 1,
                               block_hash_bytes, traffic_data.nonces[0])
    traffic_data.nonces[0] += 1
    time.sleep(2)
    implicit_account2.transfer(source_nodes[1], source_nodes[0].signer_key,
                               10**24, block_hash_bytes, traffic_data.nonces[0])
    traffic_data.nonces[0] += 1

    for height, block_hash in utils.poll_blocks(source_nodes[1],
                                                timeout=TIMEOUT):
        if not callback():
            break
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        if test0_deleted_height is None:
            traffic_data.send_transfers(source_nodes, block_hash_bytes)
        else:
            traffic_data.send_transfers(source_nodes,
                                        block_hash_bytes,
                                        skip_senders=set([0]))

        traffic_data.implicit_account.send_if_inited(
            source_nodes[1], [('test2', height), ('test1', height),
                              (implicit_account2.account_id(), height)],
            block_hash_bytes)
        if not implicit_deleted:
            implicit_account2.send_if_inited(
                source_nodes[1],
                [('test2', height), ('test0', height),
                 (traffic_data.implicit_account.account_id(), height)],
                block_hash_bytes)
        keys = [
            new_key,
            subaccount_key,
            test0_contract_key,
            test0_contract_extra_key,
            test1_contract_key,
            test0_subaccount_contract_key,
            test1_subaccount_contract_key,
        ]
        added_keys_send_transfers(source_nodes, keys, [
            traffic_data.implicit_account.account_id(),
            implicit_account2.account_id(), 'test2', 'test3'
        ], height, block_hash_bytes)

        if implicit_added is None:
            # wait for 15 blocks after we started to get some "normal" traffic
            # from this implicit account that's closer to what we usually see from
            # these (most people aren't adding access keys to implicit accounts much).
            # then after that we add an access key and delete the original one to test
            # some more code paths
            if implicit_account2.inited(
            ) and height - start_source_height >= 15:
                k = key.Key.from_random(implicit_account2.account_id())
                implicit_added = AddedKey(k)
                send_add_access_key(source_nodes[1], implicit_account2.key.key,
                                    k, implicit_account2.key.nonce,
                                    block_hash_bytes)
                implicit_account2.key.nonce += 1
        else:
            implicit_added.send_if_inited(source_nodes[1], [('test0', height)],
                                          block_hash_bytes)
            if implicit_added.inited() and not implicit_deleted:
                send_delete_access_key(source_nodes[1], implicit_added.key,
                                       implicit_account2.key.key,
                                       implicit_added.nonce, block_hash_bytes)
                implicit_added.nonce += 1
                implicit_deleted = True

        if test0_deleted_height is None and new_key.inited(
        ) and height - start_source_height >= 15:
            send_delete_access_key(source_nodes[1], new_key.key,
                                   source_nodes[0].signer_key,
                                   new_key.nonce + 1, block_hash_bytes)
            new_key.nonce += 1
            test0_deleted_height = height

        if test0_readded_key is None and test0_deleted_height is not None and height - test0_deleted_height >= 5:
            send_add_access_key(source_nodes[1], new_key.key,
                                source_nodes[0].signer_key, new_key.nonce + 1,
                                block_hash_bytes)
            test0_readded_key = AddedKey(source_nodes[0].signer_key)
            new_key.nonce += 1

        if test0_readded_key is not None:
            test0_readded_key.send_if_inited(
                source_nodes[1], [('test3', height),
                                  (implicit_account2.account_id(), height)],
                block_hash_bytes)

        if subaccount_key.inited():
            if not subaccount_contract_deployed:
                subaccount_key.nonce += 1
                deploy_addkey_contract(source_nodes[0], subaccount_key.key,
                                       CONTRACT_PATH, subaccount_key.nonce,
                                       block_hash_bytes)
                subaccount_contract_deployed = True
            elif not subaccount_staked:
                if contract_deployed(source_nodes[0],
                                     subaccount_key.account_id()):
                    subaccount_key.nonce += 1
                    call_stake(source_nodes[0], subaccount_key.key, 10**28,
                               subaccount_key.key.pk, subaccount_key.nonce,
                               block_hash_bytes)
                    subaccount_staked = True

        if height - start_source_height >= 100:
            break

#!/usr/bin/env python3

import sys, time, base58, random
import atexit
import base58
import json
import os
import pathlib
import shutil
import signal
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from mocknet import create_genesis_file
import transaction
import utils
import key

# This sets up an environment to test the tools/mirror process. It starts a localnet with a few validators
# and waits for some blocks to be produced. Then we fork the state and start a new chain from that, and
# start the mirror process that should mirror transactions from the source chain to the target chain.
# Halfway through we restart it to make sure that it still works properly when restarted

TIMEOUT = 240
NUM_VALIDATORS = 4
TARGET_VALIDATORS = ['foo0', 'foo1', 'foo2']
MIRROR_DIR = 'test-mirror'


def mkdir_clean(dirname):
    try:
        dirname.mkdir()
    except FileExistsError:
        shutil.rmtree(dirname)
        dirname.mkdir()


def dot_near():
    return pathlib.Path.home() / '.near'


def ordinal_to_port(port, ordinal):
    return f'0.0.0.0:{port + 10 + ordinal}'


def copy_genesis(home):
    shutil.copy(dot_near() / 'test0/forked/genesis.json', home / 'genesis.json')
    shutil.copy(dot_near() / 'test0/forked/records.json', home / 'records.json')


def init_target_dir(neard, home, ordinal, validator_account=None):
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
        config['network']['addr'] = ordinal_to_port(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_port(3030, ordinal)
    with open(home / 'config.json', 'w') as f:
        json.dump(config, f)

    if validator_account is None:
        os.remove(home / 'validator_key.json')


def init_target_dirs(neard):
    ordinal = NUM_VALIDATORS + 1
    dirs = []

    for account_id in TARGET_VALIDATORS:
        home = dot_near() / f'test_target_{account_id}'
        dirs.append(home)
        init_target_dir(neard, home, ordinal, validator_account=account_id)
        ordinal += 1

    return dirs


def create_forked_chain(config, near_root):
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

    dirs = init_target_dirs(neard)

    target_dir = dot_near() / f'{MIRROR_DIR}/target'
    init_target_dir(neard,
                    target_dir,
                    NUM_VALIDATORS + 1 + len(dirs),
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
        subprocess.check_output([
            neard,
            'amend-genesis',
            '--genesis-file-in',
            genesis_file_in,
            '--records-file-in',
            records_file_in,
            '--genesis-file-out',
            genesis_file_out,
            '--records-file-out',
            records_file_out,
            '--validators',
            validators_file,
            '--chain-id',
            'foonet',
            '--epoch-length',
            '20',
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"amend-genesis" command failed: output: {e.stdout}')

    for d in dirs:
        copy_genesis(d)
    copy_genesis(target_dir)

    return [str(d) for d in dirs], target_dir


def init_mirror_dir(home, source_boot_node):
    mkdir_clean(dot_near() / MIRROR_DIR)
    os.rename(home, dot_near() / f'{MIRROR_DIR}/source')
    ordinal = NUM_VALIDATORS
    with open(dot_near() / f'{MIRROR_DIR}/source/config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = source_boot_node.addr_with_pk()
        config['network']['addr'] = ordinal_to_port(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_port(3030, ordinal)
    with open(dot_near() / f'{MIRROR_DIR}/source/config.json', 'w') as f:
        json.dump(config, f)


def mirror_cleanup(process):
    process.send_signal(signal.SIGINT)
    try:
        process.wait(5)
    except:
        process.kill()
        logger.error('can\'t kill mirror process')


def start_mirror(near_root, source_home, target_home, boot_node):
    env = os.environ.copy()
    env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
        "RUST_LOG", "debug")
    with open(dot_near() / f'{MIRROR_DIR}/stdout', 'ab') as stdout, \
        open(dot_near() / f'{MIRROR_DIR}/stderr', 'ab') as stderr:
        process = subprocess.Popen([
            os.path.join(near_root, 'neard'), 'mirror', 'run', "--source-home",
            source_home, "--target-home", target_home, '--secret-file',
            target_home / 'mirror-secret.json'
        ],
                                   stdin=subprocess.DEVNULL,
                                   stdout=stdout,
                                   stderr=stderr,
                                   env=env)
    logger.info("Started mirror process")
    atexit.register(mirror_cleanup, process)
    with open(target_home / 'config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = boot_node.addr_with_pk()
    with open(target_home / 'config.json', 'w') as f:
        json.dump(config, f)
    return process


# we'll test out adding an access key and then sending txs signed with it
# since that hits some codepaths we want to test
def send_add_access_key(node, creator_key, nonce, block_hash):
    k = key.Key.from_random('test0')
    action = transaction.create_full_access_key_action(k.decoded_pk())
    tx = transaction.sign_and_serialize_transaction('test0', nonce, [action],
                                                    block_hash, 'test0',
                                                    creator_key.decoded_pk(),
                                                    creator_key.decoded_sk())
    node.send_tx(tx)
    return k


def create_subaccount(node, signer_key, nonce, block_hash):
    k = key.Key.from_random('foo.' + signer_key.account_id)
    actions = []
    actions.append(transaction.create_create_account_action())
    actions.append(transaction.create_full_access_key_action(k.decoded_pk()))
    actions.append(transaction.create_payment_action(10**24))
    # add an extra one just to exercise some more corner cases
    actions.append(
        transaction.create_full_access_key_action(
            key.Key.from_random(k.account_id).decoded_pk()))

    tx = transaction.sign_and_serialize_transaction(k.account_id, nonce,
                                                    actions, block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    node.send_tx(tx)
    return k


# a key that we added with an AddKey tx or implicit account transfer.
# just for nonce handling convenience
class AddedKey:

    def __init__(self, key):
        self.nonce = None
        self.key = key

    def send_if_inited(self, node, transfers, block_hash):
        if self.nonce is None:
            self.nonce = node.get_nonce_for_pk(self.key.account_id, self.key.pk)

        if self.nonce is not None:
            for (receiver_id, amount) in transfers:
                self.nonce += 1
                tx = transaction.sign_payment_tx(self.key, receiver_id, amount,
                                                 self.nonce, block_hash)
                node.send_tx(tx)


class ImplicitAccount:

    def __init__(self):
        self.key = AddedKey(key.Key.implicit_account())

    def account_id(self):
        return self.key.key.account_id

    def transfer(self, node, sender_key, amount, block_hash, nonce):
        tx = transaction.sign_payment_tx(sender_key, self.account_id(), amount,
                                         nonce, block_hash)
        node.send_tx(tx)
        logger.info(
            f'sent {amount} to initialize implicit account {self.account_id()}')

    def send_if_inited(self, node, transfers, block_hash):
        self.key.send_if_inited(node, transfers, block_hash)


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


def check_num_txs(source_node, target_node, start_time, end_source_height):
    with open(os.path.join(target_node.node_dir, 'genesis.json'), 'r') as f:
        genesis_height = json.load(f)['genesis_height']
    with open(os.path.join(target_node.node_dir, 'config.json'), 'r') as f:
        delay = json.load(f)['consensus']['min_block_production_delay']
        block_delay = 10**9 * int(delay['secs']) + int(delay['nanos'])
        block_delay = block_delay / 10**9

    total_source_txs = count_total_txs(source_node, min_height=genesis_height)

    # start_time is the time the mirror binary was started. Give it 20 seconds to
    # sync and then 50% more than min_block_production_delay for each block between
    # the start and end points of the source chain. Not ideal to be basing a test on time
    # like this but there's no real strong guarantee on when the transactions should
    # make it on chain, so this is some kind of reasonable timeout

    total_time_allowed = 20 + (end_source_height -
                               genesis_height) * block_delay * 1.5
    time_elapsed = time.time() - start_time
    if time_elapsed < total_time_allowed:
        time_left = total_time_allowed - time_elapsed
        logger.info(
            f'waiting for {int(time_left)} seconds to allow transactions to make it to the target chain'
        )
        time.sleep(time_left)

    total_target_txs = count_total_txs(target_node)
    assert total_source_txs == total_target_txs, (total_source_txs,
                                                  total_target_txs)
    logger.info(f'all {total_source_txs} transactions mirrored')


def main():
    config_changes = {}
    for i in range(NUM_VALIDATORS + 1):
        config_changes[i] = {"tracked_shards": [0, 1, 2, 3], "archive": True}

    config = load_config()
    near_root, node_dirs = init_cluster(num_nodes=NUM_VALIDATORS,
                                        num_observers=1,
                                        num_shards=4,
                                        config=config,
                                        genesis_config_changes=[
                                            ["epoch_length", 10],
                                        ],
                                        client_config_changes=config_changes)

    nodes = [spin_up_node(config, near_root, node_dirs[0], 0)]

    init_mirror_dir(node_dirs[NUM_VALIDATORS], nodes[0])

    for i in range(1, NUM_VALIDATORS):
        nodes.append(
            spin_up_node(config, near_root, node_dirs[i], i,
                         boot_node=nodes[0]))

    ctx = utils.TxContext([i for i in range(len(nodes))], nodes)

    implicit_account1 = ImplicitAccount()
    for height, block_hash in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
        implicit_account1.transfer(nodes[0], nodes[0].signer_key, 10**24,
                                   base58.b58decode(block_hash.encode('utf8')),
                                   ctx.next_nonce)
        ctx.next_nonce += 1
        break

    for height, block_hash in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        implicit_account1.send_if_inited(nodes[0], [('test2', height),
                                                    ('test3', height)],
                                         block_hash_bytes)
        ctx.send_moar_txs(block_hash, 10, use_routing=False)

        if height > 12:
            break

    nodes[0].kill()
    target_node_dirs, target_observer_dir = create_forked_chain(
        config, near_root)
    nodes[0].start(boot_node=nodes[1])

    ordinal = NUM_VALIDATORS + 1
    target_nodes = [
        spin_up_node(config, near_root, target_node_dirs[0], ordinal)
    ]
    for i in range(1, len(target_node_dirs)):
        ordinal += 1
        target_nodes.append(
            spin_up_node(config,
                         near_root,
                         target_node_dirs[i],
                         ordinal,
                         boot_node=target_nodes[0]))

    p = start_mirror(near_root,
                     dot_near() / f'{MIRROR_DIR}/source/', target_observer_dir,
                     target_nodes[0])
    start_time = time.time()
    start_source_height = nodes[0].get_latest_block().height
    restarted = False

    subaccount_key = AddedKey(
        create_subaccount(nodes[0], nodes[0].signer_key, ctx.next_nonce,
                          block_hash_bytes))
    ctx.next_nonce += 1

    new_key = AddedKey(
        send_add_access_key(nodes[0], nodes[0].signer_key, ctx.next_nonce,
                            block_hash_bytes))
    ctx.next_nonce += 1

    implicit_account2 = ImplicitAccount()
    # here we are gonna send a tiny amount (1 yoctoNEAR) to the implicit account and
    # then wait a bit before properly initializing it. This hits a corner case where the
    # mirror binary needs to properly look for the second tx's outcome to find the starting
    # nonce because the first one failed
    implicit_account2.transfer(nodes[0], nodes[0].signer_key, 1,
                               block_hash_bytes, ctx.next_nonce)
    ctx.next_nonce += 1
    time.sleep(2)
    implicit_account2.transfer(nodes[0], nodes[0].signer_key, 10**24,
                               block_hash_bytes, ctx.next_nonce)
    ctx.next_nonce += 1

    for height, block_hash in utils.poll_blocks(nodes[0], timeout=TIMEOUT):
        code = p.poll()
        if code is not None:
            assert code == 0
            break

        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        ctx.send_moar_txs(block_hash, 10, use_routing=False)

        implicit_account1.send_if_inited(
            nodes[0], [('test2', height), ('test1', height),
                       (implicit_account2.account_id(), height)],
            block_hash_bytes)
        implicit_account2.send_if_inited(
            nodes[1], [('test2', height), ('test0', height),
                       (implicit_account1.account_id(), height)],
            block_hash_bytes)
        new_key.send_if_inited(nodes[2],
                               [('test1', height), ('test2', height),
                                (implicit_account1.account_id(), height),
                                (implicit_account2.account_id(), height)],
                               block_hash_bytes)
        subaccount_key.send_if_inited(
            nodes[3], [('test3', height),
                       (implicit_account2.account_id(), height)],
            block_hash_bytes)

        if not restarted and height - start_source_height >= 50:
            logger.info('stopping mirror process')
            p.terminate()
            p.wait()
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
            p = start_mirror(near_root,
                             dot_near() / f'{MIRROR_DIR}/source/',
                             target_observer_dir, target_nodes[0])
            restarted = True

        if height - start_source_height >= 100:
            break

    time.sleep(5)
    # we don't need these anymore
    for node in nodes[1:]:
        node.kill()
    check_num_txs(nodes[0], target_nodes[0], start_time, height)


if __name__ == '__main__':
    main()

import sys
import time
import atexit
import json
import os
import pathlib
import shutil
import signal
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import base58
from nacl.signing import SigningKey

from configured_logger import logger
import transaction
import utils
import key

MIRROR_DIR = 'test-mirror'
TIMEOUT = 240
TARGET_VALIDATORS = ['foo0', 'foo1', 'foo2', 'foo3']

CONTRACT_PATH = pathlib.Path(__file__).resolve().parents[
    0] / 'contract/target/wasm32-unknown-unknown/release/addkey_contract.wasm'


def dot_near():
    return pathlib.Path.home() / '.near'


def run_cmd(cmd):
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'running `{" ".join([str(a) for a in cmd])}` returned '
                 f'{e.returncode}. output:\n{e.output.decode("utf-8")}')


def copy_near_home(src, dst):
    """Copy a near home dir for use by a different node.

    Clears boot_nodes and generates a fresh node_key so the copy can run
    as an independent node.
    """
    src = pathlib.Path(src)
    dst = pathlib.Path(dst)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=False)

    with open(src / 'config.json') as f:
        config = json.load(f)
    config['network']['boot_nodes'] = ''
    with open(dst / 'config.json', 'w') as f:
        json.dump(config, f, indent=2)

    with open(dst / 'node_key.json', 'w') as f:
        json.dump(key.Key.from_random('node').to_json(), f, indent=2)

    shutil.copyfile(src / 'genesis.json', dst / 'genesis.json')
    shutil.copytree(src / 'data',
                    dst / 'data',
                    ignore=shutil.ignore_patterns('*snapshot*'))

    # Copy epoch_configs if present (created by fork-network set-validators)
    epoch_configs = src / 'epoch_configs'
    if epoch_configs.exists():
        shutil.copytree(epoch_configs, dst / 'epoch_configs')


def fork_network(neard, fork_dir, target_validators):
    """Run the full fork-network workflow on fork_dir.

    Steps: init -> amend-access-keys -> set-validators -> finalize.
    Returns list of validator Key objects for the target validators.
    """
    fork_dir = pathlib.Path(fork_dir)

    run_cmd([neard, '--home', str(fork_dir), 'fork-network', 'init'])
    run_cmd(
        [neard, '--home',
         str(fork_dir), 'fork-network', 'amend-access-keys'])

    # Remove snapshots created by fork-network (not needed and waste disk)
    for snap_dir in ['fork-snapshot', 'state-snapshot']:
        snap_path = fork_dir / 'data' / snap_dir
        if snap_path.exists():
            shutil.rmtree(snap_path)

    # Generate validator keys and write validators.json
    setup_dir = fork_dir / 'setup'
    os.makedirs(setup_dir, exist_ok=True)
    validators_file = setup_dir / 'validators.json'
    validators = []
    validator_keys = []
    for name in target_validators:
        k = key.Key.from_random(name)
        validator_keys.append(k)
        validators.append({
            'account_id': name,
            'public_key': k.pk,
            'amount': str(10**33),
        })
    with open(validators_file, 'w') as f:
        json.dump(validators, f, indent=2)

    # cspell:ignore foonet
    run_cmd([
        neard, '--home',
        str(fork_dir), 'fork-network', 'set-validators', '--validators',
        str(validators_file), '--chain-id', 'foonet', '--epoch-length', '100',
        '--num-seats',
        str(len(target_validators))
    ])
    run_cmd([neard, '--home', str(fork_dir), 'fork-network', 'finalize'])

    return validator_keys


# ── MirrorProcess ────────────────────────────────────────────────────────────


def _mirror_cleanup(mirror):
    if mirror.process.poll() is not None:
        return
    mirror.process.send_signal(signal.SIGINT)
    try:
        mirror.process.wait(5)
    except:
        mirror.process.kill()
        logger.error('can\'t kill mirror process')


class MirrorProcess:
    """Manages the `neard mirror run` subprocess.

    Uses --no-secret because fork-network uses insecure (identity) key mapping.
    Automatically restarts once after 30 seconds to test resume capability.
    """

    def __init__(self, near_root, source_home, binary_name='neard'):
        self.source_home = source_home
        self.neard = os.path.join(near_root, binary_name)
        self.start()
        self.start_time = time.time()
        self.restarted = False

    def start(self):
        env = os.environ.copy()
        env["RUST_LOG"] = "mio=warn,tokio_util=warn,indexer=info," + env.get(
            "RUST_LOG", "debug")
        config_path = dot_near() / f'{MIRROR_DIR}/config.json'
        with open(dot_near() / f'{MIRROR_DIR}/stdout', 'ab') as stdout, \
            open(dot_near() / f'{MIRROR_DIR}/stderr', 'ab') as stderr, \
            open(config_path, 'w') as mirror_config:
            json.dump({'tx_batch_interval': {
                'secs': 0,
                'nanos': 600000000
            }}, mirror_config)
            args = [
                self.neard,
                'mirror',
                'run',
                "--source-home",
                self.source_home,
                "--target-home",
                dot_near() / f'{MIRROR_DIR}/target/',
                '--no-secret',
                '--config-path',
                config_path,
            ]
            self.process = subprocess.Popen(args,
                                            stdin=subprocess.DEVNULL,
                                            stdout=stdout,
                                            stderr=stderr,
                                            env=env)
        logger.info("Started mirror process")
        if not hasattr(self, '_atexit_registered'):
            atexit.register(_mirror_cleanup, self)
            self._atexit_registered = True

    def restart(self):
        logger.info('stopping mirror process')
        self.process.terminate()
        self.process.wait()
        with open(dot_near() / f'{MIRROR_DIR}/stderr', 'ab') as stderr:
            stderr.write(
                b'<><><><><><><><><><><><> restarting <><><><><><><><><><><><><><><><><><><><>\n'
            )
        self.start()

    def restart_once(self):
        """Returns False when the mirror process exits (code 0). Restarts once after 30s."""
        code = self.process.poll()
        if code is not None:
            assert code == 0
            return False

        if not self.restarted and time.time() - self.start_time > 30:
            self.restart()
            self.restarted = True
        return True


# ── Transaction helpers ──────────────────────────────────────────────────────


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


def create_subaccount(node,
                      subaccount_name,
                      signer_key,
                      nonce,
                      block_hash,
                      extra_key=False):
    k = key.Key.from_random(subaccount_name + '.' + signer_key.account_id)
    actions = [
        transaction.create_create_account_action(),
        transaction.create_full_access_key_action(k.decoded_pk()),
        transaction.create_payment_action(10**29),
    ]
    if extra_key:
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


def call_create_account(node, signer_key, account_id, public_key, nonce,
                        block_hash):
    args = json.dumps({'account_id': account_id, 'public_key': public_key})
    args = bytearray(args, encoding='utf-8')

    actions = [
        transaction.create_function_call_action('create_account', args, 10**14,
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
        f'called create account contract for {account_id}, public key: {public_key}: {res}'
    )


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


def send_add_gas_key(node, signer_key, gas_key, num_nonces, nonce, block_hash):
    action = transaction.create_gas_key_full_access_key_action(
        gas_key.decoded_pk(), num_nonces)
    tx = transaction.sign_and_serialize_transaction(signer_key.account_id,
                                                    nonce, [action], block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent add gas key tx for {signer_key.account_id} {gas_key.pk} num_nonces={num_nonces}: {res}'
    )


def fund_gas_key(node, signer_key, gas_key_pk, amount, nonce, block_hash):
    action = transaction.create_transfer_to_gas_key_action(gas_key_pk, amount)
    tx = transaction.sign_and_serialize_transaction(signer_key.account_id,
                                                    nonce, [action], block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(f'sent fund gas key tx for {signer_key.account_id}: {res}')


def withdraw_from_gas_key(node, signer_key, gas_key_pk, amount, nonce,
                          block_hash):
    action = transaction.create_withdraw_from_gas_key_action(gas_key_pk, amount)
    tx = transaction.sign_and_serialize_transaction(signer_key.account_id,
                                                    nonce, [action], block_hash,
                                                    signer_key.account_id,
                                                    signer_key.decoded_pk(),
                                                    signer_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent withdraw from gas key tx for {signer_key.account_id}: {res}')


def send_gas_key_transfer(node, gas_key, receiver_id, amount, nonce,
                          nonce_index, block_hash):
    action = transaction.create_payment_action(amount)
    tx = transaction.sign_and_serialize_transaction_v1(receiver_id, nonce,
                                                       nonce_index, [action],
                                                       block_hash,
                                                       gas_key.account_id,
                                                       gas_key.decoded_pk(),
                                                       gas_key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent gas key V1 transfer from {gas_key.account_id} nonce_index={nonce_index} to {receiver_id}: {res}'
    )


def get_gas_key_nonces(node, account_id, public_key):
    """Query gas key nonces via RPC. Returns list of nonces or None on error."""
    try:
        res = node.json_rpc(
            'query', {
                'request_type': 'view_gas_key_nonces',
                'account_id': account_id,
                'public_key': public_key,
                'finality': 'final',
            })
    except Exception as e:
        logger.warning(
            f'get_gas_key_nonces failed for {account_id} {public_key}: {e}')
        return None
    if 'error' in res or 'error' in res.get('result', {}):
        return None
    return res['result'].get('nonces')


def get_gas_key_balance(node, account_id, public_key):
    """Query gas key balance via view_access_key RPC."""
    try:
        res = node.json_rpc(
            'query', {
                'request_type': 'view_access_key',
                'account_id': account_id,
                'public_key': public_key,
                'finality': 'final',
            })
    except Exception as e:
        logger.warning(
            f'get_gas_key_balance failed for {account_id} {public_key}: {e}')
        return None
    if 'error' in res or 'error' in res.get('result', {}):
        return None
    permission = res['result'].get('permission')
    if isinstance(permission, dict) and 'GasKeyFullAccess' in permission:
        return int(permission['GasKeyFullAccess']['balance'])
    return None


def contract_deployed(node, account_id):
    return 'error' not in node.json_rpc('query', {
        "request_type": "view_code",
        "account_id": account_id,
        "finality": "final"
    })


# ── Key / account helpers ───────────────────────────────────────────────────


class AddedKey:
    """Wraps a key that was added via AddKey tx or implicit account transfer.
    Handles lazy nonce initialization from the chain."""

    def __init__(self, key):
        self.nonce = None
        self.key = key

    def get_nonce(self, node):
        if self.nonce is not None:
            self.nonce += 1
            return self.nonce
        self.nonce = node.get_nonce_for_pk(self.key.account_id,
                                           self.key.pk,
                                           finality='final')
        if self.nonce is not None:
            logger.info(
                f'added key {self.key.account_id} {self.key.pk} inited @ {self.nonce}'
            )
            self.nonce += 1
        return self.nonce

    def send_if_inited(self, node, transfers, block_hash):
        for (receiver_id, amount) in transfers:
            nonce = self.get_nonce(node)
            if not nonce:
                return
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

    def transfer_to(self, node, sender_key, amount, block_hash, nonce):
        tx = transaction.sign_payment_tx(sender_key, self.account_id(), amount,
                                         nonce, block_hash)
        node.send_tx(tx)
        logger.info(
            f'sent {amount} to initialize implicit account {self.account_id()}')

    def send_if_inited(self, node, transfers, block_hash):
        self.key.send_if_inited(node, transfers, block_hash)

    def get_nonce(self, node):
        return self.key.get_nonce(node)

    def inited(self):
        return self.key.inited()

    def signer_key(self):
        return self.key.key


# With --no-secret, the mirror maps each ed25519 public key to a new keypair
# by using the original public key bytes as the signing key seed. Named
# accounts are not mapped; implicit account IDs (hex-encoded public keys) are.


def map_key_no_secret(pk_str):
    """Map a public key through the --no-secret mirror key mapping."""
    pk_bytes = base58.b58decode(pk_str.split(':')[1].encode('ascii'))
    mapped_pk = bytes(SigningKey(pk_bytes).verify_key)
    return 'ed25519:' + base58.b58encode(mapped_pk).decode('ascii')


def map_account_no_secret(account_id):
    """Map an implicit account ID through the --no-secret mirror key mapping."""
    if len(account_id) == 64:
        pk_bytes = bytes.fromhex(account_id)
        mapped_pk = bytes(SigningKey(pk_bytes).verify_key)
        return mapped_pk.hex()
    return account_id


# ── Validation helpers ───────────────────────────────────────────────────────


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

    # Give 20 seconds to sync, then 1.5x min_block_production_delay per block
    return 20 + (end_source_height - genesis_height) * block_delay * 1.5

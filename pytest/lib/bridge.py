import atexit
import base58
import base64
from collections import defaultdict
from enum import Enum
import json
import logging
from multiprocessing import Process, Value
from pathlib import Path
import random
from retrying import retry
import signal
import shutil
import string
import subprocess
import time
import traceback
import os

from key import Key
from messages.bridge import *
from serializer import BinarySerializer
from transaction import sign_function_call_tx, sign_create_account_with_full_access_key_and_balance_tx

logger = logging.getLogger('bridge')
log_handler = logging.StreamHandler()
log_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)-8s %(process)-6d %(funcName)-18s %(message)s', '%Y-%m-%d %H:%M:%S'))
# do not propagate bridge messages to the root logger
logger.propagate = False
logger.addHandler(log_handler)
logging.getLogger('bridge').setLevel(logging.INFO)

MAX_ATTEMPTS = 10

bridge_cluster_config_changes = {
    "consensus": {
        "min_block_production_delay": {
            "secs": 4,
            "nanos": 0,
        },
        "max_block_production_delay": {
            "secs": 8,
            "nanos": 0,
        },
        "max_block_wait_delay": {
            "secs": 24,
            "nanos": 0,
        },
    }
}

class JSAdapter:

    def __init__(self, config):
        self.config = config
        self.bridge_dir = self.config['bridge_dir']
        self.cli_dir = os.path.join(self.bridge_dir, 'cli')

    def js_call(self, args, timeout):
        logger.info(' '.join(args))
        try:
            res = subprocess.check_output(args, timeout=timeout, stderr=subprocess.STDOUT, cwd=self.cli_dir).decode('ascii').strip()
            return res
        except subprocess.CalledProcessError as e:
            logger.error('js call failed, exit code %d, msg: %s' % (e.returncode, e.output))
            assert False

    def js_call_cli(self, args, timeout=3):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js'] + args
        return self.js_call(args, timeout)

    def js_call_testing(self, args, timeout=3):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js', 'TESTING'] + args
        return self.js_call(args, timeout)

    def js_start_service(self, args, stdout, stderr):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js', 'start'] + args + ['--daemon', 'false']
        logger.info(' '.join(args))
        service = subprocess.Popen(args, stdout=stdout, stderr=stderr, cwd=self.cli_dir)
        # TODO ping and wait until service really starts
        time.sleep(5)
        return service


class BridgeUser(object):

    def __init__(self, name, eth_secret_key, near_sk, near_pk):
        self.name = name
        self.eth_secret_key = eth_secret_key
        self.near_account_name = name + '.test0'
        self.tokens_expected = dict()
        self.near_signer_key = Key(self.near_account_name, near_pk, near_sk)
        # Following fields will be initialized when Bridge starts
        self.eth_address = None


# BridgeUser related fields
#
# 10000000000000000000000000000 = 1000000000 tokens
billion_tokens = 10000000000000000000000000000
bridge_master_account = BridgeUser('bridge_master_account', '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200', "ed25519:LXTdSRpvkKzz44bxuW7H6sZJbKTCD42rABwAPhXGStJmNDZU22VmDp6QwqBvkb2MBq5sqZzUjD1CmWWJsagUib2", "ed25519:nANzoQA6Ms8KLr2Tz6e67SWQBr9zhF1wH6GuZrncfxC")
alice = BridgeUser('alice', '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201', "ed25519:44zgjKVsfQUdKZPx8QqBoiMWLXDUsnpKwW1j6DBtSQDfYBuZxgGZMWZbj7WfkA1sgncZahqx95GdvTJYg9EurkiK", "ed25519:GNs8KmGWwq1BUfXF8QMCGzBrxESkN7qCNd1Pw1Gdp2po")
bob = BridgeUser('bob', '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501202', "ed25519:2Hgngtr9BRTKuV7qwCpzBoHB5y9KCQZ6VLtLtM92rmDLUFZEPrPnU6BA4tGTt7Aog4ggF6diRU6rWWk7DM8S4V2g", "ed25519:BgxhfV9Ur7sacijNtcECXDFqDMvVfzGz8Psn7zATVQCY")
carol = BridgeUser('carol', '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501203', "ed25519:4N89ZhJFLMoijh4ShPoiHGndv61J1k5P75giW9gsDDsXKCcNEnwbNGmjZHWrrV1rb44Km7AUdowhhrSPt4fujXih", "ed25519:CDo1oshUCvobiaUFmHJYD4yBYDutwdQYxHAXLGRG5LXs")

class BridgeTxDirection(Enum):
    ETH2NEAR = 0,
    NEAR2ETH = 1

class BridgeTx(object):

    def __init__(self,
                 direction,
                 sender,
                 receiver,
                 amount,
                 token_name='erc20',
                 near_token_factory_id='neartokenfactory',
                 near_client_account_id='rainbow_bridge_eth_on_near_client',
                 near_token_account_id='7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory'):
        self.id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        self.direction = direction
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.token_name = token_name
        self.near_token_factory_id = near_token_factory_id
        self.near_client_account_id = near_client_account_id
        self.near_token_account_id = near_token_account_id
        # for custom fields
        self.custom = dict()

def atexit_cleanup(obj):
    print("Cleaning %s on script exit" % (obj.__class__.__name__))
    try:
        obj.cleanup()
    except BaseException:
        print("Cleaning failed!")
        traceback.print_exc()
        pass


class Cleanable(object):

    def __init__(self, config):
        self.cleaned = False
        self.pid = Value('i', 0)
        self.config = config
        self.bridge_dir = self.config['bridge_dir']
        self.config_dir = self.config['config_dir']
        self.adapter = JSAdapter(self.config)
        self.stdout = None
        self.stderr = None
        atexit.register(atexit_cleanup, self)

    def kill(self):
        if self.pid.value != 0:
            os.kill(self.pid.value, signal.SIGKILL)
            self.pid.value = 0

    def cleanup(self):
        if self.cleaned:
            return

        try:
            if self.stdout:
                self.stdout.close()
            if self.stderr:
                self.stderr.close()
            self.kill()
        except BaseException:
            print("Kill %s failed on cleanup!" % (self.__class__.__name__))
            traceback.print_exc()
            print("\n\n")

    def restart(self):
        assert not self.cleaned
        self.cleanup()
        assert self.pid.value == 0
        self.start()
        assert self.pid.value != 0
        self.cleaned = False


class GanacheNode(Cleanable):

    def __init__(self, config):
        super(GanacheNode, self).__init__(config)
        ganache_bin = os.path.join(self.bridge_dir, self.config['ganache_bin'])
        if not os.path.exists(ganache_bin):
            self.build()

    def build(self):
        ganache_dir = os.path.join(self.bridge_dir, self.config['ganache_dir'])
        assert_success(subprocess.check_output(['yarn'], cwd=ganache_dir))

    def start(self):
        ganache_bin = os.path.join(self.bridge_dir, self.config['ganache_bin'])
        self.stdout = open(
            os.path.join(
                self.config_dir,
                'logs/ganache/out.log'),
            'w')
        self.stderr = open(
            os.path.join(
                self.config_dir,
                'logs/ganache/err.log'),
            'w')
        # TODO set params
        self.pid.value = subprocess.Popen(
            [
                ganache_bin,
                '--port',
                '9545',
                '--blockTime',
                str(self.config['ganache_block_prod_time']),
                '--gasLimit',
                '10000000',
                '--account="%s,%d"' % (bridge_master_account.eth_secret_key, billion_tokens),
                '--account="%s,%d"' % (alice.eth_secret_key, billion_tokens),
                '--account="%s,%d"' % (bob.eth_secret_key, billion_tokens),
                '--account="%s,%d"' % (carol.eth_secret_key, billion_tokens)
            ],
            stdout=self.stdout,
            stderr=self.stderr).pid
        if self.pid.value == 0:
            logger.error('Cannot start ganache')
            assert False


class Near2EthBlockRelay(Cleanable):

    def __init__(self, config):
        super(Near2EthBlockRelay, self).__init__(config)

    def start(
            self,
            eth_master_secret_key=bridge_master_account.eth_secret_key):
        self.stdout = open(
            os.path.join(
                self.config_dir,
                'logs/near2eth-relay/out.log'),
            'a')
        self.stderr = open(
            os.path.join(
                self.config_dir,
                'logs/near2eth-relay/err.log'),
            'a')
        self.pid.value = self.adapter.js_start_service(['near2eth-relay', '--eth-master-sk', eth_master_secret_key], self.stdout, self.stderr).pid
        if self.pid.value == 0:
            logger.error('Cannot start Near2EthBlockRelay')
            assert False


class Eth2NearBlockRelay(Cleanable):

    def __init__(self, config):
        super(Eth2NearBlockRelay, self).__init__(config)

    def start(self):
        self.stdout = open(
            os.path.join(
                self.config_dir,
                'logs/eth2near-relay/out.log'),
            'a')
        self.stderr = open(
            os.path.join(
                self.config_dir,
                'logs/eth2near-relay/err.log'),
            'a')
        self.pid.value = self.adapter.js_start_service('eth2near-relay', self.stdout, self.stderr).pid
        if self.pid.value == 0:
            logger.error('Cannot start Eth2NearBlockRelay')
            assert False


# this function hides complex parsing of output of different services that are executed
def assert_success(message):
    try:
        message = message.decode('ascii')
    except:
        # message is already str
        pass
    if message == '':
        # empty message is considered valid
        return
    message = message.strip().split('\n')[-1].strip().split(' ')
    if not (message[0] in ('Deployed', 'Done') or message[-1] in ('deployed', 'initialized')):
        logger.error(message)
        assert False


class RainbowBridge:

    def __init__(self, config, node):
        self.config = config
        self.node = node
        self.eth2near_block_relay = None
        self.near2eth_block_relay = None
        self.adapter = JSAdapter(self.config)
        self.bridge_dir = self.config['bridge_dir']
        self.config_dir = self.config['config_dir']
        self.cli_dir = os.path.join(self.bridge_dir, 'cli')
        if not os.path.exists(self.bridge_dir):
            self.git_clone_install()
        if not os.path.exists(os.path.expanduser("~/go")):
            logger.error('Go must be installed')
            assert False
        # TODO resolve
        # This hack is for NayDuck
        os.system(
            'wget -q https://raw.githubusercontent.com/near/nearcore/6cb489aee566b85091339d90c40b7517bdfbd2f2/pytest/lib/bridge_helpers/write_config.js -P %s' %
            self.bridge_dir)

        if os.path.exists(self.config_dir):
            assert os.path.isdir(self.config_dir)
            shutil.rmtree(self.config_dir)
        logs_dir = os.path.join(self.config_dir, 'logs')
        os.makedirs(logs_dir)
        for service in [
            'ganache',
            'near2eth-relay',
            'eth2near-relay',
                'watchdog']:
            os.mkdir(os.path.join(logs_dir, service))
            (Path(logs_dir) / service / 'err.log').touch()
            (Path(logs_dir) / service / 'out.log').touch()

        logger.info('Running write_config.js...')
        assert_success(subprocess.check_output(['node', 'write_config.js'], cwd=self.bridge_dir))
        logger.info('Setting up users\' addresses...')
        for user in [bridge_master_account, alice, bob, carol]:
            user.eth_address = self.get_eth_address(user)
            self.create_account(user, node)
        logger.info('Bridge is started')

    @retry(wait_exponential_multiplier=1.2, wait_exponential_max=10000)
    def create_account(self, user, node):
        status = node.get_status(check_storage=False)
        h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
        nonce = node.get_nonce_for_pk(node.signer_key.account_id, node.signer_key.pk)
        create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
                node.signer_key,
                user.near_account_name,
                user.near_signer_key,
                billion_tokens // 10,
                nonce + 1,
                h)
        res = node.send_tx(create_account_tx)['result']
        while True:
            try:
                created = node.get_tx(res, user.near_account_name)
                if 'status' in created['result']:
                    # tx is accepted by network
                    break
            except:
                time.sleep(1)
        # check the result of tx execution
        assert_success(created['result']['status']['SuccessValue'])

    def git_clone_install(self):
        logger.info('No rainbow-bridge repo found, cloning...')
        args = ('git clone --recurse-submodules %s %s' %
                (self.config['bridge_repo'], self.bridge_dir)).split()
        assert subprocess.check_output(args).decode('ascii').strip(
        ) == "Submodule path 'eth2near/ethashproof': checked out 'b7e7e22979a9b25043b649c22e41cb149267fbeb'"
        assert_success(subprocess.check_output(['yarn'], cwd=self.bridge_dir))
        ethash_dir = os.path.join(self.bridge_dir, 'eth2near/ethashproof')
        assert_success(subprocess.check_output(['/bin/sh', 'build.sh'], cwd=ethash_dir))
        # Make sure that ethash is assembled
        assert os.path.exists(os.path.join(ethash_dir, 'cmd/relayer/relayer'))

    def init_near_contracts(self):
        assert_success(self.adapter.js_call_cli('init-near-contracts', timeout=60))

    def init_eth_contracts(self):
        assert_success(self.adapter.js_call_cli('init-eth-ed25519', timeout=60))
        assert_success(self.adapter.js_call_cli([
            'init-eth-client',
            '--eth-client-lock-eth-amount',
            '1000000000000000000',
            '--eth-client-lock-duration',
            '30'], timeout=60))
        assert_success(self.adapter.js_call_cli('init-eth-prover', timeout=60))
        assert_success(self.adapter.js_call_cli('init-eth-erc20', timeout=60))
        assert_success(self.adapter.js_call_cli('init-eth-locker', timeout=60))

    @retry(stop_max_attempt_number=10, wait_fixed=1000)
    def init_near_token_factory(self):
        assert_success(self.adapter.js_call_cli('init-near-token-factory', timeout=60))

    def start_eth2near_block_relay(self):
        self.eth2near_block_relay = Eth2NearBlockRelay(self.config)
        self.eth2near_block_relay.start()

    def start_near2eth_block_relay(self):
        self.near2eth_block_relay = Near2EthBlockRelay(self.config)
        self.near2eth_block_relay.start()

    def update_expected_balances(self, sender, receiver, send_token_name, receive_token_name, amount):
        # The only known reason of not updating is insufficient balance of sender
        if sender.tokens_expected[send_token_name] >= amount:
            sender.tokens_expected[send_token_name] -= amount
            if receive_token_name not in receiver.tokens_expected:
                receiver.tokens_expected[receive_token_name] = 0
            receiver.tokens_expected[receive_token_name] += amount

    def transfer_tx(self, tx, withdraw_func=None, get_proof_func=None, wait_func=None, deposit_func=None, node=None):
        if not node:
            node = self.node

        if tx.direction == BridgeTxDirection.ETH2NEAR:
            if not withdraw_func:
                withdraw_func = eth2near_withdraw
            if not get_proof_func:
                get_proof_func = eth2near_get_proof
            if not wait_func:
                wait_func = eth2near_wait
            if not deposit_func:
                deposit_func = eth2near_deposit
            self.update_expected_balances(tx.sender, tx.receiver, tx.token_name, 'near-' + tx.token_name, tx.amount)
        else:
            assert tx.direction == BridgeTxDirection.NEAR2ETH
            if not withdraw_func:
                withdraw_func = near2eth_withdraw
            if not get_proof_func:
                get_proof_func = near2eth_get_proof
            if not wait_func:
                wait_func = near2eth_wait
            if not deposit_func:
                deposit_func = near2eth_deposit
            self.update_expected_balances(tx.sender, tx.receiver, 'near-' + tx.token_name, tx.token_name, tx.amount)

        p = Process(target=transfer_routine, args=(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node, self.adapter))
        p.start()

        return p

    def transfer_eth2near(self, sender, amount, receiver=None, token_name='erc20', near_token_factory_id='neartokenfactory', near_client_account_id='rainbow_bridge_eth_on_near_client', near_token_account_id='7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory', withdraw_func=None, get_proof_func=None, wait_func=None, deposit_func=None, node=None):
        if not receiver:
            receiver = sender
        assert sender.eth_address
        tx = BridgeTx(BridgeTxDirection.ETH2NEAR,
                      sender,
                      receiver,
                      amount,
                      token_name,
                      near_token_factory_id,
                      near_client_account_id,
                      near_token_account_id)
        return self.transfer_tx(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node)

    def transfer_near2eth(self, sender, amount, receiver=None, token_name='erc20', near_token_factory_id='neartokenfactory', near_client_account_id='rainbow_bridge_eth_on_near_client', near_token_account_id='7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory', withdraw_func=None, get_proof_func=None, wait_func=None, deposit_func=None, node=None):
        if not receiver:
            receiver = sender
        assert receiver.eth_address
        tx = BridgeTx(BridgeTxDirection.NEAR2ETH,
                      sender,
                      receiver,
                      amount,
                      token_name,
                      near_token_factory_id,
                      near_client_account_id,
                      near_token_account_id)
        return self.transfer_tx(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node)

    def mint_erc20_tokens(self, user, amount, token_name='erc20'):
        assert user.eth_address
        # js parses 0x as number, not as string
        res = self.adapter.js_call_testing(['mint-erc20-tokens', user.eth_address[2:], str(amount), token_name], timeout=15)
        res = res.strip().split('\n')
        if res[-1] != 'OK':
            logger.error('Mint failed, %s' % (res))
            assert False
        user.tokens_expected.setdefault(token_name, amount)

    @retry(wait_exponential_multiplier=1.2, wait_exponential_max=10000)
    def get_eth_address(self, user):
        if not user.eth_address:
            # js parses 0x as number, not as string
            user.eth_address = self.adapter.js_call_testing(['get-account-address', user.eth_secret_key[2:]])
        return user.eth_address

    @retry(wait_exponential_multiplier=1.2, wait_exponential_max=10000)
    def get_eth_balance(self, user, token_name='erc20'):
        assert user.eth_address
        # js parses 0x as number, not as string
        return int(self.adapter.js_call_testing(['get-erc20-balance', user.eth_address[2:], token_name]))

    @retry(wait_exponential_multiplier=1.2, wait_exponential_max=10000)
    def get_near_balance(self, user, token_account_id=None, node=None):
        if not node:
            node = self.node
        if not token_account_id:
            # use default token_account
            token_account_id = '7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory'
        res = node.call_function(
            token_account_id,
            'get_balance',
            base64.b64encode(
                bytes(
                    '{"owner_id": "' + user.near_account_name + '"}',
                    encoding='utf8')).decode("ascii"),
            timeout=10)
        res = int("".join(map(chr, res['result']['result']))[1:-1])
        return res

    @retry(wait_fixed=10000)
    def check_balances(self, user):
        for (token_name, amount) in user.tokens_expected.items():
            if token_name.startswith('near-'):
                # This is NEAR side token
                # TODO use token_account_id
                actual_amount = self.get_near_balance(user)
            else:
                # This is ETH side token
                actual_amount = self.get_eth_balance(user, token_name)
            logger.info('%s\'s expected amount of %s token is %d, actual amount is %d' % (user.name, token_name, amount, actual_amount))
            assert actual_amount == amount


def retry_func(attempt, delay):
    logger.setLevel(logging.INFO)
    logging.getLogger('bridge').setLevel(logging.INFO)
    logger.info('ATTEMPT %d FAILED, RETRYING' % (attempt))
    if attempt + 1 == MAX_ATTEMPTS:
        logger.warning('LAST ATTEMPT! SETTING DEBUG LOGGING LEVEL')
        logging.getLogger('bridge').setLevel(logging.DEBUG)
    return delay

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def eth2near_withdraw(tx, node, adapter):
    logger.info('TX %s, WITHDRAW PHASE' % (tx.id))

    approve = adapter.js_call_testing(['eth-to-near-approve', tx.sender.eth_address[2:], str(tx.amount), tx.token_name], timeout=15)
    logger.debug('APPROVE: %s' % (approve))
    approve = approve.strip().split('\n')
    assert approve[-1] == 'OK'
    # This tx is critical.
    # Executing it successfully without getting proper answer will lead any test to failing.
    locker = adapter.js_call_testing(['eth-to-near-lock', tx.sender.eth_address[2:], tx.receiver.near_account_name, str(tx.amount), tx.token_name], timeout=45)
    logger.debug('LOCKER: %s' % (locker))
    locker = locker.strip().split('\n')
    assert locker[-2] == 'OK'
    return locker[-1]

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def eth2near_get_proof(tx, withdraw_ticket, node, adapter):
    logger.info('TX %s, GETTING PROOF PHASE' % (tx.id))

    event = adapter.js_call_cli(['eth-to-near-find-proof', withdraw_ticket], timeout=15)
    logger.debug('EVENT: %s' % (event))
    event = event.strip().split('\n')
    event_parsed = json.loads(event[-1])
    logger.debug('EVENT PARSED: %s' % (event_parsed))
    return event_parsed

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def eth2near_wait(tx, proof_ticket, node, adapter):
    logger.info('TX %s, WAITING PHASE' % (tx.id))

    block_number = proof_ticket['block_number']
    logger.debug('BLOCK NUMBER: %s' % (str(block_number)))
    serializer = BinarySerializer(None)
    serializer.serialize_field(block_number, 'u64')
    logger.debug('BLOCK NUMBER SERIALIZED: %s' % (base64.b64encode(bytes(serializer.array)).decode("ascii")))
    while True: 
        res = node.call_function(
            tx.near_client_account_id,
            'block_hash_safe',
            base64.b64encode(bytes(serializer.array)).decode("ascii"),
            timeout=15)
        logger.info('BLOCK HASH SAFE CALL: %s' % (res))
        if 'result' in res:
            if res['result']['result'] == [0]:
                time.sleep(15 + random.randrange(10))
            else:
                return proof_ticket

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def eth2near_deposit(tx, wait_ticket, node, adapter):
    logger.info('TX %s, DEPOSIT PHASE' % (tx.id))

    proof_parsed = wait_ticket['proof_locker']
    logger.debug('PROOF PARSED: %s' % (proof_parsed))
    serializer = BinarySerializer(dict(bridge_schema))
    proof = Proof()
    proof.log_index = proof_parsed['log_index']
    proof.log_entry_data = proof_parsed['log_entry_data']
    proof.receipt_index = proof_parsed['receipt_index']
    proof.receipt_data = proof_parsed['receipt_data']
    proof.header_data = proof_parsed['header_data']
    proof.proof = proof_parsed['proof']
    proof_ser = serializer.serialize(proof)
    logger.debug('PROOF SERIALIZED: %s' % (base64.b64encode(bytes(proof_ser)).decode("ascii")))
    status = node.get_status(check_storage=False)
    logger.debug('STATUS: %s' % (status))
    h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
    logger.debug('HASH: %s %s' % (str(status['sync_info']['latest_block_hash']), str(h)))
    nonce = node.get_nonce_for_pk(tx.sender.near_account_name, tx.sender.near_signer_key.pk)
    logger.debug('NONCE: %s' % (nonce))
    deposit_tx = sign_function_call_tx(
        tx.sender.near_signer_key,
        tx.near_token_factory_id,
        'deposit',
        bytes(proof_ser),
        300000000000000,
        100000000000000000000*600,
        nonce + 1,
        h)
    logger.debug('DEPOSIT_TX: %s' % (deposit_tx))
    res = node.send_tx(deposit_tx)
    logger.debug('DEPOSIT_TX RES: %s' % (res))
    res = res['result']
    deposited = None
    for _ in range(5):
        try:
            deposited = node.get_tx(res, tx.receiver.near_account_name)
            logger.info('DEPOSIT_TX GET_TX RES: %s' % (deposited))
            assert_success(deposited['result']['status']['SuccessValue'])
            logger.info('TX %s, TRANSFER SUCCESSFUL' % (tx.id))
            return
        except:
            time.sleep(10)
    logger.info('CANNOT GET DEPOSIT TX %s' % (deposited))
    assert False

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def near2eth_withdraw(tx, node, adapter):
    logger.info('TX %s, WITHDRAW PHASE' % (tx.id))

    status = node.get_status(check_storage=False)
    logger.debug('STATUS: %s' % (status))
    h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
    logger.debug('HASH: %s %s' % (str(status['sync_info']['latest_block_hash']), str(h)))
    nonce = node.get_nonce_for_pk(tx.sender.near_account_name, tx.sender.near_signer_key.pk)
    logger.debug('NONCE: %d' % (nonce))
    receiver_address = tx.receiver.eth_address
    if receiver_address.startswith('0x'):
        receiver_address = receiver_address[2:]
    logger.debug('RECEIVER: %s' % (receiver_address))

    # This is important in case of having identical txs that are sent simultaneously.
    # Setting different nonce helps to distinguish such txs.
    nonce_increment = os.getpid() % 1000 + 1

    withdraw_tx = sign_function_call_tx(
        tx.sender.near_signer_key,
        tx.near_token_account_id,
        'withdraw',
        bytes('{"amount": "' + str(tx.amount) + '", "recipient": "' + receiver_address + '"}', encoding='utf8'),
        300000000000000,
        0,
        nonce + nonce_increment,
        h)
    logger.debug('WITHDRAW_TX: %s' % (withdraw_tx))
    withdraw = node.send_tx(withdraw_tx)
    logger.debug('WITHDRAW_TX SEND: %s' % (withdraw))
    return withdraw['result']

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def near2eth_get_proof(tx, withdraw_ticket, node, adapter):
    logger.info('TX %s, GETTING PROOF PHASE' % (tx.id))

    while True:
        try:
            withdraw_res = node.get_tx(withdraw_ticket, tx.near_token_account_id)['result']
            logger.debug('WITHDRAW_TX RES: %s' % (str(withdraw_res)))
            break
        except:
            time.sleep(10)
    receipts = withdraw_res['transaction_outcome']['outcome']['receipt_ids']
    logger.debug('RECEIPTS: %s' % (str(receipts)))
    assert len(receipts) == 1
    withdraw_receipt_id = receipts[0]
    outcomes = withdraw_res['receipts_outcome']
    logger.debug('OUTCOMES: %s' % (str(outcomes)))
    receipt_id = None
    for outcome in outcomes:
        if outcome['id'] == withdraw_receipt_id:
            receipt_id = outcome['outcome']['status']['SuccessReceiptId']
            block_hash = outcome['block_hash']
            return (receipt_id, block_hash)
    logger.debug('NO PROOF FOUND')
    
@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def near2eth_wait(tx, proof_ticket, node, adapter):
    logger.info('TX %s, WAITING PHASE' % (tx.id))

    (receipt_id, block_hash) = proof_ticket
    block = node.json_rpc('block', [block_hash], timeout=3)
    logger.debug('BLOCK: %s' % (str(block)))
    height = block['result']['header']['height']
    logger.debug('REQUIRED HEIGHT: %d' % (height))
    while True:
        final_block = node.json_rpc('block', {'finality': 'final'}, timeout=3)
        logger.debug('FINAL BLOCK: %s' % (final_block))
        final_block_height = final_block['result']['header']['height']
        logger.debug('CURRENT HEIGHT: %d' % (final_block_height))
        if final_block_height > height:
            height = final_block_height
            break
        time.sleep(3)
    while True:
        [eth_client_block_height, eth_client_block_hash] = adapter.js_call_testing(['get-client-block-height-hash']).strip().split()
        eth_client_block_height = int(eth_client_block_height)
        logger.info('ETH BLOCK HEIGHT: %d, HASH: %s' % (eth_client_block_height, str(eth_client_block_hash)))
        if eth_client_block_height > height:
            return (receipt_id, eth_client_block_hash, eth_client_block_height)
        time.sleep(15 + random.randrange(10))

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500, wait_func=retry_func)
def near2eth_deposit(tx, wait_ticket, node, adapter):
    logger.info('TX %s, DEPOSIT PHASE' % (tx.id))

    (receipt_id, eth_client_block_hash, eth_client_block_height) = wait_ticket
    logger.debug('receipt_id: %s, eth_client_block_hash: %s, eth_client_block_height: %d' % (receipt_id, eth_client_block_hash, eth_client_block_height))
    proof = node.json_rpc('light_client_proof', {"type": "receipt", "receipt_id": receipt_id, "receiver_id": tx.sender.near_account_name, "light_client_head": eth_client_block_hash}, timeout=15)
    logger.debug('PROOF: %s' % (proof))
    light_client_proof = json.dumps(proof['result'], separators=(',', ':'))
    logger.debug('PROOF TO SEND: %s' % (proof))
    unlock = adapter.js_call_testing(['near-to-eth-unlock', str(eth_client_block_height), light_client_proof], timeout=45)
    logger.debug('UNLOCK: %s' % (unlock))
    unlock = unlock.strip().split('\n')
    assert unlock[-1] == 'OK'
    logger.info('TX %s, TRANSFER SUCCESSFUL' % (tx.id))

def transfer_routine(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node, adapter):
    assert tx.sender.eth_address
    logger.info('Tranferring tokens from %s to %s, amount %d, token name %s, direction %s' % (tx.sender.name, tx. receiver.name, tx.amount, tx.token_name, tx.direction))

    withdraw_ticket = withdraw_func(tx, node, adapter)
    proof_ticket = get_proof_func(tx, withdraw_ticket, node, adapter)
    wait_ticket = wait_func(tx, proof_ticket, node, adapter)
    deposit_func(tx, wait_ticket, node, adapter)

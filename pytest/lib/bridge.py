import atexit
import base58
import base64
from enum import Enum
import json
import multiprocessing
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

MAX_ATTEMPTS = 5

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

    def call(self, args, timeout):
        print('JS call:', ' '.join(args))
        # TODO check for errors
        return subprocess.check_output(
            args, timeout=timeout, cwd=self.cli_dir).decode('ascii').strip()

    def call_cli(self, args, timeout=3):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js'] + args
        return self.call(args, timeout)

    def call_testing(self, args, timeout=3):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js', 'TESTING'] + args
        return self.call(args, timeout)

    def call_service(self, args, stdout, stderr):
        if not isinstance(args, list):
            args = [args]
        args = ['node', 'index.js', 'start'] + args + ['--daemon', 'false']
        print('JS process start:', ' '.join(args))
        service = subprocess.Popen(args, stdout=stdout, stderr=stderr, cwd=self.cli_dir)
        # TODO ping and wait until service really starts
        time.sleep(5)
        return service


class BridgeUser(object):

    def __init__(self, name, eth_secret_key, near_sk, near_pk):
        self.name = name
        self.eth_secret_key = eth_secret_key
        self.near_account_name = name + '.test0'
        self.near_sk = near_sk
        self.near_pk = near_pk
        # Following fields will be initialized when Bridge starts
        self.eth_address = None
        self.near_signer_key = None


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
        self.deposit_attempts = 0
        self.get_proof_attempts = 0
        self.wait_attempts = 0
        self.withdraw_attempts = 0

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
        self.pid = multiprocessing.Value('i', 0)
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
        assert subprocess.check_output(['yarn'], cwd=ganache_dir).decode(
            'ascii').strip().split('\n')[-1].strip().split(' ')[0] == 'Done'

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
        # TODO use blockTime
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
        assert self.pid.value != 0


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
        self.pid.value = self.adapter.call_service(['near2eth-relay', '--eth-master-sk', eth_master_secret_key], self.stdout, self.stderr).pid
        assert self.pid.value != 0


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
        self.pid.value = self.adapter.call_service('eth2near-relay', self.stdout, self.stderr).pid
        assert self.pid.value != 0


def assert_success(message):
    message = message.strip().split('\n')[-1].strip().split(' ')
    assert message[0] == 'Deployed' or message[-1] in ('deployed', 'initialized')


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
            print('Go must be installed')
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
            Path(
                os.path.join(
                    os.path.join(
                        logs_dir,
                        service),
                    'err.log')).touch()
            Path(
                os.path.join(
                    os.path.join(
                        logs_dir,
                        service),
                    'out.log')).touch()

        print('Running write_config.js...')
        assert subprocess.check_output(
            ['node', 'write_config.js'], cwd=self.bridge_dir) == b''
        print('Setting up users\' addresses...')
        for user in [bridge_master_account, alice, bob, carol]:
            user.eth_address = self.get_eth_address(user)
            user.near_signer_key = Key(user.near_account_name, user.near_pk, user.near_sk)
            self.create_account(user, node)

    @retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=250)
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
        res = node.send_tx_and_wait(create_account_tx, 15)
        assert res['result']['status']['SuccessValue'] == ''

    def git_clone_install(self):
        print('No rainbow-bridge repo found, cloning...')
        args = ('git clone --recurse-submodules %s %s' %
                (self.config['bridge_repo'], self.bridge_dir)).split()
        assert subprocess.check_output(args).decode('ascii').strip(
        ) == "Submodule path 'eth2near/ethashproof': checked out 'b7e7e22979a9b25043b649c22e41cb149267fbeb'"
        print('cwd', self.bridge_dir)
        exit_code = os.system('echo yarn --version; yarn --version')
        print('yarn --version exit code', exit_code)
        exit_code = os.system('echo node --version; node --version')
        print('node --version exit code', exit_code)
        os.system('echo which node; which node')
        assert subprocess.check_output(['yarn'], cwd=self.bridge_dir).decode(
            'ascii').strip().split('\n')[-1].strip().split(' ')[0] == 'Done'
        ethash_dir = os.path.join(self.bridge_dir, 'eth2near/ethashproof')
        assert subprocess.check_output(
            ['/bin/sh', 'build.sh'], cwd=ethash_dir) == b''
        # Make sure that ethash is assembled
        assert os.path.exists(os.path.join(ethash_dir, 'cmd/relayer/relayer'))

    def init_near_contracts(self):
        assert_success(self.adapter.call_cli('init-near-contracts', timeout=60))

    def init_eth_contracts(self):
        assert_success(self.adapter.call_cli('init-eth-ed25519', timeout=60))
        assert_success(self.adapter.call_cli([
            'init-eth-client',
            '--eth-client-lock-eth-amount',
            '1000000000000000000',
            '--eth-client-lock-duration',
            '30'], timeout=60))
        assert_success(self.adapter.call_cli('init-eth-prover', timeout=60))
        assert_success(self.adapter.call_cli('init-eth-erc20', timeout=60))
        assert_success(self.adapter.call_cli('init-eth-locker', timeout=60))

    def init_near_token_factory(self):
        assert_success(self.adapter.call_cli('init-near-token-factory', timeout=60))

    def start_eth2near_block_relay(self):
        self.eth2near_block_relay = Eth2NearBlockRelay(self.config)
        self.eth2near_block_relay.start()

    def start_near2eth_block_relay(self):
        self.near2eth_block_relay = Near2EthBlockRelay(self.config)
        self.near2eth_block_relay.start()

    def transfer_eth2near(self, sender, amount, receiver=None, token_name=None, near_token_factory_id=None, near_client_account_id=None, near_token_account_id=None, withdraw_func=None, get_proof_func=None, wait_func=None, deposit_func=None, node=None):
        if not node:
            node = self.node
        if not receiver:
            receiver = sender
        if not withdraw_func:
            withdraw_func = eth2near_withdraw
        if not get_proof_func:
            get_proof_func = eth2near_get_proof
        if not wait_func:
            wait_func = eth2near_wait
        if not deposit_func:
            deposit_func = eth2near_deposit
        assert sender.eth_address
        tx = BridgeTx(BridgeTxDirection.ETH2NEAR,
                      sender,
                      receiver,
                      amount)
        if token_name:
            tx.token_name = token_name
        if near_token_factory_id:
            tx.near_token_factory_id = near_token_factory_id
        if near_client_account_id:
            tx.near_client_account_id = near_client_account_id
        if near_token_account_id:
            tx.near_token_account_id = near_token_account_id
        p = multiprocessing.Process(target=transfer_routine, args=(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node, self.adapter))
        p.start()
        return p


    def transfer_near2eth(self, sender, amount, receiver=None, token_name=None, near_token_factory_id=None, near_client_account_id=None, near_token_account_id=None, withdraw_func=None, get_proof_func=None, wait_func=None, deposit_func=None, node=None):
        if not node:
            node = self.node
        if not receiver:
            receiver = sender
        if not withdraw_func:
            withdraw_func = near2eth_withdraw
        if not get_proof_func:
            get_proof_func = near2eth_get_proof
        if not wait_func:
            wait_func = near2eth_wait
        if not deposit_func:
            deposit_func = near2eth_deposit
        assert receiver.eth_address
        tx = BridgeTx(BridgeTxDirection.NEAR2ETH,
                      sender,
                      receiver,
                      amount)
        if token_name:
            tx.token_name = token_name
        if near_token_factory_id:
            tx.near_token_factory_id = near_token_factory_id
        if near_client_account_id:
            tx.near_client_account_id = near_client_account_id
        if near_token_account_id:
            tx.near_token_account_id = near_token_account_id
        p = multiprocessing.Process(target=transfer_routine, args=(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node, self.adapter))
        p.start()
        return p

    def mint_erc20_tokens(self, user, amount, token_name='erc20'):
        assert user.eth_address
        # js parses 0x as number, not as string
        return self.adapter.call_testing(['mint-erc20-tokens', user.eth_address[2:], str(amount), token_name], timeout=15)

    @retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=250)
    def get_eth_address(self, user):
        if not user.eth_address:
            # js parses 0x as number, not as string
            user.eth_address = self.adapter.call_testing(['get-account-address', user.eth_secret_key[2:]])
        return user.eth_address

    @retry(stop_max_attempt_number=3, wait_random_min=100, wait_random_max=250)
    def get_eth_balance(self, user, token_name='erc20'):
        assert user.eth_address
        # js parses 0x as number, not as string
        return int(self.adapter.call_testing(['get-erc20-balance', user.eth_address[2:], token_name]))

    @retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=250)
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
            timeout=2)
        res = int("".join(map(chr, res['result']['result']))[1:-1])
        return res


@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def eth2near_withdraw(tx, node, adapter):
    tx.withdraw_attempts += 1
    print(tx.id, 'WITHDRAW PHASE, ATTEMPT', tx.withdraw_attempts)
    verbose = False
    if tx.withdraw_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    approve = adapter.call_testing(['eth-to-near-approve', tx.sender.eth_address[2:], str(tx.amount), tx.token_name], timeout=15)
    if verbose:
        print('APPROVE:', approve)
    assert approve == 'OK'
    # This tx is critical.
    # Executing it successfully without getting proper answer will lead any test to failing.
    locker = adapter.call_testing(['eth-to-near-lock', tx.sender.eth_address[2:], tx.receiver.near_account_name, str(tx.amount), tx.token_name], timeout=45)
    if verbose:
        print('LOCKER:', locker)
    locker = locker.split('\n')
    assert locker[0] == 'OK'
    return locker[1]

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def eth2near_get_proof(tx, withdraw_ticket, node, adapter):
    tx.get_proof_attempts += 1
    print(tx.id, 'GETTING PROOF PHASE, ATTEMPT', tx.get_proof_attempts)
    verbose = False
    if tx.get_proof_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    event = adapter.call_cli(['eth-to-near-find-proof', withdraw_ticket], timeout=15)
    if verbose:
        print('EVENT:', event)
    event = event.split('\n')
    assert event[0] != 'Failed'
    event_parsed = json.loads(event[0])
    if verbose:
        print('EVENT PARSED:', event_parsed)
    return event_parsed

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def eth2near_wait(tx, proof_ticket, node, adapter):
    tx.wait_attempts += 1
    print(tx.id, 'WAITING PHASE, ATTEMPT', tx.wait_attempts)
    verbose = False
    if tx.wait_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    block_number = proof_ticket['block_number']
    if verbose:
        print('BLOCK NUMBER:', block_number)
    serializer = BinarySerializer(None)
    serializer.serialize_field(block_number, 'u64')
    if verbose:
        print('BLOCK NUMBER SERIALIZED:', base64.b64encode(bytes(serializer.array)).decode("ascii"))
    while True: 
        res = node.call_function(
            tx.near_client_account_id,
            'block_hash_safe',
            base64.b64encode(bytes(serializer.array)).decode("ascii"),
            timeout=15)
        if verbose:
            print('BLOCK HASH SAFE CALL:', res)
        if 'result' in res:
            if res['result']['result'] == [0]:
                time.sleep(3)
            else:
                return proof_ticket

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def eth2near_deposit(tx, wait_ticket, node, adapter):
    tx.deposit_attempts += 1
    print(tx.id, 'DEPOSIT PHASE, ATTEMPT', tx.deposit_attempts)
    verbose = False
    if tx.deposit_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    proof_parsed = wait_ticket['proof_locker']
    if verbose:
        print('PROOF PARSED:', proof_parsed)
    serializer = BinarySerializer(dict(bridge_schema))
    proof = Proof()
    proof.log_index = proof_parsed['log_index']
    proof.log_entry_data = proof_parsed['log_entry_data']
    proof.receipt_index = proof_parsed['receipt_index']
    proof.receipt_data = proof_parsed['receipt_data']
    proof.header_data = proof_parsed['header_data']
    proof.proof = proof_parsed['proof']
    proof_ser = serializer.serialize(proof)
    if verbose:
        print('PROOF SERIALIZED:', base64.b64encode(bytes(proof_ser)).decode("ascii"))
    status = node.get_status(check_storage=False)
    if verbose:
        print('STATUS:', status)
    h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
    if verbose:
        print('HASH:', status['sync_info']['latest_block_hash'], h)
    nonce = node.get_nonce_for_pk(tx.sender.near_account_name, tx.sender.near_signer_key.pk)
    if verbose:
        print('NONCE:', nonce)
    deposit_tx = sign_function_call_tx(
        tx.sender.near_signer_key,
        tx.near_token_factory_id,
        'deposit',
        bytes(proof_ser),
        300000000000000,
        100000000000000000000*600,
        nonce + 1,
        h)
    if verbose:
        print('DEPOSIT_TX:', deposit_tx)
    res = node.send_tx_and_wait(deposit_tx, 45)
    if verbose:
        print('DEPOSIT_TX RES:', res)
    assert res['result']['status']['SuccessValue'] == ''

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def near2eth_withdraw(tx, node, adapter):
    tx.withdraw_attempts += 1
    print(tx.id, 'WITHDRAW PHASE, ATTEMPT', tx.withdraw_attempts)
    verbose = False
    if tx.withdraw_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    status = node.get_status(check_storage=False)
    if verbose:
        print('STATUS:', status)
    h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
    if verbose:
        print('HASH:', status['sync_info']['latest_block_hash'], h)
    nonce = node.get_nonce_for_pk(tx.sender.near_account_name, tx.sender.near_signer_key.pk)
    if verbose:
        print('NONCE:', nonce)
    receiver_address = tx.receiver.eth_address
    if receiver_address.startswith('0x'):
        receiver_address = receiver_address[2:]
    if verbose:
        print('RECEIVER:', receiver_address)
    withdraw_tx = sign_function_call_tx(
        tx.sender.near_signer_key,
        tx.near_token_account_id,
        'withdraw',
        bytes('{"amount": "' + str(tx.amount) + '", "recipient": "' +  receiver_address + '"}', encoding='utf8'),
        300000000000000,
        0,
        nonce + 1,
        h)
    if verbose:
        print('WITHDRAW_TX:', withdraw_tx)
    # This tx is critical.
    # Executing it successfully without getting proper answer will lead any test to failing.
    withdraw = node.send_tx_and_wait(withdraw_tx, 45)
    if verbose:
        print('WITHDRAW_TX RES:', withdraw)
    return withdraw['result']

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def near2eth_get_proof(tx, withdraw_ticket, node, adapter):
    tx.get_proof_attempts += 1
    print(tx.id, 'GETTING PROOF PHASE, ATTEMPT', tx.get_proof_attempts)
    verbose = False
    if tx.get_proof_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    receipts = withdraw_ticket['transaction_outcome']['outcome']['receipt_ids']
    if verbose:
        print('RECEIPTS:', receipts)
    assert len(receipts) == 1
    withdraw_receipt_id = receipts[0]
    outcomes = withdraw_ticket['receipts_outcome']
    if verbose:
        print('OUTCOMES:', outcomes)
    receipt_id = None
    for outcome in outcomes:
        if outcome['id'] == withdraw_receipt_id:
            receipt_id = outcome['outcome']['status']['SuccessReceiptId']
            block_hash = outcome['block_hash']
            return (receipt_id, block_hash)
    if verbose:
        print('NO PROOF FOUND')
    
@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def near2eth_wait(tx, proof_ticket, node, adapter):
    tx.wait_attempts += 1
    print(tx.id, 'WAITING PHASE, ATTEMPT', tx.wait_attempts)
    verbose = False
    if tx.wait_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    (receipt_id, block_hash) = proof_ticket
    block = node.json_rpc('block', [block_hash], timeout=3)
    if verbose:
        print('BLOCK', block)
    height = block['result']['header']['height']
    if verbose:
        print('REQUIRED HEIGHT', height)
    while True:
        final_block = node.json_rpc('block', {'finality': 'final'}, timeout=3)
        if verbose:
            print('FINAL BLOCK', final_block)
        final_block_height = final_block['result']['header']['height']
        if verbose:
            print('CURRENT HEIGHT', final_block_height)
        if final_block_height > height:
            height = final_block_height
            break
        time.sleep(3)
    while True:
        [eth_client_block_height, eth_client_block_hash] = adapter.call_testing(['get-client-block-height-hash']).strip().split()
        eth_client_block_height = int(eth_client_block_height)
        print('ETH BLOCK HEIGHT & HASH', eth_client_block_height, eth_client_block_hash)
        if eth_client_block_height > height:
            return (receipt_id, eth_client_block_hash, eth_client_block_height)
        time.sleep(3)

@retry(stop_max_attempt_number=MAX_ATTEMPTS, wait_random_min=1000, wait_random_max=2500)
def near2eth_deposit(tx, wait_ticket, node, adapter):
    tx.deposit_attempts += 1
    print(tx.id, 'DEPOSIT PHASE, ATTEMPT', tx.deposit_attempts)
    verbose = False
    if tx.deposit_attempts == MAX_ATTEMPTS:
        print('LAST ATTEMPT! SETTING MAX VERBOSE')
        verbose = True

    (receipt_id, eth_client_block_hash, eth_client_block_height) = wait_ticket
    proof = node.json_rpc('light_client_proof', {"type": "receipt", "receipt_id": receipt_id, "receiver_id": tx.sender.near_account_name, "light_client_head": eth_client_block_hash}, timeout=15)
    if verbose:
        print('PROOF', proof)
    light_client_proof = json.dumps(proof['result'], separators=(',', ':'))
    unlock = adapter.call_testing(['near-to-eth-unlock', str(eth_client_block_height), light_client_proof], timeout=45)
    if verbose:
        print('UNLOCK', unlock)

def transfer_routine(tx, deposit_func, get_proof_func, wait_func, withdraw_func, node, adapter):
    assert tx.sender.eth_address

    withdraw_ticket = withdraw_func(tx, node, adapter)
    proof_ticket = get_proof_func(tx, withdraw_ticket, node, adapter)
    wait_ticket = wait_func(tx, proof_ticket, node, adapter)
    deposit_func(tx, wait_ticket, node, adapter)

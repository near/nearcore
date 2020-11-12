import atexit
import multiprocessing
import signal
import shutil
import subprocess
import time
import traceback
import os

# remove if not used
import base58
import base64

from transaction import sign_function_call_tx

def atexit_cleanup(obj):
    print("Cleaning %s on script exit" % (obj.__class__.__name__))
    try:
        obj.cleanup()
    except:
        print("Cleaning failed!")
        traceback.print_exc()
        pass

class Cleanable(object):

    def kill(self):
        if self.pid.value != 0:
            os.kill(self.pid.value, signal.SIGKILL)
            self.pid.value = 0
    
    def cleanup(self):
        if self.cleaned:
            return

        try:
            self.stdout.close()
            self.stderr.close()
            self.kill()
        except:
            print("Kill %s failed on cleanup!" % (self.__class__.__name__))
            traceback.print_exc()
            print("\n\n")



class GanacheNode(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        # TODO fix path
        bridge_dir = self.config['bridge_dir']
        ganache_bin = os.path.join(bridge_dir, self.config['ganache_bin'])
        if not os.path.exists(ganache_bin):
            self.build()
        atexit.register(atexit_cleanup, self)

    def build(self):
        bridge_dir = self.config['bridge_dir']
        ganache_dir = os.path.join(bridge_dir, self.config['ganache_dir'])
        os.system('cd %s && yarn' % (ganache_dir))

    def start(self):
        # TODO fix path
        bridge_dir = self.config['bridge_dir']
        ganache_bin = os.path.join(bridge_dir, self.config['ganache_bin'])
        config_dir = self.config['config_dir']
        # TODO fix logs
        self.stdout = open(os.path.join(config_dir, 'logs/ganache/out.log'), 'w')
        self.stderr = open(os.path.join(config_dir, 'logs/ganache/err.log'), 'w')
        # TODO use blockTime
        # TODO set params
        self.pid.value = subprocess.Popen([ganache_bin,'--port','9545','--blockTime','12','--gasLimit','10000000','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200,10000000000000000000000000000"','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201,10000000000000000000000000000"','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501202,10000000000000000000000000000"'], stdout=self.stdout, stderr=self.stderr).pid


class Near2EthBlockRelay(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        atexit.register(atexit_cleanup, self)

    def start(self):
        # TODO refactor this
        bridge_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['bridge_dir'])))
        config_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['config_dir'])))
        self.stdout = open(os.path.join(config_dir, 'logs/near2eth-relay/out.log'), 'w')
        self.stderr = open(os.path.join(config_dir, 'logs/near2eth-relay/err.log'), 'w')
        # TODO use params
        near2eth_block_relay_path = os.path.join(bridge_dir, 'near2eth/near2eth-block-relay/index.js') 
        self.pid.value = subprocess.Popen(['node', near2eth_block_relay_path, 'runNear2EthRelay', '2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201'], stdout=self.stdout, stderr=self.stderr).pid
        # TODO ping and wait until service really starts
        time.sleep(10)

class Eth2NearBlockRelay(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        atexit.register(atexit_cleanup, self)

    def start(self):
        # TODO refactor this
        bridge_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['bridge_dir'])))
        config_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['config_dir'])))
        self.stdout = open(os.path.join(config_dir, 'logs/eth2near-relay/out.log'), 'w')
        self.stderr = open(os.path.join(config_dir, 'logs/eth2near-relay/err.log'), 'w')
        # TODO use params
        eth2near_block_relay_path = os.path.join(bridge_dir, 'eth2near/eth2near-block-relay/index.js') 
        self.pid.value = subprocess.Popen(['node', eth2near_block_relay_path, 'runEth2NearRelay'], stdout=self.stdout, stderr=self.stderr).pid
        # TODO ping and wait until service really starts
        time.sleep(10)

class JSAdapter:

    def __init__(self, config):
        self.config = config
        bridge_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['bridge_dir'])))
        self.js_adapter_path = os.path.join(bridge_dir, 'testing/adapter/index.js')

    def call(self, args):
        if not isinstance(args, list):
            args = [args]
        args.insert(0, 'node')
        args.insert(1, self.js_adapter_path)
        # TODO check for errors
        return subprocess.check_output(args).decode('ascii')


class RainbowBridge:

    def __init__(self, config):
        self.config = config
        self.eth2near_block_relay = None
        self.near2eth_block_relay = None
        self.adapter = JSAdapter(self.config)
        bridge_dir = self.config['bridge_dir']
        if not os.path.exists(bridge_dir):
            self.git_clone_install()
    
    def git_clone_install(self):
        print('no rainbow-bridge repo found, cloning...')
        bridge_dir = self.config['bridge_dir']
        os.system('git clone --recurse-submodules ' + self.config['bridge_repo'] + ' ' + bridge_dir)
        # TODO remove
        os.system('cd %s && yarn' % (bridge_dir))
        # TODO use config please
        os.system('cp ./lib/js_adapter/write_config.js %s' % (bridge_dir))
        # TODO install ethash
        # TODO use config

    def init_near_contracts(self):
        bridge_dir = self.config['bridge_dir']
        # TODO use RB config
        os.system('cd %s && node write_config.js' % (bridge_dir))
        # TODO do it natively
        os.system('cd %s && cd cli && node index.js init-near-contracts' % (bridge_dir))

    def init_eth_contracts(self):
        bridge_dir = self.config['bridge_dir']
        # TODO use RB config
        os.system('cd %s && node write_config.js' % (bridge_dir))
        # TODO use adapter instead
        os.system('cd %s && cd cli && node index.js init-eth-ed25519' % (bridge_dir))
        os.system('cd %s && cd cli && node index.js init-eth-client --eth-client-lock-eth-amount 1000000000000000000 --eth-client-lock-duration 30' % (bridge_dir))
        os.system('cd %s && cd cli && node index.js init-eth-prover' % (bridge_dir))
        os.system('cd %s && cd cli && node index.js init-eth-erc20' % (bridge_dir))
        os.system('cd %s && cd cli && node index.js init-eth-locker' % (bridge_dir))

    def init_near_token_factory(self):
        bridge_dir = self.config['bridge_dir']
        # TODO use RB config
        os.system('cd %s && node write_config.js' % (bridge_dir))
        # TODO do it natively
        os.system('cd %s && cd cli && node index.js init-near-token-factory' % (bridge_dir))

    def start_eth2near_block_relay(self):
        self.eth2near_block_relay = Eth2NearBlockRelay(self.config)
        self.eth2near_block_relay.start()

    def start_near2eth_block_relay(self):
        self.near2eth_block_relay = Near2EthBlockRelay(self.config)
        self.near2eth_block_relay.start()

    def transfer_eth2near(self, sender, receiver, near_master_account, amount):
        bridge_dir = self.config['bridge_dir']
        cli_dir = os.path.join(bridge_dir, 'cli')
        args = ('node index.js transfer-eth-erc20-to-near --amount %d --eth-sender-sk %s --near-receiver-account %s --near-master-account %s' % (amount, sender, receiver, near_master_account)).split()
        return subprocess.Popen(args, cwd=cli_dir)

    def transfer_near2eth(self, sender, receiver, amount):
        bridge_dir = self.config['bridge_dir']
        cli_dir = os.path.join(bridge_dir, 'cli')
        args = ('node index.js transfer-eth-erc20-from-near --amount %d --near-sender-account %s --eth-receiver-address %s --near-sender-sk ed25519:3KyUucjyGk1L58AJBB6Rf6EZFqmpTSSKG7KKsptMvpJLDBiZmAkU4dR1HzNS6531yZ2cR5PxnTM7NLVvSfJjZPh7' % (amount, sender, receiver)).split()
        return subprocess.Popen(args, cwd=cli_dir)

    def get_eth_balance(self, address, token_address=None):
        # js parses 0x as number, not as string
        if address.startswith('0x'):
            address = address[2:]
        # TODO use specific token address
        return int(self.adapter.call(['getBalance', address]))
        
    def get_near_balance(self, node, account_id, token_account_id=None):
        if not token_account_id:
            # use default token_account
            token_account_id = '7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory'
        #return node.call_function(token_account_id, 'get_balance', '{"owner_id": "' + account_id + '"}')
        res = node.call_function(token_account_id, 'get_balance', base64.b64encode(bytes('{"owner_id": "' + account_id + '"}', encoding='utf8')).decode("ascii"))
        res = int.from_bytes(res["result"]["result"], byteorder='little')
        print('AAA', res)
        return res
        status = node.get_status()
        h = status['sync_info']['latest_block_hash']
        h = base58.b58decode(h.encode('utf8'))
        tx = sign_function_call_tx(node.signer_key, token_account_id, 'get_balance', '{"owner_id": "' + account_id + '"}', 3000000000000, 0, 20, h)
        res = node.send_tx_and_wait(tx, 20)
        print('BBB', res)
        return 0
        #return res['result']['receipts_outcome'][0]['outcome']['logs'][0]

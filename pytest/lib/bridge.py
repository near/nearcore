import atexit
import multiprocessing
import signal
import shutil
import subprocess
import time
import traceback
import os

from account import Account

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
            self.kill()
        except:
            print("Kill %s failed on cleanup!" % (obj.__class__.__name__))
            traceback.print_exc()
            print("\n\n")



class GanacheNode(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        # TODO fix path
        ganache = os.path.join('lib', self.config['ganache'])
        if not os.path.exists(ganache):
            self.build()
        atexit.register(atexit_cleanup, self)

    def build(self):
        os.system('cd ganache && yarn')

    def start(self):
        # TODO fix path
        ganache = os.path.join('lib', self.config['ganache'])
        # TODO use blockTime
        # TODO set params
        self.pid.value = subprocess.Popen([ganache,'--port','9545','--blockTime','12','--gasLimit','10000000','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501200,10000000000000000000000000000"','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201,10000000000000000000000000000"','--account="0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501202,10000000000000000000000000000"']).pid


class Near2EthBlockRelay(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        atexit.register(atexit_cleanup, self)

    def start(self):
        # TODO refactor this
        bridge_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['bridge_dir'])))
        # TODO use params
        near2eth_block_relay_path = os.path.join(bridge_dir, 'near2eth/near2eth-block-relay/index.js') 
        self.pid.value = subprocess.Popen(['node', near2eth_block_relay_path, 'runNear2EthRelay', '--eth-master-sk', '0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201']).pid


class Eth2NearBlockRelay(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        atexit.register(atexit_cleanup, self)

    def start(self):
        # TODO refactor this
        bridge_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(self.config['bridge_dir'])))
        # TODO use params
        eth2near_block_relay_path = os.path.join(bridge_dir, 'eth2near/eth2near-block-relay/index.js') 
        self.pid.value = subprocess.Popen(['node', eth2near_block_relay_path, 'runEth2NearRelay']).pid


class RainbowBridge:

    def __init__(self, config):
        self.config = config
        self.near2eth_block_relay = None
        bridge_dir = self.config['bridge_dir']
        # TODO use config
        config_dir = os.path.expanduser(self.config['config_dir'])
        # TODO clear data generously
        if os.path.exists(config_dir) and os.path.isdir(config_dir):
            shutil.rmtree(config_dir)
        if not os.path.exists(os.path.expanduser(bridge_dir)):
            self.git_clone_install()
    
    def git_clone_install(self):
        print('no rainbow-bridge repo found, cloning...')
        bridge_dir = self.config['bridge_dir']
        os.system('git clone --recurse-submodules ' + self.config['bridge_repo'] + ' ' + bridge_dir)
        # TODO remove
        os.system('cd %s && git checkout global_package' % (bridge_dir))
        os.system('cd %s && yarn' % (bridge_dir))
        os.system('cd %s && yarn pm2 ping' % (bridge_dir))
        # TODO use config please
        os.system('cp ./lib/js_adapter/write_config.js %s' % (bridge_dir))
        # TODO install ethash

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
        # TODO ping and wait until service really starts

    def start_near2eth_block_relay(self):
        self.near2eth_block_relay = Near2EthBlockRelay(self.config)
        self.near2eth_block_relay.start()
        # TODO ping and wait until service really starts
        time.sleep(10)

    def transfer_eth2near(self, sender, receiver, near_master_account, amount):
        bridge_dir = self.config['bridge_dir']
        os.system('cd %s && cd cli && node index.js transfer-eth-erc20-to-near --amount %d \
                   --eth-sender-sk %s --near-receiver-account %s --near-master-account %s' %
            (bridge_dir, amount, sender, receiver, near_master_account))

    def transfer_near2eth(self, sender, receiver, amount):
        bridge_dir = self.config['bridge_dir']
        os.system('cd %s && cd cli && node index.js transfer-eth-erc20-from-near --amount %d \
                   --near-sender-account %s --eth-receiver-address %s' %
            (bridge_dir, amount, sender, receiver))


def start_ganache(config=None):
    if not config:
        config = load_config()

    ganache_node = GanacheNode(config)
    ganache_node.start()
    return ganache_node

def start_bridge(config=None):
    if not config:
        config = load_config()

    bridge = RainbowBridge(config)
    bridge.init_near_contracts()
    bridge.init_eth_contracts()
    bridge.init_near_token_factory()
    bridge.start_near2eth_block_relay()
    bridge.start_eth2near_block_relay()
    return bridge


DEFAULT_CONFIG = {
    'local': True,
    'bridge_repo': 'https://github.com/near/rainbow-bridge.git',
    #'bridge_dir': '~/.rainbow-bridge',
    'bridge_dir': '~/near/rainbow-bridge',
    'config_dir': '~/.rainbow',
    'ganache': 'ganache/node_modules/.bin/ganache-cli',
}

def load_config():
    config = DEFAULT_CONFIG

    try:
        config_file = os.environ.get(CONFIG_ENV_VAR, '')
        if config_file:
            try:
                with open(config_file) as f:
                    new_config = json.load(f)
                    config.update(new_config)
                    print(f"Load config from {config_file}, config {config}")
            except FileNotFoundError:
                print(f"Failed to load config file, use default config {config}")
        else:
            print(f"Use default config {config}")
    except:
        print(f"No specific config found, use default config {config}")

    return config

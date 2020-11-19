import atexit
import base64
import multiprocessing
from pathlib import Path
import signal
import shutil
import subprocess
import time
import traceback
import os

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

    def start(self, eth_master_secret_key='0x2bdd21761a483f71054e14f5b827213567971c676928d9a1808cbfa4b7501201'):
        bridge_dir = self.config['bridge_dir']
        config_dir = self.config['config_dir']
        self.stdout = open(os.path.join(config_dir, 'logs/near2eth-relay/out.log'), 'a')
        self.stderr = open(os.path.join(config_dir, 'logs/near2eth-relay/err.log'), 'a')
        cli_dir = os.path.join(bridge_dir, 'cli')
        args = ('node index.js start near2eth-relay --eth-master-sk %s --daemon false' % (eth_master_secret_key)).split()
        self.pid.value = subprocess.Popen(args, stdout=self.stdout, stderr=self.stderr, cwd=cli_dir).pid
        # TODO ping and wait until service really starts
        time.sleep(5)
        assert self.pid.value != 0

    def restart(self):
        assert not self.cleaned
        self.cleanup()
        assert self.pid.value == 0
        self.start()
        assert self.pid.value != 0

class Eth2NearBlockRelay(Cleanable):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        atexit.register(atexit_cleanup, self)

    def start(self):
        bridge_dir = self.config['bridge_dir']
        config_dir = self.config['config_dir']
        self.stdout = open(os.path.join(config_dir, 'logs/eth2near-relay/out.log'), 'a')
        self.stderr = open(os.path.join(config_dir, 'logs/eth2near-relay/err.log'), 'a')
        cli_dir = os.path.join(bridge_dir, 'cli')
        args = ('node index.js start eth2near-relay --daemon false').split()
        self.pid.value = subprocess.Popen(args, stdout=self.stdout, stderr=self.stderr, cwd=cli_dir).pid
        # TODO ping and wait until service really starts
        time.sleep(5)
        assert self.pid.value != 0

    def restart(self):
        assert not self.cleaned
        self.cleanup()
        assert self.pid.value == 0
        self.start()
        assert self.pid.value != 0

class JSAdapter:

    def __init__(self, config):
        self.config = config
        self.bridge_dir = self.config['bridge_dir']
        self.cli_dir = os.path.join(self.bridge_dir, 'cli')

    def call(self, args):
        if not isinstance(args, list):
            args = [args]
        args.insert(0, 'node')
        args.insert(1, 'index.js')
        args.insert(2, 'TESTING')
        # TODO check for errors
        return subprocess.check_output(args, cwd=self.cli_dir).decode('ascii').strip()


def assert_deployed(output):
    assert output.decode('ascii').strip().split('\n')[-1].strip().split(' ')[0] == 'Deployed'

class RainbowBridge:

    def __init__(self, config):
        self.config = config
        self.eth2near_block_relay = None
        self.near2eth_block_relay = None
        self.adapter = JSAdapter(self.config)
        self.bridge_dir = self.config['bridge_dir']
        self.config_dir = self.config['config_dir']
        self.cli_dir = os.path.join(self.bridge_dir, 'cli')
        if not os.path.exists(self.bridge_dir):
            self._git_clone_install()
        if not os.path.exists(os.path.expanduser("~/go")):
            print('No go found, installing...')
            os.system('wget -q -O - https://raw.githubusercontent.com/canha/golang-tools-install-script/master/goinstall.sh | bash')
            os.system('export GOROOT=~/.go')
            os.system('export GOPATH=~/go')
            os.system('export PATH=$GOPATH/bin:$GOROOT/bin:$PATH')
        os.system('cp ./lib/bridge_helpers/write_config.js %s' % (self.bridge_dir))

        if os.path.exists(self.config_dir):
            assert os.path.isdir(self.config_dir)
            shutil.rmtree(self.config_dir)
        logs_dir = os.path.join(self.config_dir, 'logs')
        os.makedirs(logs_dir)
        for service in ['ganache', 'near2eth-relay', 'eth2near-relay', 'watchdog']:
            os.mkdir(os.path.join(logs_dir, service))
            Path(os.path.join(os.path.join(logs_dir, service), 'err.log')).touch()
            Path(os.path.join(os.path.join(logs_dir, service), 'out.log')).touch()

        assert subprocess.check_output(['node', 'write_config.js'], cwd=self.bridge_dir) == b''
    
    def _git_clone_install(self):
        print('No rainbow-bridge repo found, cloning...')
        args = ('git clone --recurse-submodules %s %s' % (self.config['bridge_repo'], self.bridge_dir)).split()
        assert subprocess.check_output(args).decode('ascii').strip() == "Submodule path 'eth2near/ethashproof': checked out 'b7e7e22979a9b25043b649c22e41cb149267fbeb'"
        print(self.bridge_dir)
        print(subprocess.check_output(['yarn']))
        print(subprocess.check_output(['node', '--version']))
        print(subprocess.check_output(['yarn'], cwd=self.bridge_dir).decode('ascii').strip().split('\n')[-1].strip().split(' ')[0] == 'Done')
        import time
        time.sleep(5)
        assert subprocess.check_output(['yarn'], cwd=self.bridge_dir).decode('ascii').strip().split('\n')[-1].strip().split(' ')[0] == 'Done'
        ethash_dir = os.path.join(self.bridge_dir, 'eth2near/ethashproof')
        assert subprocess.check_output(['/bin/sh', 'build.sh'], cwd=ethash_dir) == b''

    def init_near_contracts(self):
        print('Init NEAR contracts...')
        assert subprocess.check_output(['node', 'index.js', 'init-near-contracts'], cwd=self.cli_dir).decode('ascii').strip().split('\n')[-1] == 'ETH2NEARProver initialized'

    def init_eth_contracts(self):
        print('Init ETH contracts...')
        assert_deployed(subprocess.check_output(['node', 'index.js', 'init-eth-ed25519'], cwd=self.cli_dir))
        assert_deployed(subprocess.check_output(['node', 'index.js', 'init-eth-client', '--eth-client-lock-eth-amount', '1000000000000000000', '--eth-client-lock-duration', '30'], cwd=self.cli_dir))
        assert_deployed(subprocess.check_output(['node', 'index.js', 'init-eth-prover'], cwd=self.cli_dir))
        assert_deployed(subprocess.check_output(['node', 'index.js', 'init-eth-erc20'], cwd=self.cli_dir))
        assert_deployed(subprocess.check_output(['node', 'index.js', 'init-eth-locker'], cwd=self.cli_dir))

    def init_near_token_factory(self):
        print('Init token factory...')
        assert subprocess.check_output(['node', 'index.js', 'init-near-token-factory'], cwd=self.cli_dir).decode('ascii').    strip().split('\n')[-1].strip().split(' ')[-1] == 'deployed'

    def start_eth2near_block_relay(self):
        print('Starting ETH2NEAR relay...')
        self.eth2near_block_relay = Eth2NearBlockRelay(self.config)
        self.eth2near_block_relay.start()

    def start_near2eth_block_relay(self):
        print('Starting NEAR2ETH relay...')
        self.near2eth_block_relay = Near2EthBlockRelay(self.config)
        self.near2eth_block_relay.start()

    def transfer_eth2near(self, sender, receiver, near_master_account, amount):
        args = ('node index.js transfer-eth-erc20-to-near --amount %d --eth-sender-sk %s --near-receiver-account %s --near-master-account %s' % (amount, sender, receiver, near_master_account)).split()
        return subprocess.Popen(args, cwd=self.cli_dir)

    def transfer_near2eth(self, sender, receiver, amount):
        args = ('node index.js transfer-eth-erc20-from-near --amount %d --near-sender-account %s --eth-receiver-address %s --near-sender-sk ed25519:3KyUucjyGk1L58AJBB6Rf6EZFqmpTSSKG7KKsptMvpJLDBiZmAkU4dR1HzNS6531yZ2cR5PxnTM7NLVvSfJjZPh7' % (amount, sender, receiver)).split()
        return subprocess.Popen(args, cwd=self.cli_dir)

    def get_eth_address_by_secret_key(self, secret_key):
        # js parses 0x as number, not as string
        if secret_key.startswith('0x'):
            secret_key = secret_key[2:]
        return self.adapter.call(['get-eth-account-address', secret_key])

    def get_eth_balance(self, address, token_address=None):
        # js parses 0x as number, not as string
        if address.startswith('0x'):
            address = address[2:]
        # TODO use specific token address
        return int(self.adapter.call(['get-eth-erc20-balance', address]))
        
    def get_near_balance(self, node, account_id, token_account_id=None):
        if not token_account_id:
            # use default token_account
            token_account_id = '7cc4b1851c35959d34e635a470f6b5c43ba3c9c9.neartokenfactory'
        res = node.call_function(token_account_id, 'get_balance', base64.b64encode(bytes('{"owner_id": "' + account_id + '"}', encoding='utf8')).decode("ascii"), timeout=15)
        res = int("".join(map(chr, res['result']['result']))[1:-1])
        return res

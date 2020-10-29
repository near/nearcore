import atexit
import multiprocessing
import signal
import subprocess
import traceback
import os

from account import Account

def atexit_cleanup(obj):
    # TODO print obj type
    print("Cleaning on script exit")
    try:
        obj.cleanup()
    except:
        print("Cleaning failed!")
        traceback.print_exc()
        pass


class GanacheNode(object):

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
            print("Kill ganache failed on cleanup!")
            traceback.print_exc()
            print("\n\n")


class RainbowBridge(object):

    def __init__(self, config):
        self.cleaned = False
        self.pid = multiprocessing.Value('i', 0)
        self.config = config
        bridge_dir = self.config['bridge_dir']
        if not os.path.exists(os.path.expanduser(bridge_dir)):
            self.git_clone_install()

        atexit.register(atexit_cleanup, self)
    
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
        os.system('cd %s && cd rainbow-bridge-cli && node index.js init-near-contracts' % (bridge_dir))

    def cleanup(self):
        if self.cleaned:
            return

        try:
            pass
            #self.kill()
        except:
            print("Kill bridge failed on cleanup!")
            traceback.print_exc()
            print("\n\n")


def start_ganache(config=None):
    if not config:
        config = load_config()

    ganache_node = GanacheNode(config)
    ganache_node.start()
    return ganache_node

def start_bridge(config=None):
    if not config:
        config = load_config()

    rainbow_bridge = RainbowBridge(config)
    return rainbow_bridge


DEFAULT_CONFIG = {
    'local': True,
    'bridge_repo': 'https://github.com/near/rainbow-bridge.git',
    #'bridge_dir': '~/.rainbow-bridge',
    'bridge_dir': '~/near/rainbow-bridge',
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

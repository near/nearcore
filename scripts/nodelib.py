#!/usr/bin/env python3

import json
import os
import subprocess
import urllib

try:
    input = raw_input
except NameError:
    pass

USER = str(os.getuid()) + ':' + str(os.getgid())
"""Installs cargo/Rust."""


def install_cargo():
    try:
        subprocess.call([os.path.expanduser('~/.cargo/bin/cargo'), '--version'])
    except OSError:
        print("Installing Rust...")
        subprocess.check_output('curl https://sh.rustup.rs -sSf | sh -s -- -y',
                                shell=True)


"""Inits the node configuration using docker."""


def docker_init(image, home_dir, init_flags):
    subprocess.check_output(['mkdir', '-p', home_dir])
    subprocess.check_output([
        'docker', 'run', '-u', USER, '-v',
        '%s:/srv/near' % home_dir, '-v',
        os.path.abspath('near/res') +
        ':/near/res', image, 'near', '--home=/srv/near', 'init'
    ] + init_flags)


"""Inits the node configuration using local build."""


def nodocker_init(home_dir, is_release, init_flags):
    target = './target/%s/neard' % ('release' if is_release else 'debug')
    cmd = [target]
    if home_dir:
        cmd.extend(['--home', home_dir])
    cmd.extend(['init'])
    subprocess.call(cmd + init_flags)


def get_chain_id_from_flags(flags):
    """Retrieve requested chain id from the flags."""
    chain_id = None
    flags_iter = iter(flags)
    for flag in flags_iter:
        if flag.startswith('--chain-id='):
            chain_id = flag[len('--chain-id='):]
        elif flag == '--chain-id':
            chain_id = next(flags_iter, chain_id)
    return chain_id


"""Compile given package using cargo"""


def compile_package(package_name, is_release):
    flags = ['-p', package_name]
    if is_release:
        flags = ['--release'] + flags
    code = subprocess.call([os.path.expanduser('cargo'), 'build'] + flags)
    if code != 0:
        print("Compilation failed, aborting")
        exit(code)


"""Checks if there is already everything setup on this machine, otherwise sets up NEAR node."""


def check_and_setup(nodocker,
                    is_release,
                    image,
                    home_dir,
                    init_flags,
                    no_gas_price=False):
    if nodocker:
        compile_package('neard', is_release)

    chain_id = get_chain_id_from_flags(init_flags)
    if os.path.exists(os.path.join(home_dir, 'config.json')):
        with open(os.path.join(os.path.join(home_dir, 'genesis.json'))) as fd:
            genesis_config = json.load(fd)
        if chain_id and genesis_config['chain_id'] != chain_id:
            if chain_id == 'testnet':
                print(
                    "Folder %s already has network configuration for %s, which is not the official testnet.\n"
                    "Use ./scripts/start_localnet.py instead to keep running with existing configuration.\n"
                    "If you want to run a different network, either specify different --home or remove %s to start from scratch."
                    % (home_dir, genesis_config['chain_id'], home_dir))
            elif genesis_config['chain_id'] == 'testnet':
                print(
                    "Folder %s already has network configuration for the official testnet.\n"
                    "Use ./scripts/start_testnet.py instead to keep running it.\n"
                    "If you want to run a different network, either specify different --home or remove %s to start from scratch"
                    % (home_dir, home_dir))
            elif chain_id != '':
                print(
                    "Folder %s already has network configuration for %s. Use ./scripts/start_localnet.py to continue running it."
                    % (home_dir, genesis_config['chain_id']))
            exit(1)
        print("Using existing node configuration from %s for %s" %
              (home_dir, genesis_config['chain_id']))
        return

    print("Setting up network configuration.")
    if len([x for x in init_flags if x.startswith('--account-id')]) == 0:
        prompt = "Enter your account ID"
        if chain_id:
            prompt += " (leave empty if not going to be a validator): "
        else:
            prompt += ": "
        account_id = input(prompt)
        if account_id:
            init_flags.append('--account-id=%s' % account_id)

    if chain_id == 'testnet':
        testnet_genesis_hash = open('near/res/testnet_genesis_hash').read()
        testnet_genesis_records = 'near/res/testnet_genesis_records_%s.json' % testnet_genesis_hash
        if not os.path.exists(testnet_genesis_records):
            print('Downloading testnet genesis records')
            url = 'https://s3-us-west-1.amazonaws.com/testnet.nearprotocol.com/testnet_genesis_records_%s.json' % testnet_genesis_hash
            urllib.urlretrieve(url, testnet_genesis_records)
        init_flags.extend([
            '--genesis-config', 'near/res/testnet_genesis_config.json',
            '--genesis-records', testnet_genesis_records, '--genesis-hash',
            testnet_genesis_hash
        ])

    if nodocker:
        nodocker_init(home_dir, is_release, init_flags)
    else:
        docker_init(image, home_dir, init_flags)
    if no_gas_price:
        filename = os.path.join(home_dir, 'genesis.json')
        with open(filename) as fd:
            genesis_config = json.load(fd)
        genesis_config['gas_price'] = 0
        genesis_config['min_gas_price'] = 0
        json.dump(genesis_config, open(filename, 'w'))


def print_staking_key(home_dir):
    key_path = os.path.join(home_dir, 'validator_key.json')
    if not os.path.exists(key_path):
        return

    with open(key_path) as fd:
        key_file = json.load(fd)
    if not key_file['account_id']:
        print("Node is not staking. Re-run init to specify staking account.")
        return
    print("Stake for user '%s' with '%s'" %
          (key_file['account_id'], key_file['public_key']))


"""Stops and removes given docker container."""


def docker_stop_if_exists(name):
    try:
        subprocess.Popen(['docker', 'stop', name],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE).communicate()
    except subprocess.CalledProcessError:
        pass
    try:
        subprocess.Popen(['docker', 'rm', name],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE).communicate()
    except subprocess.CalledProcessError:
        pass


"""Checks the ports saved in config.json"""


def get_port(home_dir, net):
    config = json.load(open(os.path.join(home_dir, 'config.json')))
    p = config[net]['addr'][config[net]['addr'].find(':') + 1:]
    return p + ":" + p


"""Runs NEAR core inside the docker container for isolation and easy update with Watchtower."""


def run_docker(image, home_dir, boot_nodes, telemetry_url, verbose):
    print("Starting NEAR client and Watchtower dockers...")
    docker_stop_if_exists('watchtower')
    docker_stop_if_exists('nearcore')
    # Start nearcore container, mapping home folder and ports.
    envs = [
        '-e',
        'BOOT_NODES=%s' % boot_nodes, '-e',
        'TELEMETRY_URL=%s' % telemetry_url
    ]
    rpc_port = get_port(home_dir, 'rpc')
    network_port = get_port(home_dir, 'network')
    if verbose:
        envs.extend(['-e', 'VERBOSE=1'])
    subprocess.check_output(['mkdir', '-p', home_dir])
    subprocess.check_output([
        'docker', 'run', '-u', USER, '-d', '-p', rpc_port, '-p', network_port,
        '-v',
        '%s:/srv/near' % home_dir, '-v', '/tmp:/tmp', '--ulimit', 'core=-1',
        '--name', 'nearcore', '--restart', 'unless-stopped'
    ] + envs + [image])
    # Start Watchtower that will automatically update the nearcore container when new version appears.
    subprocess.check_output([
        'docker', 'run', '-u', USER, '-d', '--restart', 'unless-stopped',
        '--name', 'watchtower', '-v',
        '/var/run/docker.sock:/var/run/docker.sock', 'v2tec/watchtower', image
    ])
    print(
        "Node is running! \nTo check logs call: docker logs --follow nearcore")


"""Runs NEAR core outside of docker."""


def run_nodocker(home_dir, is_release, boot_nodes, telemetry_url, verbose):
    print("Starting NEAR client...")
    print(
        "Autoupdate is not supported at the moment for runs outside of docker")
    cmd = ['./target/%s/neard' % ('release' if is_release else 'debug')]
    cmd.extend(['--home', home_dir])
    if verbose:
        cmd += ['--verbose', '']
    cmd.append('run')
    if telemetry_url:
        cmd.append('--telemetry-url=%s' % telemetry_url)
    if boot_nodes:
        cmd.append('--boot-nodes=%s' % boot_nodes)
    try:
        subprocess.call(cmd)
    except KeyboardInterrupt:
        print("\nStopping NEARCore.")


def setup_and_run(nodocker,
                  is_release,
                  image,
                  home_dir,
                  init_flags,
                  boot_nodes,
                  telemetry_url,
                  verbose=False,
                  no_gas_price=False):
    if nodocker:
        install_cargo()
    else:
        try:
            subprocess.check_output(['docker', 'pull', image])
            subprocess.check_output(['docker', 'pull', 'v2tec/watchtower'])
        except subprocess.CalledProcessError as exc:
            print("Failed to fetch docker containers: %s" % exc)
            exit(1)

    check_and_setup(nodocker, is_release, image, home_dir, init_flags,
                    no_gas_price)

    print_staking_key(home_dir)

    if nodocker:
        run_nodocker(home_dir, is_release, boot_nodes, telemetry_url, verbose)
    else:
        run_docker(image, home_dir, boot_nodes, telemetry_url, verbose)


"""Stops docker for Nearcore and watchtower if they are running."""


def stop_docker():
    docker_stop_if_exists('watchtower')
    docker_stop_if_exists('nearcore')


def generate_node_key(home, is_release, nodocker, image):
    print("Generating node key...")
    if nodocker:
        cmd = [
            './target/%s/keypair-generator' %
            ('release' if is_release else 'debug')
        ]
        cmd.extend(['--home', home])
        cmd.extend(['--generate-config'])
        cmd.extend(['node-key'])
        try:
            subprocess.call(cmd)
        except KeyboardInterrupt:
            print("\nStopping NEARCore.")
    else:
        subprocess.check_output(['mkdir', '-p', home])
        subprocess.check_output([
            'docker', 'run', '-u', USER, '-v',
            '%s:/srv/keypair-generator' % home, image, 'keypair-generator',
            '--home=/srv/keypair-generator', '--generate-config', 'node-key'
        ])
    print("Node key generated")


def generate_validator_key(home, is_release, nodocker, image, account_id):
    print("Generating validator key...")
    if nodocker:
        cmd = [
            './target/%s/keypair-generator' %
            ('release' if is_release else 'debug')
        ]
        cmd.extend(['--home', home])
        cmd.extend(['--generate-config'])
        cmd.extend(['--account-id', account_id])
        cmd.extend(['validator-key'])
        try:
            subprocess.call(cmd)
        except KeyboardInterrupt:
            print("\nStopping NEARCore.")
    else:
        subprocess.check_output(['mkdir', '-p', home])
        subprocess.check_output([
            'docker', 'run', '-u', USER, '-v',
            '%s:/srv/keypair-generator' % home, image, 'keypair-generator',
            '--home=/srv/keypair-generator', '--generate-config',
            '--account-id=%s' % account_id, 'validator-key'
        ])
    print("Validator key generated")


def generate_signer_key(home, is_release, nodocker, image, account_id):
    print("Generating signer keys...")
    if nodocker:
        cmd = [
            './target/%s/keypair-generator' %
            ('release' if is_release else 'debug')
        ]
        cmd.extend(['--home', home])
        cmd.extend(['--generate-config'])
        cmd.extend(['--account-id', account_id])
        cmd.extend(['signer-keys'])
        try:
            subprocess.call(cmd)
        except KeyboardInterrupt:
            print("\nStopping NEARCore.")
    else:
        subprocess.check_output(['mkdir', '-p', home])
        subprocess.check_output([
            'docker', 'run', '-u', USER, '-v',
            '%s:/srv/keypair-generator' % home, image, 'keypair-generator',
            '--home=/srv/keypair-generator', '--generate-config',
            '--account-id=%s' % account_id, 'signer-keys'
        ])
    print("Signer keys generated")


def initialize_keys(home, is_release, nodocker, image, account_id,
                    generate_signer_keys):
    if nodocker:
        install_cargo()
        compile_package('keypair-generator', is_release)
    else:
        try:
            subprocess.check_output(['docker', 'pull', image])
        except subprocess.CalledProcessError as exc:
            print("Failed to fetch docker containers: %s" % exc)
            exit(1)
    if generate_signer_keys:
        generate_signer_key(home, is_release, nodocker, image, account_id)
    generate_node_key(home, is_release, nodocker, image)
    if account_id:
        generate_validator_key(home, is_release, nodocker, image, account_id)


def create_genesis(home, is_release, nodocker, image, chain_id, tracked_shards):
    if os.path.exists(os.path.join(home, 'genesis.json')):
        print("Genesis already exists")
        return
    print("Creating genesis...")
    if not os.path.exists(os.path.join(home, 'accounts.csv')):
        raise Exception(
            "Failed to generate genesis: accounts.csv does not exist")
    if nodocker:
        cmd = [
            './target/%s/genesis-csv-to-json' %
            ('release' if is_release else 'debug')
        ]
        if home:
            cmd.extend(['--home', home])
        if chain_id:
            cmd.extend(['--chain-id', chain_id])
        if len(tracked_shards) > 0:
            cmd.extend(['--tracked-shards', tracked_shards])
        try:
            subprocess.call(cmd)
        except KeyboardInterrupt:
            print("\nStopping NEARCore.")
    else:
        subprocess.check_output(['mkdir', '-p', home])
        cmd = [
            'docker',
            'run',
            '-u',
            USER,
            '-v',
            '%s:/srv/genesis-csv-to-json' % home,
            image,
            'genesis-csv-to-json',
            '--home=/srv/genesis-csv-to-json',
        ]
        if chain_id:
            cmd.extend(['--chain-id', chain_id])
        if tracked_shards:
            cmd.extend(['--tracked-shards', tracked_shards])
        subprocess.check_output(cmd)
    print("Genesis created")


def start_stakewars(home, is_release, nodocker, image, telemetry_url, verbose,
                    tracked_shards):
    if nodocker:
        install_cargo()
        compile_package('genesis-csv-to-json', is_release)
        compile_package('neard', is_release)
    else:
        try:
            subprocess.check_output(['docker', 'pull', image])
        except subprocess.CalledProcessError as exc:
            print("Failed to fetch docker containers: %s" % exc)
            exit(1)
    create_genesis(home, is_release, nodocker, image, 'stakewars',
                   tracked_shards)
    if nodocker:
        run_nodocker(home,
                     is_release,
                     boot_nodes='',
                     telemetry_url=telemetry_url,
                     verbose=verbose)
    else:
        run_docker(image,
                   home,
                   boot_nodes='',
                   telemetry_url=telemetry_url,
                   verbose=verbose)

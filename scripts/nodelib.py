#!/usr/bin/python

import argparse
import json
import os
import subprocess

try:
    input = raw_input
except NameError:
    pass

"""Installs cargo/Rust."""
def install_cargo():
    try:
        subprocess.call([os.path.expanduser('~/.cargo/bin/cargo'), '--version'])
    except OSError:
        print("Installing Rust...")
        subprocess.check_output('curl https://sh.rustup.rs -sSf | sh -s -- -y', shell=True)
        subprocess.call([os.path.expanduser('~/.cargo/bin/rustup'), 'default', 'nightly'])


"""Inits the node configuration using docker."""
def docker_init(image, home_dir, init_flags):
    subprocess.check_output(['docker', 'run',
        '-v', '%s:/srv/near' % home_dir, '-it',
        image, 'near', '--home=/srv/near', 'init'] + init_flags)


"""Inits the node configuration using local build."""
def local_init(home_dir, is_release, init_flags):
    target = './target/%s/near' % ('release' if is_release else 'debug')
    subprocess.call([target,
        '--home=%s' % home_dir, 'init'] + init_flags)


"""Retrieve requested chain id from the flags."""
def get_chain_id_from_flags(flags):
    chain_id_flags = [flag for flag in flags if flag.startswith('--chain-id=')]
    if len(chain_id_flags) == 1:
        return chain_id_flags[0][len('--chain-id='):]
    return ''


"""Checks if there is already everything setup on this machine, otherwise sets up NEAR node."""
def check_and_setup(is_local, is_release, image, home_dir, init_flags):
    if is_local:
        subprocess.call([os.path.expanduser('~/.cargo/bin/rustup'),
            'default', 'nightly'])
        flags = ['-p', 'near']
        if is_release:
            flags = ['--release'] + flags
        code = subprocess.call(
            [os.path.expanduser('~/.cargo/bin/cargo'), 'build'] + flags)
        if code != 0:
            print("Compilation failed, aborting")
            exit(code)

    chain_id = get_chain_id_from_flags(init_flags)
    if os.path.exists(os.path.join(home_dir, 'config.json')):
        print("")
        genesis_config = json.loads(open(os.path.join(os.path.join(home_dir, 'genesis.json'))).read())
        print(chain_id)
        if chain_id !='' and genesis_config['chain_id'] != chain_id:
            if chain_id == 'testnet':
                print("Folder %s already has network configuration for %s, which is not the official TestNet.\n"
                      "Use ./scripts/start_localnet.py instead to keep running with existing configuration.\n"
                      "If you want to run a different network, either specify different --home or remove %s to start from scratch." % (home_dir, genesis_config['chain_id'], home_dir))
            elif genesis_config['chain_id'] == 'testnet':
                print("Folder %s already has network configuration for the official TestNet.\n"
                      "Use ./scripts/start_testnet.py instead to keep running it.\n"
                      "If you want to run a different network, either specify different --home or remove %s to start from scratch" % (home_dir, home_dir))
            elif chain_id != '':
                print("Folder %s already has network configuration for %s. Use ./scripts/start_localnet.py to continue running it." % (home_dir, genesis_config['chain_id']))
            exit(1)
        print("Using existing node configuration from %s for %s" % (home_dir, genesis_config['chain_id']))
        return

    print("Setting up network configuration.")
    if len([x for x in init_flags if x.startswith('--account-id')]) == 0:
        account_id = input("Enter your account ID (leave empty if not going to be a validator): ")
        init_flags.append('--account-id=%s' % account_id)
    if is_local:
        local_init(home_dir, is_release, init_flags)
    else:
        docker_init(image, home_dir, init_flags)


def print_staking_key(home_dir):
    key_path = os.path.join(home_dir, 'validator_key.json')
    if not os.path.exists(key_path):
        return

    key_file = json.loads(open(key_path).read())
    if not key_file['account_id']:
        print("Node is not staking. Re-run init to specify staking account.")
        return
    print("Stake for user '%s' with '%s'" % (key_file['account_id'], key_file['public_key']))


"""Stops given docker container."""
def docker_stop_if_exists(name):
    try:
        result = subprocess.check_output(['docker', 'ps', '-f', 'name=%s' % name])
        if name.encode('utf-8') in result:
            subprocess.check_output(['docker', 'stop', name])
            subprocess.check_output(['docker', 'rm', name])
    except subprocess.CalledProcessError:
        pass

"""Checks the ports saved in config.json"""
def get_port(home_dir, net):
    config = json.load(open(os.path.join(home_dir, 'config.json')))
    p = config[net]['addr'][config[net]['addr'].find(':') + 1:]
    return p + ":" + p

"""Runs NEAR core inside the docker container for isolation and easy update with Watchtower."""
def run_docker(image, home_dir, boot_nodes, verbose):
    print("Starting NEAR client and Watchtower dockers...")
    docker_stop_if_exists('watchtower')
    docker_stop_if_exists('nearcore')
    # Start nearcore container, mapping home folder and ports.
    envs = ['-e', 'BOOT_NODES=%s' % boot_nodes]
    rpc_port = get_port(home_dir, 'rpc')
    network_port = get_port(home_dir, 'network')
    if verbose:
        envs.extend(['-e', 'VERBOSE=1'])
    subprocess.check_output(['docker', 'run',
                    '-d', '-p', rpc_port, '-p', network_port, '-v', '%s:/srv/near' % home_dir,
                    '--name', 'nearcore', '--restart', 'unless-stopped'] + 
                    envs + [image])
    # Start Watchtower that will automatically update the nearcore container when new version appears.
    subprocess.check_output(['docker', 'run',
                    '-d', '--restart', 'unless-stopped', '--name', 'watchtower',
                    '-v', '/var/run/docker.sock:/var/run/docker.sock',
                    'v2tec/watchtower', image])
    print("Node is running! \nTo check logs call: docker logs --follow nearcore")

"""Runs NEAR core locally."""
def run_local(home_dir, is_release, boot_nodes, verbose):
    print("Starting NEAR client...")
    print("Autoupdate is not supported at the moment for local run")
    cmd = ['./target/%s/near' % ('release' if is_release else 'debug')]
    cmd.extend(['--home', home_dir])
    if verbose:
        cmd.append('--verbose')
    cmd.append('run')
    cmd.extend(['--boot-nodes=%s' % boot_nodes])
    try:
        subprocess.call(cmd)
    except KeyboardInterrupt:
        print("\nStopping NEARCore.")


def setup_and_run(local, is_release, image, home_dir, init_flags, boot_nodes, verbose=False):
    if local:
        install_cargo()
    else:
        try:
            subprocess.check_output(['docker', 'pull', image])
            subprocess.check_output(['docker', 'pull', 'v2tec/watchtower'])
        except subprocess.CalledProcessError as exc:
            print("Failed to fetch docker containers: %s" % exc)
            exit(1)

    check_and_setup(local, is_release, image, home_dir, init_flags)

    print_staking_key(home_dir)

    if not local:
        run_docker(image, home_dir, boot_nodes, verbose)
    else:
        run_local(home_dir, is_release, boot_nodes, verbose)


"""Stops docker for Nearcore and watchtower if they are running."""
def stop_docker():
    docker_stop_if_exists('watchtower')
    docker_stop_if_exists('nearcore')

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
    subprocess.call(['docker', 'run',
        '-v', '%s:/srv/near' % home_dir, '-it',
        image, 'near', '--home=/srv/near', 'init'] + init_flags)


"""Inits the node configuration using local build."""
def local_init(home_dir, is_release, init_flags):
    target = './target/%s/near' % ('release' if is_release else 'debug')
    subprocess.call([target,
        '--home=%s' % home_dir, 'init'] + init_flags)


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

    if os.path.exists(os.path.join(home_dir, 'config.json')):
        genesis_config = json.loads(open(os.path.join(os.path.join(home_dir, 'genesis.json'))).read())
        if genesis_config['chain_id'] != 'testnet':
            print("Folder %s already has network configuration for %s, which is not the official TestNet." % (home_dir, genesis_config['chain_id']))
            exit(1)
        print("Using existing node configuration: %s" % home_dir)
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


"""Runs NEAR core inside the docker container for isolation and easy update with Watchtower."""
def run_docker(image, home_dir, boot_nodes, verbose):
    print("Starting NEAR client and Watchtower dockers...")
    subprocess.call(['docker', 'stop', 'watchtower'])
    subprocess.call(['docker', 'rm', 'watchtower'])
    subprocess.call(['docker', 'stop', 'nearcore'])
    subprocess.call(['docker', 'rm', 'nearcore'])
    # Start nearcore container, mapping home folder and ports.
    envs = ['-e', 'BOOT_NODES=%s' % boot_nodes]
    if verbose:
        envs.extend(['-e', 'VERBOSE=1'])
    subprocess.call(['docker', 'run',
                    '-d', '--network=host', '-v', '%s:/srv/near' % home_dir,
                    '--name', 'nearcore', '--restart', 'unless-stopped'] + 
                    envs + [image])
    # Start Watchtower that will automatically update the nearcore container when new version appears.
    subprocess.call(['docker', 'run',
                    '-d', '--restart', 'unless-stopped', '--name', 'watchtower',
                    '-v', '/var/run/docker.sock:/var/run/docker.sock',
                    'v2tec/watchtower', image])


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
    subprocess.call(cmd)


def setup_and_run(local, is_release, image, home_dir, init_flags, boot_nodes, verbose=False):
    if local:
        install_cargo()
    else:
        subprocess.call(['docker', 'pull', image])
        subprocess.call(['docker', 'pull', 'v2tec/watchtower'])

    check_and_setup(local, is_release, image, home_dir, init_flags)

    print_staking_key(home_dir)

    if not local:
        run_docker(image, home_dir, boot_nodes, verbose)
    else:
        run_local(home_dir, is_release, boot_nodes, verbose)

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
def docker_init(image, home_dir, account_id):
    subprocess.call(['docker', 'run',
        '-v', '%s:/srv/near' % home_dir, '-it',
        image, 'near', '--home=/srv/near', 'init', '--chain-id=testnet', 
        '--account-id=%s' % account_id])


"""Inits the node configuration using local build."""
def local_init(home_dir, account_id):
    subprocess.call(['targete/release/near',
        '--home=%s' % home_dir, 'init', '--chain-id=testnet',
        '--account-id=%s' % account_id])


"""Checks if there is already everything setup on this machine, otherwise sets up NEAR node."""
def check_and_setup(is_local, image, home_dir):
    if is_local:
        subprocess.call([os.path.expanduser('~/.cargo/bin/rustup'),
            'default', 'nightly'])
        subprocess.call([os.path.expanduser('~/.cargo/bin/cargo'),
            'build', '--release', '-p', 'near'])

    if os.path.exists(os.path.join(home_dir, 'config.json')):
        genesis_config = json.loads(open(os.path.join(os.path.join(home_dir, 'genesis.json'))).read())
        if genesis_config['chain_id'] != 'testnet':
            print("Folder %s already has network configuration for %s, which is not the official TestNet." % (home_dir, genesis_config['chain_id']))
            exit(1)
        print("Using existing node configuration: %s" % home_dir)
        return

    print("Setting up TestNet configuration.")
    account_id = input("Enter your account ID (leave empty if not going to be a validator): ")
    if is_local:
        local_init(home_dir, account_id)
    else:
        docker_init(image, home_dir, account_id)
        

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
def run_docker(image, home_dir):
    print("Starting NEAR client and Watchtower dockers...")
    subprocess.call(['docker', 'stop', 'watchtower'])
    subprocess.call(['docker', 'rm', 'watchtower'])
    subprocess.call(['docker', 'stop', 'nearcore'])
    subprocess.call(['docker', 'rm', 'nearcore'])
    # Start nearcore container, mapping home folder and ports.
    subprocess.call(['docker', 'run', 
                    '-d', '-p', '3030:3030', '-p', '26656:26656', '-v', '%s:/srv/near' % home_dir, 
                    '--name', 'nearcore', '--restart', 'unless-stopped', image])
    # Start Watchtower that will automatically update the nearcore container when new version appears.
    subprocess.call(['docker', 'run', 
                    '-d', '--restart', 'unless-stopped', '--name', 'watchtower', 
                    '-v', '/var/run/docker.sock:/var/run/docker.sock',
                    'v2tec/watchtower', image])


"""Runs NEAR core locally."""
def run_local(home_dir):
    print("Starting NEAR client...")
    print("Autoupdate is not supported at the moment for local run")
    subprocess.call(['./target/release/near', '--home', home_dir, 'run'])


if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Official TestNet *")
    print("****************************************************")

    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', help='If set, runs in the local version instead of auto-updatable docker. Otherwise runs locally')
    parser.add_argument('--home', default=os.path.expanduser('~/.near/'), help='Home path for storing configs, keys and chain data (Default: ~/.near)')
    parser.add_argument('--image', default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    args = parser.parse_args()
    home_dir = args.home

    if args.local:
        install_cargo()
    else:
        subprocess.call(['docker', 'pull', args.image])
        subprocess.call(['docker', 'pull', 'v2tec/watchtower'])

    check_and_setup(args.local, args.image, home_dir)

    print_staking_key(home_dir)
    
    if not args.local:
        run_docker(args.image, home_dir)
    else:
        run_local(home_dir)

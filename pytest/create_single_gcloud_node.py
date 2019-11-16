#!/usr/bin/env python
import argparse
from rc import gcloud, go, as_completed

AVAILABLE_VCPUS = ['1', '2', '4', '8', '16', '32', '64', '96']

def create_gcloud_node(*, name, checkout, cpu):
    print(f'Creating machine {name} of {cpu} vCPUs...')
    machine = gcloud.create(
        name=name,
        machine_type=f'n1-standard-{cpu}',
        disk_size='200G', 
        image_project='ubuntu-os-cloud',
        image_family='ubuntu-1804-lts',
        zone='us-west2-a',
        preemptible=False,
        firewall_allows=['tcp:24567', 'tcp:3030']
    )
    print('Machine created')

    print('Installing build dependencies...')
    p = machine.run('bash', input='''
# sudo apt update sometimes silently fail (exit code 0), run 3 times to ensure success
for i in `seq 1 3`
do
sudo apt update
done

sudo apt install -y tmux pkg-config libssl-dev build-essential cmake python python3 python3-pip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-05-22

# Prepend cargo to ~/.bashrc so every non interactive shell such as machine.run('bash', ...) pickup the path too
echo -e "source $HOME/.cargo/env\n$(cat ~/.bashrc)" > ~/.bashrc
''')
    if p.returncode != 0:
        print(p.stderr)
        print("Failed to install dependencies, please try install manually")
        exit(1)
    print('Installed')

    print(f'Cloning nearcore at {checkout}')
    p = machine.run('bash', input=f'''
git clone https://github.com/nearprotocol/nearcore
cd nearcore
git checkout {checkout}
''')
    if p.returncode != 0:
        print(p.stderr)
        print("Failed to git clone or checkout, please try manually")
        exit(1)
    
    def compile():
        print(f'Compiling nearcore...')
        p = machine.run('bash', input='''
cd nearcore
cargo build -p near
    ''')
        if p.returncode != 0:
            print(p.stderr)
            print("Failed to compile, please try manually")
            return False
        print('Compilation finish.')
        return True

    def install_python_dep():
        print('Create virtualenv and install dependency for pytest')
        p = machine.run('bash', input='''
sudo pip3 install virtualenv
cd nearcore/pytest
virtualenv -p `(which python3)` venv
. venv/bin/activate
pip install -r requirements.txt
''')
        if p.returncode != 0:
            print(p.stderr)
            print("Failed to create virtualenv or install python requirements, please try manually")
            return False
        print('Create virtualenv and install pytest dependencies done')
        return True

    task1 = go(compile)
    task2 = go(install_python_dep)

    for f in as_completed([task1, task2]):
        if not f.result():
            exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default='pytest-node', help='Name of the single gcloud node')
    parser.add_argument('--checkout', default='staging', help='branch or commit to checkout')
    parser.add_argument('--cpu', default='4', help='number of vCPUs of instance, 4 is CPU of 2 core 4 threads')

    args = parser.parse_args()
    if args.cpu not in ['1', '2', '4', '8', '16', '32', '64', '96']:
        print(f'cpu must choose from {AVAILABLE_VCPUS}')
        exit(1)
    
    create_gcloud_node(name=args.name, checkout=args.checkout, cpu=args.cpu)
    

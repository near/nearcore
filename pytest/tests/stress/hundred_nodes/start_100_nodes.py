#!/usr/bin/env python3
from rc import run, gcloud, pmap
import json
import datetime
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import pathlib
import time
import tempfile
from tqdm import tqdm
import shutil
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import apply_config_changes, apply_genesis_changes
from utils import user_name

try:
    image_name = sys.argv[1]
except:
    branch = run(
        'git rev-parse --symbolic-full-name --abbrev-ref HEAD').stdout.strip()
    username = user_name()
    image_name = f'near-{branch}-{datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")}-{username}'

try:
    machine_name_prefix = sys.argv[2]
except:
    machine_name_prefix = f'pytest-node-{username}-'

genesis_time = (datetime.datetime.utcnow() -
                datetime.timedelta(hours=2)).isoformat() + 'Z'

# binary search this to observe if network forks, default is 1
block_production_time = 1

client_config_changes = {
    "consensus": {
        "min_block_production_delay": {
            "secs": block_production_time,
            "nanos": 0,
        },
        "max_block_production_delay": {
            "secs": 2 * block_production_time,
            "nanos": 0,
        },
        "max_block_wait_delay": {
            "secs": 6 * block_production_time,
            "nanos": 0,
        },
    },
    "telemetry": {
        "endpoints": [],
    }
}

# default is 50; 7,7,6,6,6,6,6,6
genesis_config_changes = [
    ["num_block_producer_seats", 100],
    ["num_block_producer_seats_per_shard", [13, 13, 13, 13, 12, 12, 12, 12]],
]

num_machines = 100

# machine 0-(k-1) run docker, machine k-100 run binary
num_docker_machines = 50

# docker image to use
docker_image = 'nearprotocol/nearcore:master'

# 25 zones, each zone 4 instances
# 5 asia, 1 australia, 5 europe, 1 canada, 13 us
zones = [
    'asia-east1-a',
    # 'asia-east1-b',
    # 'asia-east1-c',
    # 'asia-east2-a',
    # 'asia-east2-b',
    'asia-east2-c',
    # 'asia-northeast1-a',
    # 'asia-northeast1-b',
    # 'asia-northeast1-c',
    # 'asia-northeast2-a',
    # 'asia-northeast2-b',
    'asia-northeast2-c',
    'asia-south1-a',
    # 'asia-south1-b',
    # 'asia-south1-c',
    # 'asia-southeast1-a',
    # 'asia-southeast1-b',
    'asia-southeast1-c',
    'australia-southeast1-a',
    # 'australia-southeast1-b',
    # 'australia-southeast1-c',
    # 'europe-north1-a',
    # 'europe-north1-b',
    'europe-north1-c',
    # 'europe-west1-b',
    # 'europe-west1-c',
    # 'europe-west1-d',
    # 'europe-west2-a',
    # 'europe-west2-b',
    'europe-west2-c',
    'europe-west3-a',
    # 'europe-west3-c',
    # 'europe-west4-a',
    # 'europe-west4-b',
    'europe-west4-c',
    'europe-west6-a',
    # 'europe-west6-b',
    # 'europe-west6-c',
    # 'northamerica-northeast1-a',
    # 'northamerica-northeast1-b',
    'northamerica-northeast1-c',
    # 'southamerica-east1-a',
    # 'southamerica-east1-b',
    # 'southamerica-east1-c',
    'us-central1-a',
    'us-central1-b',
    # 'us-central1-c',
    'us-central1-f',
    'us-east1-b',
    'us-east1-c',
    # 'us-east1-d',
    'us-east4-a',
    'us-east4-b',
    # 'us-east4-c',
    'us-west1-a',
    'us-west1-b',
    'us-west1-c',
    'us-west2-a',
    'us-west2-b',
    'us-west2-c',
]
# Unless you want to shutdown gcloud instance and restart, keep this `False'
reserve_ip = False

pbar = tqdm(total=num_machines, desc=' create machines')


def create_machine(i):
    m = gcloud.create(name=machine_name_prefix + str(i),
                      machine_type='n1-standard-2',
                      disk_size='200G',
                      image_project='near-core',
                      image=image_name,
                      zone=zones[i % len(zones)],
                      min_cpu_platform='Intel Skylake',
                      reserve_ip=reserve_ip)
    pbar.update(1)
    return m


machines = pmap(create_machine, range(num_machines))
pbar.close()
# machines = pmap(lambda name: gcloud.get(name), [
#                 f'{machine_name_prefix}{i}' for i in range(num_machines)])

tempdir = pathlib.Path(tempfile.gettempdir()) / 'near'


def get_node_dir(i):
    node_dir = tempdir / f'node{i}'
    node_dir.mkdir(parents=True, exist_ok=True)
    return node_dir


for i in range(num_machines):
    node_dir = get_node_dir(i)
    p = run('bash',
            input=f'''
# deactivate virtualenv doesn't work in non interactive shell, explicitly run with python2
cd ..
python2 scripts/start_stakewars.py --local --home {node_dir} --init --signer-keys --account-id=node{i}
''')
    assert p.returncode == 0


# Generate csv from jsons and ips
def pk_from_file(path):
    with open(path) as rd:
        return json.load(rd)['public_key']


def get_validator_key(i):
    return pk_from_file(get_node_dir(i) / 'validator_key.json')


def get_full_pks(i):
    pks = []
    for j in range(3):
        pks.append(pk_from_file(get_node_dir(i) / f'signer{j}_key.json'))
    return ','.join(pks)


def get_pubkey(i):
    return pk_from_file(get_node_dir(i) / 'node_key.json')


with open(tempdir / 'accounts.csv', 'w', newline='') as f:
    fieldnames = 'genesis_time,account_id,regular_pks,privileged_pks,foundation_pks,full_pks,amount,is_treasury,validator_stake,validator_key,peer_info,smart_contract,lockup,vesting_start,vesting_end,vesting_cliff'.split(
        ',')

    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    amount = 1000 * 10**24
    staked_amount = 10 * 10**24

    for i in range(num_machines):
        writer.writerow({
            'genesis_time': genesis_time,
            'account_id': f'node{i}',
            'full_pks': get_full_pks(i),
            'amount': amount,
            'is_treasury': 'true' if i == 0 else 'false',
            'validator_stake': staked_amount,
            'validator_key': get_validator_key(i),
            'peer_info': f'{get_pubkey(i)}@{machines[i].ip}:24567'
        })

# Generate config and genesis locally, apply changes to config/genesis locally
for i in range(num_machines):
    node_dir = get_node_dir(i)
    shutil.copy(tempdir / 'accounts.csv', node_dir / 'accounts.csv')
    p = run('bash',
            input=f'''
cd ..
target/release/genesis-csv-to-json --home {node_dir} --chain-id pytest
''')
    apply_config_changes(node_dir, client_config_changes)
    apply_genesis_changes(node_dir, genesis_config_changes)

pbar = tqdm(total=num_machines, desc=' upload nodedir')


# Upload json and accounts.csv
def upload_genesis_files(i):
    # stop if already start
    machines[i].run('tmux send-keys -t python-rc C-c')
    time.sleep(2)
    machines[i].kill_detach_tmux()
    machines[i].run('rm -rf ~/.near')
    # upload keys, config, genesis
    machines[i].upload(str(get_node_dir(i)),
                       f'/home/{machines[i].username}/.near')
    pbar.update(1)


pmap(upload_genesis_files, range(num_machines))
pbar.close()

pbar = tqdm(total=num_machines, desc=' start near')


def start_nearcore(i):
    m = machines[i]
    if i < num_docker_machines:
        m.run('bash',
              input=f'''
docker run -d -u $UID:$UID -v /home/{m.username}/.near:/srv/near \
    -p 3030:3030 -p 24567:24567 --name nearcore {docker_image} near --home=/srv/near run
''')
    else:
        m.run_detach_tmux(
            'cd nearcore && export RUST_LOG=diagnostic=trace && target/release/near run --archive'
        )
    pbar.update(1)


pmap(start_nearcore, range(len(machines)))
pbar.close()

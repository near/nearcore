#!/usr/bin/env python3
from utils import user_name
import sys
import os
import datetime
from rc import gcloud, run
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger

additional_flags = ''

try:
    image_name = sys.argv[1]
    branch = sys.argv[2]
except:
    branch = run(
        'git rev-parse --symbolic-full-name --abbrev-ref HEAD').stdout.strip()
    username = user_name()
    image_name = f'near-{branch}-{datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")}-{username}'

machine_name = f'{image_name}-image-builder'

logger.info(f"Creating machine: {machine_name}")

m = gcloud.create(name=machine_name,
                  machine_type='n1-standard-64',
                  disk_size='50G',
                  image_project='ubuntu-os-cloud',
                  image_family='ubuntu-1804-lts',
                  zone='us-west2-c',
                  firewall_allows=['tcp:3030', 'tcp:24567'],
                  min_cpu_platform='Intel Skylake')

logger.info(f'machine created: {image_name}')

p = m.run('bash',
          input=f'''
for i in `seq 1 3`; do
    sudo apt update
done

sudo apt install -y python pkg-config libssl-dev build-essential cmake clang llvm docker.io
sudo groupadd docker
sudo usermod -aG docker $USER

curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
source ~/.cargo/env

git clone --single-branch --branch {branch} https://github.com/nearprotocol/nearcore.git nearcore
cd nearcore
cargo build -p neard --release {additional_flags}
''')

assert p.returncode == 0

logger.info('near built')

m.shutdown()

logger.info('machine stopped')

m.save_image(image=image_name)

logger.info('image saved')

m.delete()

logger.info('machine deleted')

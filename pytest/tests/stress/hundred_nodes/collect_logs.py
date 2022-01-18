#!/usr/bin/env python3
from rc import gcloud, pmap
from distutils.util import strtobool
import sys
import datetime
import pathlib
import tempfile
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger
from utils import user_name

if len(sys.argv) >= 2:
    node_prefix = sys.argv[1]
else:
    node_prefix = f'pytest-node-{user_name()}'
nodes = [
    machine for machine in gcloud.list() if machine.name.startswith(node_prefix)
]

if len(sys.argv) >= 3:
    log_file = sys.argv[2]
else:
    log_file = pathlib.Path(tempfile.gettempdir()) / 'python-rc.log'

collected_place = (
    pathlib.Path(tempfile.gettempdir()) / 'near' /
    f'collected_logs_{datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")}'
)
collected_place.mkdir(parents=True, exist_ok=True)


def collect_file(node):
    logger.info(f'Download file from {node.name}')
    node.download(str(log_file), str(collected_place / f'{node.name}.txt'))
    logger.info(f'Download file from {node.name} finished')


pmap(collect_file, nodes)
logger.info(f'All download finish, log collected at {collected_place}')

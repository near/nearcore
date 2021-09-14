#!/usr/bin/env python3

# When script exit with traceback, remote node is not deleted. This script is
# to delete remote machines so test can be rerun
# DANGER: make sure not delete production nodes!

from rc import gcloud, pmap
from distutils.util import strtobool
from configured_logger import logger
import sys

sys.path.append('lib')
from utils import user_name

machines = gcloud.list()
to_delete_prefix = sys.argv[1] if len(
    sys.argv) >= 2 else f"pytest-node-{user_name()}-"
to_delete = list(filter(lambda m: m.name.startswith(to_delete_prefix),
                        machines))

if to_delete:
    a = input(
        f"going to delete {list(map(lambda m: m.name, to_delete))}\ny/n: ")
    if strtobool(a):

        def delete_machine(m):
            logger.info(f'deleting {m.name}')
            m.delete()
            logger.info(f'{m.name} deleted')

        pmap(delete_machine, to_delete)

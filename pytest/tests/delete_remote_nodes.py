#!/usr/bin/env python

# When script exit with traceback, remote node is not deleted. This script is
# to delete remote machines so test can be rerun
# DANGER: make sure not delete production nodes!

from rc import gcloud, pmap
from distutils.util import strtobool


machines = gcloud.list()
to_delete = list(filter(lambda m: m.name.startswith("pytest-node"), machines))

if to_delete:
    a = input(f"going to delete {list(map(lambda m: m.name, to_delete))}\ny/n: ")
    if strtobool(a):
        def delete_machine(m):
            print(f'deleting {m.name}')
            m.delete()
            print(f'{m.name} deleted')

        pmap(delete_machine, to_delete)
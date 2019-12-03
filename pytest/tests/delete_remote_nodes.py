#!/usr/bin/env python

# When script exit with traceback, remote node is not deleted. This script is
# to delete remote machines so test can be rerun
# DANGER: make sure not delete production nodes!

from rc import gcloud, pmap

to_delete = [f'near-pytest-{i}' for i in range(10)]
def delete_machine(machine_name):
    m = gcloud.get(machine_name)
    if m:
        m.delete()

pmap(delete_machine, to_delete)
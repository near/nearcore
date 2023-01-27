import sys
from utils import display_table, run_on_machine


def install_jq(project, host, user='ubuntu'):
    run_on_machine("sudo apt-get install jq -y", user, host, project)


def get_canonical_md5sum(project, host, user='ubuntu'):
    install_jq(project, host, user)
    return run_on_machine(
        "jq --sort-keys . ~/.near/config.json | md5sum",
        user, host, project
    )


def display_hashes(names, hashes):
    rows = sorted(zip(names, hashes), key=lambda x: x[1])
    display_table([("name", "hash")] + rows)


"""
The first argument is project.
Next arguments are host names.
Shows an md5sum of a sorted by keys config.
"""
if __name__ == '__main__':
    project = sys.argv[1]
    hosts = sys.argv[2:]
    md5sums = [get_canonical_md5sum(project, host) for host in hosts]
    display_hashes(hosts, md5sums)

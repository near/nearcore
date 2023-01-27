import sys
from utils import display_table, run_on_machine


def get_neard_info(project, host, user='ubuntu'):
    return run_on_machine("./neard -V", user, host, project)


def display_neard_info(hosts, neard_info, user='ubuntu'):
    display_table([[host] + neard_info.split(' ')[1:]
                   for (host, neard_info) in zip(hosts, neard_infos)])


"""
The first argument is project.
Next arguments are host names.
"""
if __name__ == '__main__':
    project = sys.argv[1]
    hosts = sys.argv[2:]
    neard_infos = [get_neard_info(project, host) for host in hosts]
    display_neard_info(hosts, neard_infos)

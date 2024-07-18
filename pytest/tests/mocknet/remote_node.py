#!/usr/bin/env python3
"""
defines the RemoteNeardRunner class meant to be interacted with over ssh
"""
import pathlib
import json
import os
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cmd_utils
from node_handle import NodeHandle
import mocknet


class RemoteNeardRunner:

    def __init__(self, node, neard_runner_home):
        self.node = node
        self.neard_runner_home = neard_runner_home

    def name(self):
        return self.node.instance_name

    def ip_addr(self):
        return self.node.machine.ip

    def neard_port(self):
        return 3030

    def init(self):
        cmd_utils.init_node(self.node)

    def mk_neard_runner_home(self, remove_home_dir):
        if remove_home_dir:
            cmd_utils.run_cmd(
                self.node,
                f'rm -rf {self.neard_runner_home} && mkdir -p {self.neard_runner_home}'
            )
        else:
            cmd_utils.run_cmd(self.node, f'mkdir -p {self.neard_runner_home}')

    def upload_neard_runner(self):
        self.node.machine.upload('tests/mocknet/helpers/neard_runner.py',
                                 self.neard_runner_home,
                                 switch_user='ubuntu')
        self.node.machine.upload('tests/mocknet/helpers/requirements.txt',
                                 self.neard_runner_home,
                                 switch_user='ubuntu')

    def upload_neard_runner_config(self, config):
        mocknet.upload_json(self.node,
                            os.path.join(self.neard_runner_home, 'config.json'),
                            config)

    def run_cmd(self, cmd, raise_on_fail=False, return_on_fail=False):
        r = cmd_utils.run_cmd(self.node, cmd, raise_on_fail, return_on_fail)
        return r

    def init_python(self):
        cmd = f'cd {self.neard_runner_home} && python3 -m virtualenv venv -p $(which python3)' \
        ' && ./venv/bin/pip install -r requirements.txt'
        cmd_utils.run_cmd(self.node, cmd)

    def update_python(self):
        cmd = f'cd {self.neard_runner_home} && ./venv/bin/pip install -r requirements.txt'
        cmd_utils.run_cmd(self.node, cmd)

    def stop_neard_runner(self):
        # this looks for python processes with neard_runner.py in the command line. the first word will
        # be the pid, which we extract with the last awk command
        self.node.machine.run(
            'kill $(ps -C python -o pid=,cmd= | grep neard_runner.py | awk \'{print $1};\')'
        )

    def start_neard_runner(self):
        cmd_utils.run_in_background(self.node, f'{os.path.join(self.neard_runner_home, "venv/bin/python")} {os.path.join(self.neard_runner_home, "neard_runner.py")} ' \
            f'--home {self.neard_runner_home} --neard-home /home/ubuntu/.near ' \
            '--neard-logs /home/ubuntu/neard-logs --port 3000', 'neard-runner.txt')

    def neard_runner_post(self, body):
        body = json.dumps(body)
        # '"'"' will be interpreted as ending the first quote and then concatenating it with "'",
        # followed by a new quote started with ' and the rest of the string, to get any single quotes
        # in method or params into the command correctly
        body = body.replace("'", "'\"'\"'")
        r = cmd_utils.run_cmd(self.node, f'curl localhost:3000 -d \'{body}\'')
        return json.loads(r.stdout)

    def new_test_params(self):
        return []

    def get_validators(self):
        return self.node.get_validators()


def get_nodes(mocknet_id: str):
    all_nodes = mocknet.get_nodes(pattern=mocknet_id)
    if len(all_nodes) < 1:
        sys.exit(f'no known nodes matching {mocknet_id}')

    traffic_generator = None
    nodes = []
    for n in all_nodes:
        if n.instance_name.endswith('traffic'):
            if traffic_generator is not None:
                sys.exit(
                    f'more than one traffic generator instance found. {traffic_generator.instance_name} and {n.instance_name}'
                )
            traffic_generator = n
        else:
            nodes.append(n)

    if traffic_generator is None:
        sys.exit(f'no traffic generator instance found')
    # Here we want the neard-runner home dir to be on the same disk as the target data dir since
    # we'll be making backups on that disk.
    traffic_target_home = cmd_utils.run_cmd(
        traffic_generator,
        'cat /proc/mounts | grep "/home/ubuntu/.near" | grep -v "source" | head -n 1 | awk \'{print $2};\''
    ).stdout.strip()
    # On Locust traffic generators, we don't need the data disk as we will not mirror the traffic.
    if not traffic_target_home:
        traffic_target_home = "/home/ubuntu/.near"
    traffic_runner_home = os.path.join(traffic_target_home, 'neard-runner')
    return NodeHandle(RemoteNeardRunner(traffic_generator, traffic_runner_home),
                      can_validate=False), [
                          NodeHandle(
                              RemoteNeardRunner(
                                  node, '/home/ubuntu/.near/neard-runner'))
                          for node in nodes
                      ]

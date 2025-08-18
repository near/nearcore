#!/usr/bin/env python3
"""
defines the RemoteNeardRunner class meant to be interacted with over ssh
"""
import pathlib
import json
import os
import sys
import re
from functools import wraps
from typing import Optional

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cmd_utils
from node_handle import NodeHandle
import mocknet
from utils import ScheduleContext

from configured_logger import logger

# cspell:words btrfs subvolume subvol


class RemoteNeardRunner:

    def __init__(self, node, neard_runner_home):
        self.node = node
        self.neard_runner_home = neard_runner_home

    def name(self):
        return self.node.instance_name

    def get_label(self, label_name: str) -> str:
        return self.node.get_label(label_name)

    def ip_addr(self):
        return self.node.machine.ip

    def neard_port(self):
        return 3030

    def init(self):
        cmd_utils.init_node(self.node)

    def mk_neard_runner_home(self, remove_home_dir):
        cmd = f'mkdir -p {self.neard_runner_home}'
        if remove_home_dir:
            cmd = f'rm -rf {self.neard_runner_home} && {cmd}'
        cmd_utils.run_cmd(self.node, cmd)

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

    def run_cmd(self,
                schedule_ctx: Optional[ScheduleContext],
                cmd,
                raise_on_fail=False,
                return_on_fail=False):
        if schedule_ctx is None:
            r = cmd_utils.run_cmd(self.node, cmd, raise_on_fail, return_on_fail)
        else:
            r = cmd_utils.schedule_cmd(self.node, cmd, schedule_ctx,
                                       raise_on_fail, return_on_fail)
        return r

    def upload_file(self, src, dst):
        self.node.machine.upload(src, dst, switch_user='ubuntu')

    def download_file(self, src, dst):
        self.node.machine.download(src, dst)

    def init_python(self):
        cmd = f'cd {self.neard_runner_home} && python3 -m virtualenv venv -p $(which python3)' \
        ' && ./venv/bin/pip install -r requirements.txt'
        cmd_utils.run_cmd(self.node, cmd)

    def update_python(self):
        cmd = f'cd {self.neard_runner_home} && ./venv/bin/pip install -r requirements.txt'
        cmd_utils.run_cmd(self.node, cmd)

    def stop_neard_runner(self):
        self.node.machine.run('sudo systemctl stop neard-runner;\
                               sudo systemctl reset-failed neard-runner')

    def start_neard_runner(self):
        USER = 'ubuntu'
        NEARD_RUNNER_CMD = f'{self.neard_runner_home}/venv/bin/python {self.neard_runner_home}/neard_runner.py\
            --home {self.neard_runner_home}\
            --neard-home "/home/ubuntu/.near"\
            --neard-logs-dir "/home/ubuntu/neard-logs"\
            --port 3000'

        SYSTEMD_RUN_NEARD_RUNNER_CMD = f'sudo systemd-run -u neard-runner \
            --uid={USER} \
            --property=StartLimitIntervalSec=500\
            --property=StartLimitBurst=10\
            --property=DefaultDependencies=no\
            --property=TimeoutStartSec=300\
            --property=Restart=always\
            --property=RestartSec=5s\
            -- {NEARD_RUNNER_CMD}'

        self.node.machine.run(SYSTEMD_RUN_NEARD_RUNNER_CMD)

    def neard_runner_post(self, schedule_ctx: Optional[ScheduleContext], body):
        body = json.dumps(body)
        # '"'"' will be interpreted as ending the first quote and then concatenating it with "'",
        # followed by a new quote started with ' and the rest of the string, to get any single quotes
        # in method or params into the command correctly
        body = body.replace("'", "'\"'\"'")
        cmd = f'curl localhost:3000 -d \'{body}\''
        if schedule_ctx is not None:
            r = cmd_utils.schedule_cmd(self.node, cmd, schedule_ctx)
            logger.info('{0}:\nstdout:\n{1.stdout}\nstderr:\n{1.stderr}'.format(
                self.name(), r))
            return {'result': r}
        r = cmd_utils.run_cmd(self.node, cmd)
        return json.loads(r.stdout)

    def new_test_params(self):
        return []

    def get_validators(self):
        return self.node.get_validators()

    def make_snapshot(self, snapshot_id):
        # Make a snapshot of the current default subvolume
        if not re.match(r'^[a-zA-Z0-9_-]+$', snapshot_id):
            raise ValueError(f'Invalid snapshot id: {snapshot_id}')
        cmd = f"""sudo btrfs subvolume snapshot -r /mnt/btrfs-root/$(sudo btrfs subvolume get-default /mnt/btrfs-root | awk '{{print $NF}}') /mnt/btrfs-root/{snapshot_id}"""
        return self.node.machine.run(cmd)

    def restore_snapshot(self, snapshot_id):
        if not re.match(r'^[a-zA-Z0-9_-]+$', snapshot_id):
            raise ValueError(f'Invalid snapshot id: {snapshot_id}')
        # check if snapshot exists
        check_snapshot_cmd = f"""sudo btrfs subvolume show /mnt/btrfs-root/{snapshot_id} > /dev/null"""
        # stop neard-runner if it is running
        stop_neard_runner_cmd = """systemctl is-active --quiet neard-runner && sudo systemctl stop neard-runner"""
        # get current default subvolume
        get_default_cmd = """OLD=$(sudo btrfs subvolume get-default /mnt/btrfs-root | awk '{print $NF}')"""
        # umount old snapshot
        umount_cmd = """mount | grep "subvol=/$OLD" | awk '{print $3}' | xargs -r sudo umount"""
        # reset default subvolume
        reset_default_cmd = """sudo btrfs subvolume set-default 5 /mnt/btrfs-root"""
        # delete old snapshot
        delete_old_cmd = """sudo btrfs subvolume delete /mnt/btrfs-root/$OLD"""
        # restore snapshot
        restore_cmd = f"""sudo btrfs subvolume snapshot /mnt/btrfs-root/{snapshot_id} /mnt/btrfs-root/current"""
        # set default subvolume
        set_default_cmd = """sudo btrfs subvolume set-default $(sudo btrfs subvolume list /mnt/btrfs-root | grep 'path current$' | awk '{print $2}') /mnt/btrfs-root"""
        # mount it back
        mount_cmd = f"""sudo mount -a"""
        cmd = f"""{check_snapshot_cmd} && ({stop_neard_runner_cmd} ; {get_default_cmd} && {umount_cmd} && {reset_default_cmd} && {delete_old_cmd} && {restore_cmd} && {set_default_cmd} && {mount_cmd})"""
        return self.node.machine.run(cmd)

    def list_snapshots(self):
        cmd = """sudo btrfs subvolume list /mnt/btrfs-root/ -sr | awk '{print $14, "@", $11, $12}'"""
        return self.node.machine.run(cmd)

    def delete_snapshot(self, snapshot_id):
        if not re.match(r'^[a-zA-Z0-9_-]+$', snapshot_id):
            raise ValueError(f'Invalid snapshot id: {snapshot_id}')
        cmd = f'sudo btrfs subvolume delete "/mnt/btrfs-root/{snapshot_id}"'
        return self.node.machine.run(cmd)


def get_traffic_generator_handle(traffic_generator):
    if traffic_generator is None:
        return None

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
                      can_validate=False)


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

    return get_traffic_generator_handle(traffic_generator), [
        NodeHandle(RemoteNeardRunner(node, '/home/ubuntu/.near/neard-runner'))
        for node in nodes
    ]

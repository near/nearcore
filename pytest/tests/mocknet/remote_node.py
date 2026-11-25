#!/usr/bin/env python3
"""
defines the RemoteNode class meant to be interacted with over ssh
"""
import pathlib
import os
import sys
import re
from typing import Optional

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cmd_utils
from node_handle import NodeHandle
import mocknet
from utils import ScheduleContext

# cspell:words btrfs subvolume subvol

FORKNET_DIR = 'forknet'
NODE_NEAR_HOME = '/home/ubuntu/.near'
TRAFFIC_NEAR_HOME = '/home/ubuntu/.near/target'
TRAFFIC_SOURCE_HOME = '/home/ubuntu/.near/source'


class RemoteNode:
    """
    A forknet host. neard runs there as the `neard` systemd user unit, and the
    scripts in helpers/host/ manage it. They live in `forknet_home`.
    """

    def __init__(self, node, forknet_home, is_traffic=False):
        self.node = node
        self.forknet_home = forknet_home
        self.is_traffic = is_traffic
        self.near_home = TRAFFIC_NEAR_HOME if is_traffic else NODE_NEAR_HOME

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

    def host_env(self, run_id, mocknet_store_path):
        """Values of host.env, which the scripts in helpers/host/ source."""
        return {
            'NEAR_HOME': self.near_home,
            'SOURCE_HOME': TRAFFIC_SOURCE_HOME,
            'IS_TRAFFIC': int(self.is_traffic),
            'RUN_ID': run_id,
            'MOCKNET_STORE_PATH': mocknet_store_path,
        }

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
        # Also stop the setup and reset units, which use the disk too. They
        # exist only while they run.
        stop_neard_cmd = """systemctl --user stop neard-setup 2>/dev/null ; systemctl --user stop neard-reset 2>/dev/null ; systemctl --user stop neard"""
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
        cmd = f"""{check_snapshot_cmd} && ({stop_neard_cmd} ; {get_default_cmd} && {umount_cmd} && {reset_default_cmd} && {delete_old_cmd} && {restore_cmd} && {set_default_cmd} && {mount_cmd})"""
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

    # Keep the forknet home on the same disk as the target data dir, since the
    # start backup is made there with hard links.
    traffic_target_home = cmd_utils.run_cmd(
        traffic_generator,
        'cat /proc/mounts | grep "/home/ubuntu/.near" | grep -v "source" | head -n 1 | awk \'{print $2};\''
    ).stdout.strip()
    # On Locust traffic generators, we don't need the data disk as we will not mirror the traffic.
    if not traffic_target_home:
        traffic_target_home = NODE_NEAR_HOME
    return NodeHandle(RemoteNode(traffic_generator,
                                 os.path.join(traffic_target_home, FORKNET_DIR),
                                 is_traffic=True),
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
        NodeHandle(RemoteNode(node, os.path.join(NODE_NEAR_HOME, FORKNET_DIR)))
        for node in nodes
    ]

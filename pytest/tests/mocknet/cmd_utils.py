"""
Command utilities for the mocknet.

This module provides utilities for executing commands on remote mocknet nodes.
"""
import sys
import base64
import datetime
from typing import Optional
from utils import ScheduleContext

LOG_DIR = "/home/ubuntu/logs"
STATUS_DIR = f"{LOG_DIR}/status"


def run_cmd(node, cmd, raise_on_fail=False, return_on_fail=False):
    r = node.machine.run(cmd)
    if r.exitcode != 0:
        msg = (
            f"failed running {cmd} on {node.instance_name}:\n"
            f"stdout: {r.stdout}\n"
            f"stderr: {r.stderr}"
        )
        if return_on_fail:
            return r
        if raise_on_fail:
            raise RuntimeError(msg)
        sys.exit(msg)
    return r


def schedule_cmd(
    node,
    cmd,
    schedule_ctx: ScheduleContext,
    raise_on_fail=False,
    return_on_fail=False,
):
    cmd_b64 = base64.b64encode(cmd.encode("utf-8")).decode("utf-8")
    scheduled_action = f'$(echo {cmd_b64} | base64 -d)'
    unit_name = f"mocknet-{schedule_ctx.id}"
    scheduled_cmd = (
        f'systemd-run --user --same-dir '
        f'{schedule_ctx.schedule.get_systemd_time_spec()} '
        f'--timer-property=AccuracySec=100ms --unit {unit_name} '
        f'sh -c "{scheduled_action}"'
    )
    return run_cmd(node, scheduled_cmd, raise_on_fail, return_on_fail)


def run_in_background(node, cmd, log_filename, env="", pre_cmd=None):
    setup_cmd = (
        f"truncate --size 0 {STATUS_DIR}/{log_filename} && "
        f"for i in {{8..0}}; do "
        f"if [ -f {LOG_DIR}/{log_filename}.$i ]; then "
        f"mv {LOG_DIR}/{log_filename}.$i {LOG_DIR}/{log_filename}.$((i+1)); "
        f"fi; done"
    )
    if pre_cmd is not None:
        pre_cmd += " && "
    else:
        pre_cmd = ""
    run_cmd(
        node,
        f"( {pre_cmd}{setup_cmd} && {env} nohup {cmd} "
        f"> {LOG_DIR}/{log_filename}.0 2>&1; nohup echo \"$?\" ) "
        f"> {STATUS_DIR}/{log_filename} 2>&1 &",
    )


def init_node(node):
    run_cmd(node, f"mkdir -p {LOG_DIR} && mkdir -p {STATUS_DIR}")
    # enable linger for ubuntu so that systemd-run --user works
    run_cmd(node, "sudo loginctl enable-linger ubuntu")

import sys

LOG_DIR = '/home/ubuntu/logs'
STATUS_DIR = '/home/ubuntu/logs/status'


def run_cmd(node, cmd, raise_on_fail=False, return_on_fail=False):
    r = node.machine.run(cmd)
    if r.exitcode != 0:
        msg = f'failed running {cmd} on {node.instance_name}:\nstdout: {r.stdout}\nstderr: {r.stderr}'
        if return_on_fail:
            return r
        if raise_on_fail:
            raise msg
        sys.exit(msg)
    return r


def run_in_background(node, cmd, log_filename, env='', pre_cmd=None):
    setup_cmd = f'truncate --size 0 {STATUS_DIR}/{log_filename} '
    setup_cmd += f'&& for i in {{8..0}}; do if [ -f {LOG_DIR}/{log_filename}.$i ]; then mv {LOG_DIR}/{log_filename}.$i {LOG_DIR}/{log_filename}.$((i+1)); fi done'
    if pre_cmd is not None:
        pre_cmd += ' && '
    else:
        pre_cmd = ''
    run_cmd(
        node,
        f'( {pre_cmd}{setup_cmd} && {env} nohup {cmd} > {LOG_DIR}/{log_filename}.0 2>&1; nohup echo "$?" ) > {STATUS_DIR}/{log_filename} 2>&1 &'
    )


def init_node(node):
    run_cmd(node, f'mkdir -p {LOG_DIR} && mkdir -p {STATUS_DIR}')

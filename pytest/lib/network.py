import subprocess, sys


def _run_process(cmd):
    process = subprocess.Popen(cmd,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    out, err = process.communicate()
    return (process.returncode, out, err)


def init_network_pillager():
    _run_process(["mkdir", "-p", "/sys/fs/cgroup/net_cls/block"])
    try:
        with open("/sys/fs/cgroup/net_cls/block/net_cls.classid", 'w') as f:
            f.write("42")
    except IOError as e:
        if e[0] == 13:
            print(
                "Failed to modify `/sys/fs/cgroup/net_cls/block/net_cls.classid`."
            )
            print(
                "Make sure the current user has access to it, e.g. by changing the owner:"
            )
            print("")
            print(
                "    chown <group>.<user> /sys/fs/cgroup/net_cls/block/net_cls.classid"
            )
            print("")
            sys.exit(1)
    _run_process([
        "iptables", "-A", "OUTPUT", "-m", "cgroup", "--cgroup", "42", "-j",
        "DROP"
    ])


def stop_network(pid):
    with open('/sys/fs/cgroup/net_cls/block/tasks', 'w') as f:
        f.write(str(pid))


def resume_network(pid):
    try:
        with open('/sys/fs/cgroup/net_cls/tasks', 'w') as f:
            f.write(str(pid))
    except ProcessLookupError:
        # the process was killed in the meantime
        pass


if __name__ == "__main__":
    import time
    init_network_pillager()
    handle = subprocess.Popen(["ping", "8.8.8.8"],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              universal_newlines=True)
    print(handle.pid)
    time.sleep(3)
    stop_network(handle.pid)
    time.sleep(3)
    resume_network(handle.pid)
    time.sleep(3)
    handle.kill()
    out, err = handle.communicate()
    print("STDOUT (expect ~6 entries if all goes well):")
    print(out)
    print("STDERR (expect ~3 entries if all goes well):")
    print(err)

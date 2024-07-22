import argparse
import os
import subprocess
from locust.util.timespan import parse_timespan

LOCK_FILE = "/tmp/run-ft-benchmark.lock"
REPO_DIR = os.path.expanduser("~/nearcore")


def create_lock_file(user: str) -> None:
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE, 'r') as f:
            running_user = f.read().strip()
        raise RuntimeError(f"{running_user} already running benchmark")
    with open(LOCK_FILE, 'w+') as f:
        f.write(user)


def remove_lock_file() -> None:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
    else:
        raise PermissionError("Somebody already removed the lock file!!!")


def run_benchmark(repo_dir: str, time: str, users: int, shards: int, nodes: int,
                  rump_up: int, user: str, context: str) -> None:
    benchmark_command = (
        f"./scripts/start-benchmark.sh {time} {users} {shards} {nodes} {rump_up} {user} {context}"
    )
    subprocess.run(benchmark_command, cwd=repo_dir, shell=True, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run FT benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--time',
                        type=str,
                        default='1h',
                        help="Time duration (e.g., 2h, 30m, 45s)")
    parser.add_argument('--users',
                        type=int,
                        default=1000,
                        help="Number of users")
    parser.add_argument('--shards',
                        type=int,
                        default=1,
                        help="Number of shards")
    parser.add_argument('--nodes', type=int, default=1, help="Number of nodes")
    parser.add_argument('--rump-up', type=int, default=10, help="Rump-up rate")
    parser.add_argument('--user', type=str, default='unknown', help="User name")
    parser.add_argument('--context',
                        type=str,
                        default='unknown',
                        help="Context")
    args = parser.parse_args()
    time_seconds = parse_timespan(args.time)
    try:
        create_lock_file(args.user)
        run_benchmark(REPO_DIR, time_seconds, args.users, args.shards,
                      args.nodes, args.rump_up, args.user, args.context)
    except RuntimeError as e:
        print(e)
        remove_lock_file()
    except PermissionError as e:
        print(e)


if __name__ == "__main__":
    main()

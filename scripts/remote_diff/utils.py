import subprocess
from functools import lru_cache


@lru_cache(maxsize=128)
def get_zone(project, host):
    try:
        result = subprocess.run(
            [
                "gcloud",
                "compute",
                "instances",
                "list",
                f"--project={project}",
                f"--filter=name=({host})",
                "--format=value(zone.basename())",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )

    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"Timed out while getting zone for {host}"
        )

    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        raise RuntimeError(
            f"Failed to get zone for {host} in {project}: {stderr}"
        )

    zones = result.stdout.splitlines()

    if len(zones) != 1:
        raise RuntimeError(
            f"Expected 1 zone for host {host}, got {zones}"
        )

    return zones[0]


def run_on_machine(command, user, host, project):
    zone = get_zone(project, host)

    try:
        result = subprocess.run(
            [
                "gcloud",
                "compute",
                "ssh",
                f"{user}@{host}",
                f"--project={project}",
                f"--zone={zone}",
                "--command",
                command,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=60,
        )

    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"Timed out while running command on {host}"
        )

    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        raise RuntimeError(
            f"Failed to run command on {host}: {stderr}"
        )

    return result.stdout.strip()


def display_table(rows):
    if not rows:
        return

    width = max(len(r) for r in rows)
    normalized = [
        list(map(str, row)) + [""] * (width - len(row))
        for row in rows
    ]

    widths = [
        max(len(row[i]) for row in normalized)
        for i in range(width)
    ]

    format_str = " | ".join(f"{{:<{w}}}" for w in widths)
    for row in normalized:
        print(format_str.format(*row))

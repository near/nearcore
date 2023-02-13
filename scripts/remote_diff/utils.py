import subprocess


def get_zone(project, host):
    zone_call = subprocess.run(
        [
            f"gcloud compute instances list \
        --project={project} | grep ^{host} | awk -F ' ' '{{print $2}}'"
        ],
        stdout=subprocess.PIPE,
        check=True,
        text=True,
        shell=True,
    )
    return zone_call.stdout.replace('\n', '')


def run_on_machine(command, user, host, project):
    zone = get_zone(project, host)
    call = subprocess.run(
        [
            f"gcloud compute ssh {user}@{host} --command='{command}' --project {project} --zone {zone}"
        ],
        stdout=subprocess.PIPE,
        check=True,
        text=True,
        shell=True,
    )
    return call.stdout.replace('\n', '')


def display_table(rows):
    widths = [
        max([len(str(row[i])) for row in rows]) for i in range(len(rows[0]))
    ]
    format_str = " | ".join([f"{{:<{w}}}" for w in widths])
    for row in rows:
        print(format_str.format(*row))

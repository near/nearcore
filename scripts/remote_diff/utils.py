import subprocess


def get_zone(project, host):
    zone_call = subprocess.run([
        "gcloud compute instances list \
        --project={} | grep ^{} | awk -F ' ' '{{print $2}}'"
        .format(project, host)],
        stdout=subprocess.PIPE,
        check=True,
        text=True,
        shell=True,
    )
    return zone_call.stdout.replace('\n', '')


def run_on_machine(command, user, host, project):
    zone = get_zone(project, host)
    call = subprocess.run(
        ["gcloud compute ssh {}@{} --command='{}' --project {} --zone {} "
         .format(user, host, command, project, zone)],
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
    format_str = " | ".join(["{{:<{}}}".format(w) for w in widths])
    for row in rows:
        print(format_str.format(*row))

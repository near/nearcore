import logging
import subprocess
import tarfile
import time

import toml

REPO_DIR = '/home/runner/work/nearcore/nearcore/'
G_BUCKET = 'gs://fuzzer_targets/master/'
ARCH_CONFIG_NAME = 'x86_64-unknown-linux-gnu'
BUILDER_START_TIME = time.strftime('%Y%m%d%H%M%S', time.gmtime())


def push_to_google_bucket(runner: str) -> None:
    tar_name = f'{runner}-master-{BUILDER_START_TIME}.tar.gz'
    with tarfile.open(name=tar_name, mode='w') as archiver:
        archiver.add(f"target/{ARCH_CONFIG_NAME}/release/{runner}", runner)

    _proc = subprocess.run([
        'gsutil',
        'cp',
        tar_name,
        G_BUCKET,
    ], check=True)


def main() -> None:
    crates = [
        i for i in toml.load('nightly/fuzz.toml')['target']
        if i['runner'] in ['borsh', 'serde']
    ]
    logger.debug(f"Crates from nightly/fuzz.toml: \n{crates}")

    for target in crates:
        logger.info(f"working on {target}")

        _proc = subprocess.run(
            [
                'cargo',
                '+nightly',
                'fuzz',
                'build',
                target['runner'],
            ],
            check=True,
            cwd=target['crate'],
        )

        push_to_google_bucket(target['runner'])


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig()
    main()

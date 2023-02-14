import logging
import subprocess
import tarfile
import time

import toml

REPO_DIR = '/home/runner/work/nearcore/nearcore/'
G_BUCKET = 'gs://fuzzer_targets/master/'
ARCH_CONFIG_NAME = 'x86_64-unknown-linux-gnu'
BUILDER_START_TIME = time.strftime('%Y%m%d%H%M%S', time.gmtime())
TAR_NAME = f'fuzz-targets-{BUILDER_START_TIME}.tar.gz'


def push_to_google_bucket(archive_name: str) -> None:
    try:
        subprocess.run([
            'gsutil',
            'cp',
            archive_name,
            G_BUCKET,
        ], check=True)

    except subprocess.CalledProcessError as e:
        logger.info(f"Failed to upload archive to Google Storage!")


def main() -> None:
    crates = [
        i for i in toml.load('nightly/fuzz.toml')['target']
        if i['runner'] in ['borsh', 'serde']
    ]
    logger.debug(f"Crates from nightly/fuzz.toml: \n{crates}")

    for target in crates:
        logger.info(f"working on {target}")
        runner = target['runner']
        try:
            subprocess.run(
                [
                    'cargo',
                    '+nightly',
                    'fuzz',
                    'build',
                    runner,
                ],
                check=True,
                cwd=target['crate'],
            )
            with tarfile.open(name=TAR_NAME, mode='w') as archiver:
                archiver.add(f"target/{ARCH_CONFIG_NAME}/release/{runner}",
                             runner)

        except subprocess.CalledProcessError as e:
            logger.info(f"Failed to build/archive target: {target}")

    push_to_google_bucket(TAR_NAME)


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig()
    main()

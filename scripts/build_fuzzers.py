import logging
import os
import re
import subprocess
import sys
import tarfile
import time

import toml

REPO_DIR = '/home/runner/work/nearcore/nearcore/'
G_BUCKET = 'gs://fuzzer_targets/{}/'
ARCH_CONFIG_NAME = 'x86_64-unknown-linux-gnu'
BUILDER_START_TIME = time.strftime('%Y%m%d%H%M%S', time.gmtime())
TAR_NAME = 'nearcore-{}-{}.tar.gz'
ENV = os.environ
# Default lto="fat" will cause rustc builder to use ~35GB of memory
# This reduces memory usage to <10GB
ENV.update({'RUSTFLAGS': "-C lto=thin"})


def push_to_google_bucket(archive_name: str, branch: str) -> None:
    try:
        subprocess.run(['gsutil', 'cp', archive_name,
                        G_BUCKET.format(branch)],
                       check=True)

    except subprocess.CalledProcessError as e:
        logger.info(f"Failed to upload archive to Google Storage!")


def main(branch: str) -> None:
    arch_name = TAR_NAME.format(branch, BUILDER_START_TIME)
    fuzz_bin_list = []

    crates = [i for i in toml.load('nightly/fuzz.toml')['target']]
    logger.debug(f"Crates from nightly/fuzz.toml: \n{crates}")

    for target in crates:
        logger.info(f"working on {target}")
        runner = target['runner']

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
            env=ENV,
        )
        fuzz_bin_list.append(f"target/{ARCH_CONFIG_NAME}/release/{runner}")

    logger.info(
        f"Fuzzer binaries to upload: {[os.path.basename(f) for f in fuzz_bin_list]}"
    )

    # adding to archive in 1 operation as tarfile doesn't
    # support appending to existing compressed file
    with tarfile.open(name=arch_name, mode="w:gz") as archive:
        for file_name in fuzz_bin_list:
            archive.add(file_name, os.path.basename(file_name))

    push_to_google_bucket(arch_name, branch)


if __name__ == '__main__':
    branch = sys.argv[1]
    logger = logging.getLogger()
    logging.basicConfig()
    main(branch)

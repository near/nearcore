import subprocess
import toml
import typing
import logging
import tarfile
import re
import time

REPO_DIR = '/home/runner/work/nearcore/nearcore/'
G_BUCKET = 'gs://fuzzer_binaries/'
CONFIG_NAME = 'x86_64-unknown-linux-gnu'


def get_current_rev(g_storage_files: typing.ByteString) -> int:
    """ Return current revision for the build
    
    Decodes byte strings (from subprocess stdout) to str
    Parses current revisions in GS and extracts the highest revision number.
    Cast revision to int
    Incremenets revision int by one and returns it 

    """
    binaries = g_storage_files.decode('utf-8').split('\n')

    revision = sorted(
        [int(re.match(r".*-(\d+).*", i).group(1)) for i in binaries if i])[-1]

    return revision + 1


def get_archive_name(runner: str) -> str:
    current_rev = time.strftime('%Y%m%d%H%M%S', time.gmtime())
    tar_name = f'{runner}-{current_rev}.tar.gz'
    logger.debug(f"Archive name: {tar_name}")

    return tar_name


def push_to_google_bucket(runner: str) -> None:
    tar_name = get_archive_name(runner)
    with tarfile.open(name=tar_name, mode='w') as archiver:
        archiver.add(f"target/{CONFIG_NAME}/debug/{runner}", runner)

    _proc = subprocess.run([
        'gsutil',
        'cp',
        tar_name,
        G_BUCKET,
    ])


def main() -> None:
    crates = [
        i for i in toml.load('nightly/fuzz.toml')['target']
        if i['runner'] in ['borsh', 'serde']
    ]
    logger.debug(f"Crates from nightly/fuzz.toml: \n{crates}")

    for target in crates:
        logger.info(f"Working on {target}")

        _proc = subprocess.run(
            [
                'cargo',
                '+nightly',
                'fuzz',
                'build',
                target['runner'],
                '--dev',
            ],
            cwd=target['crate'],
        )

        push_to_google_bucket(target['runner'])


if __name__ == '__main__':
    logger = logging.getLogger()
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    main()

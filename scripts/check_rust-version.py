"""Checks that we specify the same Rust version requirements everywhere."""
import collections
import os
import subprocess
import sys
import typing

import toml

# List of crates which can have a different package.rust-version
WHITELIST = [
    'core/account-id',
]


def get(toml_value, *names):
    """Returns nested field from given TOML object"""
    for name in names:
        if not isinstance(toml_value, dict):
            return None
        toml_value = toml_value.get(name, None)
    return toml_value


def get_rust_toolchain_channel() -> str:
    """Returns ‘toolchain.channel’ value from rust-toolchain.toml file.

    If the channel is not specified, raises SytemExit exception.
    """
    path = 'rust-toolchain.toml'
    with open(path) as rd:
        data = toml.load(rd)
    channel = get(data, 'toolchain', 'channel')
    if not channel:
        sys.exit(f'{path}: ‘toolchain.channel’ not set')
    return str(channel)


def get_rust_version(path: str) -> typing.Optional[str]:
    """Returns package.rust-version value from given Cargo.toml file.

    If the rust-version is specified as inherited from workspace, returns
    `None`.  If the value is not specified but workspace.package.rust-version
    is, reads that value.  If neither values are provided, raises SytemExit
    exception.
    """
    with open(path) as rd:
        data = toml.load(rd)
    version = get(data, 'package', 'rust-version')
    if version:
        if get(version, 'workspace'):
            return None
        return str(version)
    version = get(data, 'workspace', 'package', 'rust-version')
    if not version:
        sys.exit(f'{path}: ‘package.rust-version’ not set')
    return str(version)


def get_all_versions() -> typing.Dict[str, typing.Sequence[str]]:
    """Reads all rust versions specified throughout the repository"""
    versions = collections.defaultdict(list)
    versions[get_rust_toolchain_channel()].append('rust-toolchain.toml')

    whitelist = {os.path.join(crate, 'Cargo.toml') for crate in WHITELIST}

    out = subprocess.check_output(('git', 'ls-tree', '-zr', '--name-only', '@'),
                                  text=True)
    for name in out.split('\0'):
        if (not name.startswith('pytest/') and
            not name in whitelist and
            (name == 'Cargo.toml' or name.endswith('/Cargo.toml'))):
            version = get_rust_version(name)
            if version:
                versions[version].append(name)

    return versions


def main() -> int:
    versions = get_all_versions()
    if len(versions) == 1:
        return 0

    print('Found different Rust version strings', file=sys.stderr)
    for version in sorted(versions):
        files = versions[version]
        files.sort()
        print(f'[version {version}]', file=sys.stderr)
        for name in files:
            print(f'- {name}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())

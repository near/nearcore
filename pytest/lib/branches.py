import os
import pathlib
import subprocess
import sys
import tempfile
import typing

import requests
import semver
from configured_logger import logger

_UNAME = os.uname()[0]
_IS_DARWIN = _UNAME == 'Darwin'
_BASEHREF = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com'
_REPO_DIR = pathlib.Path(__file__).resolve().parents[2]
_OUT_DIR = _REPO_DIR / 'target/debug'


def current_branch():
    return os.environ.get('BUILDKITE_BRANCH') or subprocess.check_output([
        "git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"
    ]).strip().decode()


def __get_latest_deploy(chain_id: str) -> typing.Tuple[str, str]:
    """Returns latest (release, deploy) for given chain.

    Gets latest release and deploy identifier from S3 for given chain.  Those
    can be used to uniquely identify a neard executable running on the chain.
    """

    def download(url: str) -> str:
        res = requests.get(url)
        res.raise_for_status()
        return res.text

    basehref = f'{_BASEHREF}/nearcore-deploy/{chain_id}'
    release = download(f'{basehref}/latest_release')
    deploy = download(f'{basehref}/latest_deploy')

    if release != 'master':
        # Make sure it parses as a version
        release = str(semver.VersionInfo.parse(release).finalize_version())

    return release, deploy


class Executables(typing.NamedTuple):
    root: pathlib.Path
    neard: pathlib.Path
    state_viewer: pathlib.Path

    def node_config(self) -> typing.Dict[str, typing.Any]:
        return {
            'local': True,
            'neard_root': self.root,
            'binary_name': self.neard.name
        }


def _compile_binary(branch: str) -> Executables:
    """For given branch, compile binary.

    Stashes current changes, switches branch and then returns everything back.
    """
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    stash_output = subprocess.check_output(['git', 'stash'])
    subprocess.check_output(['git', 'checkout', str(branch)])
    subprocess.check_output(['git', 'pull', 'origin', str(branch)])
    result = _compile_current(branch)
    subprocess.check_output(['git', 'checkout', prev_branch])
    if stash_output != b"No local changes to save\n":
        subprocess.check_output(['git', 'stash', 'pop'])
    return result


def escaped(branch):
    return branch.replace('/', '-')


def _compile_current(branch: str) -> Executables:
    """Compile current branch."""
    subprocess.check_call(['cargo', 'build', '-p', 'neard', '--bin', 'neard'],
                          cwd=_REPO_DIR)
    subprocess.check_call(['cargo', 'build', '-p', 'near-test-contracts'],
                          cwd=_REPO_DIR)
    subprocess.check_call(['cargo', 'build', '-p', 'state-viewer'],
                          cwd=_REPO_DIR)
    branch = escaped(branch)
    neard = _OUT_DIR / f'neard-{branch}'
    state_viewer = _OUT_DIR / f'state-viewer-{branch}'
    (_OUT_DIR / 'neard').rename(neard)
    (_OUT_DIR / 'state-viewer').rename(state_viewer)
    return Executables(_OUT_DIR, neard, state_viewer)


def patch_binary(binary: pathlib.Path) -> None:
    """
    Patch a specified external binary if doing so is necessary for the host
    system to be able to run it.

    Currently only supports NixOS.
    """
    # Are we running on NixOS and require patching…?
    try:
        with open('/etc/os-release', 'r') as f:
            if not any(line.strip() == 'ID=nixos' for line in f):
                return
    except FileNotFoundError:
        return
    if os.path.exists('/lib'):
        return
    # Build an output with patchelf and interpreter in it
    nix_expr = '''
    with (import <nixpkgs> {});
    symlinkJoin {
      name = "nearcore-dependencies";
      paths = [patchelf stdenv.cc.bintools];
    }
    '''
    path = subprocess.run(('nix-build', '-E', nix_expr),
                          capture_output=True,
                          encoding='utf-8').stdout.strip()
    # Set the interpreter for the binary to NixOS'.
    patchelf = f'{path}/bin/patchelf'
    cmd = (patchelf, '--set-interpreter', f'{path}/nix-support/dynamic-linker',
           binary)
    logger.debug('Patching for NixOS ' + ' '.join(cmd))
    subprocess.check_call(cmd)


def __download_file_if_missing(filename: pathlib.Path, url: str) -> None:
    """Downloads a file from given URL if it does not exist already.

    Does nothing if file `filename` already exists.  Otherwise, downloads data
    from `url` and saves them in `filename`.  Downloading is done with `curl`
    tool and on failure (i.e. if it returns non-zero exit code) `filename` is
    not created.  On success, the file’s mode is set to 0x555 (i.e. readable and
    executable by anyone).

    Args:
        filename: Path to the file.
        url: URL of the file to download (if the file is missing).
    """
    if filename.exists():
        if not filename.is_file():
            sys.exit(f'{filename} exists but is not a file')
        return

    proto = '"=https"' if _IS_DARWIN else '=https'
    cmd = ('curl', '--proto', proto, '--tlsv1.2', '-sSfL', url)
    name = None
    try:
        with tempfile.NamedTemporaryFile(dir=filename.parent,
                                         delete=False) as tmp:
            name = pathlib.Path(tmp.name)
            logger.debug('Executing ' + ' '.join(cmd))
            subprocess.check_call(cmd, stdout=tmp)
        patch_binary(name)
        name.chmod(0o555)
        name.rename(filename)
        name = None
    finally:
        if name:
            name.unlink()


def __download_binary(release: str, deploy: str) -> Executables:
    """Download binary for given release and deploye."""
    logger.info(f'Getting neard and state-viewer for {release}@{_UNAME} '
                f'(deploy={deploy})')
    neard = _OUT_DIR / f'neard-{release}-{deploy}'
    state_viewer = _OUT_DIR / f'state-viewer-{release}-{deploy}'
    basehref = f'{_BASEHREF}/nearcore/{_UNAME}/{release}/{deploy}'
    __download_file_if_missing(neard, f'{basehref}/neard')
    __download_file_if_missing(state_viewer, f'{basehref}/state-viewer')
    return Executables(_OUT_DIR, neard, state_viewer)


class ABExecutables(typing.NamedTuple):
    stable: Executables
    current: Executables
    release: str
    deploy: str


def prepare_ab_test(chain_id: str = 'mainnet') -> ABExecutables:
    """Prepares executable at HEAD and latest deploy at given chain.

    Args:
        chain_id: Chain id to get latest deployed executable for.  Can be
            ‘master’, ‘testnet’ or ‘betanet’.
    Returns:
        An ABExecutables object where `current` describes executable built at
        current HEAD while `stable` points at executable which is deployed in
        production at given chain.  `release` and `deploy` of the returned
        object specify, well, the latest release and deploy running in
        production at the chain.
    """
    if chain_id not in ('mainnet', 'testnet', 'betanet'):
        raise ValueError(f'Unexpected chain_id: {chain_id}; '
                         'expected mainnet, testnet or betanet')

    is_nayduck = bool(os.getenv('NAYDUCK'))
    if is_nayduck:
        # On NayDuck the file is fetched from a builder host so there’s no need
        # to build it.
        current = Executables(_OUT_DIR, _OUT_DIR / 'neard',
                              _OUT_DIR / 'state-viewer')
    else:
        current = _compile_current(current_branch())

    release, deploy = __get_latest_deploy(chain_id)
    try:
        stable = __download_binary(release, deploy)
    except Exception as e:
        if is_nayduck:
            logger.exception('RC binary should be downloaded for NayDuck.', e)
        stable = _compile_binary(release)

    return ABExecutables(stable=stable,
                         current=current,
                         release=release,
                         deploy=deploy)

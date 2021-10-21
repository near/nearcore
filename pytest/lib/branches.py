import os
import pathlib
import subprocess
import sys
import tempfile
import typing

import semver
from configured_logger import logger
from github import Github


def current_branch():
    return os.environ.get('BUILDKITE_BRANCH') or subprocess.check_output([
        "git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"
    ]).strip().decode()


def get_releases():
    git = Github(None)
    repo = git.get_repo("nearprotocol/nearcore")
    releases = []

    for release in repo.get_releases():
        try:
            # make sure that the version provided is a valid semver version
            version = semver.VersionInfo.parse(release.title)
            releases.append(release)
        except Exception as e:
            pass

    return sorted(releases,
                  key=lambda release: semver.VersionInfo.parse(release.title),
                  reverse=True)


def latest_rc_branch():
    releases = list(
        filter(
            lambda release: (semver.VersionInfo.parse(release.title).prerelease
                             or "").startswith("rc"), get_releases()))

    if not releases:
        return None

    return semver.VersionInfo.parse(releases[0].title).finalize_version()


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
    subprocess.check_call(['cargo', 'build', '-p', 'neard', '--bin', 'neard'])
    subprocess.check_call(['cargo', 'build', '-p', 'near-test-contracts'])
    subprocess.check_call(['cargo', 'build', '-p', 'state-viewer'])
    branch = escaped(branch)
    build_dir = pathlib.Path('../target/debug')
    neard = build_dir / f'neard-{branch}'
    state_viewer = build_dir / f'state-viewer-{branch}'
    (build_dir / 'neard').rename(neard)
    (build_dir / 'state-viewer').rename(state_viewer)
    return Executables(build_dir, neard, state_viewer)


def download_file_if_missing(filename: pathlib.Path, url: str) -> None:
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

    proto = '"=https"' if os.uname()[0] == 'Darwin' else '=https'
    cmd = ('curl', '--proto', proto, '--tlsv1.2', '-sSfL', url)
    name = None
    try:
        with tempfile.NamedTemporaryFile(dir=filename.parent,
                                         delete=False) as tmp:
            name = pathlib.Path(tmp.name)
            subprocess.check_call(cmd, stdout=tmp)
        name.chmod(0o555)
        name.rename(filename)
        name = None
    finally:
        if name:
            name.unlink()


def download_binary(uname, branch):
    """Download binary for given platform and branch."""
    logger.info(f'Getting near & state-viewer for {branch}@{uname}')
    outdir = pathlib.Path('../target/debug')
    basehref = ('https://s3-us-west-1.amazonaws.com/build.nearprotocol.com'
                f'/nearcore/{uname}/{branch}/')
    neard = outdir / f'neard-{branch}'
    state_viewer = outdir / f'state-viewer-{branch}'
    download_file_if_missing(neard, basehref + 'neard')
    download_file_if_missing(state_viewer, basehref + 'state-viewer')
    return Executables(outdir, neard, state_viewer)


class ABExecutables(typing.NamedTuple):
    stable: Executables
    current: Executables


def prepare_ab_test(stable_branch):
    # Use NEAR_AB_BINARY_EXISTS to avoid rebuild / re-download when testing locally.
    #if not os.environ.get('NEAR_AB_BINARY_EXISTS'):
    #    _compile_current(current_branch())
    #    uname = os.uname()[0]
    #    if stable_branch in ['master', 'beta', 'stable'] and uname in ['Linux', 'Darwin']:
    #        download_binary(uname, stable_branch)
    #    else:
    is_nayduck = bool(os.getenv('NAYDUCK'))

    if is_nayduck:
        # On NayDuck the file is fetched from a builder host so there’s no need
        # to build it.
        root = pathlib.Path('../target/debug/')
        current = Executables(root, root / 'neard', root / 'state-viewer')
    else:
        current = _compile_current(current_branch())

    try:
        stable = download_binary(os.uname()[0], stable_branch)
    except Exception:
        if is_nayduck:
            sys.exit('RC binary should be downloaded for NayDuck.')
        stable = _compile_binary(str(stable_branch))
    return ABExecutables(stable=stable, current=current)

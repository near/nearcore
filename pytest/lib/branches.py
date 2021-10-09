import os
import pathlib
import subprocess
import sys
import tempfile

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


def compile_binary(branch):
    """For given branch, compile binary.

    Stashes current changes, switches branch and then returns everything back.
    """
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    stash_output = subprocess.check_output(['git', 'stash'])
    subprocess.check_output(['git', 'checkout', str(branch)])
    subprocess.check_output(['git', 'pull', 'origin', str(branch)])
    compile_current(branch)
    subprocess.check_output(['git', 'checkout', prev_branch])
    if stash_output != b"No local changes to save\n":
        subprocess.check_output(['git', 'stash', 'pop'])


def escaped(branch):
    return branch.replace('/', '-')


def compile_current(branch=None):
    """Compile current branch."""
    if branch is None:
        branch = current_branch()
    subprocess.check_call(['cargo', 'build', '-p', 'neard', '--bin', 'neard'])
    subprocess.check_call(['cargo', 'build', '-p', 'near-test-contracts'])
    subprocess.check_call(['cargo', 'build', '-p', 'state-viewer'])
    branch = escaped(branch)
    os.rename('../target/debug/neard', '../target/debug/neard-%s' % branch)
    os.rename('../target/debug/state-viewer',
              '../target/debug/state-viewer-%s' % branch)
    subprocess.check_call(['git', 'checkout', '../Cargo.lock'])


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
    download_file_if_missing(outdir / f'neard-{branch}', basehref + 'neard')
    download_file_if_missing(outdir / f'state-viewer-{branch}',
                             basehref + 'state-viewer')


def prepare_ab_test(other_branch):
    # Use NEAR_AB_BINARY_EXISTS to avoid rebuild / re-download when testing locally.
    #if not os.environ.get('NEAR_AB_BINARY_EXISTS'):
    #    compile_current()
    #    uname = os.uname()[0]
    #    if other_branch in ['master', 'beta', 'stable'] and uname in ['Linux', 'Darwin']:
    #        download_binary(uname, other_branch)
    #    else:
    uname = os.uname()[0]
    if not os.getenv('NAYDUCK'):
        compile_current()
    try:
        download_binary(uname, other_branch)
    except Exception:
        if not os.getenv('NAYDUCK'):
            compile_binary(str(other_branch))
        else:
            logger.critical('RC binary should be downloaded for NayDuck.')
            sys.exit(1)
    return '../target/debug/', [other_branch, escaped(current_branch())]

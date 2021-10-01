import os
import subprocess

import semver
import sys
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


def download_binary(uname, branch):
    """Download binary for given platform and branch."""
    url = f'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/{uname}/{branch}/neard'
    proto = '"=https"' if uname == 'Darwin' else '=https'
    logger.info(f'Downloading near & state-viewer for {branch}@{uname}')
    subprocess.check_output([
        'curl', '--proto', proto, '--tlsv1.2', '-sSfL', url, '-o',
        f'../target/debug/neard-{branch}'
    ])
    subprocess.check_output(['chmod', '+x', f'../target/debug/neard-{branch}'])
    url = f'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/{uname}/{branch}/state-viewer'
    subprocess.check_output([
        'curl', '--proto', proto, '--tlsv1.2', '-sSfL', url, '-o',
        f'../target/debug/state-viewer-{branch}'
    ])
    subprocess.check_output(
        ['chmod', '+x', f'../target/debug/state-viewer-{branch}'])


def prepare_ab_test(other_branch):
    # Use NEAR_AB_BINARY_EXISTS to avoid rebuild / re-download when testing locally.
    #if not os.environ.get('NEAR_AB_BINARY_EXISTS'):
    #    compile_current()
    #    uname = os.uname()[0]
    #    if other_branch in ['master', 'beta', 'stable'] and uname in ['Linux', 'Darwin']:
    #        download_binary(uname, other_branch)
    #    else:
    # TODO: re-enable caching
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

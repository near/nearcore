import os
import subprocess

import semver
from github import Github

NEAR_ROOT = '../target/debug/'


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

    return str(semver.VersionInfo.parse(releases[0].title).finalize_version())


def latest_beta_branch():
    releases = list(
        filter(
            lambda release: (semver.VersionInfo.parse(release.title).prerelease
                             or "").startswith("beta"), get_releases()))

    if not releases:
        return None

    return str(semver.VersionInfo.parse(releases[0].title).finalize_version())


def latest_stable_branch():
    releases = list(
        filter(
            lambda release: not semver.VersionInfo.parse(release.title).prerelease, get_releases()))

    if not releases:
        return None

    return str(semver.VersionInfo.parse(releases[0].title).finalize_version())


def get_branch(branch):
    if branch == 'rc':
        return latest_rc_branch()
    elif branch == 'beta':
        return latest_beta_branch()
    elif branch == 'stable':
        return latest_stable_branch()
    else:
        return branch



def compile_binary(branch):
    """For given branch, compile binary.

    Stashes current changes, switches branch and then returns everything back.
    """
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    stash_output = subprocess.check_output(['git', 'stash'])
    subprocess.check_output(['git', 'checkout', str(branch)])
    subprocess.check_output(['git', 'pull'])
    compile_current()
    subprocess.check_output(['git', 'checkout', prev_branch])
    if stash_output != b"No local changes to save\n":
        subprocess.check_output(['git', 'stash', 'pop'])


def escaped(branch):
    return branch.replace('/', '-')


def compile_current():
    """Compile current branch."""
    branch = current_branch()
    try:
        # Accommodate rename from near to neard
        subprocess.check_output(['cargo', 'build', '-p', 'neard'])
    except:
        subprocess.check_output(['cargo', 'build', '-p', 'near'])
    subprocess.check_output(['cargo', 'build', '-p', 'state-viewer'])
    branch = escaped(branch)
    if os.path.exists('../target/debug/near'):
        os.rename('../target/debug/near', '../target/debug/near-%s' % branch)
    else:
        os.rename('../target/debug/neard', '../target/debug/near-%s' % branch)
    os.rename('../target/debug/state-viewer',
              '../target/debug/state-viewer-%s' % branch)
    subprocess.check_output(['git', 'checkout', '../Cargo.lock'])


def download_binary(uname, branch):
    """Download binary for given platform and branch."""
    url = f'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/{uname}/{branch}/near'
    print(f'Downloading near & state-viewer for {branch}@{uname}')
    subprocess.check_output([
        'curl', '--proto', '=https', '--tlsv1.2', '-sSfL', url, '-o',
        f'../target/debug/near-{branch}'
    ])
    subprocess.check_output(['chmod', '+x', f'../target/debug/near-{branch}'])
    url = f'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/{uname}/{branch}/state-viewer'
    subprocess.check_output([
        'curl', '--proto', '=https', '--tlsv1.2', '-sSfL', url, '-o',
        f'../target/debug/state-viewer-{branch}'
    ])
    subprocess.check_output(
        ['chmod', '+x', f'../target/debug/state-viewer-{branch}'])


def prepare_binary(branch):
    try:
        uname = os.uname()[0]
        download_binary(uname, branch)
    except Exception:
        compile_binary(str(branch))


def prepare_ab_test(other_branch):
    # TODO: re-enable caching
    current_branch_ = current_branch()
    prepare_binary(current_branch_)
    prepare_binary(other_branch)
    return NEAR_ROOT, [other_branch, escaped(current_branch_)]

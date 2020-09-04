import os
import subprocess


def current_branch():
    return os.environ.get('BUILDKITE_BRANCH') or subprocess.check_output([
        "git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"
    ]).strip().decode()


def compile_binary(branch):
    """For given branch, compile binary.

    Stashes current changes, switches branch and then returns everything back.
    """
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    stash_output = subprocess.check_output(['git', 'stash'])
    subprocess.check_output(['git', 'checkout', branch])
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
    os.rename('../target/debug/near', '../target/debug/near-%s' % branch)
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


def prepare_ab_test(other_branch):
    # Use NEAR_AB_BINARY_EXISTS to avoid rebuild / re-download when testing locally.
    #if not os.environ.get('NEAR_AB_BINARY_EXISTS'):
    #    compile_current()
    #    uname = os.uname()[0]
    #    if other_branch in ['master', 'beta', 'stable'] and uname in ['Linux', 'Darwin']:
    #        download_binary(uname, other_branch)
    #    else:
    compile_binary(other_branch)
    return '../target/debug/', [other_branch, escaped(current_branch())]

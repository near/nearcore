#!/usr/bin/env python3
"""Runs integration tests in the cloud on NayDuck.

To request a new run, use the following command:

   python3 scripts/nayduck.py      \
       --branch    <your_branch>   \
       --test-file <test_file>.txt

Scheduled runs can be seen at <https://nayduck.near.org/>.

See README.md in nightly directory for documentation of the test suite file
format.  Note that you must be a member of the Near or Near Protocol
organisation on GitHub to authenticate (<https://github.com/orgs/near/people>).

The source code for NayDuck itself is at <https://github.com/near/nayduck>.
"""

import getpass
import json
import os
import pathlib
import shlex
import subprocess
import sys
import typing

REPO_DIR = pathlib.Path(__file__).resolve().parents[1]

DEFAULT_TEST_FILE = 'nightly/nightly.txt'
NAYDUCK_BASE_HREF = 'https://nayduck.near.org'


def _parse_args():
    import argparse

    default_test_path = (pathlib.Path(__file__).parent.parent /
                         DEFAULT_TEST_FILE)

    parser = argparse.ArgumentParser(description='Run tests.')
    parser.add_argument('--branch',
                        '-b',
                        help='Branch to test. By default gets current one.')
    parser.add_argument(
        '--sha',
        '-s',
        help=
        'Commit sha to test. By default gets current one. This is ignored if branch name is provided'
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--test-file',
                       '-t',
                       default=default_test_path,
                       help=f'Test set file; {DEFAULT_TEST_FILE} by default.')
    group.add_argument('--stdin',
                       '-i',
                       action='store_true',
                       help='Read test set from standard input.')
    parser.add_argument('--run-locally',
                        '-l',
                        action='store_true',
                        help='Run tests locally.')
    parser.add_argument(
        '--dry-run',
        '-n',
        action='store_true',
        help='Prints list of tests to execute, without doing anything')
    args = parser.parse_args()

    return args


def get_sha(branch: str):
    try:
        sha = subprocess.check_output(['git', 'rev-parse', branch], text=True)

    except subprocess.CalledProcessError as e:
        remote_branch = 'remotes/origin/' + branch
        print(
            f"Couldn't find a local branch \'{branch}\'. Trying remote: {remote_branch}"
        )
        sha = subprocess.check_output(
            ['git', 'rev-parse', remote_branch],
            text=True,
        )

    return sha


def get_branch(sha: str = ""):
    if sha:
        return subprocess.check_output(['git', 'name-rev', sha],
                                       text=True).strip().split(' ')[-1]
    else:
        return subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', "HEAD"], text=True).strip()


FileReader = typing.Callable[[pathlib.Path], str]


def read_tests_from_file(
    path: pathlib.Path,
    *,
    include_comments: bool = False,
    reader: FileReader = lambda path: path.read_text()
) -> typing.Iterable[str]:
    """Reads lines from file handling `./<path>` includes.

    Returns an iterable over lines in given file but also handles `./<path>`
    syntax for including other files in the output and optionally filters
    commented out lines.

    A `./<path>` syntax acts like C's #include directive, Rust's `include!`
    macro or shell's `source` command.  All lines are read from file at <path>
    as if they were directly in the source file.  The `./<path>` directives are
    handled recursively up to three levels deep.

    If include_comments is True, `#./<path>` lines are handled as well with all
    included line commented out.  This is useful to comment out a include a with
    TODO comment included and have check_nightly.py and check_pytest.py scripts
    still recognise those includes.  Note that the line must start with `#./`,
    i.e. there must be no space between hash and the dot.

    Args:
        path: Path to the file to read.
        include_comments: By default empty lines and lines whose first non-space
            character is hash are ignored and not included in the output.  With
            this set to `True` such lines are included as well.
        reader: A callback which reads text content of a given file.  This is
            used by NayDuck.
    Returns:
        An iterable over lines in the given file.  All lines are stripped of
        leading and trailing white space.
    """
    return __read_tests(reader(path).splitlines(),
                        filename=path,
                        dirpath=path.parent,
                        include_comments=include_comments,
                        reader=reader)


def read_tests_from_stdin(
    *,
    include_comments: bool = False,
    reader: FileReader = lambda path: path.read_text()
) -> typing.Iterable[str]:
    """Reads lines from standard input handling `./<path>` includes.

    Behaves like `read_tests_from_file` but rather than reading contents of
    given file reads lines from standard input.  `./<path>` includes are
    resolved relative to current working directory.

    Returns:
        An iterable over lines in the given file.  All lines are stripped of
        leading and trailing white space.
    """
    return __read_tests(sys.stdin,
                        filename='<stdin>',
                        dirpath=pathlib.Path.cwd(),
                        include_comments=include_comments,
                        reader=reader)


def __read_tests(
    lines: typing.Iterable[str],
    *,
    filename: typing.Union[str, pathlib.Path],
    dirpath: pathlib.Path,
    include_comments: bool = False,
    reader: FileReader = lambda path: path.read_text()
) -> typing.Iterable[str]:

    def impl(lines: typing.Iterable[str],
             filename: typing.Union[str, pathlib.Path],
             dirpath: pathlib.Path,
             depth: int = 1,
             comment: bool = False) -> typing.Iterable[str]:
        for lineno, line in enumerate(lines, start=1):
            line = line.rstrip()
            if line.startswith('./') or (include_comments and
                                         line.startswith('#./')):
                if depth == 3:
                    print(f'{filename}:{lineno}: ignoring {line}; '
                          f'would exceed depth limit of {depth}')
                else:
                    incpath = dirpath / line.lstrip('#')
                    yield from impl(
                        reader(incpath).splitlines(), incpath, incpath.parent,
                        depth + 1, comment or line.startswith('#'))
            elif include_comments or (line and line[0] != '#'):
                if comment and not line.startswith('#'):
                    line = '#' + line
                yield line

    return impl(lines, filename, dirpath)


def github_auth(code_path: pathlib.Path):
    print('Go to the following link in your browser:\n\n{}/login/cli\n'.format(
        NAYDUCK_BASE_HREF))
    code = getpass.getpass('Enter authorisation code: ')
    code_path.parent.mkdir(parents=True, exist_ok=True)
    code_path.write_text(code)
    return code


def _parse_timeout(timeout: typing.Optional[str]) -> typing.Optional[int]:
    """Parses timeout interval and converts it into number of seconds.

    Args:
        timeout: An integer with an optional ‘h’, ‘m’ or ‘s’ suffix which
            multiply the integer by 3600, 60 and 1 respectively.
    Returns:
        Interval in seconds.
    """
    if not timeout:
        return None
    mul_ary = {'h': 3600, 'm': 60, 's': 1}
    mul = mul_ary.get(timeout[-1])
    if mul:
        timeout = timeout[:-1]
    else:
        mul = 1
    return int(timeout) * mul


def run_locally(args, tests):
    for test in tests:
        # See nayduck specs at https://github.com/near/nayduck/blob/master/lib/testspec.py
        fields = test.split()

        timeout = None
        index = 1
        ignored = []
        while len(fields) > index and fields[index].startswith('--'):
            if fields[index].startswith("--timeout="):
                timeout = fields[index][10:]
            elif fields[index] != '--skip-build':
                ignored.append(fields[index])
            index += 1

        del fields[1:index]
        message = f'Running ‘{"".join(fields)}’'
        if ignored:
            message = f'{message} (ignoring flags ‘{" ".join(ignored)}`)'
        if not args.dry_run:
            print(message)

        if fields[0] == 'expensive':
            # TODO --test doesn't work
            cmd = [
                'cargo',
                'test',
                '-p',
                fields[1],  # '--test', fields[2],
                '--features',
                'expensive_tests',
                '--',
                '--exact',
                fields[3]
            ]
            cwd = REPO_DIR
        elif fields[0] in ('pytest', 'mocknet'):
            fields[0] = sys.executable
            fields[1] = os.path.join('tests', fields[1])
            cmd = fields
            cwd = REPO_DIR / 'pytest'
        else:
            print(f'Unrecognised test category ‘{fields[0]}’', file=sys.stderr)
            continue
        if args.dry_run:
            print('( cd {} && {} )'.format(
                shlex.quote(str(cwd)),
                ' '.join(shlex.quote(str(arg)) for arg in cmd)))
            continue
        print(f"RUNNING COMMAND cwd = {cwd} cmd = {cmd}")
        subprocess.check_call(cmd, cwd=cwd, timeout=_parse_timeout(timeout))


def run_remotely(args, tests):
    import requests

    try:
        import colorama
        styles = (colorama.Fore.RED, colorama.Fore.GREEN,
                  colorama.Style.RESET_ALL)
    except ImportError:
        styles = ('', '', '')

    code_path = pathlib.Path(
        os.environ.get('XDG_CONFIG_HOME') or
        pathlib.Path.home() / '.config') / 'nayduck-code'
    if code_path.is_file():
        code = code_path.read_text().strip()
    else:
        code = github_auth(code_path)

    if args.dry_run:
        for test in tests:
            print(test)
        return

    if args.branch:
        test_branch = args.branch
        test_sha = get_sha(test_branch).strip()

    else:
        test_sha = args.sha or get_sha("HEAD").strip()
        test_branch = get_branch(test_sha)

    post = {'branch': test_branch, 'sha': test_sha, 'tests': list(tests)}

    print("Scheduling tests for: \n"
          f"branch - {post['branch']} \n"
          f"commit hash - {post['sha']}")

    while True:
        print('Sending request ...')
        res = requests.post(NAYDUCK_BASE_HREF + '/api/run/new',
                            json=post,
                            cookies={'nay-code': code})
        if res.status_code != 401:
            break
        print(f'{styles[0]}Unauthorised.{styles[2]}\n')
        code = github_auth(code_path)

    if res.status_code == 200:
        json_res = json.loads(res.text)
        print(styles[json_res['code'] == 0] + json_res['response'] + styles[2])
    else:
        print(f'{styles[0]}Got status code {res.status_code}:{styles[2]}\n')
        print(res.text)
    code = res.cookies.get('nay-code')
    if code:
        code_path.write_text(code)


def main():
    args = _parse_args()

    if args.stdin:
        tests = list(read_tests_from_stdin())
    else:
        tests = list(read_tests_from_file(pathlib.Path(args.test_file)))

    if args.run_locally:
        run_locally(args, tests)
    else:
        run_remotely(args, tests)


if __name__ == "__main__":
    main()

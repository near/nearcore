import os
import pathlib
import random
import sys
import typing

import nayduck

REPO_DIR = pathlib.Path(__file__).parent.parent


def discard(elements: typing.List[str], element: str) -> bool:
    """Discard element from a list if it exists there.

    Args:
        elements: Lists to discard element from.
        element: Element to discar
    Returns:
        Whether the element has been discarded, i.e. whether the list contained
        the element.
    """
    try:
        elements.remove(element)
        return True
    except ValueError:
        return False


def find_fuzz_targets() -> typing.Iterable[typing.Tuple[str, str]]:
    """Lists all fuzz targets defined in the repository.

    A list target is a Rust source file sans its extension in a `fuzz_targets`
    directory which (the directory) is a sibling to `Cargo.toml` file.  For
    example, `borsh` and `serde` are fuzz targets if the following directory
    structure is given:

        chain/jsonrpc/fuzz/Cargo.toml
                          /fuzz_targets/borsh.rs
                                       /serde.rs

    Yields:
        Yields (dirpath, name) tuples where first element is path to the package
        containing the target (`chain/jsonrpc/fuzz` in the example above) and
        name of the target (`borsh` or `serde` in the example above.
    """
    for dirpath, dirnames, filenames in os.walk(REPO_DIR):
        dirnames[:] = [
            name for name in dirnames
            if name[0] != '.' and name not in ('target', 'res', 'pytest')
        ]
        try:
            dirnames.remove('fuzz_targets')
        except ValueError:
            continue
        if 'Cargo.toml' in filenames:
            pkg_dir = pathlib.Path(dirpath)
            package = str(pkg_dir.relative_to(REPO_DIR))
            for name in os.listdir(pkg_dir / 'fuzz_targets'):
                if name[0] != '.' and name.endswith('.rs'):
                    yield package, name[:-3]


def find_nightly_fuzz_tests() -> typing.Iterable[typing.Tuple[str, str]]:
    """Collects list of nightly fuzz tests which use ‘fuzz.py’ runner."""
    for line in nayduck.read_tests_from_file(REPO_DIR /
                                             nayduck.DEFAULT_TEST_FILE,
                                             include_comments=True):
        # Looking for lines like:
        # pytest --skip-build --timeout=2h fuzz.py test-utils/runtime-tester/fuzz runtime-fuzzer
        test = line.strip('#').split()
        try:
            if test[0] != 'pytest':
                continue
            idx = 1
            while test[idx].startswith('--'):
                idx += 1
            if test[idx] == 'fuzz.py':
                yield test[idx + 1], test[idx + 2]
        except IndexError:
            pass


def main() -> typing.Optional[str]:
    targets = set(find_fuzz_targets())
    missing = list(targets.difference(find_nightly_fuzz_tests()))
    count = len(missing)
    if not count:
        print(f'All {len(targets)} fuzz targets included')
        return None

    pkg_len = max(len(package) for package, _ in missing)
    missing.sort()
    lines = tuple(f'    pytest --skip-build --timeout=2h fuzz.py {pkg} {target}'
                  for pkg, target in missing)
    return '''\
Found {count} fuzz target{s} which aren’t included in any of the nightly/*.txt files:

{missing}

Add the test{s} to the list in nightly/fuzzing.txt (or another appropriate
nightly/*.txt) file.  For example as:

{lines}

If a test is temporarily disabled, add it with an appropriate TODO comment.  For
example:

    # TODO(#1234): Enable the test again once <some condition>
    # {example}

Note that the TODO comment must reference a GitHub issue (i.e. must
contain a #<number> string).'''.format(
        count=count,
        s='s' if count > 1 else '',
        missing='\n'.join(f'  * {package.ljust(pkg_len)} {target}'
                          for package, target in missing),
        lines='\n'.join(lines),
        example=random.choice(lines).lstrip())


if __name__ == '__main__':
    sys.exit(main())

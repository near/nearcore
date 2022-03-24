import os
import pathlib
import random
import sys
import textwrap
import typing

import toml

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


def find_fuzz_tests() -> typing.Iterable[typing.Tuple[str, str]]:
    """Collects list of configured fuzz tests."""
    with open(REPO_DIR / 'nightly/fuzz.toml') as rd:
        data = toml.load(rd)
    for target in data.get('target'):
        yield target['crate'], target['runner']


def main() -> typing.Optional[str]:
    targets = set(find_fuzz_targets())
    missing = sorted(targets.difference(find_fuzz_tests()))
    if not missing:
        print(f'All {len(targets)} fuzz targets included')
        return None

    count = len(missing)
    pkg_len = max(len(package) for package, _ in missing)

    def target_object(crate: str, runner: str, weight=10) -> dict:
        return {'crate': crate, 'runner': runner, 'weight': weight, 'flags': []}

    def format_targets(targets: dict) -> str:
        formatted = toml.dumps({'target': list(targets)})
        return textwrap.indent(formatted.rstrip(), '    ')

    missing_list = '\n'.join(
        f'  * {package.ljust(pkg_len)} {target}' for package, target in missing)
    missing_formatted = format_targets(target_object(*item) for item in missing)
    example_formatted = format_targets(
        [target_object(*random.choice(missing), weight=0)])

    s = 's' if count > 1 else ''
    isnt = 'aren’t' if count > 1 else 'isn’t'

    return f'''\
Found {count} fuzz target{s} which {isnt} included in nightly/fuzz.toml.txt file:

{missing_list}

Add the test{s} to the file.  For example as:

{missing_formatted}

If a test is temporarily disabled, add it with zero weight and an appropriate
TODO comment.  For example:

    # TODO(#1234): Enable the test again once <some condition>
{example_formatted}
    '''


if __name__ == '__main__':
    sys.exit(main())

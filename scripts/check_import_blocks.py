#!/usr/bin/env python3
"""Checks that Rust imports form a single block without blank line separators.

Usage:
    python3 scripts/check_import_blocks.py          # check for violations
    python3 scripts/check_import_blocks.py --fix     # auto-fix violations

See: https://near.github.io/nearcore/practices/style.html#import-blocks
"""

import pathlib
import re
import subprocess
import sys

USE_RE = re.compile(r'^(pub(\s*\(.*?\))?\s+)?use\s')
COMMENT_OR_ATTR_RE = re.compile(r'^(//|#!?\[|/\*|\*)')


def get_indent(line):
    """Return the leading whitespace of a line."""
    return line[:len(line) - len(line.lstrip())]


def check_file(path):
    try:
        with open(path) as f:
            lines = f.readlines()
    except (UnicodeDecodeError, FileNotFoundError):
        return []

    violations = []
    in_multiline_use = False
    brace_depth = 0
    last_use_line = None
    last_use_indent = None
    saw_gap = False
    gap_line = None

    for lineno, raw in enumerate(lines, 1):
        line = raw.rstrip('\n')

        if in_multiline_use:
            brace_depth += line.count('{') - line.count('}')
            if brace_depth <= 0:
                in_multiline_use = False
                last_use_line = lineno
            continue

        stripped = line.lstrip()
        if USE_RE.match(stripped):
            indent = get_indent(line)
            if last_use_line is not None and saw_gap and indent == last_use_indent:
                violations.append((gap_line, last_use_line, lineno))

            open_braces = line.count('{')
            close_braces = line.count('}')
            if open_braces > close_braces:
                in_multiline_use = True
                brace_depth = open_braces - close_braces
            else:
                last_use_line = lineno
            last_use_indent = indent
            saw_gap = False
        elif line.strip() == '':
            if last_use_line is not None and not saw_gap:
                saw_gap = True
                gap_line = lineno
        elif COMMENT_OR_ATTR_RE.match(stripped):
            pass
        else:
            last_use_line = None
            last_use_indent = None
            saw_gap = False

    return violations


def fix_file(path):
    """Remove blank lines between import statements at the same indent level.

    Returns True if the file was modified.
    """
    try:
        with open(path) as f:
            lines = f.readlines()
    except (UnicodeDecodeError, FileNotFoundError):
        return False

    result = []
    in_imports = False
    import_indent = None
    in_multiline_use = False
    brace_depth = 0
    buffer = []

    for raw in lines:
        line = raw.rstrip('\n')

        if in_multiline_use:
            result.append(raw)
            brace_depth += line.count('{') - line.count('}')
            if brace_depth <= 0:
                in_multiline_use = False
            continue

        stripped = line.lstrip()
        if USE_RE.match(stripped):
            indent = get_indent(line)
            if in_imports and indent == import_indent:
                # Between imports at same indent: discard blank lines.
                for buf in buffer:
                    if buf.strip():
                        result.append(buf)
            else:
                result.extend(buffer)
            buffer.clear()
            result.append(raw)
            in_imports = True
            import_indent = indent

            open_braces = line.count('{')
            close_braces = line.count('}')
            if open_braces > close_braces:
                in_multiline_use = True
                brace_depth = open_braces - close_braces
        elif in_imports and (line.strip() == ''
                             or COMMENT_OR_ATTR_RE.match(stripped)):
            buffer.append(raw)
        else:
            result.extend(buffer)
            buffer.clear()
            result.append(raw)
            in_imports = False
            import_indent = None

    result.extend(buffer)

    if result != lines:
        with open(path, 'w') as f:
            f.writelines(result)
        return True
    return False


def get_rust_files(repo_dir):
    result = subprocess.run(
        ['git', 'ls-files', '*.rs'],
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=True,
    )
    return [repo_dir / f for f in result.stdout.strip().split('\n') if f]


def main():
    repo_dir = pathlib.Path(__file__).parent.parent
    fix_mode = '--fix' in sys.argv

    files = get_rust_files(repo_dir)

    if fix_mode:
        fixed = 0
        for path in sorted(files):
            if fix_file(path):
                print(f"fixed {path.relative_to(repo_dir)}")
                fixed += 1
        if fixed:
            print(
                f"\nFixed {fixed} file(s). Run `cargo fmt` to re-sort imports."
            )
        else:
            print("No violations found.")
        return 0

    errors = 0
    for path in sorted(files):
        for gap_line, prev_use, next_use in check_file(path):
            rel = path.relative_to(repo_dir)
            print(
                f"{rel}:{gap_line}: blank line between imports (lines {prev_use} and {next_use})"
            )
            errors += 1

    if errors:
        print(f"\nFound {errors} import block violation(s).")
        print(
            "Remove blank lines between use statements to form a single import block."
        )
        print(
            "Run `python3 scripts/check_import_blocks.py --fix` to auto-fix.")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())

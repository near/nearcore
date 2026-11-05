"""Parses GitHub PR file patches and validates review comment locations.

GitHub rejects a whole review when any inline comment points at a line that
is not part of the PR diff, so every location the model proposes is checked
here before posting.
"""

import dataclasses
import re

LEFT = "LEFT"
RIGHT = "RIGHT"
SIDES = (LEFT, RIGHT)

HUNK_HEADER_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


@dataclasses.dataclass
class FileDiff:
    path: str
    status: str
    previous_path: str | None
    patch: str | None
    additions: int
    deletions: int
    # side -> {line number: hunk index}
    lines: dict[str, dict[int, int]]

    @classmethod
    def from_api(cls, entry: dict) -> "FileDiff":
        """Builds a FileDiff from one item of the `pulls/{n}/files` API."""
        patch = entry.get("patch") or None
        return cls(
            path=entry["filename"],
            status=entry.get("status", "modified"),
            previous_path=entry.get("previous_filename"),
            patch=patch,
            additions=entry.get("additions", 0),
            deletions=entry.get("deletions", 0),
            lines=parse_patch(patch) if patch else {
                LEFT: {},
                RIGHT: {}
            },
        )

    def has_patch(self) -> bool:
        return bool(self.lines[LEFT] or self.lines[RIGHT])


@dataclasses.dataclass(frozen=True)
class Location:
    path: str
    side: str
    line: int
    start_line: int | None = None


def _patch_lines(patch: str) -> list[str]:
    lines = patch.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def parse_patch(patch: str) -> dict[str, dict[int, int]]:
    """Returns the lines of a patch that can have comments, for each side.

    Context lines can have comments on both sides, removed lines on LEFT and
    added lines on RIGHT. Each line maps to the index of its hunk, because a
    multi-line comment must start and end in the same hunk.
    """
    result = {LEFT: {}, RIGHT: {}}
    hunk = -1
    old = new = 0
    for text in _patch_lines(patch):
        header = HUNK_HEADER_RE.match(text)
        if header:
            hunk += 1
            old = int(header.group(1))
            new = int(header.group(3))
            continue
        if hunk < 0 or text.startswith("\\"):
            continue
        op = text[:1]
        if op == "-":
            result[LEFT][old] = hunk
            old += 1
        elif op == "+":
            result[RIGHT][new] = hunk
            new += 1
        else:
            # A context line. An empty string is treated as context too, in
            # case a tool stripped the leading space of a blank line.
            result[LEFT][old] = hunk
            result[RIGHT][new] = hunk
            old += 1
            new += 1
    return result


def annotate_patch(patch: str) -> str:
    """Prefixes every patch line with its old (L) and new (R) line numbers.

    The model cannot count lines reliably, so it gets the numbers it must use
    in inline comments written next to each line.
    """
    out = []
    old = new = 0
    in_hunk = False
    for text in _patch_lines(patch):
        header = HUNK_HEADER_RE.match(text)
        if header:
            in_hunk = True
            old = int(header.group(1))
            new = int(header.group(3))
            out.append(text)
            continue
        if not in_hunk or text.startswith("\\"):
            out.append(text)
            continue
        op, body = text[:1], text[1:]
        if op == "-":
            out.append(f"-{'L' + str(old):>7} {'':>7} | {body}")
            old += 1
        elif op == "+":
            out.append(f"+{'':>7} {'R' + str(new):>7} | {body}")
            new += 1
        else:
            out.append(f" {'L' + str(old):>7} {'R' + str(new):>7} | {body}")
            old += 1
            new += 1
    return "\n".join(out)


def describe_ranges(lines: dict[int, int]) -> str:
    """Formats line numbers as compact ranges, e.g. "3-5, 9"."""
    numbers = sorted(lines)
    if not numbers:
        return "none"
    ranges = []
    start = prev = numbers[0]
    for n in numbers[1:]:
        if n == prev + 1:
            prev = n
            continue
        ranges.append((start, prev))
        start = prev = n
    ranges.append((start, prev))
    return ", ".join(f"{a}-{b}" if a != b else f"{a}" for a, b in ranges)


def _is_line_number(value) -> bool:
    # bool is a subclass of int, so True would otherwise pass as line 1.
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_location(
    files: dict[str, FileDiff],
    path,
    side,
    line,
    start_line=None,
) -> tuple[Location | None, str | None]:
    """Checks a proposed inline comment location against the PR diff.

    Returns `(location, None)` when GitHub will accept it, otherwise
    `(None, reason)`. The reason lists the valid ranges so the model can fix
    the location.
    """
    if not isinstance(path, str) or path not in files:
        return None, f"{path!r} is not a file changed by this PR"
    file = files[path]
    if not file.has_patch():
        return None, (f"{path} has no diff from GitHub (binary, too large or "
                      "unchanged content), so it cannot have inline comments")
    if side not in SIDES:
        return None, f"side must be LEFT or RIGHT, got {side!r}"
    valid = file.lines[side]
    if not _is_line_number(line) or line not in valid:
        return None, (f"{path}:{line} is not a {side} line of the diff; "
                      f"valid {side} lines: {describe_ranges(valid)}")
    if start_line is None or start_line == line:
        return Location(path, side, line), None
    if not _is_line_number(start_line) or start_line not in valid:
        return None, (f"start_line {start_line} of {path} is not a {side} line "
                      f"of the diff; valid {side} lines: "
                      f"{describe_ranges(valid)}")
    if start_line > line:
        return None, f"start_line {start_line} is after line {line}"
    if valid[start_line] != valid[line]:
        return None, (f"{path} lines {start_line}-{line} span more than one "
                      "diff hunk; use a range inside one hunk")
    return Location(path, side, line, start_line), None

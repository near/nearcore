"""Read-only repository tools that the review model can call.

The PR is fetched into a bare repository and read from git objects only.
There is no working tree, so a symlink in the PR cannot make the model read
files outside the repository (for example /proc/self/environ), and no PR
file is ever executed.
"""

# cspell:ignore NOSYSTEM textconv

import fnmatch
import inspect
import json
import os
import re
import subprocess
import threading

from . import diff

SHA_RE = re.compile(r"^[0-9a-f]{40}$")

GIT_TIMEOUT = 60
FETCH_TIMEOUT = 300
GREP_TIMEOUT = 20
MAX_OUTPUT_CHARS = 20_000
MAX_DIFF_CHARS = 30_000
MAX_READ_LINES = 400
MAX_LINE_CHARS = 1000
MAX_FILE_BYTES = 5 * 1024 * 1024
MAX_GREP_LINES = 100
MAX_GREP_BYTES = 1024 * 1024
MAX_LIST_ENTRIES = 300
MAX_FIND_RESULTS = 200
MAX_PATTERN_CHARS = 300
MAX_PATH_CHARS = 1024

MODE_SYMLINK = "120000"
MODE_SUBMODULE = "160000"


class ToolError(Exception):
    """An error that is reported back to the model as the tool result."""


class GitError(Exception):
    """A git command failed. Not shown to the model."""


def git_env(home: str) -> dict[str, str]:
    """Minimal environment for git: no secrets, no user or system config."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": home,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
        "LC_ALL": "C",
    }


class Repo:
    """A bare repository holding the commits under review."""

    def __init__(self, git_dir: str, home: str):
        self.git_dir = git_dir
        self.env = git_env(home)

    def _args(self, args: list[str]) -> list[str]:
        return ["git", "--git-dir", self.git_dir, "-c", "core.quotePath=false"
               ] + args

    def run(self, args: list[str], timeout: int = GIT_TIMEOUT) -> bytes:
        try:
            proc = subprocess.run(self._args(args),
                                  stdin=subprocess.DEVNULL,
                                  capture_output=True,
                                  env=self.env,
                                  timeout=timeout,
                                  check=False)
        except subprocess.TimeoutExpired as e:
            raise GitError(f"git {args[0]} timed out") from e
        if proc.returncode != 0:
            raise GitError(f"git {args[0]} failed with exit code "
                           f"{proc.returncode}: "
                           f"{proc.stderr.decode(errors='replace')[:500]}")
        return proc.stdout

    def run_limited(self, args: list[str], timeout: int,
                    max_bytes: int) -> tuple[int, bytes, bytes, bool, bool]:
        """Runs git, keeping at most `max_bytes` of stdout.

        Returns (exit code, stdout, stderr, truncated, timed out). A broad
        grep can print far more than we would ever show the model, so the
        process is killed once enough output has been read.
        """
        proc = subprocess.Popen(self._args(args),
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=self.env)
        timed_out = threading.Event()

        def kill_on_timeout():
            timed_out.set()
            proc.kill()

        timer = threading.Timer(timeout, kill_on_timeout)
        timer.start()
        try:
            out = proc.stdout.read(max_bytes)
            truncated = len(out) >= max_bytes
            if truncated:
                proc.kill()
            err = proc.stderr.read(4096)
            proc.wait()
        finally:
            timer.cancel()
            proc.stdout.close()
            proc.stderr.close()
        return proc.returncode, out, err, truncated, timed_out.is_set()

    @classmethod
    def fetch(cls, git_dir: str, home: str, url: str,
              shas: list[str]) -> "Repo":
        """Fetches the given commits (depth 1) into a new bare repository.

        The fetch is anonymous, so no GitHub token is ever written to disk.
        """
        for sha in shas:
            if not SHA_RE.match(sha):
                raise GitError(f"not a full commit sha: {sha!r}")
        repo = cls(git_dir, home)
        subprocess.run(["git", "init", "--quiet", "--bare", git_dir],
                       stdin=subprocess.DEVNULL,
                       capture_output=True,
                       env=repo.env,
                       timeout=GIT_TIMEOUT,
                       check=True)
        repo.run(["fetch", "--quiet", "--depth=1", "--no-tags", url] +
                 sorted(set(shas)),
                 timeout=FETCH_TIMEOUT)
        for sha in shas:
            object_type = repo.run(["cat-file", "-t", sha]).decode().strip()
            if object_type != "commit":
                raise GitError(f"fetched object {sha} is a {object_type}")
        return repo


def safe_path(path, allow_root: bool = False) -> str:
    """Validates a repository-relative path given by the model."""
    if not isinstance(path, str):
        raise ToolError("path must be a string")
    if path.startswith("./"):
        path = path[2:]
    if path in ("", "."):
        if allow_root:
            return ""
        raise ToolError("path must not be empty")
    if len(path) > MAX_PATH_CHARS:
        raise ToolError("path is too long")
    if any(c in path for c in "\0\n\r"):
        raise ToolError("path contains a control character")
    if path.startswith(("/", "-", ":")):
        raise ToolError("path must be relative to the repository root")
    parts = path.rstrip("/").split("/")
    for part in parts:
        if part in ("", ".", "..") or part.lower() == ".git":
            raise ToolError(f"invalid path component {part!r} in {path!r}")
    return "/".join(parts)


def _int_arg(value, name: str, default: int, low: int, high: int) -> int:
    if value is None:
        value = default
    elif not isinstance(value, int) or isinstance(value, bool):
        raise ToolError(f"{name} must be an integer")
    return min(max(value, low), high)


def parse_arguments(raw) -> dict:
    """Parses tool call arguments, which should be a JSON object string.

    Some OpenAI-compatible servers send an already decoded object instead.
    """
    if isinstance(raw, dict):
        return raw
    if raw is None or raw == "":
        return {}
    if not isinstance(raw, str):
        raise ValueError("arguments must be a JSON object")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("arguments are not valid JSON") from None
    if not isinstance(value, dict):
        raise ValueError("arguments must be a JSON object")
    return value


def clip_output(text: str, limit: int | None = None) -> str:
    """Cuts text at a line boundary and says how much was left out."""
    if limit is None:
        limit = MAX_OUTPUT_CHARS
    if len(text) <= limit:
        return text
    cut = text.rfind("\n", 0, limit)
    if cut <= 0:
        cut = limit
    rest = text[cut:].strip("\n")
    left_out = rest.count("\n") + 1 if rest else 0
    return (text[:cut] + f"\n[truncated: {left_out} more lines; "
            "narrow the request to see them]")


READ_ONLY_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "list_changed_files",
            "description":
                "List the files changed by the PR with status, line counts "
                "and whether GitHub provided a diff for inline comments.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_file_diff",
            "description":
                "Show the PR diff of one changed file. Every line is "
                "prefixed with its old (L<n>) and new (R<n>) line number. "
                "Use these numbers for inline findings: R numbers with "
                "side RIGHT, L numbers with side LEFT.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string"
                    },
                    "start_hunk": {
                        "type":
                            "integer",
                        "description":
                            "First hunk to show (0-based), for long diffs."
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description":
                "Read numbered lines of a file. ref 'head' is the PR "
                "version (default), ref 'base' is the version the PR "
                f"branched from. At most {MAX_READ_LINES} lines per call.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string"
                    },
                    "ref": {
                        "type": "string",
                        "enum": ["head", "base"]
                    },
                    "start_line": {
                        "type": "integer"
                    },
                    "end_line": {
                        "type": "integer"
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_dir",
            "description": "List the entries of a directory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Directory, empty for the root."
                    },
                    "ref": {
                        "type": "string",
                        "enum": ["head", "base"]
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_files",
            "description":
                "Find file paths matching a glob, for example "
                "'chain/**/*.rs' or '*Cargo.toml'. '*' also matches '/'.",
            "parameters": {
                "type": "object",
                "properties": {
                    "glob": {
                        "type": "string"
                    },
                    "ref": {
                        "type": "string",
                        "enum": ["head", "base"]
                    },
                },
                "required": ["glob"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "grep",
            "description":
                "Search file contents with git grep. The pattern is a POSIX "
                "extended regex unless fixed is true. path_glob limits the "
                "search, for example 'chain/**/*.rs' ('*' does not match "
                "'/', '**' does).",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string"
                    },
                    "path_glob": {
                        "type": "string"
                    },
                    "ref": {
                        "type": "string",
                        "enum": ["head", "base"]
                    },
                    "fixed": {
                        "type": "boolean"
                    },
                    "ignore_case": {
                        "type": "boolean"
                    },
                    "context": {
                        "type": "integer",
                        "description": "Lines of context, 0 to 3."
                    },
                },
                "required": ["pattern"],
            },
        },
    },
]


class RepoTools:
    """Implements the read-only tools over one PR's commits."""

    def __init__(self, repo: Repo, head_sha: str, base_sha: str,
                 files: dict[str, diff.FileDiff]):
        self.repo = repo
        self.refs = {"head": head_sha, "base": base_sha}
        self.files = files
        self._tree_cache: dict[str, list[str]] = {}

    def call(self, name: str, raw_arguments: str) -> str:
        """Runs one tool call and returns the text shown to the model."""
        handlers = {
            "list_changed_files": self.list_changed_files,
            "get_file_diff": self.get_file_diff,
            "read_file": self.read_file,
            "list_dir": self.list_dir,
            "find_files": self.find_files,
            "grep": self.grep,
        }
        handler = handlers.get(name)
        if handler is None:
            return f"error: unknown tool {name!r}"
        try:
            arguments = parse_arguments(raw_arguments)
        except ValueError as e:
            return f"error: {e}"
        try:
            inspect.signature(handler).bind(**arguments)
        except TypeError:
            return f"error: unexpected or missing arguments for {name}"
        try:
            return clip_output(handler(**arguments))
        except ToolError as e:
            return f"error: {e}"
        except GitError:
            return "error: the repository lookup failed"

    def _sha(self, ref) -> str:
        if ref is None:
            ref = "head"
        if ref not in self.refs:
            raise ToolError("ref must be 'head' or 'base'")
        return self.refs[ref]

    def _entry(self, sha: str, path: str) -> tuple[str, str, str] | None:
        out = self.repo.run(["ls-tree", "-z", sha, "--", path])
        for record in out.split(b"\0"):
            if not record:
                continue
            meta, _, name = record.decode(errors="replace").partition("\t")
            if name == path:
                mode, object_type, oid = meta.split()
                return mode, object_type, oid
        return None

    def list_changed_files(self) -> str:
        rows = []
        for file in self.files.values():
            note = "" if file.has_patch() else ", no diff (binary or too large)"
            renamed = (f" (renamed from {file.previous_path})"
                       if file.previous_path else "")
            rows.append(f"{file.status:9} +{file.additions} -{file.deletions} "
                        f"{file.path}{renamed}{note}")
        return "\n".join(rows) if rows else "no changed files"

    def get_file_diff(self, path, start_hunk=None) -> str:
        path = safe_path(path)
        file = self.files.get(path)
        if file is None:
            raise ToolError(f"{path} is not changed by this PR")
        if not file.patch:
            return (f"{path}: GitHub provided no diff (binary or too large). "
                    "Inline comments are not possible on this file; use "
                    "read_file to inspect it.")
        hunks = _split_hunks(diff.annotate_patch(file.patch))
        start = _int_arg(start_hunk, "start_hunk", 0, 0, len(hunks))
        out = [f"{path} ({file.status}), hunks {start}-{len(hunks) - 1}:"]
        size = len(out[0])
        for index in range(start, len(hunks)):
            if size + len(hunks[index]) > MAX_DIFF_CHARS and index > start:
                out.append(f"[more hunks: call again with start_hunk={index}]")
                break
            out.append(hunks[index])
            size += len(hunks[index])
        return "\n".join(out)

    def read_file(self, path, ref=None, start_line=None, end_line=None) -> str:
        path = safe_path(path)
        sha = self._sha(ref)
        entry = self._entry(sha, path)
        if entry is None:
            return f"{path} does not exist at {ref or 'head'}"
        mode, object_type, oid = entry
        if object_type == "tree":
            return f"{path} is a directory; use list_dir"
        if mode == MODE_SUBMODULE:
            return f"{path} is a submodule at commit {oid}"
        if mode == MODE_SYMLINK:
            target = self.repo.run(["cat-file", "blob", oid])
            return (f"{path} is a symlink -> "
                    f"{target.decode(errors='replace')[:MAX_PATH_CHARS]} "
                    "(not followed)")
        size = int(self.repo.run(["cat-file", "-s", oid]).decode())
        if size > MAX_FILE_BYTES:
            return f"{path} is too large to read ({size} bytes)"
        data = self.repo.run(["cat-file", "blob", oid])
        if b"\0" in data[:8000]:
            return f"{path} is a binary file ({size} bytes)"
        lines = data.decode(errors="replace").split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        total = len(lines)
        if total == 0:
            return f"{path} is empty"
        start = _int_arg(start_line, "start_line", 1, 1, total)
        end = _int_arg(end_line, "end_line", start + MAX_READ_LINES - 1, start,
                       total)
        end = min(end, start + MAX_READ_LINES - 1)
        out = [f"{path} ({ref or 'head'}), lines {start}-{end} of {total}:"]
        for number in range(start, end + 1):
            text = lines[number - 1]
            if len(text) > MAX_LINE_CHARS:
                text = text[:MAX_LINE_CHARS] + " [line clipped]"
            out.append(f"{number:>6}| {text}")
        return "\n".join(out)

    def list_dir(self, path=None, ref=None) -> str:
        path = safe_path(path if path is not None else "", allow_root=True)
        sha = self._sha(ref)
        args = ["ls-tree", "-z", sha]
        if path:
            entry = self._entry(sha, path)
            if entry is None:
                return f"{path} does not exist at {ref or 'head'}"
            if entry[1] != "tree":
                return f"{path} is not a directory"
            args += ["--", path + "/"]
        entries = []
        for record in self.repo.run(args).split(b"\0"):
            if not record:
                continue
            meta, _, name = record.decode(errors="replace").partition("\t")
            mode, object_type, _ = meta.split()
            name = name.rsplit("/", 1)[-1]
            if object_type == "tree":
                entries.append(name + "/")
            elif mode == MODE_SYMLINK:
                entries.append(name + " (symlink)")
            elif mode == MODE_SUBMODULE:
                entries.append(name + " (submodule)")
            else:
                entries.append(name)
        if len(entries) > MAX_LIST_ENTRIES:
            more = len(entries) - MAX_LIST_ENTRIES
            entries = entries[:MAX_LIST_ENTRIES] + [f"[{more} more entries]"]
        return "\n".join(entries) if entries else "empty directory"

    def _all_paths(self, sha: str) -> list[str]:
        if sha not in self._tree_cache:
            out = self.repo.run(["ls-tree", "-r", "-z", "--name-only", sha])
            self._tree_cache[sha] = [
                p.decode(errors="replace") for p in out.split(b"\0") if p
            ]
        return self._tree_cache[sha]

    def find_files(self, glob, ref=None) -> str:
        if not isinstance(glob, str) or not glob:
            raise ToolError("glob must be a non-empty string")
        if len(glob) > MAX_PATTERN_CHARS:
            raise ToolError("glob is too long")
        matches = [
            p for p in self._all_paths(self._sha(ref))
            if fnmatch.fnmatchcase(p, glob)
        ]
        if not matches:
            return "no matching files"
        shown = matches[:MAX_FIND_RESULTS]
        if len(matches) > len(shown):
            shown.append(f"[{len(matches) - len(shown)} more matches]")
        return "\n".join(shown)

    def grep(self,
             pattern,
             path_glob=None,
             ref=None,
             fixed=False,
             ignore_case=False,
             context=None) -> str:
        if not isinstance(pattern, str) or not pattern:
            raise ToolError("pattern must be a non-empty string")
        if len(pattern) > MAX_PATTERN_CHARS or "\n" in pattern:
            raise ToolError("pattern is too long or has a newline")
        context_lines = _int_arg(context, "context", 0, 0, 3)
        sha = self._sha(ref)
        args = ["grep", "-n", "-I", "--no-color", "--no-textconv"]
        args.append("-F" if fixed is True else "-E")
        if ignore_case is True:
            args.append("-i")
        if context_lines:
            args.append(f"-C{context_lines}")
        # -e keeps a pattern that starts with '-' from being read as an option.
        args += ["-e", pattern, sha, "--"]
        if path_glob is not None:
            if (not isinstance(path_glob, str) or
                    len(path_glob) > MAX_PATTERN_CHARS or
                    any(c in path_glob for c in "\0\n")):
                raise ToolError("invalid path_glob")
            args.append(":(glob)" + path_glob)
        code, out, err, truncated, timed_out = self.repo.run_limited(
            args, GREP_TIMEOUT, MAX_GREP_BYTES)
        if timed_out:
            return "error: grep timed out; narrow the pattern or path_glob"
        if code == 1 and not out:
            return "no matches"
        if code not in (0, 1) and not truncated:
            message = err.decode(errors="replace").strip().splitlines()
            return "error: grep failed: " + (message[-1][:300]
                                             if message else "unknown error")
        prefix = sha + ":"
        lines = [
            line[len(prefix):] if line.startswith(prefix) else line
            for line in out.decode(errors="replace").split("\n")
            if line
        ]
        limit = MAX_GREP_LINES * (1 + 2 * context_lines)
        if truncated or len(lines) > limit:
            lines = lines[:limit] + [
                "[more matches not shown; narrow the pattern or path_glob]"
            ]
        return "\n".join(line[:300] for line in lines)


def _split_hunks(annotated: str) -> list[str]:
    hunks: list[list[str]] = []
    for line in annotated.split("\n"):
        if diff.HUNK_HEADER_RE.match(line) or not hunks:
            hunks.append([])
        hunks[-1].append(line)
    return ["\n".join(h) for h in hunks]

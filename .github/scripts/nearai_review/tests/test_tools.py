# cspell:ignore cacheinfo uploadpack nbbbb ncccc

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

from nearai_review import diff, tools

LIB_HEAD = "".join(f"line {n}\n" for n in range(1, 501))


def _git(cwd, env, *args):
    return subprocess.run(["git", *args],
                          cwd=cwd,
                          env=env,
                          check=True,
                          capture_output=True).stdout.decode().strip()


class ToolsTestBase(unittest.TestCase):
    """Builds a source repo with two commits and fetches it like CI does."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.home = os.path.join(cls.tmp, "home")
        os.mkdir(cls.home)
        src = os.path.join(cls.tmp, "src")
        os.mkdir(src)
        env = dict(tools.git_env(cls.home),
                   GIT_AUTHOR_NAME="t",
                   GIT_AUTHOR_EMAIL="t@example.com",
                   GIT_COMMITTER_NAME="t",
                   GIT_COMMITTER_EMAIL="t@example.com")
        _git(src, env, "init", "--quiet")
        _git(src, env, "config", "uploadpack.allowAnySHA1InWant", "true")

        def write(path, data):
            full = os.path.join(src, path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            mode = "wb" if isinstance(data, bytes) else "w"
            with open(full, mode) as f:
                f.write(data)

        write("src/lib.rs", "fn a() {}\nfn b() {}\n")
        write("README.md", "readme\n")
        _git(src, env, "add", "-A")
        _git(src, env, "commit", "--quiet", "-m", "base")
        cls.base = _git(src, env, "rev-parse", "HEAD")

        write("src/lib.rs", LIB_HEAD)
        write("a/b/c.rs", "let secret_token = 1;\nlet Other = 2;\n")
        write("a/notes.txt", "uses --version flag\ncall(x\nsecret_token\n")
        write("bin.dat", b"\x00\x01binary")
        write("big.txt", "x" * 5000 + "\n")
        write("long.txt", "y" * 2000 + "\n")
        write("empty.txt", "")
        os.symlink("src/lib.rs", os.path.join(src, "link_in"))
        os.symlink("/proc/self/environ", os.path.join(src, "link_out"))
        os.symlink("../../etc/passwd", os.path.join(src, "link_up"))
        _git(src, env, "add", "-A")
        _git(src, env, "update-index", "--add", "--cacheinfo",
             f"160000,{cls.base},vendor/sub")
        _git(src, env, "commit", "--quiet", "-m", "head")
        cls.head = _git(src, env, "rev-parse", "HEAD")

        cls.repo = tools.Repo.fetch(os.path.join(cls.tmp, "pr.git"), cls.home,
                                    "file://" + src, [cls.head, cls.base])
        cls.src = src

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        files = {
            "src/lib.rs":
                diff.FileDiff.from_api({
                    "filename": "src/lib.rs",
                    "status": "modified",
                    "additions": 2,
                    "deletions": 1,
                    "patch": "@@ -1,2 +1,3 @@\n-fn a() {}\n+line 1\n+line 2\n"
                             " fn b() {}\n@@ -10,1 +11,1 @@\n-x\n+y",
                }),
            "bin.dat":
                diff.FileDiff.from_api({
                    "filename": "bin.dat",
                    "status": "added"
                }),
            "new.rs":
                diff.FileDiff.from_api({
                    "filename": "new.rs",
                    "previous_filename": "old.rs",
                    "status": "renamed",
                    "patch": "@@ -1 +1 @@\n-a\n+b",
                }),
        }
        self.tools = tools.RepoTools(self.repo, self.head, self.base, files)

    def call(self, name, **arguments):
        return self.tools.call(name, json.dumps(arguments))


class FetchTest(ToolsTestBase):

    def test_fetched_both_commits(self):
        self.assertTrue(os.path.isdir(os.path.join(self.tmp, "pr.git")))
        self.assertIn("line 1", self.call("read_file", path="src/lib.rs"))

    def test_rejects_non_sha(self):
        with self.assertRaises(tools.GitError):
            tools.Repo.fetch(os.path.join(self.tmp, "bad.git"), self.home,
                             "file://" + self.src, ["HEAD"])

    def test_missing_commit_fails(self):
        with self.assertRaises(tools.GitError):
            tools.Repo.fetch(os.path.join(self.tmp, "missing.git"), self.home,
                             "file://" + self.src, ["0" * 40])

    def test_git_env_has_no_secrets(self):
        with mock.patch.dict(os.environ, {"NEARAI_API_KEY": "sk-secret"}):
            env = tools.git_env(self.home)
        self.assertNotIn("NEARAI_API_KEY", env)
        self.assertNotIn("sk-secret", env.values())


class SafePathTest(unittest.TestCase):

    def test_valid(self):
        self.assertEqual(tools.safe_path("src/lib.rs"), "src/lib.rs")
        self.assertEqual(tools.safe_path("./src/lib.rs"), "src/lib.rs")
        self.assertEqual(tools.safe_path("src/"), "src")
        self.assertEqual(tools.safe_path("", allow_root=True), "")

    def test_rejected(self):
        for bad in ("", ".", "/etc/passwd", "../x", "src/../x", ".git/config",
                    "a/.GIT/x", "-x", ":(top)x", "a\0b", "a\nb", "a//b",
                    "x" * 2000, 123, None):
            with self.subTest(bad=bad):
                with self.assertRaises(tools.ToolError):
                    tools.safe_path(bad)


class ReadFileTest(ToolsTestBase):

    def test_default_range_is_capped(self):
        out = self.call("read_file", path="src/lib.rs")
        self.assertIn("lines 1-400 of 500", out)
        self.assertIn("     1| line 1", out)
        self.assertIn("   400| line 400", out)
        self.assertNotIn("line 401", out)

    def test_explicit_range_and_clamping(self):
        out = self.call("read_file",
                        path="src/lib.rs",
                        start_line=499,
                        end_line=900)
        self.assertIn("lines 499-500 of 500", out)
        out = self.call("read_file",
                        path="src/lib.rs",
                        start_line=10,
                        end_line=5)
        self.assertIn("lines 10-10 of 500", out)
        out = self.call("read_file", path="src/lib.rs", start_line=9999)
        self.assertIn("lines 500-500 of 500", out)

    def test_base_ref(self):
        out = self.call("read_file", path="src/lib.rs", ref="base")
        self.assertIn("fn a() {}", out)
        self.assertIn("does not exist at base",
                      self.call("read_file", path="bin.dat", ref="base"))

    def test_bad_arguments(self):
        self.assertIn("ref must be",
                      self.call("read_file", path="src/lib.rs", ref="main"))
        self.assertIn(
            "start_line must be an integer",
            self.call("read_file", path="src/lib.rs", start_line=True))
        self.assertIn("invalid path component",
                      self.call("read_file", path="../outside"))

    def test_symlinks_are_not_followed(self):
        out = self.call("read_file", path="link_out")
        self.assertIn("symlink -> /proc/self/environ (not followed)", out)
        self.assertNotIn("PATH=", out)
        self.assertIn("symlink -> ../../etc/passwd",
                      self.call("read_file", path="link_up"))
        self.assertIn("symlink -> src/lib.rs",
                      self.call("read_file", path="link_in"))
        self.assertIn("does not exist", self.call("read_file",
                                                  path="link_in/x"))

    def test_special_entries(self):
        self.assertIn("is a directory", self.call("read_file", path="src"))
        self.assertIn("is a submodule at commit " + self.base,
                      self.call("read_file", path="vendor/sub"))
        self.assertIn("is a binary file", self.call("read_file",
                                                    path="bin.dat"))
        self.assertIn("is empty", self.call("read_file", path="empty.txt"))
        self.assertIn("does not exist", self.call("read_file", path="nope.rs"))

    def test_large_file_and_long_line(self):
        with mock.patch.object(tools, "MAX_FILE_BYTES", 1000):
            self.assertIn("too large", self.call("read_file", path="big.txt"))
        out = self.call("read_file", path="long.txt")
        self.assertIn("[line clipped]", out)
        self.assertLess(len(out), 1200)


class ListAndFindTest(ToolsTestBase):

    def test_list_root(self):
        out = self.call("list_dir").split("\n")
        for expected in ("src/", "a/", "vendor/", "link_out (symlink)",
                         "README.md"):
            self.assertIn(expected, out)

    def test_list_subdir_and_errors(self):
        self.assertEqual(self.call("list_dir", path="vendor"),
                         "sub (submodule)")
        self.assertIn("b/", self.call("list_dir", path="a/").split("\n"))
        self.assertIn("not a directory", self.call("list_dir",
                                                   path="README.md"))
        self.assertIn("does not exist", self.call("list_dir", path="zzz"))
        self.assertIn("lib.rs", self.call("list_dir", path="src", ref="base"))

    def test_list_cap(self):
        with mock.patch.object(tools, "MAX_LIST_ENTRIES", 2):
            out = self.call("list_dir").split("\n")
        self.assertEqual(len(out), 3)
        self.assertTrue(out[-1].endswith("more entries]"))

    def test_find_files(self):
        out = self.call("find_files", glob="*.rs").split("\n")
        self.assertEqual(sorted(out), ["a/b/c.rs", "src/lib.rs"])
        self.assertEqual(self.call("find_files", glob="*.go"),
                         "no matching files")
        self.assertEqual(self.call("find_files", glob="*.rs", ref="base"),
                         "src/lib.rs")
        self.assertIn("glob must be", self.call("find_files", glob=""))
        with mock.patch.object(tools, "MAX_FIND_RESULTS", 1):
            self.assertIn("1 more matches", self.call("find_files",
                                                      glob="*.rs"))


class GrepTest(ToolsTestBase):

    def test_match_has_path_and_line_without_sha(self):
        out = self.call("grep", pattern="secret_token")
        self.assertIn("a/b/c.rs:1:let secret_token = 1;", out)
        self.assertIn("a/notes.txt:3:secret_token", out)
        self.assertNotIn(self.head, out)

    def test_pattern_starting_with_dash(self):
        out = self.call("grep", pattern="--version", fixed=True)
        self.assertIn("a/notes.txt:1:uses --version flag", out)

    def test_invalid_regex_and_fixed(self):
        self.assertIn("error: grep failed", self.call("grep", pattern="call("))
        self.assertIn("a/notes.txt:2:call(x",
                      self.call("grep", pattern="call(", fixed=True))

    def test_options(self):
        self.assertIn("a/b/c.rs:2:let Other = 2;",
                      self.call("grep", pattern="other", ignore_case=True))
        self.assertEqual(self.call("grep", pattern="other"), "no matches")
        out = self.call("grep", pattern="^line 250$", context=1)
        self.assertIn("src/lib.rs-249-line 249", out)
        self.assertIn("src/lib.rs:250:line 250", out)
        self.assertEqual(
            self.call("grep", pattern="secret_token", path_glob="a/*.txt"),
            "a/notes.txt:3:secret_token")
        self.assertEqual(self.call("grep", pattern="secret_token", ref="base"),
                         "no matches")
        self.assertIn("context must be an integer",
                      self.call("grep", pattern="x", context="2"))

    def test_bad_inputs(self):
        self.assertIn("pattern must be", self.call("grep", pattern=""))
        self.assertIn("too long", self.call("grep", pattern="x" * 400))
        self.assertIn("invalid path_glob",
                      self.call("grep", pattern="x", path_glob=["a"]))

    def test_output_cap(self):
        with mock.patch.object(tools, "MAX_GREP_LINES", 5):
            out = self.call("grep", pattern="^line").split("\n")
        self.assertEqual(len(out), 6)
        self.assertIn("more matches not shown", out[-1])
        with mock.patch.object(tools, "MAX_GREP_BYTES", 50):
            out = self.call("grep", pattern="^line")
        self.assertIn("more matches not shown", out)

    def test_timeout(self):
        with mock.patch.object(self.repo,
                               "run_limited",
                               return_value=(-9, b"", b"", False, True)):
            self.assertIn("timed out", self.call("grep", pattern="x"))


class DiffToolsTest(ToolsTestBase):

    def test_list_changed_files(self):
        out = self.call("list_changed_files")
        self.assertIn("modified  +2 -1 src/lib.rs", out)
        self.assertIn("bin.dat, no diff (binary or too large)", out)
        self.assertIn("new.rs (renamed from old.rs)", out)

    def test_get_file_diff(self):
        out = self.call("get_file_diff", path="src/lib.rs")
        self.assertIn("hunks 0-1", out)
        self.assertIn("R2 | line 2", out)
        self.assertIn("@@ -10,1 +11,1 @@", out)
        self.assertIn("no diff",
                      self.call("get_file_diff", path="bin.dat").lower())
        self.assertIn("not changed by this PR",
                      self.call("get_file_diff", path="README.md"))

    def test_get_file_diff_paging(self):
        with mock.patch.object(tools, "MAX_DIFF_CHARS", 60):
            out = self.call("get_file_diff", path="src/lib.rs")
            self.assertIn("call again with start_hunk=1", out)
            out = self.call("get_file_diff", path="src/lib.rs", start_hunk=1)
            self.assertIn("@@ -10,1 +11,1 @@", out)
            self.assertNotIn("fn b()", out)


class CallTest(ToolsTestBase):

    def test_errors(self):
        self.assertIn("unknown tool", self.tools.call("rm", "{}"))
        self.assertIn("not valid JSON", self.tools.call("read_file", "{"))
        self.assertIn("must be a JSON object",
                      self.tools.call("read_file", "[1]"))
        self.assertIn("missing arguments", self.tools.call("read_file", "{}"))
        self.assertIn(
            "unexpected or missing arguments",
            self.tools.call("read_file", '{"path": "README.md", "x": 1}'))
        self.assertIn("readme",
                      self.tools.call("read_file", '{"path": "README.md"}'))

    def test_git_failure_is_generic(self):
        with mock.patch.object(self.repo,
                               "run",
                               side_effect=tools.GitError("details")):
            out = self.call("read_file", path="README.md")
        self.assertEqual(out, "error: the repository lookup failed")

    def test_output_is_clipped(self):
        with mock.patch.object(tools, "MAX_OUTPUT_CHARS", 200):
            out = self.call("read_file", path="src/lib.rs")
        self.assertLess(len(out), 300)
        self.assertIn("[truncated:", out)


class ClipOutputTest(unittest.TestCase):

    def test_clip(self):
        self.assertEqual(tools.clip_output("short", 10), "short")
        out = tools.clip_output("aaaa\nbbbb\ncccc\n", 10)
        self.assertTrue(out.startswith("aaaa\nbbbb\n[truncated: 1 more lines"))
        out = tools.clip_output("x" * 30, 10)
        self.assertTrue(out.startswith("x" * 10 + "\n[truncated: 1 more lines"))


if __name__ == "__main__":
    unittest.main()

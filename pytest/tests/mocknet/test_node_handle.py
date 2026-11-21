#!/usr/bin/env python3
"""
Unit tests for the shell command builders in pytest/tests/mocknet/node_handle.py.
NayDuck runs them in the merge queue and nightly (nightly/pytest-mocknet.txt).
They run the built commands with the local bash and jq. They do not connect
to any host.

    python3 pytest/tests/mocknet/test_node_handle.py
"""
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import types
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from node_handle import (HOST_SCRIPTS_DIR, NodeHandle, env_file_content,
                         jq_set_cmd, parse_config_changes, require_state_cmd,
                         write_file_cmd)


def bash(cmd):
    return subprocess.run(['bash', '-c', cmd], capture_output=True, text=True)


class LocalNode:
    """Runs NodeHandle commands with the local bash, in `forknet_home`."""

    def __init__(self, forknet_home):
        self.forknet_home = forknet_home
        self.near_home = forknet_home

    def run_cmd(self,
                schedule_ctx,
                cmd,
                raise_on_fail=False,
                return_on_fail=False):
        r = bash(cmd)
        if r.returncode != 0 and not return_on_fail:
            raise AssertionError(f'{cmd} failed: {r.stderr}')
        return types.SimpleNamespace(exitcode=r.returncode,
                                     stdout=r.stdout,
                                     stderr=r.stderr)


class TestCommandBuilders(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)

    def test_env_file_content(self):
        values = {'A': 'two words', 'B': "it's \"quoted\" $HOME", 'C': None}
        path = self.dir / 'env'
        path.write_text(env_file_content(values))

        r = bash(f'source {path}; printf "%s|%s|%s" "$A" "$B" "$C"')

        self.assertEqual(r.stdout, 'two words|it\'s "quoted" $HOME|')

    def test_write_file_cmd(self):
        content = 'line "one" $x\n\'two\'\n'
        path = self.dir / 'out'

        r = bash(write_file_cmd(str(path), content))

        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(path.read_text(), content)

    def test_require_state_cmd(self):
        state = self.dir / 'state'
        state.write_text('ready\n')

        ok = bash(f'{require_state_cmd(str(state), "ready")} && echo next')
        bad = bash(f'{require_state_cmd(str(state), "resetting")} && echo next')

        self.assertEqual(ok.stdout, 'next\n')
        self.assertNotEqual(bad.returncode, 0)
        self.assertEqual(bad.stdout, '')
        self.assertIn('state is ready, expected resetting', bad.stderr)

    def test_parse_config_changes(self):
        changes = parse_config_changes('a.b.c=5;d="x";e={"f":[1,2]}')

        self.assertEqual(changes, [
            (['a', 'b', 'c'], 5),
            (['d'], 'x'),
            (['e'], {
                'f': [1, 2]
            }),
        ])

    @unittest.skipUnless(shutil.which('jq'), 'jq is not installed')
    def test_jq_set_cmd(self):
        path = self.dir / 'config.json'
        path.write_text(json.dumps({'a': {'keep': 1, 'b': 0}, 's': 'old'}))
        changes = [(['a', 'b'], 2), (['s'], "it's"), (['new', 'x'], [1])]

        r = bash(jq_set_cmd(str(path), changes))

        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(path.read_text()), {
            'a': {
                'keep': 1,
                'b': 2
            },
            's': "it's",
            'new': {
                'x': [1]
            },
        })

    def test_update_binaries_replaces_one_url(self):
        # Setup: a forknet home with the host scripts and two local binaries.
        shutil.copytree(HOST_SCRIPTS_DIR, self.dir, dirs_exist_ok=True)
        (self.dir / 'host.env').write_text('')
        binaries = self.dir / 'binaries'
        binaries.mkdir()
        old, new = self.dir / 'old-neard', self.dir / 'new-neard'
        old.write_text('old')
        new.write_text('new')
        (binaries / 'urls').write_text(f'{old}\n{old}\n')
        node = NodeHandle(LocalNode(str(self.dir)))

        node.update_binaries(str(new), 1)

        # Check: only line 2 changed, and neard1 links to the new binary,
        # which download-binaries.sh made executable.
        self.assertEqual((binaries / 'urls').read_text(), f'{old}\n{new}\n')
        self.assertEqual((binaries / 'neard1').resolve(), new.resolve())
        self.assertTrue(new.stat().st_mode & 0o111)
        with self.assertRaises(AssertionError):
            node.update_binaries(str(new), 2)


if __name__ == '__main__':
    unittest.main()

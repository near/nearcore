#!/usr/bin/env python
from rc import run, run_stream, STDOUT, STDERR
from rc.test.util import Timer
import glob

files_to_test = map(lambda x: x.replace('tests/', ''), glob.glob('tests/*/*.py'))
files_to_skip = ['sanity/remote.py']


def run_test(path):
    exitcode = None
    with Timer(path):
        q, _ = run_stream(['python', 'tests/'+path])
        with open('/tmp/near-pytest-output.txt', 'w') as f:
            while True:
                out = q.get()
                if out[0] == STDOUT or out[0] == STDERR:
                    f.write(out[1])
                    print(f'{path}: {out[1]}', end='')
                else:
                    exitcode = out[1]
                    f.write(f'EXIT CODE: {exitcode}')
                    break
                q.task_done()
    
    if exitcode != 0:
        dir, pyfile = path.split('/')
        outfile = pyfile + '.txt'
        run(['mkdir', '-p', 'failed_tests_output/' + dir])
        run(['mv', '/tmp/near-pytest-output.txt', 'failed_tests_output/'+dir+'/'+outfile])
        return False
    return True


fails = []
for f in files_to_test:
    if f not in files_to_skip:
        if not run_test(f):
            fails.append(f)
if fails:
    print('Failed tests:', fails)
    exit(1)
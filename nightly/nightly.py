# This is a quick and dirty nightly tests runner, in the temporary absence of a better CI tool
#
# Running tests:
#  - full set:
#     python nightly.py run nightly.txt <outdir>
#  - small test set:
#     python nightly.py run nightly_small.txt <outdir>
#
# Serving the UI:
#  - run
#
#     python night.py serve
#
#    from a folder that contains all the <outdir>s from the above.
#

import sys, os, subprocess, shutil, time, psutil
from stat import ST_CTIME
from multiprocessing import Process
import tornado.ioloop
import tornado.web

DEFAULT_TIMEOUT = 180

def usage():
    print("Usage:")
    print("    python nightly.py run <tests list file> <outdir>")
    print(" or")
    print("    python nightly.py serve")
    sys.exit(1)


def get_test_cmd(test):
    try:
        if test[0] == 'basic':
            return ["cargo", "test", "-j2", "--all"]
        elif test[0] == 'pytest':
            return ["python", "tests/" + test[1]] + test[2:]
        elif test[0] == 'expensive':
            return ["cargo", "test", "-j2", "--color=always", "--package", test[1], "--test", test[2], test[3], "--all-features", "--", "--exact", "--nocapture"]
        else:
            assert False, test
    except:
        print(test)
        raise


def get_test_hash(test):
    return "_".join(test).replace(":", "_").replace('/', '_').replace('.', '_')
        

def run_test(outdir, test):
    timeout = DEFAULT_TIMEOUT
    if len(test) > 1 and test[1].startswith('--timeout='):
        timeout = int(test[1][10:])
        test = [test[0]] + test[2:]

    test_hash = get_test_hash(test)
    dir_name = os.path.join(outdir, test_hash)
    os.makedirs(dir_name)

    cmd = get_test_cmd(test)

    if test[0] == 'pytest':
        node_dirs = subprocess.check_output("find ~/.near/test* -maxdepth 0 || true", shell=True).decode('utf-8').strip().split('\n')
        for node_dir in node_dirs:
            if node_dir:
                shutil.rmtree(node_dir)

    print("[RUNNING] %s" % (' '.join(test)))

    stdout = open(os.path.join(dir_name, 'stdout'), 'w')
    stderr = open(os.path.join(dir_name, 'stderr'), 'w')

    env = os.environ.copy()
    env["RUST_BACKTRACE"] = "1"

    handle = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, env=env)
    try:
        ret = handle.wait(timeout)
        if ret == 0:
            ignored = False
            if test[0] == 'expensive':
                with open(os.path.join(dir_name, 'stdout')) as f:
                    lines = f.readlines()
                    while len(lines) and lines[-1].strip() == '':
                        lines.pop()
                    if len(lines) == 0:
                        ignored = True
                    else:
                        if '0 passed' in lines[-1]:
                            ignored = True

            outcome = "PASSED" if not ignored else "IGNORED"
        else:
            outcome = "FAILED"
    except subprocess.TimeoutExpired as e:
        outcome = "TIMEOUT"
        print("Sending SIGINT to %s" % handle.pid)
        for child in psutil.Process(handle.pid).children(recursive=True):
            child.terminate()
        handle.terminate()
        handle.communicate()

    if test[0] == 'pytest':
        node_dirs = subprocess.check_output("find ~/.near/test* -maxdepth 0 || true", shell=True).decode('utf-8').strip().split('\n')
        for node_dir in node_dirs:
            shutil.copytree(node_dir, os.path.join(dir_name, os.path.basename(node_dir)))

    print("[%7s] %s" % (outcome, ' '.join(test)))

    with open(os.path.join(outdir, "log"), 'a') as f:
        f.write("%s %s %s\n" % (outcome, test_hash, ' '.join(test)))


def runner(test_type, outdir, tests):
    if test_type == 'pytest':
        os.chdir('../pytest')
    else:
        os.chdir('..')

    for test in tests:
        run_test(outdir, test)


def get_stdout_path(run_name, line):
    return os.path.join(run_name, line[2], line[1], 'stdout')


def get_stderr_path(run_name, line):
    return os.path.join(run_name, line[2], line[1], 'stderr')


def prettify_size(size):
    if size < 1024: return size
    size //= 1024
    if size < 1024: return "%sK" % size
    size //= 1024
    if size < 1024: return "<b>%s</b>M" % size
    size //= 1024
    if size < 1024: return "<font color=darkred><b>%s</b></font>G" % size


def format_one_run(run_name, line):
    if line[0] == 'PASSED':
        clr = 'darkgreen'
    elif line[0] == 'FAILED':
        clr = 'darkred'
    elif line[0] == 'TIMEOUT':
        clr = 'brown'
    elif line[0] == 'IGNORED':
        clr = 'blue'
    else:
        assert False, line[0]
    status = '<font color="%s">%s</font>' % (clr, line[0])
    test_type = '<b>%s</b>' % line[2]

    stdout_size = os.path.getsize(get_stdout_path(run_name, line))
    stderr_size = os.path.getsize(get_stderr_path(run_name, line))

    path_common = "%s/%s/%s" % (run_name, line[2], line[1])
    links = '<td><a href="/run/%s/stdout">stdout (%s)</a></td><td><a href=/run/%s/stderr>stderr (%s)</a></td>' % (path_common, prettify_size(stdout_size), path_common, prettify_size(stderr_size))
    if line[2] == 'basic':
        rest = '<a href="/test/basic/basic">basic</a>'
        links += '<td></td>'
    elif line[2] == 'expensive':
        rest = '<font color="grey">%s</font> ' % ' '.join(line[3:-1]) + '<a href="/test/expensive/%s">%s</a>' % (line[1], line[-1])
        links += '<td></td>'
    elif line[2] == 'pytest':
        rest = '<a href="/test/pytest/%s">%s</a>' % (line[1], line[3]) + ' <font color="grey">%s</font>' % ' '.join(line[4:])
        links += '<td>'

        for i in range(100):
            if os.path.exists(os.path.join(run_name, line[2], line[1], "test%s_finished" % i)):
                links += ' <a href="/run/%s/pytest/err/%s">log%s</a>' % (path_common, i, i)
            else:
                break

        links += '</td>'

    else:
        assert False, test_type
    return "<tr><td>[%s]</td>%s<td>%s %s</td></tr>" % (status, links, test_type, rest)


class ShowRunsListHandler(tornado.web.RequestHandler):
    def get(self):
        ret = ''
        subfolders = [f.path for f in os.scandir('.') if f.is_dir()]
        subfolders = reversed(sorted([(os.stat(x), x) for x in subfolders]))
        subfolders = [x[1] for x in subfolders]
        for subfolder in subfolders:
            name = os.path.split(subfolder)[-1]
            if '.' not in name:
                ret += '<a href="/run/%s">%s<aa><br>' % (name, name)
        self.finish(ret)
            


class ShowRunHandler(tornado.web.RequestHandler):
    def get(self, run_name):
        assert('.' not in run_name)
        assert('/' not in run_name)
        ret = '<table>'
        for test_type in ['basic', 'expensive', 'pytest']:
            try:
                with open(os.path.join(run_name, test_type, 'log')) as f:
                    for line in f.readlines():
                        ret += format_one_run(run_name, line.split())
            except Exception as e:
                print(e)
        ret += '</table>'
        self.finish(ret)


class ServeFileHandler(tornado.web.RequestHandler):
    def serve(self, path_parts):
        for part in path_parts:
            assert('.' not in part)
            assert('/' not in part)
        path = os.path.join(*path_parts)
        self.set_header("Content-Type", 'text/plain; charset="utf-8"')
        with open(path) as f:
            self.finish(f.read())


class ShowStdOutHandler(ServeFileHandler):
    def get(self, run_name, test_type, test_name):
        self.serve([run_name, test_type, test_name, 'stdout'])


class ShowStdErrHandler(ServeFileHandler):
    def get(self, run_name, test_type, test_name):
        self.serve([run_name, test_type, test_name, 'stderr'])


class ShowPytestLogHandler(ServeFileHandler):
    def get(self, run_name, test_type, test_name, kind, ordinal):
        assert kind in ['out', 'err']
        ordinal = int(ordinal)
        self.serve([run_name, test_type, test_name, 'test%s_finished' % ordinal, 'std%s' % kind])


class ShowTestHandler(tornado.web.RequestHandler):
    def get(self, test_type, test_name):
        ret = '<table>'
        subfolders = [f.path for f in os.scandir('.') if f.is_dir()]
        subfolders = reversed(sorted([(os.stat(x), x) for x in subfolders]))
        subfolders = [x[1] for x in subfolders]
        for subfolder in subfolders:
            run_name = os.path.split(subfolder)[-1]
            if '.' not in run_name:
                test_log = os.path.join(run_name, test_type, 'log')
                if os.path.exists(test_log):
                    with open(test_log) as f:
                        for line in f.readlines():
                            if test_name in line:
                                testline = format_one_run(run_name, line.split())
                                testline = testline.replace('<tr>', '<tr><td><a href="/run/%s">%s</a></td>' % (run_name, run_name))
                                ret += testline
                                break
        ret += '</table>'
        self.finish(ret)


def make_app():
    return tornado.web.Application([
        (r"/", ShowRunsListHandler),
        (r"/run/([^/]+)", ShowRunHandler),
        (r"/test/([^/]+)/([^/]+)", ShowTestHandler),
        (r"/run/([^/]+)/([^/]+)/([^/]+)/stdout", ShowStdOutHandler),
        (r"/run/([^/]+)/([^/]+)/([^/]+)/stderr", ShowStdErrHandler),
        (r"/run/([^/]+)/([^/]+)/([^/]+)/pytest/([^/]+)/([^/]+)", ShowPytestLogHandler),
    ], autoreload=True)


if __name__ == "__main__":

    if len(sys.argv) < 2:
        usage()

    mode = sys.argv[1]

    if mode == 'run':
        if len(sys.argv) < 4:
            usage()

        outdir = os.path.abspath(sys.argv[3])

        with open(sys.argv[2]) as f:
            tests = [x.split() for x in f.readlines() if len(x.strip()) and x[0] != '#']

        if os.path.exists(outdir):
            print("Directory %s already exists, not overwriting" % outdir)
            sys.exit(1)

        print("Running: \n" + '\n'.join(["   %s" % ' '.join(test) for test in tests]))

        os.makedirs(outdir)
        
        ps = []
        for test_type in ["basic", "pytest", "expensive"]:
            cur_outdir = os.path.join(outdir, test_type)
            os.makedirs(cur_outdir)
            p = Process(target=runner, args=(test_type, cur_outdir, [x for x in tests if x[0] == test_type]))
            p.start()
            ps.append(p)

        for p in ps:
            p.join()

    elif mode == 'serve':
        app = make_app()
        app.listen(8888)
        tornado.ioloop.IOLoop.current().start()

    else:
        usage()


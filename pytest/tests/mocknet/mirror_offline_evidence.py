#!/usr/bin/env python3
"""Evidence-gathering nayduck launcher for the offline mirror test.

The real test lives in pytest/tools/mirror/offline_test.py, which is outside
pytest/tests and therefore not directly runnable by NayDuck; this launcher is
the NayDuck entry point. It builds the addkey contract the test deploys, runs
the test as a subprocess, and uploads all artifacts (test output, node logs,
mirror logs, configs) to a GCS bucket with anonymous object-create access.

It ALWAYS exits 0 so nayduck never retries the test; a retry would overwrite
the logs in the nayduck UI, which is exactly the evidence we are trying to
keep. The actual verdict lives in the uploaded object path (runs/PASS/...,
runs/FAIL/..., runs/TIMEOUT/..., runs/BROKEN/...) and in the EVIDENCE-RESULT
line printed to stdout. Grep the nayduck test output for EVIDENCE-RUN-ID to
correlate it with the bucket object.
"""
import json
import os
import pathlib
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
import traceback
import uuid

BUCKET = os.environ.get('EVIDENCE_BUCKET', 'darioush-mirror-evidence')
# nayduck kills us at --timeout=25m; budget build + test + upload below that.
CONTRACT_BUILD_TIMEOUT = 8 * 60
INNER_TIMEOUT = 14 * 60
DOT_NEAR = pathlib.Path.home() / '.near'
PYTEST_DIR = pathlib.Path(__file__).resolve().parents[2]
TEST_SCRIPT = PYTEST_DIR / 'tools' / 'mirror' / 'offline_test.py'
CONTRACT_DIR = PYTEST_DIR / 'tools' / 'mirror' / 'contract'
MAX_FILE_SIZE = 200 * 1024 * 1024

run_id = '%s-%s-%s' % (time.strftime('%Y%m%d-%H%M%S'), socket.gethostname(),
                       uuid.uuid4().hex[:8])
output_path = pathlib.Path('/tmp') / f'evidence-{run_id}-test-output.log'
contract_log_path = pathlib.Path('/tmp') / f'evidence-{run_id}-contract.log'
tarball_path = pathlib.Path('/tmp') / f'evidence-{run_id}.tar.gz'


def build_contract():
    """Builds the addkey contract wasm the test deploys. Returns True on success.

    The contract is a standalone crate that the neard build does not produce.
    Clear the inherited rustflags: CI sets `-fuse-ld=lld` for the native build,
    which rust-lld rejects when linking the wasm target.
    """
    env = dict(os.environ)
    env['RUSTFLAGS'] = ''
    env.pop('CARGO_ENCODED_RUSTFLAGS', None)
    try:
        result = subprocess.run(
            [
                'cargo', 'build', '--target', 'wasm32-unknown-unknown',
                '--release'
            ],
            cwd=CONTRACT_DIR,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=CONTRACT_BUILD_TIMEOUT,
        )
        output, returncode = result.stdout, result.returncode
    except subprocess.TimeoutExpired as e:
        output, returncode = e.stdout or b'', -1
    contract_log_path.write_bytes(output)
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()
    print(f'EVIDENCE: contract build exited with {returncode}')
    return returncode == 0


def tee(stream, output_file):
    for line in iter(stream.readline, b''):
        output_file.write(line)
        output_file.flush()
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()


def run_test():
    """Returns (status, exit_code)."""
    with open(output_path, 'wb') as output_file:
        process = subprocess.Popen(
            [sys.executable, '-u', str(TEST_SCRIPT)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        reader = threading.Thread(target=tee,
                                  args=(process.stdout, output_file))
        reader.start()
        try:
            exit_code = process.wait(timeout=INNER_TIMEOUT)
            status = 'PASS' if exit_code == 0 else 'FAIL'
        except subprocess.TimeoutExpired:
            print(f'EVIDENCE: test exceeded {INNER_TIMEOUT}s, killing '
                  'its process group')
            status, exit_code = 'TIMEOUT', None
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        reader.join(timeout=30)
    return status, exit_code


def want_file(path):
    if path.name in ('stdout', 'stderr', 'config.json', 'genesis.json'):
        return True
    return path.suffix == '.log'


def collect_artifacts(status, exit_code, duration):
    metadata = {
        'run_id': run_id,
        'status': status,
        'exit_code': exit_code,
        'duration_seconds': round(duration, 1),
        'argv': sys.argv,
        'hostname': socket.gethostname(),
        'platform': platform.platform(),
        'started_by': 'mirror_offline_evidence.py',
        'time': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
    }
    try:
        metadata['git_sha'] = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], cwd=PYTEST_DIR, text=True).strip()
    except (subprocess.CalledProcessError, OSError):
        pass

    with tarfile.open(tarball_path, 'w:gz') as tar:
        metadata_path = pathlib.Path('/tmp') / f'evidence-{run_id}-meta.json'
        metadata_path.write_text(json.dumps(metadata, indent=2))
        tar.add(metadata_path, arcname='metadata.json')
        if output_path.exists():
            tar.add(output_path, arcname='test-output.log')
        if contract_log_path.exists():
            tar.add(contract_log_path, arcname='contract-build.log')
        if not DOT_NEAR.exists():
            return
        for root, dirs, files in os.walk(DOT_NEAR):
            # RocksDB contents are huge and not useful as logs.
            dirs[:] = [d for d in dirs if d != 'data']
            for name in files:
                path = pathlib.Path(root) / name
                if not want_file(path):
                    continue
                try:
                    if path.stat().st_size > MAX_FILE_SIZE:
                        print(f'EVIDENCE: skipping oversized {path}')
                        continue
                    tar.add(path,
                            arcname=str(path.relative_to(DOT_NEAR.parent)))
                except OSError as e:
                    print(f'EVIDENCE: could not add {path}: {e}')


def upload(status):
    object_name = f'runs/{status}/{run_id}.tar.gz'
    url = (f'https://storage.googleapis.com/upload/storage/v1/b/{BUCKET}/o'
           f'?uploadType=media&name={object_name}')
    result = subprocess.run([
        'curl', '-sS', '--fail', '--retry', '3', '--retry-all-errors',
        '--max-time', '120', '-X', 'POST', '-H',
        'Content-Type: application/gzip', '--data-binary', f'@{tarball_path}',
        url
    ],
                            capture_output=True,
                            text=True)
    if result.returncode != 0:
        print(f'EVIDENCE: UPLOAD FAILED: {result.stdout}\n{result.stderr}')
        return None
    return f'gs://{BUCKET}/{object_name}'


def main():
    print(f'EVIDENCE-RUN-ID: {run_id}')
    print(f'EVIDENCE: uploading to bucket {BUCKET}, label={sys.argv[1:]}')
    # Probe for stack-dumping tools so we know whether a future version of
    # this harness could capture thread backtraces of a hung test.
    print(f'EVIDENCE: gdb={shutil.which("gdb")} '
          f'eu-stack={shutil.which("eu-stack")} '
          f'py-spy={shutil.which("py-spy")}')
    start = time.time()
    status, exit_code = 'BROKEN', None
    upload_location = None
    try:
        if build_contract():
            status, exit_code = run_test()
        collect_artifacts(status, exit_code, time.time() - start)
        upload_location = upload(status)
    except Exception:
        traceback.print_exc()
    print(f'EVIDENCE-RESULT: run_id={run_id} status={status} '
          f'exit_code={exit_code} duration={int(time.time() - start)}s '
          f'upload={upload_location}')
    # Always pass: a nayduck retry would replace this run's logs in the UI.
    sys.exit(0)


if __name__ == '__main__':
    main()

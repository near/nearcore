#!/usr/bin/env python3
"""
Adversarial load: deploy a fresh, byte-unique WASM on every tx so chunk
producers are forced to recompile each contract (no compilation-cache hit).

`--contract SPEC` (default: synth1) selects the WASM source. SPEC is
resolved in order:

  1. Known keyword (`synth1` is the only generator today; entries in
     SOURCE_CONTRACT_URLS are friendly aliases for canned URLs).
     - synth1: synthesizes a module with N_FUNCS functions of 1000 i64
       params each, bodies filled with randomized (i64.const; drop) pairs
       plus a unique export-name tag. Already byte-unique per deploy.
       Limits (cross-checked against
       `core/parameters/res/runtime_configs/parameters.snap`):
         max_functions_number_per_contract = 10_000
         max_contract_size                 = 4 MiB
         max_locals_per_contract           = 1_000_000  (declared locals)
       Stresses the compiler with a parameter/locals-heavy module.
     - sample1 (and other entries in SOURCE_CONTRACT_URLS): a real, large
       compiled contract — stresses things that key off of contract hash
       (storage, runtime caching).
  2. URL (`http://` or `https://`) — downloaded once, cached under
     ~/.cache/near-mocknet-adversarial/ keyed by URL basename.
  3. Otherwise — treated as a local filesystem path.

Resalting (`--resalt`) appends a fresh `hash-salt` custom section to
the WASM on every deploy so each contract hash is unique. It is enabled
by default, and works with any contract source (it's redundant for
synth1 but supported).

Runs indefinitely.

Examples:
  # single signer, default synth1
  python3 pytest/tests/mocknet/slow_compile_adversarial.py \
      --rpc-url http://rpc-0.forknet.example:3030 \
      --account-id astro-stakers.poolv1.near \
      --public-key ed25519:93zQfXQsfWEkDG2n5qKfbTQUxLZdMrvGpBtwpezWpWTJ \
      --private-key ed25519:5GnmuWueJptLxKYoirp6rHHDJpu7vLgM1BCXwfvc8CJ8cmoettg9vYVaN2mqJZPbiRcrqFuPb7AXjf2jCJyVpyNQ \
      --tps 2

  # local wasm
  ... --contract ./path/to/contract.wasm

  # remote wasm
  ... --contract https://example.com/contract.wasm

  # multiple signers; --tps is per-signer (one deploy per signer per tick).
  # b.near and c.near fall back to the shared --public-key/--private-key.
  python3 pytest/tests/mocknet/slow_compile_adversarial.py \
      --rpc-url http://rpc-0.forknet.example:3030 \
      --signer 'a.near,ed25519:<pk>,ed25519:<sk>' \
      --signer b.near \
      --signer c.near \
      --public-key ed25519:<shared-pk> \
      --private-key ed25519:<shared-sk> \
      --tps 1
"""

import argparse
import pathlib
import random
import sys
from urllib.parse import urlsplit

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import requests

from configured_logger import logger
from load_test_runner import LoadTestRunner
from transaction import sign_deploy_contract_tx

# Hardcoded after benchmarking (see runtime/near-vm-runner/benchmarks).
# Compile time scales with funcs × params; bodies are nearly free.
# Validator rejects contracts with funcs × params > ~1M virtual locals.
N_FUNCS = 990  # just under the ~1M virtual-locals validator cap
N_PARAMS = 1000  # params dominate per-function compile cost
BODY_OPS = 50  # bodies add little compile cost; keep small (~580 KB wasm)

WASM_MAGIC = b'\x00asm\x01\x00\x00\x00'
I64 = 0x7E
OP_NOP, OP_DROP, OP_I64_CONST, OP_END = 0x01, 0x1A, 0x42, 0x0B


def leb128_u(n: int) -> bytes:
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        if n == 0:
            out.append(b)
            return bytes(out)
        out.append(b | 0x80)


def leb128_s(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        sign = b & 0x40
        if (n == 0 and not sign) or (n == -1 and sign):
            out.append(b)
            return bytes(out)
        out.append(b | 0x80)


def _section(sid: int, payload: bytes) -> bytes:
    return bytes([sid]) + leb128_u(len(payload)) + payload


def build_synth1_wasm(rng: random.Random) -> bytes:
    # Shared type: (i64 * N_PARAMS) -> ().
    func_type = bytes(
        [0x60]) + leb128_u(N_PARAMS) + bytes([I64]) * N_PARAMS + leb128_u(0)
    type_section = _section(1, leb128_u(1) + func_type)

    # All N_FUNCS functions use type idx 0.
    func_section = _section(3, leb128_u(N_FUNCS) + bytes([0]) * N_FUNCS)

    # Unique export names per deploy.
    tag = rng.randint(0, 2**63 - 1)
    exports = bytearray()
    for i in range(N_FUNCS):
        name = f'run_{tag:016x}_{i}'.encode('ascii')
        exports += leb128_u(len(name)) + name + bytes([0x00]) + leb128_u(i)
    export_section = _section(7, leb128_u(N_FUNCS) + bytes(exports))

    # Bodies: (i64.const R; drop) * BODY_OPS with random R => byte-unique,
    # net-zero stack effect, forces real codegen.
    code_payload = bytearray(leb128_u(N_FUNCS))
    for _ in range(N_FUNCS):
        body = bytearray(leb128_u(0))  # no declared locals
        for _ in range(BODY_OPS):
            body.append(OP_I64_CONST)
            body += leb128_s(rng.getrandbits(63) - (1 << 62))
            body.append(OP_DROP)
        body += bytes([OP_NOP]) * rng.randint(4, 16)
        body.append(OP_END)
        code_payload += leb128_u(len(body)) + body
    code_section = _section(10, bytes(code_payload))

    return WASM_MAGIC + type_section + func_section + export_section + code_section


CACHE_DIR = pathlib.Path.home() / '.cache' / 'near-mocknet-adversarial'

# Friendly aliases for canned source contracts. New samples can be added here.
SOURCE_CONTRACT_URLS = {
    'sample1': ('https://github.com/near/nearcore/raw/'
                'defuse-high-memory-repro/repro/defuse.wasm'),
}


def _download_cached(url: str, cache_filename: str) -> bytes:
    """Download `url` into CACHE_DIR/<cache_filename> on first use; reuse it
    afterwards. Returns the bytes."""
    cache_path = CACHE_DIR / cache_filename
    if cache_path.exists():
        return cache_path.read_bytes()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f'downloading {url} -> {cache_path}')
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    cache_path.write_bytes(r.content)
    return r.content


def load_contract_wasm(spec: str) -> bytes:
    """Resolve `--contract` for non-generator sources: alias, URL, or path."""
    if spec in SOURCE_CONTRACT_URLS:
        return _download_cached(SOURCE_CONTRACT_URLS[spec], f'{spec}.wasm')
    if spec.startswith(('http://', 'https://')):
        basename = pathlib.Path(urlsplit(spec).path).name
        if not basename:
            raise ValueError(f'cannot derive cache filename from URL: {spec!r}')
        return _download_cached(spec, basename)
    return pathlib.Path(spec).read_bytes()


def append_hash_salt(wasm: bytes, salt: bytes) -> bytes:
    """Append a `hash-salt` WASM custom section (id 0) carrying `salt`.

    Custom sections do not affect execution but change the contract hash,
    so each deploy bypasses runtime caching.
    """
    name = b'hash-salt'
    payload = leb128_u(len(name)) + name + salt
    return wasm + _section(0, payload)


def resolve_make_wasm(args):
    """Pick a WASM factory based on --contract / --resalt."""
    if args.contract == 'synth1':
        base_make_wasm = build_synth1_wasm
    else:
        base_wasm = load_contract_wasm(args.contract)
        logger.info(f'source wasm: {args.contract} ({len(base_wasm)}B)')
        base_make_wasm = lambda rng: base_wasm

    if args.resalt:
        return lambda rng: append_hash_salt(
            base_make_wasm(rng),
            rng.getrandbits(128).to_bytes(16, 'big'))
    return base_make_wasm


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    LoadTestRunner.add_cli_args(p)
    aliases = ', '.join(sorted({'synth1'} | SOURCE_CONTRACT_URLS.keys()))
    p.add_argument('--contract',
                   default='synth1',
                   metavar='SPEC',
                   help='WASM source for every deploy. SPEC is resolved as: '
                   f'(1) a known alias ({aliases}); (2) an http(s):// URL '
                   '(downloaded and cached under '
                   '~/.cache/near-mocknet-adversarial/); (3) otherwise a '
                   'local filesystem path. Default: synth1')
    p.add_argument('--resalt',
                   action=argparse.BooleanOptionalAction,
                   default=True,
                   help='append a unique `hash-salt` custom section per deploy '
                   '(default: on; pass --no-resalt to disable)')
    return p.parse_args()


def main():
    args = parse_args()
    make_wasm = resolve_make_wasm(args)
    logger.info(f'contract={args.contract} resalt={args.resalt}')

    def build_tx(worker_key, nonce, block_hash, rng):
        wasm = make_wasm(rng)
        signed = sign_deploy_contract_tx(worker_key, wasm, nonce, block_hash)
        return signed, f'size={len(wasm)}B'

    LoadTestRunner.from_args(args).run(build_tx)


if __name__ == '__main__':
    main()

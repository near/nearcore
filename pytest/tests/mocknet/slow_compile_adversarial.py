#!/usr/bin/env python3
"""
Adversarial load: deploy a fresh, byte-unique WASM on every tx so chunk
producers are forced to recompile each contract (no compilation-cache hit).

Contract modes:

  --contract synth1 (default)
    Synthesizes a module with N_FUNCS functions of 1000 i64 params each,
    bodies filled with randomized (i64.const; drop) pairs plus a unique
    export-name tag. Already byte-unique per deploy. Limits
    (cross-checked against `core/parameters/res/runtime_configs/parameters.snap`):
      max_functions_number_per_contract = 10_000
      max_contract_size                 = 4 MiB
      max_locals_per_contract           = 1_000_000  (declared locals only)
    Stresses the compiler with a parameter/locals-heavy module.

  --contract sample1
    Uses a real, large compiled source contract. By default the source
    contract is downloaded once (cached under
    ~/.cache/near-mocknet-adversarial/) — pass --source-wasm PATH to
    override. Stresses things that key off of contract hash (storage,
    runtime caching).

Resalting (`--resalt`) appends a fresh `hash-salt` custom section to
the WASM on every deploy so each contract hash is unique. It is enabled
by default, and works with any contract type (it's redundant for
synth1 but supported).

Runs indefinitely.

Examples:
  # single signer
  python3 pytest/tests/mocknet/slow_compile_adversarial.py \
      --rpc-url http://rpc-0.forknet.example:3030 \
      --account-id astro-stakers.poolv1.near \
      --public-key ed25519:93zQfXQsfWEkDG2n5qKfbTQUxLZdMrvGpBtwpezWpWTJ \
      --private-key ed25519:5GnmuWueJptLxKYoirp6rHHDJpu7vLgM1BCXwfvc8CJ8cmoettg9vYVaN2mqJZPbiRcrqFuPb7AXjf2jCJyVpyNQ \
      --tps 2

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
import base64
import itertools
import os
import pathlib
import queue
import random
import sys
import threading
import time
from urllib.parse import urlsplit

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import requests

from configured_logger import logger
from key import Key
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc
from transaction import (create_full_access_key_action,
                         sign_and_serialize_transaction,
                         sign_deploy_contract_tx)

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

# Per-contract source URL. New samples can be added here.
SOURCE_CONTRACT_URLS = {
    'sample1': ('https://github.com/near/nearcore/raw/'
                'defuse-high-memory-repro/repro/defuse.wasm'),
}


def fetch_source_contract(name: str) -> bytes:
    """Return source contract bytes for `name`, downloading on first use."""
    url = SOURCE_CONTRACT_URLS[name]
    cache_path = CACHE_DIR / f'{name}.wasm'
    if cache_path.exists():
        return cache_path.read_bytes()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f'downloading {url} -> {cache_path}')
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    cache_path.write_bytes(r.content)
    return r.content


def load_wasm_bytes(spec: str) -> bytes:
    """Resolve `--source-wasm`: accepts a local path or an http(s) URL.

    URLs are cached under `CACHE_DIR` keyed by their path basename.
    """
    if spec.startswith(('http://', 'https://')):
        basename = pathlib.Path(urlsplit(spec).path).name
        if not basename:
            raise ValueError(f'cannot derive cache filename from URL: {spec!r}')
        cache_path = CACHE_DIR / basename
        if cache_path.exists():
            return cache_path.read_bytes()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f'downloading {spec} -> {cache_path}')
        r = requests.get(spec, timeout=60)
        r.raise_for_status()
        cache_path.write_bytes(r.content)
        return r.content
    return pathlib.Path(spec).read_bytes()


def append_hash_salt(wasm: bytes, salt: bytes) -> bytes:
    """Append a `hash-salt` WASM custom section (id 0) carrying `salt`.

    Custom sections do not affect execution but change the contract hash,
    so each deploy bypasses runtime caching.
    """
    name = b'hash-salt'
    payload = leb128_u(len(name)) + name + salt
    return wasm + _section(0, payload)


def _split_rpc(url: str):
    if '://' not in url:
        url = 'http://' + url
    p = urlsplit(url)
    return p.hostname, str(p.port or 3030)


def parse_signer(spec: str, default_pk: str, default_sk: str) -> Key:
    """Parse a --signer CSV entry: `account_id[,public_key,private_key]`.

    Missing per-entry keys fall back to the shared `default_pk`/`default_sk`.
    """
    parts = [p.strip() for p in spec.split(',')]
    if len(parts) == 1:
        account_id, pk, sk = parts[0], default_pk, default_sk
    elif len(parts) == 3:
        account_id, pk, sk = parts
    else:
        raise ValueError(
            f'--signer expects "account_id[,public_key,private_key]", '
            f'got: {spec!r}')
    return Key.from_json({
        'account_id': account_id,
        'public_key': pk,
        'secret_key': sk,
    })


def sign_add_key_tx(master_key: Key, new_pk_bytes: bytes, nonce: int,
                    block_hash: bytes) -> bytes:
    """Sign an AddKey(FullAccess) tx that adds `new_pk_bytes` to
    `master_key.account_id`, authorized by `master_key`."""
    action = create_full_access_key_action(new_pk_bytes)
    return sign_and_serialize_transaction(master_key.account_id, nonce,
                                          [action], block_hash,
                                          master_key.account_id,
                                          master_key.decoded_pk(),
                                          master_key.decoded_sk())


def worker_key_seed(account_id: str, i: int) -> str:
    """Stable seed for the i-th worker key on `account_id`. Same input always
    derives the same keypair so AddKey is a one-time setup per (account, i).

    `Key.from_seed_testonly` only consumes the first 32 bytes of the seed,
    so the index must lead so it actually influences the keypair."""
    return f'{i:04d}-sca-{account_id}'


def _list_access_keys(account_id: str, addr: str, port: str):
    resp = json_rpc('query', {
        'request_type': 'view_access_key_list',
        'account_id': account_id,
        'finality': 'optimistic',
    },
                    addr=addr,
                    port=port)
    return {
        k['public_key']: k['access_key']['nonce']
        for k in resp.get('result', {}).get('keys', [])
    }


def add_worker_keys(master: Key,
                    new_keys,
                    addr: str,
                    port: str,
                    max_attempts: int = 5,
                    attempt_timeout_s: float = 30.0):
    """Ensure every key in `new_keys` is registered on `master.account_id`.

    Idempotent and resilient to dropped txs: each attempt re-reads the
    on-chain access-key set, broadcasts AddKey only for the keys still
    missing (with a fresh block_hash + nonce), then polls for inclusion up
    to `attempt_timeout_s`. Repeats up to `max_attempts` times before
    giving up. Returns `{public_key: on-chain nonce}` for every key in
    `new_keys`."""
    expected_pks = {k.pk for k in new_keys}

    for attempt in range(1, max_attempts + 1):
        try:
            on_chain = _list_access_keys(master.account_id, addr, port)
        except Exception as e:
            logger.warning(f'{master.account_id}: attempt {attempt}: '
                           f'view_access_key_list failed: {e!r}; retrying')
            time.sleep(2.0)
            continue

        missing_keys = [k for k in new_keys if k.pk not in on_chain]
        if not missing_keys:
            if attempt == 1:
                logger.info(f'{master.account_id}: all {len(new_keys)} worker '
                            f'keys already registered')
            else:
                logger.info(
                    f'{master.account_id}: all {len(new_keys)} worker keys '
                    f'registered after {attempt} attempt(s)')
            return {k.pk: on_chain[k.pk] for k in new_keys}

        logger.info(f'{master.account_id}: attempt {attempt}/{max_attempts}: '
                    f'broadcasting AddKey for {len(missing_keys)}/'
                    f'{len(new_keys)} missing worker keys')

        try:
            block_hash = get_latest_block_hash(addr=addr, port=port)
            nonce = get_nonce_for_key(master, addr=addr, port=port)
        except Exception as e:
            logger.warning(f'{master.account_id}: attempt {attempt}: '
                           f'block-hash/nonce fetch failed: {e!r}; retrying')
            time.sleep(2.0)
            continue

        for new_key in missing_keys:
            nonce += 1
            try:
                tx = sign_add_key_tx(master, new_key.decoded_pk(), nonce,
                                     block_hash)
                resp = json_rpc('broadcast_tx_async',
                                [base64.b64encode(tx).decode('ascii')],
                                addr=addr,
                                port=port)
                if 'error' in resp:
                    logger.warning(f'{master.account_id}: AddKey broadcast '
                                   f'error for {new_key.pk}: {resp["error"]}')
            except Exception as e:
                logger.warning(f'{master.account_id}: AddKey broadcast threw '
                               f'for {new_key.pk}: {e!r}')

        attempt_deadline = time.monotonic() + attempt_timeout_s
        while time.monotonic() < attempt_deadline:
            try:
                on_chain = _list_access_keys(master.account_id, addr, port)
            except Exception as e:
                logger.warning(
                    f'{master.account_id}: poll view_access_key_list failed: '
                    f'{e!r}')
                time.sleep(2.0)
                continue
            still_missing = expected_pks - on_chain.keys()
            if not still_missing:
                return {pk: on_chain[pk] for pk in expected_pks}
            time.sleep(2.0)
        logger.warning(f'{master.account_id}: attempt {attempt} timed out '
                       f'with {len(expected_pks - on_chain.keys())} keys '
                       f'still missing; retrying')

    raise RuntimeError(
        f'AddKey for {master.account_id} did not register all '
        f'{len(new_keys)} worker keys after {max_attempts} attempts')


def run(args):
    rpcs = [_split_rpc(u.strip()) for u in args.rpc_url.split(',') if u.strip()]
    if args.signer:
        signers = [
            parse_signer(spec, args.public_key, args.private_key)
            for spec in args.signer
        ]
    else:
        signers = [
            Key.from_json({
                'account_id': args.account_id,
                'public_key': args.public_key,
                'secret_key': args.private_key,
            })
        ]

    if args.source_wasm:
        base_wasm = load_wasm_bytes(args.source_wasm)
        logger.info(f'source wasm: {args.source_wasm} '
                    f'({len(base_wasm)}B)')
        base_make_wasm = lambda rng: base_wasm
    elif args.contract == 'synth1':
        base_make_wasm = build_synth1_wasm
    else:
        base_wasm = fetch_source_contract(args.contract)
        logger.info(f'source wasm: cached {args.contract} '
                    f'({len(base_wasm)}B)')
        base_make_wasm = lambda rng: base_wasm

    if args.resalt:
        make_wasm = lambda rng: append_hash_salt(
            base_make_wasm(rng),
            rng.getrandbits(128).to_bytes(16, 'big'))
    else:
        make_wasm = base_make_wasm

    logger.info(f'signers={[s.account_id for s in signers]} rpcs={rpcs} '
                f'contract={args.contract} resalt={args.resalt} '
                f'workers/account={args.concurrency} tps/account={args.tps}')

    # Derive `concurrency` deterministic access keys per account (via a
    # stable seed) so re-running the script reuses the same keys instead of
    # piling fresh ones onto the account on every run. `add_worker_keys`
    # AddKeys only the ones not already on chain.
    addr, port = rpcs[0]
    generated_keys = {
        s.account_id: [
            Key.from_seed_testonly(s.account_id,
                                   worker_key_seed(s.account_id, i))
            for i in range(args.concurrency)
        ] for s in signers
    }
    initial_nonces = {}  # account_id -> { pk: nonce }
    for s in signers:
        initial_nonces[s.account_id] = add_worker_keys(
            s, generated_keys[s.account_id], addr, port)

    # Shared block hash, refreshed in the background.
    block_hash = [get_latest_block_hash(addr=addr, port=port)]
    block_hash_lock = threading.Lock()

    rpc_counter = itertools.count()
    rpc_counter_lock = threading.Lock()

    def pick_rpc():
        with rpc_counter_lock:
            i = next(rpc_counter)
        return rpcs[i % len(rpcs)]

    def refresh_block_hash_loop():
        while True:
            time.sleep(20.0)
            try:
                a, p = pick_rpc()
                bh = get_latest_block_hash(addr=a, port=p)
                with block_hash_lock:
                    block_hash[0] = bh
            except Exception as e:
                logger.warning(f'block hash refresh failed: {e}')

    threading.Thread(target=refresh_block_hash_loop, daemon=True).start()

    # One queue per account; ticker enqueues, workers consume. Bounded so a
    # stuck account doesn't grow memory without bound.
    queues = {
        s.account_id: queue.Queue(maxsize=args.concurrency * 2) for s in signers
    }

    sent_counter = itertools.count(1)

    def worker(account_id: str, worker_key: Key):
        nonce = initial_nonces[account_id][worker_key.pk]
        short = worker_key.pk.split(':', 1)[-1][:8]
        q = queues[account_id]
        while True:
            q.get()
            try:
                idx = next(sent_counter)
                a, p = pick_rpc()
                with block_hash_lock:
                    bh = block_hash[0]
                local_rng = random.Random(os.urandom(16))
                wasm = make_wasm(local_rng)
                nonce += 1
                signed = sign_deploy_contract_tx(worker_key, wasm, nonce, bh)
                t0 = time.monotonic()
                resp = json_rpc('broadcast_tx_async',
                                [base64.b64encode(signed).decode('ascii')],
                                addr=a,
                                port=p)
                rpc_ms = (time.monotonic() - t0) * 1000
                if 'error' in resp:
                    logger.warning(f'broadcast error rpc={rpc_ms:.0f}ms: '
                                   f'{resp["error"]}')
                else:
                    logger.info(f'[{idx}] {account_id}/{short} nonce={nonce} '
                                f'size={len(wasm)}B rpc={rpc_ms:.0f}ms '
                                f'tx={resp.get("result")}')
            except requests.RequestException as e:
                logger.warning(f'rpc send failed: {e}')
            except Exception as e:
                logger.warning(f'worker error: {e!r}')

    for s in signers:
        for gk in generated_keys[s.account_id]:
            threading.Thread(target=worker,
                             args=(s.account_id, gk),
                             daemon=True).start()

    # Ticker: enqueue one deploy task per account every 1/tps seconds.
    interval = 1.0 / args.tps if args.tps > 0 else 0.0
    while True:
        tick = time.monotonic()
        for s in signers:
            try:
                queues[s.account_id].put_nowait(None)
            except queue.Full:
                logger.warning(f'queue full for {s.account_id}, dropping tick')
        if interval:
            elapsed = time.monotonic() - tick
            if elapsed < interval:
                time.sleep(interval - elapsed)


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--rpc-url',
                   required=True,
                   help='comma-separated forknet RPC endpoints')
    p.add_argument('--account-id',
                   default='astro-stakers.poolv1.near',
                   help='single-signer account id (used only when --signer '
                   'is not given)')
    p.add_argument(
        '--public-key',
        default='ed25519:93zQfXQsfWEkDG2n5qKfbTQUxLZdMrvGpBtwpezWpWTJ',
        help='public key for the single-signer fallback and for --signer '
        'entries that omit per-entry keys (e.g. ed25519:...)')
    p.add_argument(
        '--private-key',
        default=
        'ed25519:5GnmuWueJptLxKYoirp6rHHDJpu7vLgM1BCXwfvc8CJ8cmoettg9vYVaN2mqJZPbiRcrqFuPb7AXjf2jCJyVpyNQ',
        help='private key for the single-signer fallback and for --signer '
        'entries that omit per-entry keys (e.g. ed25519:...)')
    p.add_argument('--signer',
                   action='append',
                   default=[],
                   metavar='account_id[,public_key,private_key]',
                   help='signer entry in CSV form; repeatable. With 1 field, '
                   'the entry falls back to --public-key/--private-key. With '
                   '3 fields, the per-entry keys are used. When --signer is '
                   'given, one deploy is fanned out per signer per tick')
    p.add_argument('--contract',
                   choices=('synth1',) + tuple(SOURCE_CONTRACT_URLS),
                   default='synth1',
                   help='which adversarial contract to deploy')
    p.add_argument('--source-wasm',
                   help='deploy this wasm on every tx instead of synthesizing '
                   'one. Accepts a local path or an http(s):// URL (URLs are '
                   'cached under ~/.cache/near-mocknet-adversarial/). Takes '
                   'precedence over --contract')
    p.add_argument('--resalt',
                   action=argparse.BooleanOptionalAction,
                   default=True,
                   help='append a unique `hash-salt` custom section per deploy '
                   '(default: on; pass --no-resalt to disable)')
    p.add_argument('--tps',
                   type=float,
                   default=1.0,
                   help='target transactions per second per signer '
                   '(total tx rate ~ tps * num_signers); 0 = no throttle')
    p.add_argument('--concurrency',
                   type=int,
                   default=8,
                   help='worker threads per signer; each gets its own '
                   'access key (auto-AddKey on chain at startup) so a slow '
                   'broadcast on one worker does not starve the per-account '
                   'tps')
    return p.parse_args()


if __name__ == '__main__':
    run(parse_args())

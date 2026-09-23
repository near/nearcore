"""Transaction-agnostic load-test runner for mocknet/forknet RPCs.

`LoadTestRunner` owns the orchestration plumbing — signer/RPC argument
parsing, idempotent worker AddKey setup, RPC round-robin, background
block-hash refresh, per-account bounded queues, and a fixed-tps ticker.
Callers supply a `build_tx` callback that constructs the signed
transaction bytes for each tick; the runner has no knowledge of what is
being sent.
"""

# cspell:words poolv urandom

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
from collections import Counter
from urllib.parse import urlsplit

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import requests

from configured_logger import logger
from key import Key
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc
from transaction import (create_full_access_key_action,
                         sign_and_serialize_transaction)


def _split_rpc(url: str):
    if '://' not in url:
        url = 'http://' + url
    p = urlsplit(url)
    if not p.hostname:
        raise ValueError(f'--rpc-url entry has no host: {url!r}')
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


def _sign_add_key_tx(master_key: Key, new_pk_bytes: bytes, nonce: int,
                     block_hash: bytes) -> bytes:
    """Sign an AddKey(FullAccess) tx that adds `new_pk_bytes` to
    `master_key.account_id`, authorized by `master_key`."""
    action = create_full_access_key_action(new_pk_bytes)
    return sign_and_serialize_transaction(master_key.account_id, nonce,
                                          [action], block_hash,
                                          master_key.account_id,
                                          master_key.decoded_pk(),
                                          master_key.decoded_sk())


def _worker_key_seed(account_id: str, i: int) -> str:
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


def _add_worker_keys(master: Key,
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
                tx = _sign_add_key_tx(master, new_key.decoded_pk(), nonce,
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


class LoadTestRunner:
    """Transaction-agnostic mocknet load-test driver.

    The caller supplies a `build_tx` callback to `run()`; the runner
    handles signer/RPC parsing, worker-key registration, block-hash
    refresh, RPC round-robin, a per-account bounded queue, and a ticker.
    """

    def __init__(self, rpcs, signers, tps: float, concurrency: int):
        self.rpcs = rpcs
        self.signers = signers
        self.tps = tps
        self.concurrency = concurrency

    @staticmethod
    def add_cli_args(p: argparse.ArgumentParser) -> None:
        """Register the runner's CLI flags on `p`."""
        p.add_argument('--rpc-url',
                       required=True,
                       help='comma-separated forknet RPC endpoints')
        p.add_argument('--account-id',
                       default='astro-stakers.poolv1.near',
                       help='single-signer account id (used only when '
                       '--signer is not given)')
        p.add_argument(
            '--public-key',
            default='ed25519:93zQfXQsfWEkDG2n5qKfbTQUxLZdMrvGpBtwpezWpWTJ',
            help='public key for the single-signer fallback and for '
            '--signer entries that omit per-entry keys (e.g. ed25519:...)')
        p.add_argument(
            '--private-key',
            default=
            'ed25519:5GnmuWueJptLxKYoirp6rHHDJpu7vLgM1BCXwfvc8CJ8cmoettg9vYVaN2mqJZPbiRcrqFuPb7AXjf2jCJyVpyNQ',
            help='private key for the single-signer fallback and for '
            '--signer entries that omit per-entry keys (e.g. ed25519:...)')
        p.add_argument('--signer',
                       action='append',
                       default=[],
                       metavar='account_id[,public_key,private_key]',
                       help='signer entry in CSV form; repeatable. With 1 '
                       'field, the entry falls back to '
                       '--public-key/--private-key. With 3 fields, the '
                       'per-entry keys are used. When --signer is given, '
                       'one deploy is fanned out per signer per tick')
        p.add_argument('--tps',
                       type=float,
                       default=1.0,
                       help='target transactions per second per signer '
                       '(total tx rate ~ tps * num_signers); 0 = no throttle')
        p.add_argument('--concurrency',
                       type=int,
                       default=8,
                       help='worker threads per signer; each gets its own '
                       'access key (auto-AddKey on chain at startup) so a '
                       'slow broadcast on one worker does not starve the '
                       'per-account tps')

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> 'LoadTestRunner':
        rpcs = [
            _split_rpc(u.strip()) for u in args.rpc_url.split(',') if u.strip()
        ]
        if not rpcs:
            raise ValueError('--rpc-url produced no usable endpoints')
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
        dupes = sorted(a for a, n in Counter(s.account_id
                                             for s in signers).items() if n > 1)
        if dupes:
            raise ValueError(
                f'duplicate signer account_id(s): {", ".join(dupes)}')
        return cls(rpcs=rpcs,
                   signers=signers,
                   tps=args.tps,
                   concurrency=args.concurrency)

    def run(self, build_tx) -> None:
        """Run the load test forever.

        `build_tx(worker_key, nonce, block_hash, rng) -> (bytes, str)`
        returns a signed, serialized transaction and a short log fragment
        describing the payload (e.g. `size=580000B`). The fragment is
        interpolated into the worker's INFO line between `nonce=...` and
        `rpc=...ms`.
        """
        logger.info(f'signers={[s.account_id for s in self.signers]} '
                    f'rpcs={self.rpcs} workers/account={self.concurrency} '
                    f'tps/account={self.tps}')

        # Derive `concurrency` deterministic access keys per account (via a
        # stable seed) so re-running the script reuses the same keys instead
        # of piling fresh ones onto the account on every run.
        # `_add_worker_keys` AddKeys only the ones not already on chain.
        addr, port = self.rpcs[0]
        generated_keys = {
            s.account_id: [
                Key.from_seed_testonly(s.account_id,
                                       _worker_key_seed(s.account_id, i))
                for i in range(self.concurrency)
            ] for s in self.signers
        }
        initial_nonces = {}  # account_id -> { pk: nonce }
        for s in self.signers:
            initial_nonces[s.account_id] = _add_worker_keys(
                s, generated_keys[s.account_id], addr, port)

        # Shared block hash, refreshed in the background.
        block_hash = [get_latest_block_hash(addr=addr, port=port)]
        block_hash_lock = threading.Lock()

        rpc_counter = itertools.count()
        rpc_counter_lock = threading.Lock()

        def pick_rpc():
            with rpc_counter_lock:
                i = next(rpc_counter)
            return self.rpcs[i % len(self.rpcs)]

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

        # One queue per account; ticker enqueues, workers consume. Bounded so
        # a stuck account doesn't grow memory without bound.
        queues = {
            s.account_id: queue.Queue(maxsize=self.concurrency * 2)
            for s in self.signers
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
                    nonce += 1
                    signed, extra = build_tx(worker_key, nonce, bh, local_rng)
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
                        logger.info(f'[{idx}] {account_id}/{short} '
                                    f'nonce={nonce} {extra} '
                                    f'rpc={rpc_ms:.0f}ms '
                                    f'tx={resp.get("result")}')
                except requests.RequestException as e:
                    logger.warning(f'rpc send failed: {e}')
                except Exception as e:
                    logger.warning(f'worker error: {e!r}')

        for s in self.signers:
            for gk in generated_keys[s.account_id]:
                threading.Thread(target=worker,
                                 args=(s.account_id, gk),
                                 daemon=True).start()

        # Ticker: enqueue one task per account every 1/tps seconds.
        interval = 1.0 / self.tps if self.tps > 0 else 0.0
        while True:
            tick = time.monotonic()
            for s in self.signers:
                try:
                    queues[s.account_id].put_nowait(None)
                except queue.Full:
                    logger.warning(
                        f'queue full for {s.account_id}, dropping tick')
            if interval:
                elapsed = time.monotonic() - tick
                if elapsed < interval:
                    time.sleep(interval - elapsed)
